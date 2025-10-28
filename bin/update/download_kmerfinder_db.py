#!/usr/bin/env python3
"""
download_lmerfinder_db.py

Weekly-safe updater for the KmerFinder database.

Behavior:
1. Check that the KmerFinder server is reachable.
2. Scrape the "Database version: (YYYY-MM-DD)" date from the KmerFinder web UI. it points to kmerfinder_db bitbucket that is obsolete
   but there is not other indication for database version
3. Compare that date to the locally stored timestamp file in output_dir/timestamp.
4. If unchanged -> exit 0, no download.
5. If changed (or no timestamp yet):
   - Download the big kmerfinder_db.tar.gz from CGE.
   - Extract ONLY the bacteria subset (bacteria/* and bacteria.*) into output_dir/bacteria.
   - Overwrite output_dir/timestamp with the new version date.

Full kmerfinder db is large ~64GB and there is no/can't find an archive for bacteria (ftp server worked up to ~28.10.2025)
"""

from __future__ import annotations

import os
import sys
import re
import tarfile
import shutil
import tempfile
import logging
import click
import requests
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

KMERFINDER_WEB = "https://cge.food.dtu.dk/services/KmerFinder/"
KMERFINDER_DB = "https://cge.food.dtu.dk/services/KmerFinder/etc/kmerfinder_db.tar.gz"

TIMESTAMP_FILE = "timestamp"
SUBDIR_NAME = "bacteria"
LOGFILE_NAME = "log.log"

HEADERS = {
    # pretend to be a browser to reduce chance of being served HTML shell
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    ),
    "Accept-Language": "en",
}


def setup_logging(output_dir: Path):

    log_file_path = os.path.join(output_dir, LOGFILE_NAME)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging initialized. Output log: %s", log_file_path)

def _is_valid_date(date_str: str) -> bool:
    """
    Check YYYY-MM-DD format and that it's a real date.
    """
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _parse_db_version_from_html(html: str) -> str:
    """
    Extracts the KmerFinder 'Database version' date (YYYY-MM-DD)
    from the HTML of https://cge.food.dtu.dk/services/KmerFinder/.

    Returns the date string, e.g. '2022-07-11'.
    Falls back to '2010-01-01' if parsing fails (forces download).
    """
    soup = BeautifulSoup(html, "lxml")

    db_text_node = None
    for t in soup.find_all(string=re.compile(r"Database\s*version", re.I)):
        db_text_node = t
        break

    if db_text_node is None:
        raise ValueError("Could not find 'Database version:' text in HTML")

    link = db_text_node.find_next("a")
    if link is None:
        raise ValueError("Found 'Database version:' but no following <a> tag")


    raw_text = link.get_text(strip=True)  # e.g. "(2022-07-11)"

    m = re.search(r"([0-9]{4}-[0-9]{2}-[0-9]{2})", raw_text)
    if not m:
        return "2010-01-01"
    else:
        return m.group(1)


def kmerfinder_server_available() -> bool:
    """
    Check that the KmerFinder page is reachable at all.
    HEAD first (cheap); if HEAD fails, try GET.

    """
    try:
        r = requests.head(KMERFINDER_WEB, headers=HEADERS, timeout=10,
                          verify=False)  # There is some issue with ssl certificate here
        if r.status_code == 200:
            return True
        r = requests.get(KMERFINDER_WEB, headers=HEADERS, timeout=10, verify=False)
        return r.status_code == 200
    except Exception as e:
        logging.warning("Server availability check failed: %s", e)
        return False



def get_remote_db_version() -> str:
    """
    Fetch the KMERFINDER_WEB page and parse the 'Database version: (YYYY-MM-DD)'.

    Returns:
        version_str (e.g. '2022-07-11')

    Raises:
        RuntimeError if we can't parse a proper date.
    """
    logging.info("Fetching remote DB version from %s", KMERFINDER_WEB)
    resp = requests.get(KMERFINDER_WEB, headers=HEADERS, timeout=20, verify=False)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch KmerFinder page {KMERFINDER_WEB} "
            f"(status {resp.status_code})"
        )

    version_str = _parse_db_version_from_html(resp.text)
    logging.info("Remote KmerFinder database version: %s", version_str)
    return version_str


def get_local_db_version(output_dir: Path) -> str | None:
    """
    Read the locally stored timestamp file (output_dir/timestamp), which should
    contain the last version date we synced, e.g. '2022-07-11'.

    Returns:
        date string or None if not present / invalid.
    """
    ts_file = output_dir / TIMESTAMP_FILE
    if not ts_file.exists():
        logging.info("No local timestamp file found at %s", ts_file)
        return None

    try:
        raw = ts_file.read_text().strip()
    except Exception as e:
        logging.warning("Failed to read timestamp file: %s", e)
        return None

    if not _is_valid_date(raw):
        logging.warning(
            "Local timestamp '%s' is not a valid YYYY-MM-DD. Ignoring.", raw
        )
        return None

    logging.info("Local KmerFinder database version: %s", raw)
    return raw


def download_large_file(url: str, dest: Path) -> None:
    """
    Stream-download a very large file (tens of GB) to dest without loading into RAM.
    """

    with requests.get(url, stream=True, timeout=60, verify=False) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=128 * 1024 * 1024):  # 128 MB chunks
                if chunk:
                    f.write(chunk)

    size_gb = dest.stat().st_size / (1024**3)
    logging.info("Download complete: %.2f GiB", size_gb, dest)


def extract_bacteria_subset(archive_path: Path, output_dir: Path) -> None:
    """
    Extract only the 'bacteria' subset from the big KmerFinder tarball.

    Final layout after this runs:
        output_dir/
            bacteria/
                <all files and subdirs from tar's bacteria/>
            bacteria.md5   (and similar bacteria.* files)
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="kmerfinder_extract_"))
    try:
        # 1. Extract ONLY members starting with 'bacteria/' or files named like 'bacteria.*'
        with tarfile.open(archive_path, "r:gz") as tar:
            members = [
                m for m in tar.getmembers()
                if m.name.startswith("bacteria/") or re.match(r"^bacteria\.", m.name)
            ]
            if not members:
                raise RuntimeError("Archive does not contain any 'bacteria' entries")

            tar.extractall(path=tmpdir, members=members)

        # 2. Define target dir
        target_dir = output_dir / "bacteria"

        # 3. Move the extracted bacteria directory into place
        extracted_bacteria_dir = tmpdir / "bacteria"

        if extracted_bacteria_dir.is_dir():
            # simply move extracted dir to target

            shutil.move(str(extracted_bacteria_dir), str(target_dir))
        else:
            # Fallback in weird case there's no dir, only bacteria.* files
            target_dir.mkdir(parents=True, exist_ok=True)

        # 4. Move any top-level bacteria.* files (e.g. bacteria.md5) next to that dir
        for f in tmpdir.iterdir():
            if f.is_file() and f.name.startswith("bacteria."):
                shutil.move(str(f), str(target_dir / f.name))

    finally:
        # 5. Clean up temp extraction dir
        shutil.rmtree(tmpdir, ignore_errors=True)


def download_kmerfinder_db(output_dir: Path, remote_version: str) -> None:
    """
    Download the full KmerFinder DB tarball and extract the bacteria subset.

    Steps:
    - Create a temp dir for staging.
    - Stream the tar.gz there.
    - Extract only 'bacteria/*' and 'bacteria.*'.
    - Update timestamp.
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix="kmerfinder_download_"))
    archive_path = tmp_dir / "kmerfinder_db.tar.gz"

    try:
        # 0. clean output_dir from follwoing dirs and files
        for name in ["bacteria", "bacteria.md5", "config", "timestamp"]:
            p = output_dir / name
            if not p.exists():
                continue
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)

        # 1. Download large file to tmp_dir
        download_large_file(KMERFINDER_DB, archive_path)

        # 2. Extract bacterial subset into output_dir/SUBDIR_NAME
        extract_bacteria_subset(archive_path, output_dir)

        # 3. Save version marker
        (output_dir / TIMESTAMP_FILE).write_text(remote_version + "\n")

        logging.info("Installation complete")
    finally:
        # Cleanup tmp download dir
        shutil.rmtree(tmp_dir, ignore_errors=True)

@click.command()
@click.option("-o", "--output_dir", required=True, type=click.Path(), help="Output directory for kmerfinder")
def main(output_dir):

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_logging(output_dir)

    # 1. Check server availability
    if not kmerfinder_server_available():
        logging.error("KmerFinder server is not reachable. Aborting update/download")
        sys.exit(0)

    # 2. Remote version (from webpage)
    # Server is available extracting db version
    remote_version = get_remote_db_version()

    # 3. Local version (can be none in there is no timestamp file)
    local_version = get_local_db_version(output_dir)

    if not local_version:
        # No local timestamp force download
        logging.info("No version of a local DB, forcing download")
        try:
            download_kmerfinder_db(output_dir, remote_version)
        except Exception as e:
            logging.error("Update failed: %s", e)

    else:
        if local_version == remote_version:
            # Up to date
            logging.info(
                "Local DB (%s) is up-to-date with remote (%s). Nothing to do.",
                local_version,
                remote_version,
            )
            sys.exit(0)
        else:
            logging.info(
                "Local DB version is %s, remote version is %s -> downloading new version.",
                local_version,
                remote_version,
            )

            # 5. Download + extract bacteria subset + update timestamp
            try:
                download_kmerfinder_db(output_dir, remote_version)
            except Exception as e:
                logging.error("Update failed: %s", e)


if __name__ == "__main__":
    main()
