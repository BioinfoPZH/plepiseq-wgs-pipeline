#!/usr/bin/env python3

from __future__ import annotations

import logging
import os
import re
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import requests

from utils.download_helpers import _download_file_with_retry
from utils.generic_helpers import _execute_command, remove_old_workspace
from utils.net import StatusType, check_url_available, HEADERS as NET_HEADERS
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.validation import get_timestamp, verify_expected_files
from utils.updates_helpers import parse_md5_text
from utils.version_manifest import read_version_manifest, write_version_manifest


DATABASE = {"name": "metaphlan", "category": "taxonomy profiling"}
SOURCE = {
    "source_type": "https",
    "reference": "http://cmprod1.cibio.unitn.it/biobakery4/metaphlan_databases/",
    # Schema requires minItems=1. The actual versioned artifacts are validated dynamically.
    "expected_raw_files": ["mpa_latest"],
    "expected_processed_files": ["mpa_latest"],
}


DEFAULT_BASE_URL = "http://cmprod1.cibio.unitn.it/biobakery4/metaphlan_databases/"
DEFAULT_BOWTIE_URL = "http://cmprod1.cibio.unitn.it/biobakery4/metaphlan_databases/bowtie2_indexes/"


def _strip(s: str) -> str:
    return (s or "").strip()


def _http_get_text(
    *,
    url: str,
    auth: Optional[Tuple[str, str]],
    timeout_s: int = 60,
) -> str:
    r = requests.get(url, headers=NET_HEADERS, auth=auth, timeout=timeout_s, allow_redirects=True)
    r.raise_for_status()
    return r.text


def _parse_apache_listing_filenames(html: str) -> List[str]:
    """
    Extract href targets from a directory listing page.
    Returns decoded-ish hrefs (no URL-join performed), filtered to plain file-like entries.
    """
    # Grab href targets. Works for common Apache/NGINX indexes.
    hrefs = re.findall(r'href=[\'"]([^\'"]+)[\'"]', html, flags=re.IGNORECASE)
    out: List[str] = []
    seen: set[str] = set()

    for h in hrefs:
        h = _strip(h)
        if not h:
            continue
        # Remove query/fragment
        h = h.split("#", 1)[0].split("?", 1)[0]
        if not h or h in ("/", "./", "../"):
            continue
        # Skip parent dir links and subdirs
        if h.endswith("/"):
            continue
        # If it is a full URL, take just the basename
        if "://" in h:
            h = h.rsplit("/", 1)[-1]
        # Basic sanitization
        if "/" in h:
            h = h.rsplit("/", 1)[-1]
        if not h:
            continue
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def _list_remote_files_for_prefix(
    *,
    listing_url: str,
    prefix: str,
    auth: Optional[Tuple[str, str]],
    logger: logging.Logger,
) -> List[str]:
    html = _http_get_text(url=listing_url, auth=auth, timeout_s=60)
    all_files = _parse_apache_listing_filenames(html)
    matched = sorted([f for f in all_files if prefix in f])
    logger.info("Listing %s: %d files matched prefix %s", listing_url, len(matched), prefix)
    return matched


def _checksums_from_manifest(manifest: Dict[str, str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for k in sorted(manifest.keys()):
        if k == "mpa_latest":
            continue
        out.append({"file_name": k, "checksum": manifest[k]})
    return out


def determine_update_status_metaphlan(
    *,
    output_dir: Path,
    base_url: str,
    bowtie_url: str,
    auth: Optional[Tuple[str, str]],
    logger: logging.Logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str], str]:
    """
    Decide update based on a local JSON baseline manifest vs (remote_prefix + remote md5s).

    Returns:
      milestone,
      update_decision kwargs,
      update_required,
      remote_manifest,
      remote_prefix
    """
    started_at = get_timestamp()

    # Keep naming consistent with other checksum-based updaters (e.g. Kraken).
    manifest_path = output_dir / "current_md5.json"
    local_manifest = read_version_manifest(manifest_path)
    first_build = not bool(local_manifest)

    # Fetch remote prefix
    try:
        remote_prefix = _strip(_http_get_text(url=f"{base_url.rstrip('/')}/mpa_latest", auth=auth, timeout_s=60))
    except Exception as e:
        msg = f"Failed to fetch remote mpa_latest: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {"url": f"{base_url.rstrip('/')}/mpa_latest"},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg, "first_build": first_build},
            False,
            {},
            "",
        )

    main_md5_url = f"{base_url.rstrip('/')}/{remote_prefix}.md5"
    bt2_md5_url = f"{bowtie_url.rstrip('/')}/{remote_prefix}_bt2.md5"

    try:
        main_parsed = parse_md5_text(_http_get_text(url=main_md5_url, auth=auth, timeout_s=60))
        bt2_parsed = parse_md5_text(_http_get_text(url=bt2_md5_url, auth=auth, timeout_s=60))
    except Exception as e:
        msg = f"Failed to fetch remote md5 files: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {"main_md5_url": main_md5_url, "bt2_md5_url": bt2_md5_url},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg, "first_build": first_build},
            False,
            {},
            remote_prefix,
        )

    if main_parsed is None or bt2_parsed is None:
        msg = "Failed to parse one or more remote md5 files."
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {"main_md5_url": main_md5_url, "bt2_md5_url": bt2_md5_url},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg, "first_build": first_build},
            False,
            {},
            remote_prefix,
        )

    main_fname, main_md5 = main_parsed
    bt2_fname, bt2_md5 = bt2_parsed

    main_fname = os.path.basename(str(main_fname))
    bt2_fname = os.path.basename(str(bt2_fname))

    remote_manifest: Dict[str, str] = {
        "mpa_latest": remote_prefix,
        main_fname: str(main_md5).strip().lower(),
        bt2_fname: str(bt2_md5).strip().lower(),
    }

    update_required = local_manifest != remote_manifest
    finished_at = get_timestamp()

    if update_required:
        msg = "No local baseline: treating as first build." if first_build else "Update required: remote prefix/md5 differs from local baseline."
        remove_old_workspace(output_dir, keep=("logs", "reports", manifest_path.name), logger=logger)

        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "first_build": first_build,
                "manifest_path": str(manifest_path),
                "remote_prefix": remote_prefix,
                "update_required": True,
            },
        }

        update_decision = {
            "mode": "checksum_manifest",
            "result": "updated",
            "message": msg,
            "first_build": first_build,
            "checksums_before": _checksums_from_manifest(local_manifest),
            "checksums_after": _checksums_from_manifest(remote_manifest),
        }
        return milestone, update_decision, True, remote_manifest, remote_prefix

    msg = "No update required: remote md5 matches local baseline."
    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "first_build": first_build,
            "manifest_path": str(manifest_path),
            "remote_prefix": remote_prefix,
            "update_required": False,
        },
    }
    update_decision = {
        "mode": "checksum_manifest",
        "result": "latest_version_present",
        "message": msg,
        "first_build": first_build,
        "checksums_before": _checksums_from_manifest(local_manifest),
        "checksums_after": _checksums_from_manifest(remote_manifest),
    }
    return milestone, update_decision, False, remote_manifest, remote_prefix


def download_metaphlan_files(
    *,
    output_dir: Path,
    base_url: str,
    bowtie_url: str,
    prefix: str,
    auth: Optional[Tuple[str, str]],
    logger: logging.Logger,
    max_retries: int = 3,
    wait_seconds: int = 300,
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    started_at = get_timestamp()

    # Re-download mpa_latest so local always reflects the chosen prefix.
    ok_latest, attempts_latest = _download_file_with_retry(
        url=f"{base_url.rstrip('/')}/mpa_latest",
        output_path=output_dir / "mpa_latest",
        logger=logger,
        max_retries=max_retries,
        wait_seconds=wait_seconds,
        timeout_s=120,
        auth=auth,
    )
    if not ok_latest:
        return (
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download mpa_latest",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": attempts_latest,
                "retryable": True,
                "metrics": {"url": f"{base_url.rstrip('/')}/mpa_latest"},
            },
            [],
            [],
        )

    # Build file lists by scraping listings (preserve current behavior)
    main_files = _list_remote_files_for_prefix(listing_url=base_url, prefix=prefix, auth=auth, logger=logger)
    bt2_files = _list_remote_files_for_prefix(listing_url=bowtie_url, prefix=prefix, auth=auth, logger=logger)

    (output_dir / "files_main.txt").write_text("\n".join(main_files) + ("\n" if main_files else ""), encoding="utf-8")
    (output_dir / "files_bowtie_indexes.txt").write_text("\n".join(bt2_files) + ("\n" if bt2_files else ""), encoding="utf-8")

    attempts_used_max = max(1, attempts_latest)
    failed_file: Optional[str] = None
    failed_url: Optional[str] = None

    tasks: List[Tuple[str, Path, str]] = []
    for fname in main_files:
        tasks.append((f"{base_url.rstrip('/')}/{fname}", output_dir / fname, fname))
    for fname in bt2_files:
        tasks.append((f"{bowtie_url.rstrip('/')}/{fname}", output_dir / fname, fname))

    for url, dest, fname in tasks:
        ok, attempts = _download_file_with_retry(
            url=url,
            output_path=dest,
            logger=logger,
            max_retries=max_retries,
            wait_seconds=wait_seconds,
            timeout_s=3600,
            auth=auth,
        )
        attempts_used_max = max(attempts_used_max, attempts)
        if not ok:
            failed_file = fname
            failed_url = url
            break

    finished_at = get_timestamp()

    if failed_file is not None:
        return (
            {
                "status": StatusType.FAILED.value,
                "message": f"Failed to download requested file: {failed_file}",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts_used_max,
                "retryable": True,
                "metrics": {"failed_file": failed_file, "failed_url": failed_url, "main_files": len(main_files), "bt2_files": len(bt2_files)},
            },
            main_files,
            bt2_files,
        )

    return (
        {
            "status": StatusType.PASSED.value,
            "message": "All files downloaded successfully",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {"main_files": len(main_files), "bt2_files": len(bt2_files)},
        },
        main_files,
        bt2_files,
    )


def process_metaphlan_downloads(
    *,
    output_dir: Path,
    logger: logging.Logger,
) -> Dict[str, Any]:
    started_at = get_timestamp()

    # Extract all tar files in the output dir (main and bt2 tarballs)
    tar_files = sorted(output_dir.glob("*.tar"))
    extracted = 0
    tar_fail: List[str] = []

    for tar_path in tar_files:
        ok = _execute_command(["tar", "-xf", str(tar_path), "-C", str(output_dir)], logger=logger)
        if ok:
            extracted += 1
        else:
            tar_fail.append(tar_path.name)

    if tar_fail:
        return {
            "status": StatusType.FAILED.value,
            "message": "Failed to extract one or more tar files.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"tar_total": len(tar_files), "tar_extracted": extracted, "tar_failed": tar_fail},
        }

    # Decompress all .bz2
    bz2_files = sorted(output_dir.glob("*.bz2"))
    bz2_ok = 0
    bz2_fail: List[Dict[str, str]] = []

    for bz_path in bz2_files:
        # NOTE: MetaPhlAn distributes .bz2 (bzip2) files; use bzip2/bunzip2, not bgzip (gzip-only).
        ok = _execute_command(["bzip2", "-d", "-f", str(bz_path)], logger=logger)
        if ok and not bz_path.exists():
            bz2_ok += 1
        else:
            bz2_fail.append({"file": bz_path.name, "error": "bzip2_failed", "output": str(bz_path.with_suffix(""))})

    if bz2_fail:
        return {
            "status": StatusType.FAILED.value,
            "message": "Failed to decompress one or more bz2 files.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"bz2_total": len(bz2_files), "bz2_ok": bz2_ok, "bz2_failed": bz2_fail[:25]},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "Processing completed successfully (tar extraction + bz2 decompression).",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {"tar_total": len(tar_files), "tar_extracted": extracted, "bz2_total": len(bz2_files), "bz2_decompressed": bz2_ok},
    }


def _expected_files_for_prefix(prefix: str) -> List[str]:
    """
    Dynamic expected files based on resolved prefix.
    Kept to high-signal essentials to match your current directory composition.
    """
    bt2 = [
        f"{prefix}.1.bt2l",
        f"{prefix}.2.bt2l",
        f"{prefix}.3.bt2l",
        f"{prefix}.4.bt2l",
        f"{prefix}.rev.1.bt2l",
        f"{prefix}.rev.2.bt2l",
    ]

    return [
        "mpa_latest",
        "files_main.txt",
        "files_bowtie_indexes.txt",
        f"{prefix}.md5",
        f"{prefix}.nwk",
        f"{prefix}.tar",
        f"{prefix}_bt2.md5",
        f"{prefix}_bt2.tar",
        f"{prefix}_marker_info.txt",
        f"{prefix}_species.txt",
        *bt2,
    ]


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default="/home/external_databases/metaphlan", help="Output directory.")
@click.option("--base_url", type=str, default=DEFAULT_BASE_URL, help="Base URL for MetaPhlAn databases.")
@click.option("--bowtie_url", type=str, default=DEFAULT_BOWTIE_URL, help="Base URL for bowtie2 index tarballs.")
@click.option("--http_user", type=str, default="anonymous", help="HTTP basic auth user.")
@click.option("--http_password", type=str, default="anonymous", help="HTTP basic auth password.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/metaphlan",
    base_url: str = DEFAULT_BASE_URL,
    bowtie_url: str = DEFAULT_BOWTIE_URL,
    http_user: str = "anonymous",
    http_password: str = "anonymous",
) -> None:
    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # logging
    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting MetaPhlAn DB updater")

    auth = (http_user, http_password) if http_user and http_password else None

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}",
        "user": user,
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    if report_file is None:
        report_file = f"{run_id}.json"
    else:
        if run_id not in report_file:
            report_file = f"{run_id}.json"

    report_dir = out_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    remaining_steps = list(ALL_STEPS)

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source={**SOURCE, "reference": base_url},
        log_file=f"{workspace}/{DATABASE['name']}/reports/{report_file}",
    )

    def skip_remaining_steps(steps: List[str], reason: str) -> None:
        for s in steps:
            rb.add_skipped(s, reason)

    # 1) PREFLIGHT_CONNECTIVITY
    pre = check_url_available("https://www.google.com", retries=3, interval=30, logger=logger)
    rb.add_named_milestone("PREFLIGHT_CONNECTIVITY", pre)
    remaining_steps.remove("PREFLIGHT_CONNECTIVITY")
    if pre["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed preflight connectivity.")
        rb.fail(code="NO_INTERNET", message=pre.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 2) DATABASE_AVAILABILITY
    avail = check_url_available(base_url, retries=3, interval=10, logger=logger, auth=auth)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_manifest, remote_prefix = determine_update_status_metaphlan(
        output_dir=out_dir,
        base_url=base_url,
        bowtie_url=bowtie_url,
        auth=auth,
        logger=logger,
    )
    rb.add_named_milestone("UPDATE_STATUS", upd_milestone)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(**update_decision)
    if upd_milestone["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed update decision.")
        rb.fail(code="UPDATE_DECISION_FAILED", message=upd_milestone.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    if not update_required:
        skip_remaining_steps(remaining_steps, "Skipped: latest version already present.")
        rb.finalize("SKIPPED")
        rb.write(str(report_dir / report_file))
        return

    # 4) REMOTE_FILES_DOWNLOAD_STATUS
    dl_report, _main_files, _bt2_files = download_metaphlan_files(
        output_dir=out_dir,
        base_url=base_url,
        bowtie_url=bowtie_url,
        prefix=remote_prefix,
        auth=auth,
        logger=logger,
        max_retries=3,
        wait_seconds=300,
    )
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_report)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl_report["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = process_metaphlan_downloads(output_dir=out_dir, logger=logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    try:
        final_prefix = _strip((out_dir / "mpa_latest").read_text(encoding="utf-8")) or remote_prefix
    except Exception:
        final_prefix = remote_prefix
    expected = _expected_files_for_prefix(final_prefix)
    final = verify_expected_files(base_dir=out_dir, expected_files=expected)
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist new baseline *only after* successful final validation
    try:
        if remote_manifest:
            write_version_manifest(out_dir / "current_md5.json", remote_manifest)
    except Exception as e:
        logger.warning("Failed to write manifest: %s", e)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()