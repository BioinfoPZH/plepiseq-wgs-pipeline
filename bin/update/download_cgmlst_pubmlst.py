#!/usr/bin/env python3

from __future__ import annotations

import getpass
import json
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.blast_helpers import run_makeblastdb
from utils.download_helpers import _download_file_with_retry
from utils.generic_helpers import remove_old_workspace
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check, file_md5sum
from utils.validation import get_timestamp, verify_expected_files


PUBMLST_BASE = "https://rest.pubmlst.org"

DATABASE = {"name": "pubmlst_schemes", "category": "typing"}
SOURCE = {
    "source_type": "https",
    "reference": PUBMLST_BASE,
    # Raw artifacts persisted locally to decide update
    # NOTE: update decision is based only on profiles_csv.tsv checksum to avoid JSON key-order
    # instability from the /schemes and /loci endpoints.
    "expected_raw_files": ["profiles_csv.tsv"],
    # expected_processed_files is computed dynamically from loci list + scheme type
    "expected_processed_files": [],
}


def _parse_kv_file(path: Path) -> Dict[str, str]:
    """
    Parse a simple key=value file. Ignores blank lines and comments (#...).
    """
    out: Dict[str, str] = {}
    raw = path.read_text(encoding="utf-8", errors="replace")
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _maybe_oauth_auth(*, oauth_credentials_file: Optional[Path], logger):
    """
    Return a `requests`-compatible auth object if requests_oauthlib is available and creds provided.

    Supports two-legged OAuth1 (client_id/client_secret) and optionally access token/secret.
    Returns None if not configured.
    """
    if oauth_credentials_file is None:
        return None
    try:
        if not oauth_credentials_file.exists():
            logger.warning("OAuth credentials file not found (%s); proceeding unauthenticated.", oauth_credentials_file)
            return None
        creds = _parse_kv_file(oauth_credentials_file)
        client_id = creds.get("client_id") or creds.get("client_key") or ""
        client_secret = creds.get("client_secret") or ""
        access_token = creds.get("access_token") or creds.get("resource_owner_key") or ""
        access_token_secret = creds.get("access_token_secret") or creds.get("resource_owner_secret") or ""
        if not client_id:
            logger.warning("OAuth credentials file present but missing client_id; proceeding unauthenticated.")
            return None
    except Exception as e:
        logger.warning("Failed to parse OAuth credentials file (%s): %s; proceeding unauthenticated.", oauth_credentials_file, e)
        return None

    try:
        from requests_oauthlib import OAuth1  # type: ignore
    except Exception:
        logger.warning("requests_oauthlib not available in image; proceeding unauthenticated.")
        return None

    if access_token and access_token_secret:
        logger.info("Using OAuth1 (client + access token) for PubMLST requests.")
        return OAuth1(client_id, client_secret, access_token, access_token_secret)

    logger.info("Using OAuth1 (client credentials only) for PubMLST requests.")
    return OAuth1(client_id, client_secret)


def _first_line(path: Path) -> str:
    with path.open("rt", encoding="utf-8", errors="replace") as f:
        return (f.readline() or "").rstrip("\n")


def _write_profiles_local_stub(*, output_dir: Path, logger) -> None:
    local_dir = output_dir / "local"
    local_dir.mkdir(parents=True, exist_ok=True)
    stub = local_dir / "profiles_local.list"
    if stub.exists():
        return
    header = _first_line(output_dir / "profiles.list")
    stub.write_text(header + "\n", encoding="utf-8")
    logger.info("Created local profiles stub: %s", stub)


def _download_text_atomic_with_retry(
    *,
    url: str,
    output_path: Path,
    logger,
    auth: Any = None,
    max_retries: int = 3,
    wait_seconds: int = 30,
    timeout_s: int = 60,
) -> Tuple[bool, int]:
    """
    Download any text/binary resource over HTTP to output_path with retries.
    Uses the shared helper, but provides auth if supported.
    """
    # NOTE: utils.download_helpers._download_file_with_retry supports `auth` tuple, not OAuth objects.
    # For OAuth, we fall back to requests in a small local implementation.
    if auth is None:
        return _download_file_with_retry(
            url=url,
            output_path=output_path,
            logger=logger,
            max_retries=max_retries,
            wait_seconds=wait_seconds,
            timeout_s=timeout_s,
        )

    # OAuth path (requests-oauthlib provides an auth object)
    import time
    import requests

    output_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, max_retries + 1):
        try:
            if output_path.exists():
                output_path.unlink()
            with requests.get(url, stream=True, timeout=timeout_s, auth=auth) as r:
                if r.status_code == 401 or r.status_code == 403:
                    # Optional auth: fall back to no-auth once
                    logger.warning("OAuth request got HTTP %s for %s; retrying once without auth.", r.status_code, url)
                    break
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}")
                with output_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return True, attempt
        except Exception as e:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, url, e)
            if attempt < max_retries:
                time.sleep(wait_seconds)

    # Fallback to no-auth if OAuth not required/accepted
    return _download_file_with_retry(
        url=url,
        output_path=output_path,
        logger=logger,
        max_retries=max_retries,
        wait_seconds=wait_seconds,
        timeout_s=timeout_s,
    )


def _find_scheme_link(*, schemes_json_path: Path, scheme_name: str) -> Optional[str]:
    parsed = json.loads(schemes_json_path.read_text(encoding="utf-8"))
    for scheme in parsed.get("schemes", []):
        try:
            if scheme.get("description") == scheme_name:
                return scheme.get("scheme")
        except Exception:
            continue
    return None


def _read_loci_list(*, loci_json_path: Path) -> List[str]:
    parsed = json.loads(loci_json_path.read_text(encoding="utf-8"))
    loci = parsed.get("loci", [])
    out: List[str] = []
    for locus_uri in loci:
        if isinstance(locus_uri, str) and locus_uri:
            out.append(locus_uri)
    return out


def _scheme_kind_from_name(scheme_name: str) -> str:
    """
    Infer scheme kind from description.

    - Treat anything containing 'cgMLST' as cgmlst
    - Treat anything containing 'MLST' (but not cgMLST) as mlst
    - Fallback: cgmlst (historically this script was cgMLST-focused)
    """
    s = (scheme_name or "").lower()
    if "cgmlst" in s:
        return "cgmlst"
    if "mlst" in s:
        return "mlst"
    return "cgmlst"


def _expected_processed_files(*, loci_json_path: Path, scheme_kind: str) -> List[str]:
    """
    Build a stable, small expected-file list without enumerating every locus output.
    """
    expected: List[str] = ["profiles.list", "local/profiles_local.list"]

    loci_uris = _read_loci_list(loci_json_path=loci_json_path)
    locus_names = [u.rstrip("/").split("/")[-1] for u in loci_uris if u]
    locus_names = [n for n in locus_names if n]

    # Prefer known cgMLST sentinels when present (maintains historic checks),
    # otherwise pick first locus as a sentinel.
    sentinels: List[str] = []
    if scheme_kind == "cgmlst":
        for n in ("CAMP1069", "CAMP0509"):
            if n in locus_names:
                sentinels.append(n)
        if not sentinels and locus_names:
            sentinels.append(locus_names[0])
    else:
        # MLST schemes are small; pick first locus as sentinel.
        if locus_names:
            sentinels.append(locus_names[0])

    for s in sentinels[:2]:
        expected.append(f"{s}.fasta")
        expected.append(f"{s}.fasta.nsq")

    if scheme_kind == "mlst":
        expected.append("all_allels.fasta")

    return expected


def _build_profiles_list(*, profiles_csv_path: Path, output_path: Path) -> Dict[str, Any]:
    """
    Convert PubMLST profiles_csv (tab-delimited) to legacy `profiles.list` format:
    - tab-delimited
    - replace 'N' with '0'
    - preserve column count based on header
    """
    started_at = get_timestamp()

    lines = profiles_csv_path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return {
            "status": StatusType.FAILED.value,
            "message": "profiles_csv.tsv is empty.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"path": str(profiles_csv_path)},
        }

    header_cols = (lines[0].split("\t") if "\t" in lines[0] else lines[0].split())
    col_count = len(header_cols)
    if col_count == 0:
        return {
            "status": StatusType.FAILED.value,
            "message": "Could not parse header columns from profiles_csv.tsv.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"header": lines[0][:200]},
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with output_path.open("wt", encoding="utf-8") as f:
        for raw in lines:
            cols = (raw.split("\t") if "\t" in raw else raw.split())
            # normalize length to header columns
            if len(cols) < col_count:
                cols = cols + ([""] * (col_count - len(cols)))
            elif len(cols) > col_count:
                cols = cols[:col_count]
            cols = ["0" if x == "N" else x for x in cols]
            f.write("\t".join(cols) + "\n")
            written += 1

    return {
        "status": StatusType.PASSED.value,
        "message": "profiles.list generated successfully.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {"lines_written": written, "columns": col_count},
    }


def _trim_profiles_list_columns(*, profiles_list_path: Path, keep_columns: int) -> Dict[str, Any]:
    """
    Trim tab-delimited profiles.list to first N columns (N derived from scheme locus count).
    """
    started_at = get_timestamp()
    try:
        lines = profiles_list_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not lines:
            return {
                "status": StatusType.FAILED.value,
                "message": "profiles.list is empty; cannot trim columns.",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": False,
                "metrics": {"path": str(profiles_list_path)},
            }

        out_lines: List[str] = []
        for raw in lines:
            cols = raw.split("\t")
            out_lines.append("\t".join(cols[:keep_columns]))
        profiles_list_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")

        return {
            "status": StatusType.PASSED.value,
            "message": "profiles.list trimmed successfully.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"keep_columns": keep_columns, "lines": len(out_lines)},
        }
    except Exception as e:
        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to trim profiles.list columns: {e}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"keep_columns": keep_columns},
        }


def _concat_fastas_in_dir(*, src_dir: Path, out_path: Path) -> None:
    fasta_files = sorted(p for p in src_dir.glob("*.fasta") if p.is_file() and p.name != out_path.name)
    with out_path.open("wb") as out:
        for p in fasta_files:
            out.write(p.read_bytes())


@click.command()
@click.option("--workspace", type=str, default=None, help="Workspace path used in report metadata.")
@click.option("--run_id", type=str, default=None, help="Unique run ID (defaults to generated).")
@click.option("--container_image", type=str, default="unknown", help="Container image name (report metadata).")
@click.option("--report_file", type=str, default=None, help="Report JSON file name (defaults to {run_id}.json).")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, default=None, help="User name (report metadata).")
@click.option("--host", type=str, default=None, help="Host name (report metadata).")
@click.option("--oauth_credentials_file", type=click.Path(), default=None, help="Optional OAuth credentials file (bind-mounted).")
@click.option("--download_workers", type=int, default=4, show_default=True, help="Max concurrent PubMLST downloads (capped at 4).")
@click.option("-c", "--cpus", default=4, show_default=True, help="Number of CPUs to use for BLAST indexing.")
@click.option("-o", "--output_dir", help="[REQUIRED] Output directory.", required=True, type=click.Path())
@click.option(
    "-d",
    "--database",
    help="[REQUIRED] Database config name in PubMLST",
    type=click.Choice(["pubmlst_campylobacter_seqdef", "pubmlst_campylobacter_nonjejuni_seqdef"]),
    required=True,
)
@click.option(
    "-s",
    "--scheme_name",
    help="[REQUIRED] Name of the cgMLST scheme in PubMLST",
    type=str,
    required=True,
)
def main(
    workspace: Optional[str],
    run_id: Optional[str],
    container_image: str,
    report_file: Optional[str],
    log_file: str,
    user: Optional[str],
    host: Optional[str],
    oauth_credentials_file: Optional[str],
    download_workers: int,
    cpus: int,
    output_dir: str,
    database: str,
    scheme_name: str,
) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    # Logging
    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)

    # Report setup
    if report_file is None or run_id not in report_file:
        report_file = f"{run_id}.json"
    report_dir = out_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}" if workspace else str(out_dir),
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    remaining_steps = list(ALL_STEPS)
    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=SOURCE,
        log_file=f"{execution_context['workspace']}/logs/{log_file}",
    )

    def skip_remaining_steps(steps: List[str], reason: str) -> None:
        for s in steps:
            rb.add_skipped(s, reason)

    # Optional OAuth
    oauth_path = Path(oauth_credentials_file) if oauth_credentials_file else None
    oauth_auth = _maybe_oauth_auth(oauth_credentials_file=oauth_path, logger=logger)

    # Cap PubMLST concurrency at 4 per their public guidance
    if download_workers < 1:
        download_workers = 1
    if download_workers > 4:
        logger.warning("download_workers=%d exceeds PubMLST guidance; capping to 4.", download_workers)
        download_workers = 4

    schemes_url = f"{PUBMLST_BASE}/db/{database}/schemes"
    schemes_json_path = out_dir / "schemes.json"
    loci_json_path = out_dir / "loci.json"
    profiles_csv_path = out_dir / "profiles_csv.tsv"
    manifest_path = out_dir / "pubmlst_md5.json"
    scheme_kind = _scheme_kind_from_name(scheme_name)

    # -----------------------------
    # 1) PREFLIGHT_CONNECTIVITY
    # -----------------------------
    pre = check_url_available("https://www.google.com", retries=3, interval=30, logger=logger)
    rb.add_named_milestone("PREFLIGHT_CONNECTIVITY", pre)
    remaining_steps.remove("PREFLIGHT_CONNECTIVITY")
    if pre["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed preflight connectivity.")
        rb.fail(code="NO_INTERNET", message=pre.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 2) DATABASE_AVAILABILITY
    # -----------------------------
    db_avail = composite_availability_check([PUBMLST_BASE, schemes_url], logger, retries=3, interval=30)
    rb.add_named_milestone("DATABASE_AVAILABILITY", db_avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if db_avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=db_avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 3) REMOTE_FILES_DOWNLOAD_STATUS (metadata-first)
    # -----------------------------
    started_at = get_timestamp()

    ok_schemes, a_schemes = _download_text_atomic_with_retry(
        url=schemes_url,
        output_path=schemes_json_path,
        logger=logger,
        auth=oauth_auth,
        max_retries=3,
        wait_seconds=30,
        timeout_s=60,
    )

    if not ok_schemes:
        rb.add_named_milestone(
            "REMOTE_FILES_DOWNLOAD_STATUS",
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download schemes.json",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": a_schemes,
                "retryable": True,
                "metrics": {"url": schemes_url},
            },
        )
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download metadata artifacts.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    scheme_link = _find_scheme_link(schemes_json_path=schemes_json_path, scheme_name=scheme_name)
    if not scheme_link:
        rb.add_named_milestone(
            "REMOTE_FILES_DOWNLOAD_STATUS",
            {
                "status": StatusType.FAILED.value,
                "message": f"Could not find scheme link for scheme_name={scheme_name}",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": False,
                "metrics": {"scheme_name": scheme_name},
            },
        )
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: scheme not found.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    loci_url = scheme_link.rstrip("/") + "/loci"
    profiles_url = scheme_link.rstrip("/") + "/profiles_csv"

    ok_loci, a_loci = _download_text_atomic_with_retry(
        url=loci_url,
        output_path=loci_json_path,
        logger=logger,
        auth=oauth_auth,
        max_retries=3,
        wait_seconds=30,
        timeout_s=60,
    )
    ok_profiles, a_profiles = _download_text_atomic_with_retry(
        url=profiles_url,
        output_path=profiles_csv_path,
        logger=logger,
        auth=oauth_auth,
        max_retries=3,
        wait_seconds=30,
        timeout_s=120,
    )

    if not ok_loci or not ok_profiles:
        rb.add_named_milestone(
            "REMOTE_FILES_DOWNLOAD_STATUS",
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download one or more metadata artifacts (loci.json / profiles_csv.tsv).",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": max(a_schemes, a_loci, a_profiles),
                "retryable": True,
                "metrics": {
                    "scheme_link": scheme_link,
                    "loci_ok": ok_loci,
                    "profiles_ok": ok_profiles,
                },
            },
        )
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download metadata artifacts.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    loci_list = []
    try:
        loci_list = _read_loci_list(loci_json_path=loci_json_path)
    except Exception:
        loci_list = []

    rb.add_named_milestone(
        "REMOTE_FILES_DOWNLOAD_STATUS",
        {
            "status": StatusType.PASSED.value,
            "message": "Metadata artifacts downloaded successfully.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": max(a_schemes, a_loci, a_profiles),
            "retryable": True,
            "metrics": {
                "scheme_link": scheme_link,
                "loci_count": len(loci_list),
                "schemes_bytes": int(schemes_json_path.stat().st_size) if schemes_json_path.exists() else 0,
                "loci_bytes": int(loci_json_path.stat().st_size) if loci_json_path.exists() else 0,
                "profiles_bytes": int(profiles_csv_path.stat().st_size) if profiles_csv_path.exists() else 0,
            },
        },
    )
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")

    # -----------------------------
    # 4) UPDATE_STATUS (checksum manifest)
    # -----------------------------
    started_at = get_timestamp()

    old_md5: Dict[str, str] = {}
    first_build = False
    if manifest_path.exists():
        try:
            old_md5 = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
            if not isinstance(old_md5, dict):
                old_md5 = {}
        except Exception:
            old_md5 = {}
    else:
        first_build = True

    # Decide update based ONLY on profiles_csv.tsv checksum:
    # - profiles represent full scheme profile set; changes imply new/updated alleles are present.
    # - avoids false-positive rebuilds due to non-deterministic JSON key order in schemes/loci endpoints.
    new_md5: Dict[str, str] = {"profiles_csv.tsv": file_md5sum(str(profiles_csv_path))}
    changed = [k for k, v in new_md5.items() if old_md5.get(k, "") != v]

    expected_processed = _expected_processed_files(loci_json_path=loci_json_path, scheme_kind=scheme_kind)
    required_present = all((out_dir / rel).exists() for rel in expected_processed)
    update_required = (not manifest_path.exists()) or bool(changed) or (not required_present)

    if update_required:
        msg = "No previous manifest: treating as first build." if first_build else ("Update required: checksum change detected." if changed else "Update required: expected files missing locally.")
        remove_old_workspace(
            out_dir,
            keep=("logs", "reports", manifest_path.name, schemes_json_path.name, loci_json_path.name, profiles_csv_path.name),
            logger=logger,
        )
        manifest_path.write_text(json.dumps(new_md5, indent=2) + "\n", encoding="utf-8")
        upd_milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"changed_files": changed, "required_files_present": required_present, "update_required": True},
        }
        update_decision = {
            "mode": "checksum_manifest",
            "result": "updated",
            "message": msg,
            "first_build": first_build,
            "checksums_before": [{"file_name": k, "checksum": v} for k, v in old_md5.items()],
            "checksums_after": [{"file_name": k, "checksum": v} for k, v in new_md5.items()],
        }
    else:
        msg = "No update required: checksums match previous manifest and expected files are present."
        upd_milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"changed_files": [], "required_files_present": True, "update_required": False},
        }
        update_decision = {
            "mode": "checksum_manifest",
            "result": "latest_version_present",
            "message": msg,
            "first_build": first_build,
            "checksums_before": [{"file_name": k, "checksum": v} for k, v in old_md5.items()],
            "checksums_after": [{"file_name": k, "checksum": v} for k, v in new_md5.items()],
        }

    rb.add_named_milestone("UPDATE_STATUS", upd_milestone)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(**update_decision)

    if not update_required:
        skip_remaining_steps(remaining_steps, "Skipped: latest version already present.")
        rb.finalize("SKIPPED")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 5) PROCESSING_STATUS
    # -----------------------------
    started_at = get_timestamp()

    # Re-download metadata after workspace cleanup (kept files should still exist, but be safe)
    if not schemes_json_path.exists():
        ok_schemes, _a = _download_text_atomic_with_retry(url=schemes_url, output_path=schemes_json_path, logger=logger, auth=oauth_auth)
        if not ok_schemes:
            proc = {
                "status": StatusType.FAILED.value,
                "message": "Missing schemes.json after cleanup and failed to re-download.",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {},
            }
            rb.add_named_milestone("PROCESSING_STATUS", proc)
            remaining_steps.remove("PROCESSING_STATUS")
            skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
            rb.finalize("FAIL")
            rb.write(str(report_dir / report_file))
            return

    scheme_link = _find_scheme_link(schemes_json_path=schemes_json_path, scheme_name=scheme_name) or scheme_link
    loci_url = scheme_link.rstrip("/") + "/loci"
    profiles_url = scheme_link.rstrip("/") + "/profiles_csv"

    if not loci_json_path.exists():
        ok_loci, _a = _download_text_atomic_with_retry(url=loci_url, output_path=loci_json_path, logger=logger, auth=oauth_auth)
        if not ok_loci:
            proc = {
                "status": StatusType.FAILED.value,
                "message": "Missing loci.json after cleanup and failed to re-download.",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {},
            }
            rb.add_named_milestone("PROCESSING_STATUS", proc)
            remaining_steps.remove("PROCESSING_STATUS")
            skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
            rb.finalize("FAIL")
            rb.write(str(report_dir / report_file))
            return

    if not profiles_csv_path.exists():
        ok_profiles, _a = _download_text_atomic_with_retry(url=profiles_url, output_path=profiles_csv_path, logger=logger, auth=oauth_auth)
        if not ok_profiles:
            proc = {
                "status": StatusType.FAILED.value,
                "message": "Missing profiles_csv.tsv after cleanup and failed to re-download.",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": True,
                "metrics": {},
            }
            rb.add_named_milestone("PROCESSING_STATUS", proc)
            remaining_steps.remove("PROCESSING_STATUS")
            skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
            rb.finalize("FAIL")
            rb.write(str(report_dir / report_file))
            return

    # Build profiles.list
    profiles_list_path = out_dir / "profiles.list"
    m_profiles = _build_profiles_list(profiles_csv_path=profiles_csv_path, output_path=profiles_list_path)
    if m_profiles["status"] != StatusType.PASSED.value:
        rb.add_named_milestone("PROCESSING_STATUS", m_profiles)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # MLST-specific postprocessing:
    #  - trim profiles.list to (1 + number_of_loci) columns
    #  - create all_allels.fasta for downstream tools expecting a single concatenated file
    if scheme_kind == "mlst":
        loci_uris = _read_loci_list(loci_json_path=loci_json_path)
        loci_count = len(loci_uris)
        keep_cols = 1 + loci_count if loci_count > 0 else 0
        if keep_cols > 0:
            m_trim = _trim_profiles_list_columns(profiles_list_path=profiles_list_path, keep_columns=keep_cols)
            if m_trim["status"] != StatusType.PASSED.value:
                rb.add_named_milestone("PROCESSING_STATUS", m_trim)
                remaining_steps.remove("PROCESSING_STATUS")
                skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
                rb.finalize("FAIL")
                rb.write(str(report_dir / report_file))
                return

    # Download allele FASTAs with bounded concurrency (<=4)
    loci_list = _read_loci_list(loci_json_path=loci_json_path)

    def dl_one(locus_uri: str) -> Tuple[str, bool, int, str]:
        locus_name = locus_uri.rstrip("/").split("/")[-1]
        url = locus_uri.rstrip("/") + "/alleles_fasta"
        dest = out_dir / f"{locus_name}.fasta"
        ok, attempts_used = _download_text_atomic_with_retry(
            url=url,
            output_path=dest,
            logger=logger,
            auth=oauth_auth,
            max_retries=3,
            wait_seconds=30,
            timeout_s=120,
        )
        return locus_name, ok, attempts_used, url

    downloaded = 0
    failed_loci: List[str] = []
    attempts_used_max = 1

    with ThreadPoolExecutor(max_workers=download_workers) as ex:
        futs = {ex.submit(dl_one, u): u for u in loci_list}
        for fut in as_completed(futs):
            locus_name, ok, attempts_used, _url = fut.result()
            attempts_used_max = max(attempts_used_max, attempts_used)
            if ok:
                downloaded += 1
            else:
                failed_loci.append(locus_name)
                # fail fast: cancel remaining
                for f in futs:
                    f.cancel()
                break

    if failed_loci:
        proc = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download allele FASTA for locus: {failed_loci[0]}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {"downloaded": downloaded, "failed_locus": failed_loci[0], "loci_total": len(loci_list), "download_workers": download_workers},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=proc["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # BLAST indexing (local CPU bound)
    fasta_paths = [out_dir / f"{u.rstrip('/').split('/')[-1]}.fasta" for u in loci_list]
    fasta_paths = [p for p in fasta_paths if p.exists()]

    def index_one(p: Path) -> Tuple[str, bool]:
        ok, _m = run_makeblastdb(p, dbtype="nucl", logger=logger)
        return str(p), ok

    threads_idx = max(1, int(cpus or 1))
    failed_idx: List[str] = []

    with ThreadPoolExecutor(max_workers=threads_idx) as ex:
        futs = [ex.submit(index_one, p) for p in fasta_paths]
        for fut in as_completed(futs):
            path_str, ok = fut.result()
            if not ok:
                failed_idx.append(path_str)

    if failed_idx:
        proc = {
            "status": StatusType.FAILED.value,
            "message": f"makeblastdb failed for {len(failed_idx)} file(s).",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"failed": failed_idx[:20], "failed_truncated": len(failed_idx) > 20},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    if scheme_kind == "mlst":
        all_allels_path = out_dir / "all_allels.fasta"
        logger.info("Creating %s ...", all_allels_path)
        _concat_fastas_in_dir(src_dir=out_dir, out_path=all_allels_path)

    # Create local stub
    _write_profiles_local_stub(output_dir=out_dir, logger=logger)

    proc = {
        "status": StatusType.PASSED.value,
        "message": "Downloaded alleles, generated profiles.list, and created BLAST indices.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {"loci_total": len(loci_list), "downloaded": downloaded, "download_workers": download_workers, "indexed": len(fasta_paths), "index_workers": threads_idx},
    }
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # -----------------------------
    # 6) FINAL_STATUS
    # -----------------------------
    final = verify_expected_files(base_dir=out_dir, expected_files=_expected_processed_files(loci_json_path=loci_json_path, scheme_kind=scheme_kind))
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")

    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()
