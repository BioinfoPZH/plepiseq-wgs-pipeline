#!/usr/bin/env python3


import os
import re
import json
import getpass
import socket
import logging
from pathlib import Path
from typing import Optional
from typing import Dict, Any, Tuple, List

import click

from utils.report import ReportBuilder, ALL_STEPS, SCHEMA_VERSION
from utils.net import check_url_available, StatusType
from utils.run_id import generate_run_id
from utils.validation import verify_expected_files
from utils.setup_logging import _setup_logging
from utils.generic_helpers import remove_old_workspace, get_timestamp
from utils.s3_helpers import (

    check_s3_connectivity,
    list_available_databases,
    find_latest_database,
    download_remote_md5,
    download_file_and_extract_s3,
)

# --------------------
# Constants
# --------------------
DB_NAMES = [
    'standard', 'standard_08gb', 'standard_16gb', 'viral', 'minusb',
    'pluspf', 'pluspf_08gb', 'pluspf_16gb', 'pluspfp', 'pluspfp_08gb',
    'pluspfp_16gb', 'nt', 'eupathdb48'
]

S3_BUCKET = "genome-idx"
S3_PREFIX = "kraken/"

DATABASE = {"name": "kraken2_db", "category": "taxonomy"}
SOURCE = {
    "source_type": "s3",
    "reference": f"s3://{S3_BUCKET}/{S3_PREFIX}",
    # For Kraken we download+extract in one step and remove the tarball, so the "raw"
    # artifacts we can reliably expect are the extracted DB files themselves.
    # Schema requires minItems=1.
    "expected_raw_files": ["database200mers.kmer_distrib",
                           "nodes.dmp",
                           "taxo.k2d",
                           "hash.k2d"],
    "expected_processed_files": ["database200mers.kmer_distrib",
                                 "nodes.dmp",
                                 "taxo.k2d",
                                 "hash.k2d"],
}

def determine_update_status_for_kraken(
    bucket: str,
    prefix: str,
    db_name: str,
    output_dir: Path,
    logger: logging.Logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    """
    Locate latest candidate on S3, find matching remote .md5 from S3 listing,
    parse it, and compare with local current_md5.json.

    Returns:
      (milestone, update_decision, update_required, new_md5_dict)
    """
    started = get_timestamp()

    # Tarballs live like: <prefix>k2_<db_name>_<YYYYMMDD>.tar.gz
    db_name_regexp = re.compile(re.escape(prefix) + rf"k2_{re.escape(db_name)}_(?P<date>\d{{8}})\.tar\.gz$")

    # 1) List S3 objects under prefix
    try:
        databases = list_available_databases(bucket, prefix)
    except Exception as e:
        finished = get_timestamp()
        msg = f"Failed to list S3 objects: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": True,
                "metrics": {"bucket": bucket, "prefix": prefix},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )

    # 2) Find latest tarball key
    target_db = find_latest_database(databases, db_name_regexp)
    if not target_db:
        finished = get_timestamp()
        msg = "No matching database found on S3."
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": False,
                "metrics": {"searched": len(databases), "db_name": db_name},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )

    tar_name = os.path.basename(target_db)

    # parse k2_<label>_<date>.tar.gz
    m = re.match(r"^k2_(?P<label>.+)_(?P<date>\d{8})\.tar\.gz$", tar_name)
    if not m:
        finished = get_timestamp()
        msg = f"Unexpected tarball name format: {tar_name}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": False,
                "metrics": {"tar_name": tar_name, "target_db": target_db},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )

    label = m.group("label")
    date_part = m.group("date")

    # 3) Find md5 key by searching S3 listing (don’t guess filename)
    md5_dir_prefix = f"{prefix}{label}_{date_part}/"
    md5_candidates = sorted(
        k for k in databases
        if k.startswith(md5_dir_prefix) and k.endswith(".md5")
    )

    if not md5_candidates:
        finished = get_timestamp()
        msg = f"No .md5 file found under {md5_dir_prefix}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": True,
                "metrics": {"md5_dir_prefix": md5_dir_prefix, "tar_name": tar_name, "target_db": target_db},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )

    md5_key = md5_candidates[0]
    logger.info("Selected md5 key: %s", md5_key)

    # 4) Download + parse md5
    md5_tmp = output_dir / os.path.basename(md5_key)
    try:
        parsed = download_remote_md5(bucket, md5_key, str(md5_tmp))  # returns {filename_token: md5}
    except Exception as e:
        finished = get_timestamp()
        msg = f"Failed to download remote md5: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": True,
                "metrics": {"md5_key": md5_key, "tar_name": tar_name, "target_db": target_db},
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )
    finally:
        try:
            if md5_tmp.exists():
                md5_tmp.unlink()
        except Exception:
            pass

    # 5) Pick the right md5 entry robustly
    def pick_remote_md5(parsed_map: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
        # Try exact tar_name match by basename
        for fname, md5 in parsed_map.items():
            if os.path.basename(fname) == tar_name:
                return md5, fname

        # Try without "k2_" prefix (common in md5 manifests)
        alt_tar = tar_name
        if alt_tar.startswith("k2_"):
            alt_tar = alt_tar[3:]
        for fname, md5 in parsed_map.items():
            if os.path.basename(fname) == alt_tar:
                return md5, fname

        # Fall back to any tar.gz line; prefer ones containing the date
        tar_candidates: List[Tuple[str, str]] = []
        for fname, md5 in parsed_map.items():
            if ".tar.gz" in fname:
                tar_candidates.append((fname, md5))

        if not tar_candidates:
            return None, None

        # Prefer candidates with the date in name
        for fname, md5 in tar_candidates:
            if date_part in fname:
                return md5, fname

        # Otherwise just take the first
        return tar_candidates[0][1], tar_candidates[0][0]

    remote_md5, matched_name = pick_remote_md5(parsed)

    if not remote_md5:
        finished = get_timestamp()
        msg = f"Remote md5 manifest did not contain a usable tar.gz entry for {tar_name}"
        sample_keys = list(parsed.keys())[:10]
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started,
                "finished_at": finished,
                "attempts": 1,
                "retryable": True,
                "metrics": {
                    "md5_key": md5_key,
                    "tar_name": tar_name,
                    "target_db": target_db,
                    "parsed_keys_sample": sample_keys,
                    "parsed_total_keys": len(parsed),
                },
            },
            {"mode": "checksum_manifest", "result": "error", "message": msg},
            False,
            {},
        )

    # 6) Read local baseline
    # Baseline format:
    #   - current_md5.json -> {"<tarball_name>": "<md5>"}
    old_md5 = ""
    old_name = "unknown_baseline"
    has_local = False

    local_md5_json_path = output_dir / "current_md5.json"

    try:
        if local_md5_json_path.exists():
            raw = local_md5_json_path.read_text(encoding="utf-8")
            parsed = json.loads(raw) if raw.strip() else {}
            if isinstance(parsed, dict) and parsed:
                # take the first (and expected only) entry
                old_name, old_md5 = next(iter(parsed.items()))
                old_name = os.path.basename(str(old_name))
                old_md5 = str(old_md5).strip()
                has_local = True
    except Exception:
        pass

    finished = get_timestamp()

    # 7) Decide update
    new_md5 = {tar_name: remote_md5}

    if not has_local:
        msg = "No local checksum baseline: treating as first build."
        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started,
            "finished_at": finished,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "md5_present": False,
                "tarball": tar_name,
                "target_db": target_db,
                "md5_key": md5_key,
                "matched_name": matched_name,
            },
        }
        update_decision = {
            "mode": "checksum_manifest",
            "result": "updated",
            "message": msg,
            "first_build": True,
            "checksums_before": [],
            "checksums_after": [{"file_name": tar_name, "checksum": remote_md5}],
        }
        return milestone, update_decision, True, new_md5

    # Dual-check update decision:
    #   1) if baseline tarball name is known and differs -> update
    #   2) else compare md5
    name_mismatch = (old_name != "unknown_baseline") and (old_name != tar_name)
    md5_mismatch = (remote_md5 or "") != (old_md5 or "")

    update_required = name_mismatch or md5_mismatch

    if update_required:
        if name_mismatch:
            msg = "Update required: remote tarball name differs from local baseline."
        else:
            msg = "Update required: remote md5 differs from local baseline."
        remove_old_workspace(output_dir, keep=("logs", "reports", "current_md5.json"), logger=logger)

        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started,
            "finished_at": finished,
            "attempts": 1,
            "retryable": False,
            "metrics": {"tarball": tar_name, "target_db": target_db, "md5_key": md5_key, "matched_name": matched_name},
        }
        update_decision = {
            "mode": "checksum_manifest",
            "result": "updated",
            "message": msg,
            "first_build": False,
            "checksums_before": [{"file_name": old_name, "checksum": old_md5}],
            "checksums_after": [{"file_name": tar_name, "checksum": remote_md5}],
        }
        return milestone, update_decision, True, new_md5

    msg = "No update required: remote md5 matches local."
    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started,
        "finished_at": finished,
        "attempts": 1,
        "retryable": False,
        "metrics": {"tarball": tar_name, "target_db": target_db, "md5_key": md5_key, "matched_name": matched_name},
    }
    update_decision = {
        "mode": "checksum_manifest",
        "result": "latest_version_present",
        "message": msg,
        "first_build": False,
        "checksums_before": [{"file_name": old_name, "checksum": old_md5}],
        "checksums_after": [{"file_name": tar_name, "checksum": remote_md5}],
    }
    return milestone, update_decision, False, new_md5



def download_and_extract_if_needed(bucket: str, key: str, local_path: Path, logger: logging.Logger):
    started = get_timestamp()
    try:
        dest_tar = str(local_path / os.path.basename(key))
        res = download_file_and_extract_s3(bucket, key, dest_tar, extract_to=str(local_path), logger=logger)
        finished = get_timestamp()
        return {
            "status": StatusType.PASSED.value,
            "message": "Downloaded and extracted tarball",
            "started_at": started,
            "finished_at": finished,
            "attempts": 1,
            "retryable": False,
            "metrics": res,
        }
    except Exception as e:
        finished = get_timestamp()
        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download/extract: {e}",
            "started_at": started,
            "finished_at": finished,
            "attempts": 1,
            "retryable": True,
            "metrics": {"key": key},
        }


def processing_status_dummy(output_dir: Path, logger: logging.Logger):
    """Kraken needs no extra processing beyond extraction; return SKIPPED with info."""
    started = get_timestamp()
    finished = get_timestamp()
    return {
        "status": StatusType.SKIPPED.value,
        "message": "No additional processing required for Kraken (extraction-only).",
        "started_at": started,
        "finished_at": finished,
        "attempts": 1,
        "retryable": False,
        "metrics": {},
    }


@click.command()
@click.option("-o", "--local_path", required=True, type=click.Path(), help="[REQUIRED] Full path to the destination directory")
@click.option("-d", "--db_name", required=True, type=click.Choice(DB_NAMES), help="Database name (if unsure, use 'standard')")
@click.option("--workspace", type=str, default=str(Path.cwd()), help="Workspace path. Used only in reports.  env variable pas to the container that includes a root where all databases are stored in separate subdirectories.")
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default='log.log', help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
def main(local_path: str, db_name: str, workspace: Optional[str], run_id: Optional[str], container_image: Optional[str], report_file: Optional[str], log_file: str, user: Optional[str], host: Optional[str]):
    # Prepare runtime identifiers
    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    output_dir = Path(local_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # logging
    log_dir = output_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix


    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting Kraken2 DB updater (refactored)")

    execution_context = {
        "workspace": f"{workspace}/kraken2",
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    if report_file is None:
        report_file = f"{run_id}.json"

    report_dir = output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    remaining_steps = list(ALL_STEPS)

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=SOURCE,
        log_file=f"{workspace}/kraken2/reports/{report_file}",
    )

    def skip_remaining_steps(remaining_steps: list[str], reason: str) -> None:
        for step in remaining_steps:
            rb.add_skipped(step, reason)

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
    db_avail = check_s3_connectivity(S3_BUCKET, S3_PREFIX, attempts=3, interval_sec=30, logger=logger)
    rb.add_named_milestone("DATABASE_AVAILABILITY", db_avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if db_avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=db_avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS (MD5-only)
    upd_milestone, update_decision, update_required, _new_md5 = determine_update_status_for_kraken(S3_BUCKET, S3_PREFIX, db_name, output_dir, logger)
    rb.add_named_milestone("UPDATE_STATUS", upd_milestone)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(**update_decision)
    if upd_milestone["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed update decision.")
        rb.fail(code="UPDATE_DECISION_FAILED", message=upd_milestone["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    if not update_required:
        skip_remaining_steps(remaining_steps, "Skipped: latest version already present.")
        rb.finalize("SKIPPED")
        rb.write(str(report_dir / report_file))
        return

    # 4) REMOTE_FILES_DOWNLOAD_STATUS (only if update_required)
    # Find latest db key again to download
    db_name_regexp = re.compile(S3_PREFIX + r"k2_" + db_name + r"_(?P<date>\d{8})\.tar\.gz")
    databases = list_available_databases(S3_BUCKET, S3_PREFIX)
    target_db = find_latest_database(databases, db_name_regexp)
    if not target_db:
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", {
            "status": StatusType.FAILED.value,
            "message": "No target tarball found for download",
            "started_at": get_timestamp(),
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {},
        })
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: no target tarball found for download.")
        rb.fail(code="NO_TARGET_TARBALL", message="No target tarball found for download", retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    dl_report = download_and_extract_if_needed(S3_BUCKET, target_db, output_dir, logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_report)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl_report["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = processing_status_dummy(output_dir, logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value and proc["status"] != StatusType.SKIPPED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=output_dir, expected_files=SOURCE["expected_processed_files"])
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()
