#!/usr/bin/env python3

from __future__ import annotations

import getpass
import os
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import click
import requests

from utils.download_helpers import _download_file_with_retry
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check
from utils.validation import get_timestamp, verify_expected_files


DATABASE = {"name": "phiercc_local", "category": "cgMLST clustering"}

PHIERCC_REPO_RAW_BASE = "https://github.com/michallaz/phiercc_pzh_mod/raw/refs/heads/main/plepiseq_data"
TIMESTAMP_URL = f"{PHIERCC_REPO_RAW_BASE}/timestamp"

RAW_FILES: Tuple[str, ...] = (
    "profile_complete_linkage.HierCC.gz",
    "profile_complete_linkage.HierCC.index",
    "profile_single_linkage.HierCC.gz",
    "profile_single_linkage.HierCC.index",
)

# IMPORTANT: local subdir layout is taken from bin/update/update.sh (shell baseline)
GENUS_CONFIG: Dict[str, Dict[str, str]] = {
    # remote dir: Campylobacter/, local dir: Campylobacter/jejuni/
    "Campylobacter": {"remote_subdir": "Campylobacter", "local_subdir": "Campylobacter/jejuni"},
    # remote dir: Escherichia/, local dir: Escherichia/
    "Escherichia": {"remote_subdir": "Escherichia", "local_subdir": "Escherichia"},
    # remote dir: Salmonella/, local dir: Salmonella/
    "Salmonella": {"remote_subdir": "Salmonella", "local_subdir": "Salmonella"},
}


def _read_remote_timestamp(*, url: str, timeout_s: int = 30) -> Tuple[bool, str, str]:
    """
    Fetch remote timestamp (a short string like '01/24/26').
    Returns (ok, timestamp, error_message).
    """
    try:
        r = requests.get(url, timeout=timeout_s)
        if r.status_code != 200:
            return False, "", f"HTTP {r.status_code}"
        ts = (r.text or "").strip()
        if not ts:
            return False, "", "Empty timestamp content"
        return True, ts, ""
    except Exception as e:
        return False, "", str(e)


def _determine_update_status_timestamp(
    *,
    output_dir: Path,
    logger,
    baseline_filename: str = "current_timestamp.txt",
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, str]:
    """
    Compare remote repo timestamp vs local baseline file in output_dir.

    Returns:
      milestone payload,
      update_decision kwargs (for ReportBuilder.set_update_decision),
      update_required,
      remote_timestamp_str
    """
    started_at = get_timestamp()

    ok, remote_ts, err = _read_remote_timestamp(url=TIMESTAMP_URL, timeout_s=30)
    if not ok:
        finished_at = get_timestamp()
        msg = f"Failed to read remote timestamp: {err}"
        milestone = {
            "status": StatusType.FAILED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": True,
            "metrics": {"timestamp_url": TIMESTAMP_URL},
        }
        update_decision = {"mode": "timestamp", "result": "error", "message": msg, "first_build": True,
                           "timestamp_local": "", "timestamp_remote": ""}
        return milestone, update_decision, False, ""

    baseline_path = output_dir / baseline_filename
    local_ts = ""
    first_build = True
    if baseline_path.exists():
        try:
            local_ts = baseline_path.read_text(encoding="utf-8").strip()
            first_build = False
        except Exception:
            local_ts = ""
            first_build = False

    update_required = first_build or (local_ts != remote_ts)

    finished_at = get_timestamp()
    if update_required:
        msg = (
            "No local timestamp baseline: treating as first build."
            if first_build
            else "Update required: remote timestamp differs from local baseline."
        )
        logger.info("%s local=%r remote=%r", msg, local_ts, remote_ts)
        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "timestamp_url": TIMESTAMP_URL,
                "baseline_path": str(baseline_path),
                "first_build": first_build,
                "update_required": True,
            },
        }
        update_decision = {
            "mode": "timestamp",
            "result": "updated",
            "message": msg,
            "first_build": first_build,
            "timestamp_local": local_ts,
            "timestamp_remote": remote_ts,
        }
        return milestone, update_decision, True, remote_ts

    msg = "No update required: remote timestamp matches local baseline."
    logger.info("%s local=%r remote=%r", msg, local_ts, remote_ts)
    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "timestamp_url": TIMESTAMP_URL,
            "baseline_path": str(baseline_path),
            "first_build": first_build,
            "update_required": False,
        },
    }
    update_decision = {
        "mode": "timestamp",
        "result": "latest_version_present",
        "message": msg,
        "first_build": first_build,
        "timestamp_local": local_ts,
        "timestamp_remote": remote_ts,
    }
    return milestone, update_decision, False, remote_ts


def _targets_for_genus(genus: str) -> Sequence[str]:
    if genus == "all":
        return tuple(GENUS_CONFIG.keys())
    return (genus,)


def _expected_paths_for_targets(targets: Sequence[str]) -> List[str]:
    expected: List[str] = []
    for g in targets:
        local_subdir = GENUS_CONFIG[g]["local_subdir"]
        for fname in RAW_FILES:
            expected.append(f"{local_subdir}/{fname}")
    return expected


def _download_phiercc_files(
    *,
    output_dir: Path,
    targets: Sequence[str],
    logger,
    max_retries: int = 3,
    wait_seconds: int = 30,
) -> Dict[str, Any]:
    started_at = get_timestamp()

    # Build (url, dest) list
    jobs: List[Tuple[str, Path]] = []
    for g in targets:
        remote_subdir = GENUS_CONFIG[g]["remote_subdir"]
        local_subdir = GENUS_CONFIG[g]["local_subdir"]
        for fname in RAW_FILES:
            url = f"{PHIERCC_REPO_RAW_BASE}/{remote_subdir}/{fname}"
            dest = output_dir / local_subdir / fname
            jobs.append((url, dest))

    # Backup existing files (only if they exist)
    backups: List[Tuple[Path, Path]] = []
    for _url, dest in jobs:
        old = dest.with_name(dest.name + ".old")
        if dest.exists():
            try:
                if old.exists():
                    old.unlink()
            except Exception:
                pass
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(dest, old)
            backups.append((dest, old))

    attempts_used_max = 1
    failed_url: Optional[str] = None

    for url, dest in jobs:
        ok, attempts = _download_file_with_retry(
            url=url,
            output_path=dest,
            logger=logger,
            max_retries=max_retries,
            wait_seconds=wait_seconds,
            timeout_s=120,
        )
        attempts_used_max = max(attempts_used_max, attempts)
        if not ok:
            failed_url = url
            break

    finished_at = get_timestamp()

    if failed_url is not None:
        # Restore backups
        for dest, old in backups:
            try:
                if dest.exists():
                    dest.unlink()
            except Exception:
                pass
            try:
                if old.exists():
                    os.replace(old, dest)
            except Exception:
                pass

        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download file from: {failed_url}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {"files_total": len(jobs), "failed_url": failed_url, "targets": list(targets)},
        }

    # Success: remove backups
    for _dest, old in backups:
        try:
            if old.exists():
                old.unlink()
        except Exception:
            pass

    return {
        "status": StatusType.PASSED.value,
        "message": "All pHierCC files downloaded successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": attempts_used_max,
        "retryable": True,
        "metrics": {"files_total": len(jobs), "files_downloaded": len(jobs), "targets": list(targets)},
    }


def _processing_status_dummy() -> Dict[str, Any]:
    started_at = get_timestamp()
    finished_at = get_timestamp()
    return {
        "status": StatusType.SKIPPED.value,
        "message": "No additional processing required for phierCC (download-only).",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {},
    }


def _persist_timestamp_baseline(*, output_dir: Path, remote_ts: str) -> None:
    """
    Persist baseline for future comparisons, and keep a timestamped history copy.
    """
    (output_dir / "timestamp_history").mkdir(parents=True, exist_ok=True)
    now = get_timestamp().replace(":", "").replace("-", "")
    # history file is keyed by local write time (UTC)
    (output_dir / "timestamp_history" / f"{now}.txt").write_text(remote_ts + "\n", encoding="utf-8")
    (output_dir / "current_timestamp.txt").write_text(remote_ts + "\n", encoding="utf-8")


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option(
    "--output_dir",
    type=str,
    default="/home/external_databases/phiercc_local",
    help="Output directory.",
)
@click.option(
    "--genus",
    type=click.Choice(["Campylobacter", "Escherichia", "Salmonella", "all"]),
    required=True,
    help="Genus to download (or 'all').",
)
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    genus: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/phiercc_local",
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
    logger.info("Starting pHierCC local updater")

    execution_context = {
        "workspace": f"{workspace}/phiercc",
        "user": user or getpass.getuser(),
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

    source = {
        "source_type": "https",
        "reference": "https://github.com/michallaz/phiercc_pzh_mod",
        "expected_raw_files": list(RAW_FILES),
        "expected_processed_files": _expected_paths_for_targets(_targets_for_genus(genus)),
    }

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=source,
        log_file=f"{workspace}/phiercc/reports/{report_file}",
    )

    def skip_remaining_steps(steps: List[str], reason: str) -> None:
        for s in steps:
            rb.add_skipped(s, reason)

    targets = _targets_for_genus(genus)

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
    # check timestamp endpoint + one representative file per requested genus
    urls_to_check: List[str] = [TIMESTAMP_URL]
    for g in targets:
        remote_subdir = GENUS_CONFIG[g]["remote_subdir"]
        urls_to_check.append(f"{PHIERCC_REPO_RAW_BASE}/{remote_subdir}/{RAW_FILES[0]}")

    avail = composite_availability_check(urls_to_check, logger, retries=3, interval=10)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_ts = _determine_update_status_timestamp(
        output_dir=out_dir,
        logger=logger,
        baseline_filename="current_timestamp.txt",
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
    dl = _download_phiercc_files(output_dir=out_dir, targets=targets, logger=logger, max_retries=3, wait_seconds=30)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = _processing_status_dummy()
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=_expected_paths_for_targets(targets))
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist baseline only after success
    try:
        _persist_timestamp_baseline(output_dir=out_dir, remote_ts=remote_ts)
    except Exception as e:
        logger.warning("Failed to persist timestamp baseline: %s", e)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

