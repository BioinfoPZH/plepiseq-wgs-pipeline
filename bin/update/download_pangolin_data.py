#!/usr/bin/env python3

from __future__ import annotations

import getpass
import socket
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.generic_helpers import remove_old_workspace
from utils.github_helpers import build_version_string, get_github_head_sha
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check
from utils.validation import get_timestamp, verify_expected_files
from utils.version_manifest import read_version_manifest, write_version_manifest


DATABASE = {"name": "pangolin_data", "category": "viral lineages"}

SOURCE = {
    "source_type": "git",
    "reference": "https://github.com/cov-lineages/pangolin-data",
    "expected_raw_files": ["pangolin_data/__init__.py"],
    "expected_processed_files": [
        "pangolin_data/__init__.py",
        "pangolin_data/data/alias_key.json",
        "pangolin_data/data/lineages.hash.csv",
        "pangolin_data/data/lineageTree.pb",
    ],
}


def _run_cmd_capture(
    *,
    cmd: List[str],
    cwd: Optional[Path] = None,
    timeout_s: int = 3600,
) -> Tuple[bool, int, str, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    stdout = (p.stdout or "")[:8000]
    stderr = (p.stderr or "")[:8000]
    return p.returncode == 0, int(p.returncode), stdout, stderr


def determine_update_status_from_github_sha(
    *,
    output_dir: Path,
    logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    started_at = get_timestamp()

    manifest_path = output_dir / "current_version.json"
    local_versions = read_version_manifest(manifest_path)
    first_build = not bool(local_versions)

    metrics: Dict[str, Any] = {"github": {}}

    try:
        sha, sha_metrics = get_github_head_sha(owner="cov-lineages", repo="pangolin-data", ref="main", logger=logger)
        remote_versions = {"pangolin-data": sha}
        metrics["github"]["pangolin-data"] = sha_metrics
    except Exception as e:
        finished_at = get_timestamp()
        msg = f"Failed to fetch remote version SHA: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": True,
                "metrics": metrics,
            },
            {"mode": "version_endpoint", "result": "error", "message": msg},
            False,
            {},
        )

    finished_at = get_timestamp()

    local_str = build_version_string(local_versions) if local_versions else ""
    remote_str = build_version_string(remote_versions)

    update_required = local_versions != remote_versions

    if update_required:
        msg = (
            "No local version baseline: treating as first build."
            if first_build
            else "Update required: remote GitHub commit differs from local baseline."
        )
        remove_old_workspace(output_dir, keep=("logs", "reports", "current_version.json"), logger=logger)

        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                **metrics,
                "manifest_path": str(manifest_path),
                "first_build": first_build,
                "local_version": local_str,
                "remote_version": remote_str,
                "update_required": True,
            },
        }

        update_decision = {
            "mode": "version_endpoint",
            "result": "updated",
            "message": msg,
            "first_build": first_build,
            "version_local": local_str,
            "version_remote": remote_str,
        }
        return milestone, update_decision, True, remote_versions

    msg = "No update required: remote GitHub commit matches local baseline."
    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            **metrics,
            "manifest_path": str(manifest_path),
            "first_build": first_build,
            "local_version": local_str,
            "remote_version": remote_str,
            "update_required": False,
        },
    }

    update_decision = {
        "mode": "version_endpoint",
        "result": "latest_version_present",
        "message": msg,
        "first_build": first_build,
        "version_local": local_str,
        "version_remote": remote_str,
    }

    return milestone, update_decision, False, remote_versions


def install_pangolin_data(*, output_dir: Path, logger) -> Dict[str, Any]:
    started_at = get_timestamp()
    ok, rc, stdout, stderr = _run_cmd_capture(
        cmd=[
            "python3",
            "-m",
            "pip",
            "install",
            "--no-input",
            "--disable-pip-version-check",
            "--no-cache-dir",
            "--target",
            str(output_dir),
            "git+https://github.com/cov-lineages/pangolin-data.git",
        ],
        timeout_s=3600,
    )
    finished_at = get_timestamp()

    if not ok:
        return {
            "status": StatusType.FAILED.value,
            "message": f"pip install failed (rc={rc})",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": True,
            "metrics": {"rc": rc, "stdout_snippet": stdout, "stderr_snippet": stderr},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "pangolin-data installed successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {"rc": rc, "stdout_snippet": stdout, "stderr_snippet": stderr},
    }


def processing_status_dummy() -> Dict[str, Any]:
    started_at = get_timestamp()
    finished_at = get_timestamp()
    return {
        "status": StatusType.SKIPPED.value,
        "message": "No additional processing required for pangolin-data (pip install only).",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {},
    }


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default="/home/external_databases/pangolin", help="Output directory.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/pangolin",
) -> None:
    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting pangolin-data updater")

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}" or str(Path.cwd()),
        "user": user if user else getpass.getuser(),
        "host": host if host else socket.gethostname(),
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
        source=SOURCE,
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
    avail = composite_availability_check(
        ["https://api.github.com/", "https://github.com/cov-lineages/pangolin-data"],
        logger,
        retries=3,
        interval=10,
    )
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_versions = determine_update_status_from_github_sha(
        output_dir=out_dir,
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

    # 4) REMOTE_FILES_DOWNLOAD_STATUS (pip install)
    dl = install_pangolin_data(output_dir=out_dir, logger=logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to install pangolin-data.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = processing_status_dummy()
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=SOURCE["expected_processed_files"])
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    try:
        write_version_manifest(out_dir / "current_version.json", remote_versions)
    except Exception as e:
        logger.warning("Failed to write version manifest: %s", e)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

