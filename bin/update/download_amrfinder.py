#!/usr/bin/env python3

from __future__ import annotations

import getpass
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.blast_helpers import index_if_fasta
from utils.ftp_helpers import ftp_connect, ftp_list_regular_files, ftp_download_file_atomic, ftp_read_text
from utils.generic_helpers import remove_old_workspace
from utils.net import StatusType, check_url_available, check_ftp_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.validation import get_timestamp, verify_expected_files
from utils.version_manifest import read_version_manifest, write_version_manifest


FTP_HOST = "ftp.ncbi.nlm.nih.gov"
FTP_DIR = "/pathogen/Antimicrobial_resistance/AMRFinderPlus/database/latest"
VERSION_FILE = "version.txt"

REQUIRED_FILES = [
    "AMR.LIB",
    "AMR_CDS.fa",
    "database_format_version.txt",
]

DATABASE = {"name": "amrfinder_plus", "category": "antimicrobial resistance"}
SOURCE = {
    "source_type": "ftp",
    "reference": f"ftp://{FTP_HOST}{FTP_DIR}",
    "expected_raw_files": [VERSION_FILE],
    "expected_processed_files": list(REQUIRED_FILES),
}


def _get_remote_version(*, logger) -> Tuple[Optional[str], Dict[str, Any]]:
    ftp = None
    try:
        ftp = ftp_connect(host=FTP_HOST, directory=FTP_DIR, timeout_s=60, logger=logger, retries=3, interval=60)
        remote_version = ftp_read_text(ftp, VERSION_FILE, logger=logger, retries=3, interval=60)
        metrics = {"host": FTP_HOST, "dir": FTP_DIR, "version_file": VERSION_FILE}
        return remote_version.strip(), metrics
    finally:
        try:
            if ftp is not None:
                ftp.quit()
        except Exception:
            pass


def determine_update_status_from_ftp_version(
    *,
    output_dir: Path,
    logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    """
    Decide update based on FTP version.txt, persisted in current_version.json.
    """
    started_at = get_timestamp()

    manifest_path = output_dir / "current_version.json"
    local_versions = read_version_manifest(manifest_path)
    first_build = not bool(local_versions)

    # Fetch remote version
    try:
        remote_version, metrics = _get_remote_version(logger=logger)
        if not remote_version:
            raise RuntimeError("Empty remote version.txt")
        remote_versions = {"AMRFinderPlus": remote_version}
    except Exception as e:
        finished_at = get_timestamp()
        msg = f"Failed to fetch remote version.txt: {e}"
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": True,
                "metrics": {"ftp": {"host": FTP_HOST, "dir": FTP_DIR, "error": str(e)}},
            },
            {"mode": "version_endpoint", "result": "error", "message": msg},
            False,
            {},
        )

    local_version = local_versions.get("AMRFinderPlus", "").strip() if local_versions else ""
    finished_at = get_timestamp()

    # Require both: version match + required files present to skip
    required_present = all((output_dir / f).exists() for f in REQUIRED_FILES)
    update_required = (local_version != remote_version) or (not required_present)

    if update_required:
        msg = (
            "No local version baseline: treating as first build."
            if first_build
            else ("Update required: remote version differs from local baseline." if local_version != remote_version else "Update required: required files missing locally.")
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
                "manifest_path": str(manifest_path),
                "first_build": first_build,
                "version_local": local_version,
                "version_remote": remote_version,
                "required_files_present": required_present,
                "ftp": metrics,
                "update_required": True,
            },
        }
        update_decision = {
            "mode": "version_endpoint",
            "result": "updated",
            "message": msg,
            "first_build": first_build,
            "version_local": local_version,
            "version_remote": remote_version,
        }
        return milestone, update_decision, True, remote_versions

    msg = "No update required: remote version matches local baseline and required files present."
    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "manifest_path": str(manifest_path),
            "first_build": first_build,
            "version_local": local_version,
            "version_remote": remote_version,
            "required_files_present": True,
            "ftp": metrics,
            "update_required": False,
        },
    }
    update_decision = {
        "mode": "version_endpoint",
        "result": "latest_version_present",
        "message": msg,
        "first_build": first_build,
        "version_local": local_version,
        "version_remote": remote_version,
    }
    return milestone, update_decision, False, remote_versions


def download_amrfinder_ftp(*, output_dir: Path, logger) -> Tuple[Dict[str, Any], List[Path]]:
    started_at = get_timestamp()
    ftp = None
    downloaded: List[Path] = []
    try:
        ftp = ftp_connect(host=FTP_HOST, directory=FTP_DIR, timeout_s=60, logger=logger, retries=3, interval=60)
        names = ftp_list_regular_files(ftp, logger=logger, retries=3, interval=60)
        if not names:
            finished_at = get_timestamp()
            return (
                {
                    "status": StatusType.FAILED.value,
                    "message": "Could not list files on FTP server",
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "attempts": 1,
                    "retryable": True,
                    "metrics": {"host": FTP_HOST, "dir": FTP_DIR},
                },
                downloaded,
            )

        bytes_total = 0
        for name in names:
            dest = output_dir / Path(name).name
            logger.info("Downloading: %s", name)
            m = ftp_download_file_atomic(ftp, name, dest, logger=logger, retries=3, interval=60)
            bytes_total += int(m.get("bytes_written", 0) or 0)
            downloaded.append(dest)

        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.PASSED.value,
                "message": "All files downloaded successfully",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": True,
                "metrics": {"files_total": len(names), "bytes_total": bytes_total},
            },
            downloaded,
        )
    except Exception as e:
        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.FAILED.value,
                "message": f"FTP download failed: {e}",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": True,
                "metrics": {"host": FTP_HOST, "dir": FTP_DIR},
            },
            downloaded,
        )
    finally:
        try:
            if ftp is not None:
                ftp.quit()
        except Exception:
            pass


def processing_makeblastdb(*, files: List[Path], logger) -> Dict[str, Any]:
    started_at = get_timestamp()

    indexed = 0
    skipped = 0
    failed: List[str] = []
    metrics: Dict[str, Any] = {"indexed": [], "skipped": [], "failed": []}

    for p in files:
        ok, m = index_if_fasta(p, logger=logger)
        if m.get("skipped"):
            skipped += 1
            metrics["skipped"].append(m)
            continue
        if ok:
            indexed += 1
            metrics["indexed"].append(m)
        else:
            failed.append(str(p))
            metrics["failed"].append(m)

    finished_at = get_timestamp()
    if failed:
        return {
            "status": StatusType.FAILED.value,
            "message": f"makeblastdb failed for {len(failed)} file(s)",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {"indexed_count": indexed, "skipped_count": skipped, "failed_count": len(failed), **metrics},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "BLAST indexing completed successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {"indexed_count": indexed, "skipped_count": skipped},
    }


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default="/home/external_databases/amrfinder_plus", help="Output directory.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/amrfinder_plus",
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
    logger.info("Starting AMRFinderPlus DB updater")

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

    # 2) DATABASE_AVAILABILITY (FTP)
    avail = check_ftp_available(FTP_HOST, FTP_DIR, retries=3, interval=60, timeout_s=60, logger=logger)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_versions = determine_update_status_from_ftp_version(
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

    # 4) REMOTE_FILES_DOWNLOAD_STATUS
    dl, downloaded_files = download_amrfinder_ftp(output_dir=out_dir, logger=logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = processing_makeblastdb(files=downloaded_files, logger=logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=REQUIRED_FILES)
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist new baseline only after successful final validation
    try:
        write_version_manifest(out_dir / "current_version.json", remote_versions)
    except Exception as e:
        logger.warning("Failed to write version manifest: %s", e)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()
