#!/usr/bin/env python3

from __future__ import annotations

import getpass
import gzip
import os
import shutil
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.download_helpers import _download_file_with_retry
from utils.generic_helpers import backup_paths, remove_backup_files, restore_backups
from utils.github_helpers import build_version_string, get_github_head_sha
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check
from utils.validation import get_timestamp, verify_expected_files
from utils.version_manifest import read_version_manifest, write_version_manifest


DATABASE = {"name": "freyja", "category": "viral barcodes"}
SOURCE = {
    "source_type": "https",
    "reference": "https://github.com/andersen-lab/Freyja-data",
    # expected_raw_files: only used to populate the JSON report metadata;
    # not validated against the filesystem. Schema requires minItems=1.
    # usher_barcodes is fetched as a gzipped artifact and decompressed during PROCESSING.
    "expected_raw_files": [
        "sarscov2/lineages.yml",
        "sarscov2/curated_lineages.json",
        "sarscov2/usher_barcodes.csv.gz",
        "H1N1/barcode.csv",
        "FLU-B-VIC/barcode.csv",
    ],
    # expected_processed_files: used by verify_expected_files() in FINAL_STATUS
    # to confirm the database was successfully downloaded.
    "expected_processed_files": [
        "sarscov2/lineages.yml",
        "sarscov2/curated_lineages.json",
        "sarscov2/usher_barcodes.csv",
        "H1N1/barcode.csv",
        "H1N1/reference.fasta",
        "H1N1/auspice_tree.json",
        "H3N2/barcode.csv",
        "H3N2/reference.fasta",
        "H3N2/auspice_tree.json",
        "H5Nx/barcode.csv",
        "H5Nx/reference.fasta",
        "H5Nx/auspice_tree.json",
        "Victoria/barcode.csv",
        "Victoria/reference.fasta",
        "Victoria/auspice_tree.json",
        "RSV_A/barcode.csv",
        "RSV_A/reference.fasta",
        "RSV_A/auspice_tree.json",
        "RSV_B/barcode.csv",
        "RSV_B/reference.fasta",
        "RSV_B/auspice_tree.json",
    ],
}


FREYJA_DATA_RAW_BASE = "https://raw.githubusercontent.com/andersen-lab/Freyja-data/main"
FREYJA_BARCODES_RAW_BASE = "https://raw.githubusercontent.com/andersen-lab/Freyja-barcodes/main"

# Upstream artifact name for the SARS-CoV-2 UShER barcodes. The repo used to ship a
# Git-LFS-tracked "usher_barcodes.csv" (fetched via media.githubusercontent.com); it now
# ships a plain gzipped "usher_barcodes.csv.gz" committed directly (served from raw.*).
USHER_BARCODES_GZ = "usher_barcodes.csv.gz"
USHER_BARCODES_CSV = "usher_barcodes.csv"


def _gunzip_file(src: Path, dest: Path) -> None:
    """Decompress a .gz file to dest (streaming, suitable for large files)."""
    with gzip.open(src, "rb") as f_in, open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def _cleanup_staging(staging_dir: Path, logger) -> None:
    """Best-effort removal of the temporary staging directory."""
    if staging_dir.exists():
        try:
            shutil.rmtree(staging_dir, ignore_errors=True)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Failed to clean staging dir %s: %s", staging_dir, e)


def determine_update_status_from_github_commits(
    *,
    output_dir: Path,
    logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    """
    Update decision for Freyja based on GitHub commit SHAs.

    Returns:
      milestone,
      update_decision kwargs for ReportBuilder.set_update_decision,
      update_required,
      remote_versions mapping
    """
    started_at = get_timestamp()

    manifest_path = output_dir / "current_version.json"
    local_versions = read_version_manifest(manifest_path)
    first_build = not bool(local_versions)

    # Fetch remote SHAs
    remote_versions: Dict[str, str] = {}
    metrics: Dict[str, Any] = {"github": {}}

    try:
        sha_data, m_data = get_github_head_sha(owner="andersen-lab", repo="Freyja-data", ref="main", logger=logger)
        sha_barcodes, m_barcodes = get_github_head_sha(owner="andersen-lab", repo="Freyja-barcodes", ref="main", logger=logger)
        remote_versions = {
            "Freyja-data": sha_data,
            "Freyja-barcodes": sha_barcodes,
        }
        metrics["github"]["Freyja-data"] = m_data
        metrics["github"]["Freyja-barcodes"] = m_barcodes
    except Exception as e:
        finished_at = get_timestamp()
        msg = f"Failed to fetch remote version SHAs: {e}"
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
        msg = "Update required: remote GitHub commit differs from local baseline." if not first_build else "No local version baseline: treating as first build."

        # NOTE: we intentionally do NOT wipe the existing workspace here. New files are
        # downloaded into a temporary staging dir and only promoted into place after a
        # successful download + processing, with the previous files backed up first.
        # This keeps the last good database intact if an upstream change breaks a download.

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


def download_freyja_files(staging_dir: Path, logger) -> Dict[str, Any]:
    """Download all Freyja raw files into a temporary staging directory."""
    started_at = get_timestamp()

    # Map of remote dir -> local dir for Flu/RSV
    species_map = {
        "H1N1": "H1N1",
        "H3N2": "H3N2",
        "H5Nx": "H5Nx",
        "FLU-B-VIC": "Victoria",
        "RSVa": "RSV_A",
        "RSVb": "RSV_B",
    }

    files: List[Tuple[str, Path]] = []

    # SARS-CoV-2 files from Freyja-data. usher_barcodes is shipped as a plain (non-LFS)
    # gzipped file committed to the repo, so it is fetched from raw.githubusercontent.com
    # and decompressed later in PROCESSING_STATUS.
    sars_dir = staging_dir / "sarscov2"
    files.extend(
        [
            (f"{FREYJA_DATA_RAW_BASE}/lineages.yml", sars_dir / "lineages.yml"),
            (f"{FREYJA_DATA_RAW_BASE}/curated_lineages.json", sars_dir / "curated_lineages.json"),
            (f"{FREYJA_DATA_RAW_BASE}/{USHER_BARCODES_GZ}", sars_dir / USHER_BARCODES_GZ),
        ]
    )

    # Barcode sets from Freyja-barcodes
    for remote_species, local_species in species_map.items():
        base = f"{FREYJA_BARCODES_RAW_BASE}/{remote_species}/latest"
        local_dir = staging_dir / local_species
        files.extend(
            [
                (f"{base}/barcode.csv", local_dir / "barcode.csv"),
                (f"{base}/reference.fasta", local_dir / "reference.fasta"),
                (f"{base}/auspice_tree.json", local_dir / "auspice_tree.json"),
            ]
        )

    attempts_used_max = 1
    failed_url: Optional[str] = None

    for url, dest in files:
        ok, attempts = _download_file_with_retry(
            url=url,
            output_path=dest,
            logger=logger,
            max_retries=3,
            wait_seconds=30,
            timeout_s=120,
        )
        attempts_used_max = max(attempts_used_max, attempts)
        if not ok:
            failed_url = url
            break

    finished_at = get_timestamp()

    if failed_url is not None:
        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download file from: {failed_url}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {"files_total": len(files), "failed_url": failed_url},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "All Freyja files downloaded successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": attempts_used_max,
        "retryable": True,
        "metrics": {"files_total": len(files), "files_downloaded": len(files)},
    }


def process_and_promote_freyja_files(
    *,
    staging_dir: Path,
    out_dir: Path,
    expected_files: List[str],
    logger,
) -> Tuple[Dict[str, Any], List[Tuple[Path, Path]]]:
    """
    Post-download processing for Freyja.

    Steps:
      1. Decompress the gzipped UShER barcodes into the staging dir.
      2. Verify the staged set contains every expected processed file.
      3. Back up the existing live files and promote the staged files into out_dir.

    On any error the live files are restored from their backups so the previous good
    database is preserved. Returns (milestone, backups). The caller keeps the backups to
    either remove them after FINAL_STATUS passes or restore them if it fails.
    """
    started_at = get_timestamp()
    backups: List[Tuple[Path, Path]] = []

    try:
        # 1) Decompress usher_barcodes.csv.gz -> usher_barcodes.csv (in staging)
        gz_path = staging_dir / "sarscov2" / USHER_BARCODES_GZ
        csv_path = staging_dir / "sarscov2" / USHER_BARCODES_CSV
        if not gz_path.exists():
            raise FileNotFoundError(f"Expected gzipped barcodes not found: {gz_path}")
        logger.info("Decompressing %s -> %s", gz_path.name, csv_path.name)
        _gunzip_file(gz_path, csv_path)
        gz_path.unlink(missing_ok=True)

        # 2) Verify the staged set is complete before touching the live data
        staged_check = verify_expected_files(base_dir=staging_dir, expected_files=expected_files)
        if staged_check["status"] != StatusType.PASSED.value:
            raise RuntimeError(f"Staged files incomplete: {staged_check.get('message', '')}")

        # 3) Back up existing live files, then promote staged files into place
        targets = [out_dir / rel for rel in expected_files]
        backups = backup_paths(targets, logger)
        for rel in expected_files:
            src = staging_dir / rel
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dest)
    except Exception as e:
        restore_backups(backups, logger)
        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.FAILED.value,
                "message": f"Failed to process/promote Freyja files (restored backups): {e}",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": False,
                "metrics": {"staging_dir": str(staging_dir), "backup_count": len(backups)},
            },
            backups,
        )

    finished_at = get_timestamp()
    return (
        {
            "status": StatusType.PASSED.value,
            "message": "Decompressed barcodes and promoted Freyja files into place.",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {"files_promoted": len(expected_files), "backup_count": len(backups)},
        },
        backups,
    )


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default=str(Path.cwd() / "freyja"), help="Output directory.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/freyja",
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
    logger.info("Starting Freyja DB updater")

    execution_context = {
        "workspace": f"{workspace}/freyja",
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
        log_file=f"{workspace}/freyja/reports/{report_file}",
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
    urls_to_check = [
        "https://api.github.com/",
        f"{FREYJA_DATA_RAW_BASE}/lineages.yml",
        f"{FREYJA_DATA_RAW_BASE}/{USHER_BARCODES_GZ}",
        f"{FREYJA_BARCODES_RAW_BASE}/H1N1/latest/barcode.csv",
    ]
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
    upd_milestone, update_decision, update_required, remote_versions = determine_update_status_from_github_commits(
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
    # Download into an isolated staging directory; the live workspace is left untouched
    # until processing succeeds and the new files are promoted into place.
    staging_dir = out_dir / f".tmp_freyja_{run_id}"
    _cleanup_staging(staging_dir, logger)
    staging_dir.mkdir(parents=True, exist_ok=True)

    dl = download_freyja_files(staging_dir, logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        _cleanup_staging(staging_dir, logger)
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=dl.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS (decompress barcodes, back up existing files, promote staged files)
    expected = list(SOURCE["expected_processed_files"])
    proc, backups = process_and_promote_freyja_files(
        staging_dir=staging_dir,
        out_dir=out_dir,
        expected_files=expected,
        logger=logger,
    )
    _cleanup_staging(staging_dir, logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=expected)
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        restore_backups(backups, logger)
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist new baseline *only after* successful final validation
    try:
        write_version_manifest(out_dir / "current_version.json", remote_versions)
    except Exception as e:
        logger.warning("Failed to write version manifest: %s", e)

    # Update succeeded end-to-end: drop the *.old backups of the previous version.
    remove_backup_files(backups, logger)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

