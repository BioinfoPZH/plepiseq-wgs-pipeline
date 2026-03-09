#!/usr/bin/env python3

from __future__ import annotations

import getpass
import gzip
import json
import os
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.download_helpers import _download_file_with_retry
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check, file_md5sum
from utils.generic_helpers import backup_paths, restore_backups, remove_backup_files
from utils.validation import get_timestamp, verify_expected_files


DATABASE = {"name": "alphafold", "category": "protein structure prediction"}
SOURCE = {
    "source_type": "https",
    "reference": "UniProt UniRef/UniProt Knowledgebase",
    # Schema requires minItems=1; for AlphaFold (in this pipeline) the “raw” artifacts
    # are already in the final layout consumed by downstream code.
    "expected_raw_files": [
        "uniref50/uniref50_viral.fasta",
        "uniref50/uniref50.fasta",
        "uniprot/uniprot_sprot.fasta",
    ],
    "expected_processed_files": [
        "uniref50/uniref50_viral.fasta",
        "uniref50/uniref50.fasta",
        "uniprot/uniprot_sprot.fasta",
    ],
}

# Files that are required by AlphaFold in this pipeline but are not managed by this updater.
# Safeguard: refuse to run if output_dir does not contain these assets.
STATIC_REQUIRED_FILES = [
    "params/params_model_1.npz",
    "pdb70/pdb70_a3m.ffdata",
    "pdb_mmcif/obsolete.dat",
    "pdb_seqres/pdb_seqres.txt",
]


UNIREF50_VIRAL_URL = (
    "https://rest.uniprot.org/uniref/stream?format=fasta&query=%28%28taxonomy_id%3A10239%29+AND+%28identity%3A0.5%29%29"
)
UNIREF50_GZ_URL = "https://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref50/uniref50.fasta.gz"
UNIPROT_SPROT_GZ_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz"
)

MANIFEST_FILENAME = "current_version.json"


def _cleanup_paths(paths: List[Path], *, logger) -> None:
    for p in paths:
        try:
            if p.exists():
                logger.info("Removing partial/new file: %s", p)
                p.unlink()
        except Exception as e:
            logger.warning("Failed to remove %s: %s", p, e)



def _read_manifest(path: Path) -> Dict[str, Any]:
    """
    Read a JSON manifest. Returns {} if missing/invalid.
    """
    try:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _write_manifest(path: Path, data: Dict[str, Any]) -> None:
    """
    Atomically write JSON manifest.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _checksum_list_for_files(*, base_dir: Path, rel_files: List[str]) -> List[Dict[str, str]]:
    """
    Create schema-compatible checksum_list for update_decision.
    Only includes files that exist.
    """
    out: List[Dict[str, str]] = []
    for rel in rel_files:
        p = base_dir / rel
        if not p.exists():
            continue
        out.append({"file_name": rel, "checksum": file_md5sum(str(p))})
    return out


def _manifest_payload(*, base_dir: Path, rel_files: List[str], run_id: str) -> Dict[str, Any]:
    files: Dict[str, Any] = {}
    for rel in rel_files:
        p = base_dir / rel
        if not p.exists():
            continue
        files[rel] = {
            "md5": file_md5sum(str(p)),
            "bytes": int(p.stat().st_size),
        }
    return {
        "generated_at": get_timestamp(),
        "run_id": run_id,
        "files": files,
        "sources": {
            "uniref50_viral_stream": UNIREF50_VIRAL_URL,
            "uniref50_fasta_gz": UNIREF50_GZ_URL,
            "uniprot_sprot_fasta_gz": UNIPROT_SPROT_GZ_URL,
        },
    }


def _gunzip_atomic(*, gz_path: Path, out_path: Path, logger) -> Dict[str, Any]:
    """
    Decompress gz_path to out_path using a temp file + atomic replace.
    Returns metrics dict.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    bytes_written = 0
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass

    with gzip.open(gz_path, "rb") as fin, open(tmp, "wb") as fout:
        while True:
            chunk = fin.read(1024 * 1024)
            if not chunk:
                break
            fout.write(chunk)
            bytes_written += len(chunk)

    os.replace(tmp, out_path)
    return {"gz_path": str(gz_path), "out_path": str(out_path), "bytes_written": bytes_written}


def download_raw_files(
    *,
    out_dir: Path,
    logger,
    max_retries: int = 3,
    interval_s: int = 300,
) -> Tuple[Dict[str, Any], int]:
    """
    Download raw artifacts (viral FASTA + 2 gz files). Returns (milestone, attempts_used_max).
    """
    started_at = get_timestamp()

    uniref_dir = out_dir / "uniref50"
    uniprot_dir = out_dir / "uniprot"
    uniref_dir.mkdir(parents=True, exist_ok=True)
    uniprot_dir.mkdir(parents=True, exist_ok=True)

    viral_fasta = uniref_dir / "uniref50_viral.fasta"
    uniref_gz = uniref_dir / "uniref50.fasta.gz"
    sprot_gz = uniprot_dir / "uniprot_sprot.fasta.gz"

    attempts_used_max = 1

    ok, a = _download_file_with_retry(
        url=UNIREF50_VIRAL_URL,
        output_path=viral_fasta,
        logger=logger,
        max_retries=max_retries,
        wait_seconds=interval_s,
        timeout_s=120,
    )
    attempts_used_max = max(attempts_used_max, a)
    if not ok:
        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download UniRef50 viral stream FASTA",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts_used_max,
                "retryable": True,
                "metrics": {"url": UNIREF50_VIRAL_URL, "dest": str(viral_fasta)},
            },
            attempts_used_max,
        )

    ok, a = _download_file_with_retry(
        url=UNIREF50_GZ_URL,
        output_path=uniref_gz,
        logger=logger,
        max_retries=max_retries,
        wait_seconds=interval_s,
        timeout_s=120,
    )
    attempts_used_max = max(attempts_used_max, a)
    if not ok:
        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download UniRef50 FASTA gzip",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts_used_max,
                "retryable": True,
                "metrics": {"url": UNIREF50_GZ_URL, "dest": str(uniref_gz)},
            },
            attempts_used_max,
        )

    ok, a = _download_file_with_retry(
        url=UNIPROT_SPROT_GZ_URL,
        output_path=sprot_gz,
        logger=logger,
        max_retries=max_retries,
        wait_seconds=interval_s,
        timeout_s=120,
    )
    attempts_used_max = max(attempts_used_max, a)
    if not ok:
        finished_at = get_timestamp()
        return (
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download UniProt SwissProt FASTA gzip",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts_used_max,
                "retryable": True,
                "metrics": {"url": UNIPROT_SPROT_GZ_URL, "dest": str(sprot_gz)},
            },
            attempts_used_max,
        )

    finished_at = get_timestamp()
    return (
        {
            "status": StatusType.PASSED.value,
            "message": "All raw files downloaded successfully",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {
                "files": [
                    {"url": UNIREF50_VIRAL_URL, "dest": str(viral_fasta)},
                    {"url": UNIREF50_GZ_URL, "dest": str(uniref_gz)},
                    {"url": UNIPROT_SPROT_GZ_URL, "dest": str(sprot_gz)},
                ]
            },
        },
        attempts_used_max,
    )


def process_downloads(*, out_dir: Path, logger) -> Dict[str, Any]:
    """
    Decompress gz files into final fasta layout and remove gz files.
    """
    started_at = get_timestamp()

    uniref_dir = out_dir / "uniref50"
    uniprot_dir = out_dir / "uniprot"

    uniref_gz = uniref_dir / "uniref50.fasta.gz"
    sprot_gz = uniprot_dir / "uniprot_sprot.fasta.gz"

    uniref_fa = uniref_dir / "uniref50.fasta"
    sprot_fa = uniprot_dir / "uniprot_sprot.fasta"

    metrics: Dict[str, Any] = {"gunzip": []}

    try:
        if not uniref_gz.exists() or not sprot_gz.exists():
            finished_at = get_timestamp()
            return {
                "status": StatusType.FAILED.value,
                "message": "Missing expected gz files for decompression",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": False,
                "metrics": {
                    "missing": [str(p) for p in [uniref_gz, sprot_gz] if not p.exists()],
                },
            }

        m1 = _gunzip_atomic(gz_path=uniref_gz, out_path=uniref_fa, logger=logger)
        metrics["gunzip"].append(m1)
        try:
            uniref_gz.unlink()
        except Exception:
            pass

        m2 = _gunzip_atomic(gz_path=sprot_gz, out_path=sprot_fa, logger=logger)
        metrics["gunzip"].append(m2)
        try:
            sprot_gz.unlink()
        except Exception:
            pass

        finished_at = get_timestamp()
        return {
            "status": StatusType.PASSED.value,
            "message": "Decompression completed successfully",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": metrics,
        }
    except Exception as e:
        finished_at = get_timestamp()
        return {
            "status": StatusType.FAILED.value,
            "message": f"Decompression failed: {e}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": metrics,
        }


@click.command()
@click.option("--workspace", default="/home/update", show_default=True, help="Updater workspace for report metadata.")
@click.option("--run_id", default=None, help="Run id (default: auto-generated).")
@click.option("--container_image", default=None, help="Container image string for report metadata.")
@click.option("--user", default=None, help="User string for report metadata.")
@click.option("--host", default=None, help="Host string for report metadata.")
@click.option("--output_dir", required=True, type=click.Path(path_type=Path), help="AlphaFold database root directory.")
@click.option("--report_file", default=None, help="Report filename (default: <run_id>.json).")
@click.option("--log_file", default="alphafold.log", show_default=True, help="Log filename.")
def main(
    workspace: str,
    run_id: Optional[str],
    container_image: Optional[str],
    user: Optional[str],
    host: Optional[str],
    output_dir: Path,
    report_file: Optional[str],
    log_file: str,
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
    logger.info("Starting AlphaFold DB updater (selective files only)")

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}",
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    if report_file is None:
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

    manifest_path = out_dir / MANIFEST_FILENAME
    manifest_before = _read_manifest(manifest_path)
    first_build = not bool(manifest_before)
    checksums_before = _checksum_list_for_files(base_dir=out_dir, rel_files=list(SOURCE["expected_processed_files"]))

    # 1) PREFLIGHT_CONNECTIVITY (general internet)
    pre = check_url_available("https://www.google.com", retries=3, interval=30, logger=logger)
    rb.add_named_milestone("PREFLIGHT_CONNECTIVITY", pre)
    remaining_steps.remove("PREFLIGHT_CONNECTIVITY")
    if pre["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed preflight connectivity.")
        rb.fail(code="NO_INTERNET", message=pre.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 2) DATABASE_AVAILABILITY (UniProt endpoints)
    avail = composite_availability_check(
        urls=[UNIREF50_VIRAL_URL, UNIREF50_GZ_URL, UNIPROT_SPROT_GZ_URL],
        logger=logger,
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

    # 3) UPDATE_STATUS (always update; do not wipe unrelated AlphaFold assets)
    started_at = get_timestamp()
    upd = {
        "status": StatusType.PASSED.value,
        "message": "Always update: UniRef/UniProt FASTA artifacts are regenerated/retrieved each run.",
        "started_at": started_at,
        "finished_at": None,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "update_required": True,
            "manifest_path": str(manifest_path),
            "first_build": first_build,
        },
    }

    # Safeguard: ensure we are operating on a directory that looks like a real AlphaFold DB root.
    static_check = verify_expected_files(base_dir=out_dir, expected_files=list(STATIC_REQUIRED_FILES))
    upd["metrics"]["static_required_files"] = static_check.get("metrics", {})
    if static_check["status"] != StatusType.PASSED.value:
        upd["status"] = StatusType.FAILED.value
        upd["message"] = (
            "Refusing to update: output_dir is missing required AlphaFold assets that this updater does not manage. "
            "Ensure the AlphaFold DB install is complete, then re-run."
        )
        upd["finished_at"] = get_timestamp()
        rb.add_named_milestone("UPDATE_STATUS", upd)
        remaining_steps.remove("UPDATE_STATUS")
        rb.set_update_decision(
            mode="always_rebuild",
            result="error",
            message=upd["message"],
            first_build=first_build,
            checksums_before=checksums_before,
        )
        skip_remaining_steps(remaining_steps, "Skipped: failed AlphaFold DB layout safeguard.")
        rb.fail(code="ALPHAFOLD_STATIC_ASSETS_MISSING", message=static_check.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    upd["finished_at"] = get_timestamp()
    rb.add_named_milestone("UPDATE_STATUS", upd)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(
        mode="always_rebuild",
        result="updated",
        message=f"{upd['message']} Manifest is persisted at: {MANIFEST_FILENAME}",
        first_build=first_build,
        checksums_before=checksums_before,
    )

    # Selective backup for managed files only (never delete other /alphafold assets)
    managed_targets = [
        out_dir / "uniref50" / "uniref50_viral.fasta",
        out_dir / "uniref50" / "uniref50.fasta.gz",
        out_dir / "uniref50" / "uniref50.fasta",
        out_dir / "uniprot" / "uniprot_sprot.fasta.gz",
        out_dir / "uniprot" / "uniprot_sprot.fasta",
    ]
    backups: List[Tuple[Path, Path]] = backup_paths(managed_targets, logger)

    # 4) REMOTE_FILES_DOWNLOAD_STATUS
    dl, _attempts = download_raw_files(out_dir=out_dir, logger=logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        _cleanup_paths(managed_targets, logger=logger)
        restore_backups(backups, logger)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS
    proc = process_downloads(out_dir=out_dir, logger=logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc.get("message", ""), retry_recommended=False)
        _cleanup_paths(managed_targets, logger=logger)
        restore_backups(backups, logger)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=SOURCE["expected_processed_files"])
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        _cleanup_paths(managed_targets, logger=logger)
        restore_backups(backups, logger)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist manifest + attach after-checksums to update_decision (do not remove manifest on future updates)
    checksums_after = _checksum_list_for_files(base_dir=out_dir, rel_files=list(SOURCE["expected_processed_files"]))
    rb.set_update_decision(
        mode="always_rebuild",
        result="updated",
        message=f"{upd['message']} Manifest is persisted at: {MANIFEST_FILENAME}",
        first_build=first_build,
        checksums_before=checksums_before,
        checksums_after=checksums_after,
    )
    try:
        _write_manifest(
            manifest_path,
            _manifest_payload(base_dir=out_dir, rel_files=list(SOURCE["expected_processed_files"]), run_id=run_id),
        )
    except Exception as e:
        logger.warning("Failed to write manifest %s: %s", manifest_path, e)

    # Success: remove backups
    remove_backup_files(backups, logger)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

