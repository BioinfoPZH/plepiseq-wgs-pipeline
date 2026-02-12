#!/usr/bin/env python3
from __future__ import annotations

import getpass
import json
import shutil
import socket
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import requests

from utils.download_helpers import _download_file_with_retry
from utils.net import HEADERS as NET_HEADERS
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check, file_md5sum
from utils.validation import get_timestamp

DATABASE = {"name": "speciesfinder", "category": "genomicepidemiology"}

SPECIESFINDER_URL = "https://download.genepi.dk/speciesfinder_db.tar.gz"
TARBALL_NAME = "speciesfinder_db.tar.gz"
MD5_MANIFEST_NAME = "speciesfinder_md5.json"
AVAILABILITY_HOST = "https://genepi.dk"

EXPECTED_TOP_LEVEL_DIRS = ("archaea", "bacteria", "eukaryotes", "virus")

SOURCE = {
    "source_type": "https",
    "reference": SPECIESFINDER_URL,
    "expected_raw_files": [TARBALL_NAME],
    # Note: filenames inside each subdir are dynamic; FINAL_STATUS performs structural validation.
    "expected_processed_files": [*EXPECTED_TOP_LEVEL_DIRS, "README.md"],
}


def _milestone_failed(*, step: str, message: str, started_at: str, metrics: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "status": StatusType.FAILED.value,
        "message": message,
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {"step": step, **(metrics or {})},
    }


def _safe_extract_tarball(*, tar_path: Path, dest_dir: Path) -> None:
    """
    Extract tarball with basic path traversal protection.
    """
    dest_dir = dest_dir.resolve()
    with tarfile.open(tar_path, "r:gz") as tf:
        members = tf.getmembers()
        for m in members:
            # Prevent absolute paths and .. traversal
            target = (dest_dir / m.name).resolve()
            if not str(target).startswith(str(dest_dir) + "/") and target != dest_dir:
                raise RuntimeError(f"Unsafe tar member path detected: {m.name}")
        tf.extractall(path=dest_dir, members=members)

def _head_content_length(*, url: str, timeout_s: int = 30) -> tuple[Optional[int], Dict[str, Any]]:
    """
    Best-effort HEAD request to obtain Content-Length (bytes).
    """
    metrics: Dict[str, Any] = {"url": url}
    try:
        r = requests.head(url, headers=NET_HEADERS, timeout=timeout_s, allow_redirects=True)
        metrics["status_code"] = r.status_code
        cl = r.headers.get("Content-Length")
        metrics["content_length_header"] = cl
        metrics["etag"] = r.headers.get("ETag")
        metrics["last_modified"] = r.headers.get("Last-Modified")
        if cl is None:
            return None, metrics
        try:
            return int(cl), metrics
        except Exception:
            return None, metrics
    except Exception as e:
        metrics["error"] = str(e)
        return None, metrics


def _find_extracted_root(*, staging_dir: Path) -> Path:
    """
    The tarball may either unpack directly into staging_dir, or into a single top-level folder.
    Return the directory that contains the expected layout.
    """
    if all((staging_dir / d).is_dir() for d in EXPECTED_TOP_LEVEL_DIRS):
        return staging_dir

    children = [p for p in staging_dir.iterdir() if p.is_dir() and not p.name.startswith(".")]
    if len(children) == 1:
        candidate = children[0]
        if all((candidate / d).is_dir() for d in EXPECTED_TOP_LEVEL_DIRS):
            return candidate

    return staging_dir


def _remove_path(p: Path) -> None:
    if not p.exists():
        return
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
    else:
        p.unlink(missing_ok=True)


def _swap_in_layout(*, src_root: Path, dest_root: Path, run_id: str, logger) -> None:
    """
    Swap in extracted layout without deleting the old one up-front.
    On any failure, restore the old layout.
    """
    # Only swap items we expect at top-level (dynamic internal filenames stay under these dirs)
    items = list(EXPECTED_TOP_LEVEL_DIRS) + ["README.md"]

    backups: List[tuple[Path, Path]] = []  # (backup_path, original_path)
    moved_new: List[Path] = []

    try:
        # 1) Move old items to backups
        for name in items:
            dst = dest_root / name
            if not dst.exists():
                continue
            backup = dest_root / f".old_{run_id}_{name}"
            _remove_path(backup)
            dst.rename(backup)
            backups.append((backup, dst))

        # 2) Move new items into place
        for name in items:
            src = src_root / name
            if not src.exists():
                continue
            dst = dest_root / name
            _remove_path(dst)
            src.rename(dst)
            moved_new.append(dst)

        # 3) Cleanup backups after success
        for backup, _orig in backups:
            _remove_path(backup)

    except Exception as e:
        logger.warning("Swap-in failed: %s. Rolling back to previous layout.", e)
        # remove any new items that were moved in
        for dst in reversed(moved_new):
            _remove_path(dst)
        # restore backups
        for backup, orig in reversed(backups):
            if backup.exists():
                _remove_path(orig)
                backup.rename(orig)
        raise


def _validate_speciesfinder_layout(*, base_dir: Path) -> Dict[str, Any]:
    started_at = get_timestamp()

    metrics: Dict[str, Any] = {"base_dir": str(base_dir)}
    missing_top: List[str] = []

    for d in EXPECTED_TOP_LEVEL_DIRS:
        if not (base_dir / d).is_dir():
            missing_top.append(d)

    readme_present = (base_dir / "README.md").exists()
    metrics["readme_present"] = readme_present
    metrics["missing_top_level_dirs"] = missing_top

    if missing_top:
        return {
            "status": StatusType.FAILED.value,
            "message": f"Missing expected top-level directories: {', '.join(missing_top)}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": metrics,
        }

    # Per-subdir checks: require at least one *.tax and one *.name; also check common KMA binary artifacts.
    per_dir: Dict[str, Any] = {}
    for d in EXPECTED_TOP_LEVEL_DIRS:
        p = base_dir / d
        counts = {
            "tax": len(list(p.glob("*.tax"))),
            "name": len(list(p.glob("*.name"))),
            "seq_b": len(list(p.glob("*.seq.b"))),
            "comp_b": len(list(p.glob("*.comp.b"))),
            "length_b": len(list(p.glob("*.length.b"))),
        }
        per_dir[d] = counts

    metrics["per_dir_counts"] = per_dir

    problems: List[str] = []
    for d, c in per_dir.items():
        if c["tax"] < 1:
            problems.append(f"{d}: missing *.tax")
        if c["name"] < 1:
            problems.append(f"{d}: missing *.name")
        # Not strictly required by your note, but consistent with current layout and useful validation.
        for k in ("seq_b", "comp_b", "length_b"):
            if c[k] < 1:
                problems.append(f"{d}: missing *.{k.replace('_', '.')} (expected KMA binary component)")

    finished_at = get_timestamp()
    if problems:
        return {
            "status": StatusType.FAILED.value,
            "message": "SpeciesFinder DB layout validation failed.",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {**metrics, "problems": problems},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "SpeciesFinder DB layout looks valid.",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": metrics,
    }


@click.command()
@click.option("--workspace", type=str, help="Workspace path (used in report metadata).", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default="/home/external_databases/speciesfinder", help="Output directory.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/speciesfinder",
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
    logger.info("Starting SpeciesFinder DB updater")

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
    # Some environments cannot resolve the download subdomain; check the main host for availability.
    avail = composite_availability_check([AVAILABILITY_HOST], logger, retries=3, interval=10)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS (always rebuild)
    step = "UPDATE_STATUS"
    started_at = get_timestamp()
    manifest_path = out_dir / MD5_MANIFEST_NAME
    first_build = not manifest_path.exists()
    msg = "Update decision based on checksum manifest of the raw tarball."
    md5_before: Optional[str] = None
    if manifest_path.exists():
        try:
            old = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
            md5_before = old.get("md5")
        except Exception:
            md5_before = None

    # Do NOT wipe existing extracted DB here. We only replace it after a fully downloaded and validated tarball.
    upd_milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "first_build": first_build,
            "manifest_path": str(manifest_path),
            "md5_before_present": bool(md5_before),
        },
    }

    rb.set_update_decision(
        mode="checksum_manifest",
        result="error",
        message="Pending checksum comparison (download not attempted yet).",
        first_build=first_build,
        checksums_before=([{"file_name": TARBALL_NAME, "checksum": md5_before}] if md5_before else []),
        checksums_after=[],
    )

    rb.add_named_milestone(step, upd_milestone)
    remaining_steps.remove(step)

    # 4) REMOTE_FILES_DOWNLOAD_STATUS
    step = "REMOTE_FILES_DOWNLOAD_STATUS"
    started_at = get_timestamp()
    tar_path = out_dir / f"{TARBALL_NAME}.new"

    expected_bytes, head_metrics = _head_content_length(url=SPECIESFINDER_URL)

    ok, attempts = _download_file_with_retry(
        url=SPECIESFINDER_URL,
        output_path=tar_path,
        logger=logger,
        max_retries=3,
        wait_seconds=30,
        timeout_s=300,
    )

    downloaded_bytes = tar_path.stat().st_size if tar_path.exists() else 0

    # Basic safeguard: if Content-Length is known and doesn't match, treat it as a failed/partial download.
    size_ok = True
    if expected_bytes is not None and downloaded_bytes != expected_bytes:
        size_ok = False

    dl_milestone = {
        "status": StatusType.PASSED.value if (ok and size_ok) else StatusType.FAILED.value,
        "message": "Tarball downloaded successfully" if ok else f"Failed to download tarball from {SPECIESFINDER_URL}",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": int(attempts),
        "retryable": True,
        "metrics": {
            "url": SPECIESFINDER_URL,
            "output_path": str(tar_path),
            "bytes_downloaded": downloaded_bytes,
            "expected_bytes": expected_bytes,
            "head": head_metrics,
            "size_ok": size_ok,
        },
    }
    rb.add_named_milestone(step, dl_milestone)
    remaining_steps.remove(step)
    if dl_milestone["status"] != StatusType.PASSED.value:
        # Keep the existing extracted DB; delete failed partial tarball.
        _remove_path(tar_path)
        skip_remaining_steps(remaining_steps, "Skipped: failed to download raw files.")
        rb.set_update_decision(
            mode="checksum_manifest",
            result="error",
            message="Download failed or tarball size mismatch; leaving existing extracted DB untouched.",
            first_build=first_build,
            checksums_before=([{"file_name": TARBALL_NAME, "checksum": md5_before}] if md5_before else []),
            checksums_after=[],
        )
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Compute checksum of the downloaded tarball to decide whether an update is needed.
    md5_after = file_md5sum(str(tar_path))

    update_required = first_build or (md5_before != md5_after)
    decision_msg = (
        "No previous checksum baseline: treating as first build."
        if first_build
        else ("Update required: tarball checksum differs from manifest." if update_required else "No update required: tarball checksum matches manifest.")
    )

    rb.set_update_decision(
        mode="checksum_manifest",
        result="updated" if update_required else "latest_version_present",
        message=decision_msg,
        first_build=first_build,
        checksums_before=([{"file_name": TARBALL_NAME, "checksum": md5_before}] if md5_before else []),
        checksums_after=[{"file_name": TARBALL_NAME, "checksum": md5_after}],
    )

    if not update_required:
        # Nothing to do: remove the freshly downloaded tarball and exit without touching current extracted layout.
        _remove_path(tar_path)
        skip_remaining_steps(remaining_steps, "Skipped: latest version already present (checksum match).")
        rb.finalize("SKIPPED")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS (extract + validate + swap-in + write manifest)
    step = "PROCESSING_STATUS"
    started_at = get_timestamp()
    try:
        staging_dir = out_dir / f".staging_{run_id}"
        _remove_path(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)

        _safe_extract_tarball(tar_path=tar_path, dest_dir=staging_dir)

        extracted_root = _find_extracted_root(staging_dir=staging_dir)
        val = _validate_speciesfinder_layout(base_dir=extracted_root)
        if val["status"] != StatusType.PASSED.value:
            raise RuntimeError(f"Staged extraction failed validation: {val.get('message', '')}")

        # Swap in validated layout without deleting old upfront.
        _swap_in_layout(src_root=extracted_root, dest_root=out_dir, run_id=run_id, logger=logger)

        # Persist manifest for the *installed* version.
        manifest_payload = {
            "file": TARBALL_NAME,
            "md5": md5_after,
            "url": SPECIESFINDER_URL,
            "computed_at": get_timestamp(),
            "expected_bytes": expected_bytes,
            "etag": head_metrics.get("etag"),
            "last_modified": head_metrics.get("last_modified"),
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_payload, f, indent=2)

        # Remove tarball after successful swap-in
        _remove_path(tar_path)
        _remove_path(staging_dir)

        proc_milestone = {
            "status": StatusType.PASSED.value,
            "message": "Extraction completed successfully.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "tarball": str(tar_path),
                "manifest": str(manifest_path),
                "md5": md5_after,
                "tarball_removed": not tar_path.exists(),
                "staging_dir": str(staging_dir),
            },
        }
    except Exception as e:
        # Best-effort cleanup of staging artifacts; keep current extracted DB untouched.
        try:
            _remove_path(out_dir / f".staging_{run_id}")
        except Exception:
            pass
        try:
            _remove_path(tar_path)
        except Exception:
            pass
        proc_milestone = {
            "status": StatusType.FAILED.value,
            "message": f"Processing failed: {e}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"tarball": str(tar_path), "manifest": str(manifest_path)},
        }

    rb.add_named_milestone(step, proc_milestone)
    remaining_steps.remove(step)
    if proc_milestone["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc_milestone.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    step = "FINAL_STATUS"
    final = _validate_speciesfinder_layout(base_dir=out_dir)
    rb.add_named_milestone(step, final)
    remaining_steps.remove(step)
    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

