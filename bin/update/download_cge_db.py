#!/usr/bin/env python3

from __future__ import annotations

import getpass
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.generic_helpers import remove_old_workspace
from utils.github_helpers import build_version_string
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import composite_availability_check
from utils.validation import get_timestamp, verify_expected_files
from utils.version_manifest import read_version_manifest, write_version_manifest


BITBUCKET_ORG = "genomicepidemiology"
BITBUCKET_BASE = "https://bitbucket.org"


# Files required to exist after a successful update (user-provided contract).
#
# NOTE: Paths are relative to the database root directory (output_dir).
EXPECTED_PROCESSED_FILES: Dict[str, List[str]] = {
    "disinfinder_db": [
        "disinfectants.length.b",
        "disinfectants.fsa",
    ],
    "mlst_db": [
        "senterica/senterica.fsa",
        "senterica/senterica.seq.b",
        "ecoli1/ecoli1.comp.b",
        "ecoli1/ecoli1.name",
        "ecoli2/ecoli2.comp.b",
        "ecoli2/ecoli2.name",
        "cjejuni/cjejuni.name",
        "cjejuni/cjejuni.fsa",
    ],
    "plasmidfinder_db": [
        "Inc18.length.b",
        "Rep1.length.b",
        "RepA_N.length.b",
    ],
    "pointfinder_db": [
        "campylobacter/23S.fsa",
        "campylobacter/campylobacter.fsa",
        "salmonella/parE.fsa",
        "salmonella/salmonella.comp.b",
        "escherichia_coli/escherichia_coli.length.b",
        "escherichia_coli/gyrA.fsa",
    ],
    "resfinder_db": [
        "nitroimidazole.comp.b",
        "colistin.length.b",
        "rifampicin.comp.b",
        "tetracycline.length.b",
    ],
    "spifinder_db": [
        "SPI.length.b",
        "SPI.name",
    ],
    "virulencefinder_db": [
        "virulence_ecoli.comp.b",
        "virulence_ecoli.name",
    ],
}


def _db_source(db: str) -> Dict[str, Any]:
    clone_url = f"{BITBUCKET_BASE}/{BITBUCKET_ORG}/{db}.git"
    repo_page = f"{BITBUCKET_BASE}/{BITBUCKET_ORG}/{db}/"
    expected_processed = EXPECTED_PROCESSED_FILES[db]
    # Schema requires minItems=1 for raw files; keep it lightweight and stable.
    expected_raw = ["config"]

    return {
        "source_type": "git",
        "reference": clone_url,
        "repo_page": repo_page,
        "expected_raw_files": expected_raw,
        "expected_processed_files": expected_processed,
    }


def _run_cmd_capture(
    *,
    cmd: List[str],
    cwd: Optional[Path] = None,
    timeout_s: int = 1800,
) -> Tuple[bool, int, str, str]:
    """
    Run a command and capture stdout/stderr (truncated).
    """
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


def get_bitbucket_head_sha(*, clone_url: str, logger) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch remote HEAD commit id via git ls-remote.
    """
    started_at = get_timestamp()
    ok, rc, stdout, stderr = _run_cmd_capture(cmd=["git", "ls-remote", clone_url, "HEAD"], timeout_s=60)
    finished_at = get_timestamp()

    metrics: Dict[str, Any] = {
        "clone_url": clone_url,
        "rc": rc,
        "stdout_snippet": stdout.strip(),
        "stderr_snippet": stderr.strip(),
        "started_at": started_at,
        "finished_at": finished_at,
    }

    if not ok:
        raise RuntimeError(f"git ls-remote failed (rc={rc}) for {clone_url}: {stderr.strip()}")

    # Expected output: "<sha>\tHEAD"
    line = (stdout or "").strip().splitlines()[0] if (stdout or "").strip() else ""
    sha = line.split()[0].strip() if line else ""
    if not sha or len(sha) < 7:
        raise RuntimeError(f"Unexpected git ls-remote output for {clone_url}: {stdout.strip()}")

    logger.info("Remote HEAD for %s: %s", clone_url, sha)
    return sha, metrics


def determine_update_status_from_bitbucket_head(
    *,
    db: str,
    output_dir: Path,
    clone_url: str,
    logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    """
    Decide update based on Bitbucket repo HEAD commit SHA.

    Returns:
      milestone,
      update_decision kwargs for ReportBuilder.set_update_decision,
      update_required,
      remote_versions mapping (for manifest)
    """
    started_at = get_timestamp()

    manifest_path = output_dir / "current_version.json"
    local_versions = read_version_manifest(manifest_path)
    first_build = not bool(local_versions)

    metrics: Dict[str, Any] = {"git": {}}

    try:
        sha, sha_metrics = get_bitbucket_head_sha(clone_url=clone_url, logger=logger)
        remote_versions = {db: sha}
        metrics["git"][db] = sha_metrics
    except Exception as e:
        finished_at = get_timestamp()
        msg = f"Failed to fetch remote HEAD SHA: {e}"
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
            else "Update required: remote HEAD differs from local baseline."
        )
        # In-place wipe behaviour (keep logs/reports/version manifest)
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

    msg = "No update required: remote HEAD matches local baseline."
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


def clone_repo_into_output_dir(*, clone_url: str, output_dir: Path, logger) -> Dict[str, Any]:
    started_at = get_timestamp()

    tmp_root = None
    attempts = 1
    try:
        tmp_root = Path(tempfile.mkdtemp(prefix="cge_clone_", dir=str(output_dir)))
        clone_target = tmp_root / "repo"

        ok, rc, stdout, stderr = _run_cmd_capture(
            cmd=["git", "clone", "--depth", "1", clone_url, str(clone_target)],
            timeout_s=3600,
        )
        if not ok:
            finished_at = get_timestamp()
            return {
                "status": StatusType.FAILED.value,
                "message": f"git clone failed (rc={rc})",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts,
                "retryable": True,
                "metrics": {"clone_url": clone_url, "rc": rc, "stdout": stdout, "stderr": stderr},
            }

        # Move repo content to output_dir root, keeping output_dir/logs and output_dir/reports.
        moved = 0
        for child in clone_target.iterdir():
            dest = output_dir / child.name
            if dest.exists():
                # Should not happen after remove_old_workspace, but be defensive.
                if dest.is_dir():
                    shutil.rmtree(dest, ignore_errors=True)
                else:
                    dest.unlink(missing_ok=True)
            shutil.move(str(child), str(dest))
            moved += 1

        finished_at = get_timestamp()
        return {
            "status": StatusType.PASSED.value,
            "message": "Repository cloned successfully",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts,
            "retryable": False,
            "metrics": {"clone_url": clone_url, "items_moved": moved},
        }
    except Exception as e:
        finished_at = get_timestamp()
        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to clone repository: {e}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts,
            "retryable": True,
            "metrics": {"clone_url": clone_url},
        }
    finally:
        try:
            if tmp_root and tmp_root.exists():
                shutil.rmtree(tmp_root, ignore_errors=True)
        except Exception:
            pass


def run_install_py(*, output_dir: Path, kma_binary: str, logger) -> Dict[str, Any]:
    started_at = get_timestamp()
    install_script = output_dir / "INSTALL.py"
    if not install_script.exists():
        finished_at = get_timestamp()
        return {
            "status": StatusType.FAILED.value,
            "message": "Missing INSTALL.py in cloned repository",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {"expected": str(install_script)},
        }

    ok, rc, stdout, stderr = _run_cmd_capture(
        cmd=["python3", "INSTALL.py", kma_binary, "non_interactive"],
        cwd=output_dir,
        timeout_s=7200,
    )
    finished_at = get_timestamp()

    if not ok:
        return {
            "status": StatusType.FAILED.value,
            "message": f"INSTALL.py failed (rc={rc})",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {"rc": rc, "stdout": stdout, "stderr": stderr, "kma_binary": kma_binary},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": "INSTALL.py completed successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {"rc": rc, "kma_binary": kma_binary},
    }


def processing_status_dummy() -> Dict[str, Any]:
    started_at = get_timestamp()
    finished_at = get_timestamp()
    return {
        "status": StatusType.SKIPPED.value,
        "message": "No additional processing required beyond INSTALL.py.",
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
@click.option(
    "--db",
    type=click.Choice(sorted(EXPECTED_PROCESSED_FILES.keys())),
    required=True,
    help="CGE database repository name.",
)
@click.option("--output_dir", type=str, default=None, help="Output directory (database root).")
@click.option(
    "--kma_binary",
    type=str,
    default=None,
    help="Path to KMA binary to pass into INSTALL.py. Defaults: /home/kma/kma for mlst_db, /home/kma/kma_index otherwise.",
)
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    db: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: Optional[str] = None,
    kma_binary: Optional[str] = None,
) -> None:
    if not run_id:
        run_id = generate_run_id(db)

    if not output_dir:
        output_dir = f"/home/external_databases/{db}"

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # logging
    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting CGE DB updater for %s", db)

    execution_context = {
        "workspace": f"{workspace}/{db}",
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

    source = _db_source(db)
    clone_url = str(source["reference"])
    repo_page = str(source["repo_page"])

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database={"name": db, "category": "genomicepidemiology"},
        execution_context=execution_context,
        run_id=run_id,
        source={
            "source_type": "git",
            "reference": clone_url,
            "expected_raw_files": source["expected_raw_files"],
            "expected_processed_files": source["expected_processed_files"],
        },
        log_file=f"{workspace}/{db}/reports/{report_file}",
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
    # Check Bitbucket base + repo page (standardized composite milestone).
    avail = composite_availability_check([BITBUCKET_BASE, repo_page], logger, retries=3, interval=10)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_versions = determine_update_status_from_bitbucket_head(
        db=db,
        output_dir=out_dir,
        clone_url=clone_url,
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
    dl = clone_repo_into_output_dir(clone_url=clone_url, output_dir=out_dir, logger=logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download repository.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 5) PROCESSING_STATUS (INSTALL.py does indexing)
    if not kma_binary:
        kma_binary = "/home/kma/kma" if db == "mlst_db" else "/home/kma/kma_index"

    proc = run_install_py(output_dir=out_dir, kma_binary=kma_binary, logger=logger)
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")
    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: INSTALL.py failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=source["expected_processed_files"])
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

