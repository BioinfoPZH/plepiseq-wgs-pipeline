#!/usr/bin/env python3

from __future__ import annotations

import getpass
import json
import logging
import socket
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.validation import get_timestamp, verify_expected_files
from utils.version_manifest import read_version_manifest, write_version_manifest


DATABASE = {"name": "nextclade", "category": "viral clade datasets"}

# Keep this mapping aligned with the zip names expected by the pipeline.
# Keys: nextclade dataset shortcut/name passed to `nextclade dataset get --name=...`
# Values: zip file name stored in output_dir
DATASETS: Dict[str, str] = {
    "sars-cov-2": "sars-cov-2.zip",
    "flu_h1n1pdm_ha": "H1N1_HA.zip",
    "flu_h1n1pdm_na": "H1N1_NA.zip",
    "flu_h3n2_ha": "H3N2_HA.zip",
    "flu_h3n2_na": "H3N2_NA.zip",
    "flu_vic_ha": "Victoria_HA.zip",
    "flu_vic_na": "Victoria_NA.zip",
    "flu_yam_ha": "Yamagata_HA.zip",
    "rsv_a": "RSV_A.zip",
    "rsv_b": "RSV_B.zip",
}

SOURCE = {
    "source_type": "https",
    "reference": "nextclade dataset list/get (Nextclade CLI)",
    # Schema requires minItems=1.
    "expected_raw_files": ["sars-cov-2.zip"],
    "expected_processed_files": list(DATASETS.values()),
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
    stdout = (p.stdout or "")[:20000]
    stderr = (p.stderr or "")[:20000]
    return p.returncode == 0, int(p.returncode), stdout, stderr


def _parse_dataset_list_json(
    *,
    stdout: str,
    requested_name: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse JSON returned by `nextclade dataset list --json` (optionally filtered by --name).

    Returns:
      (dataset_obj, error_message)
    """
    try:
        data = json.loads(stdout)
    except Exception as e:
        return None, f"Failed to parse JSON for {requested_name}: {e}"

    if not isinstance(data, list):
        return None, f"Unexpected JSON type for {requested_name}: expected list, got {type(data).__name__}"
    if not data:
        return None, f"No datasets returned for name={requested_name}"

    # There may be multiple hits; prefer one where shortcut/path exactly matches requested.
    def is_match(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        if obj.get("path") == requested_name:
            return True
        sc = obj.get("shortcuts") or []
        return isinstance(sc, list) and requested_name in sc

    chosen = None
    for obj in data:
        if is_match(obj):
            chosen = obj
            break

    if chosen is None:
        # Fallback to first entry if no exact match; still usable for tag extraction.
        first = data[0]
        if not isinstance(first, dict):
            return None, f"Unexpected dataset entry type for {requested_name}: {type(first).__name__}"
        chosen = first

    return chosen, None


def _extract_current_tag(dataset_obj: Dict[str, Any]) -> Optional[str]:
    """
    Extract the current version tag from a dataset list entry.

    Expected structure (as in user example):
      { ..., "version": {"tag": "..."} }

    Fallback: if `version.tag` is missing, attempt to use newest `versions[0].tag`.
    """
    v = dataset_obj.get("version")
    if isinstance(v, dict):
        tag = v.get("tag")
        if isinstance(tag, str) and tag.strip():
            return tag.strip()

    versions = dataset_obj.get("versions")
    if isinstance(versions, list) and versions:
        first = versions[0]
        if isinstance(first, dict):
            tag = first.get("tag")
            if isinstance(tag, str) and tag.strip():
                return tag.strip()

    return None


def nextclade_list_all(*, nextclade_bin: str, logger: logging.Logger) -> Dict[str, Any]:
    started_at = get_timestamp()
    ok, rc, stdout, stderr = _run_cmd_capture(
        cmd=[nextclade_bin, "dataset", "list", "--json"],
        timeout_s=120,
    )
    finished_at = get_timestamp()

    if not ok:
        return {
            "status": StatusType.FAILED.value,
            "message": f"nextclade dataset list failed (rc={rc})",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": True,
            "metrics": {"rc": rc, "stdout_snippet": stdout, "stderr_snippet": stderr},
        }

    # Validate that it's parseable JSON list
    try:
        parsed = json.loads(stdout)
        n = len(parsed) if isinstance(parsed, list) else None
    except Exception as e:
        logger.warning("nextclade dataset list returned non-JSON output: %s", e)
        n = None

    return {
        "status": StatusType.PASSED.value,
        "message": "nextclade datasets list available",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": True,
        "metrics": {"rc": rc, "datasets_count": n, "stdout_snippet": stdout[:2000], "stderr_snippet": stderr},
    }


def determine_update_status_from_nextclade_tags(
    *,
    output_dir: Path,
    nextclade_bin: str,
    logger: logging.Logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str], List[Tuple[str, str]]]:
    """
    Determine whether each dataset zip needs refresh by comparing remote `tag`
    from `nextclade dataset list --name=<dataset> --json` to the local manifest.

    Returns:
      milestone,
      update_decision kwargs,
      update_required,
      remote_versions mapping (zip_name -> tag),
      changed list of (dataset_name, zip_name)
    """
    started_at = get_timestamp()

    manifest_path = output_dir / "current_version.json"
    local_versions = read_version_manifest(manifest_path)
    first_build = not bool(local_versions)

    remote_versions: Dict[str, str] = {}
    errors: Dict[str, Any] = {}

    for dataset_name, zip_name in DATASETS.items():
        ok, rc, stdout, stderr = _run_cmd_capture(
            cmd=[nextclade_bin, "dataset", "list", f"--name={dataset_name}", "--json"],
            timeout_s=120,
        )
        if not ok:
            errors[dataset_name] = {"rc": rc, "stderr_snippet": stderr, "stdout_snippet": stdout}
            continue

        dataset_obj, err = _parse_dataset_list_json(stdout=stdout, requested_name=dataset_name)
        if err:
            errors[dataset_name] = {"error": err, "stdout_snippet": stdout[:2000], "stderr_snippet": stderr}
            continue

        tag = _extract_current_tag(dataset_obj)
        if not tag:
            errors[dataset_name] = {"error": "Missing version tag in dataset list JSON", "dataset_obj_keys": list(dataset_obj.keys())[:30]}
            continue

        remote_versions[zip_name] = tag

    finished_at = get_timestamp()

    if errors:
        msg = f"Failed to determine remote tags for {len(errors)}/{len(DATASETS)} dataset(s)."
        return (
            {
                "status": StatusType.FAILED.value,
                "message": msg,
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": 1,
                "retryable": True,
                "metrics": {"errors": errors},
            },
            {"mode": "version_endpoint", "result": "error", "message": msg},
            False,
            {},
            [],
        )

    changed: List[Tuple[str, str]] = []
    unchanged: List[str] = []
    missing_local_files: List[str] = []

    for dataset_name, zip_name in DATASETS.items():
        local_tag = local_versions.get(zip_name, "").strip()
        remote_tag = remote_versions.get(zip_name, "").strip()
        zip_path = output_dir / zip_name

        if not zip_path.exists():
            missing_local_files.append(zip_name)

        if (not local_tag) or (local_tag != remote_tag) or (not zip_path.exists()):
            changed.append((dataset_name, zip_name))
        else:
            unchanged.append(zip_name)

    update_required = len(changed) > 0

    if update_required:
        msg = (
            "No local version baseline: treating as first build."
            if first_build
            else f"Update required: {len(changed)} dataset zip(s) changed."
        )
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
                "update_required": True,
                "changed_zips": [z for (_d, z) in changed],
                "unchanged_zips": unchanged,
                "missing_local_files": missing_local_files,
            },
        }

        update_decision = {
            "mode": "version_endpoint",
            "result": "updated",
            "message": f"{msg} Updated: {', '.join([z for (_d, z) in changed])}",
            "first_build": first_build,
            "version_local": ";".join(f"{k}={v}" for k, v in sorted(local_versions.items())),
            "version_remote": ";".join(f"{k}={v}" for k, v in sorted(remote_versions.items())),
        }
        return milestone, update_decision, True, remote_versions, changed

    msg = "No update required: all dataset zips match remote tags."
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
            "update_required": False,
            "changed_zips": [],
            "unchanged_zips": unchanged,
            "missing_local_files": missing_local_files,
        },
    }
    update_decision = {
        "mode": "version_endpoint",
        "result": "latest_version_present",
        "message": msg,
        "first_build": first_build,
        "version_local": ";".join(f"{k}={v}" for k, v in sorted(local_versions.items())),
        "version_remote": ";".join(f"{k}={v}" for k, v in sorted(remote_versions.items())),
    }
    return milestone, update_decision, False, remote_versions, []


def download_nextclade_zips(
    *,
    output_dir: Path,
    nextclade_bin: str,
    changed: List[Tuple[str, str]],
    logger: logging.Logger,
) -> Dict[str, Any]:
    started_at = get_timestamp()

    per_dataset: Dict[str, Any] = {}
    any_failed = False

    for dataset_name, zip_name in changed:
        zip_path = output_dir / zip_name
        # Remove existing zip first to avoid leaving stale/partial output behind.
        try:
            if zip_path.exists():
                zip_path.unlink()
        except Exception as e:
            per_dataset[dataset_name] = {
                "ok": False,
                "rc": None,
                "zip": str(zip_path),
                "error": f"Failed to remove existing zip before download: {e}",
            }
            any_failed = True
            continue

        ok, rc, stdout, stderr = _run_cmd_capture(
            cmd=[nextclade_bin, "dataset", "get", f"--name={dataset_name}", "--output-zip", str(zip_path)],
            timeout_s=3600,
        )
        per_dataset[dataset_name] = {
            "ok": ok,
            "rc": rc,
            "zip": str(zip_path),
            "stdout_snippet": stdout[:2000],
            "stderr_snippet": stderr[:2000],
        }
        if not ok:
            any_failed = True

    finished_at = get_timestamp()

    if any_failed:
        failed = [k for k, v in per_dataset.items() if not v.get("ok")]
        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download {len(failed)}/{len(changed)} dataset zip(s): {', '.join(failed)}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": True,
            "metrics": {"per_dataset": per_dataset},
        }

    return {
        "status": StatusType.PASSED.value,
        "message": f"Downloaded {len(changed)} dataset zip(s).",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {"per_dataset": per_dataset},
    }


def processing_status_dummy() -> Dict[str, Any]:
    started_at = get_timestamp()
    finished_at = get_timestamp()
    return {
        "status": StatusType.SKIPPED.value,
        "message": "No additional processing required for Nextclade (zip archives are final artifacts).",
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
@click.option("--output_dir", type=str, default="/home/external_databases/nextclade", help="Output directory.")
@click.option("--nextclade_bin", type=str, default="/opt/nextclade/bin/nextclade", help="Path to nextclade binary.")
def main(
    workspace: str,
    container_image: str,
    user: str,
    host: str,
    run_id: Optional[str] = None,
    report_file: Optional[str] = None,
    log_file: str = "log.log",
    output_dir: str = "/home/external_databases/nextclade",
    nextclade_bin: str = "/opt/nextclade/bin/nextclade",
) -> None:
    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting Nextclade DB updater")

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}",
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

    # 2) DATABASE_AVAILABILITY (nextclade dataset list --json)
    avail = nextclade_list_all(nextclade_bin=nextclade_bin, logger=logger)
    rb.add_named_milestone("DATABASE_AVAILABILITY", avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 3) UPDATE_STATUS
    upd_milestone, update_decision, update_required, remote_versions, changed = (
        determine_update_status_from_nextclade_tags(output_dir=out_dir, nextclade_bin=nextclade_bin, logger=logger)
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

    # 4) REMOTE_FILES_DOWNLOAD_STATUS (only changed datasets)
    dl = download_nextclade_zips(output_dir=out_dir, nextclade_bin=nextclade_bin, changed=changed, logger=logger)
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
    if dl["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped: failed to download dataset zips.")
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

    # Persist new baseline *only after* successful final validation
    try:
        write_version_manifest(out_dir / "current_version.json", remote_versions)
    except Exception as e:
        logger.warning("Failed to write version manifest: %s", e)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()

