#!/usr/bin/env python3

from __future__ import annotations

import getpass
import gzip
import json
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import requests
from multiprocessing.dummy import Pool as ThreadPool

from utils.blast_helpers import run_makeblastdb
from utils.download_helpers import _download_file_with_retry
from utils.generic_helpers import _execute_command, remove_old_workspace
from utils.net import StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import file_md5sum, parse_credentials_file, get_enterobase_auth
from utils.validation import verify_expected_files, get_timestamp


REQUIRED_SENTINELS_CGMLST: Dict[str, List[str]] = {
    # a few stable loci used as sentinels for validation
    "senterica": ["STMMW_17971", "t1733"],
    "ecoli": ["b0784", "NCTC12130_00627"],
}

REQUIRED_SENTINELS_MLST: Dict[str, List[str]] = {
    "senterica": ["aroC", "dnaN", "purE"],
    "ecoli": ["adk", "fumC", "recA"],
}


def _first_line(path: Path) -> str:
    with path.open("rt", encoding="utf-8", errors="replace") as f:
        return (f.readline() or "").rstrip("\n")


def _write_profiles_local_stub(*, output_dir: Path, logger) -> None:
    """
    Ensure `local/profiles_local.list` exists with the header line from `profiles.list`.
    """
    local_dir = output_dir / "local"
    local_dir.mkdir(parents=True, exist_ok=True)
    stub_path = local_dir / "profiles_local.list"
    if stub_path.exists():
        return
    header = _first_line(output_dir / "profiles.list")
    stub_path.write_text(header + "\n", encoding="utf-8")
    logger.info("Created local profiles stub: %s", stub_path)


def _concat_fastas_in_dir(*, src_dir: Path, out_path: Path) -> None:
    """
    Concatenate all `*.fasta` files in src_dir (sorted) into out_path.
    """
    fasta_files = sorted(p for p in src_dir.glob("*.fasta") if p.is_file() and p.name != out_path.name)
    with out_path.open("wb") as out:
        for p in fasta_files:
            out.write(p.read_bytes())


def _download_json_with_retry(
    *,
    url: str,
    output_path: Path,
    logger,
    auth: Optional[Tuple[str, str]] = None,
    max_retries: int = 3,
    wait_seconds: int = 30,
    timeout_s: int = 60,
) -> Tuple[bool, int, Optional[Dict[str, Any]]]:
    """
    Download JSON from a URL with retries. Returns (ok, attempts_used, parsed_json).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    last_err: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        try:
            if output_path.exists():
                output_path.unlink()
            r = requests.get(url, timeout=timeout_s, auth=auth)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                raise RuntimeError(last_err)
            parsed = r.json()
            # Canonicalize to make update decisions stable across runs:
            # - Sort loci list by locus name
            # - Dump with sorted keys
            try:
                loci = parsed.get("loci")
                if isinstance(loci, list):
                    loci_sorted = sorted(
                        (x for x in loci if isinstance(x, dict)),
                        key=lambda d: str(d.get("locus", "")),
                    )
                    parsed = dict(parsed)
                    parsed["loci"] = loci_sorted
            except Exception:
                pass

            output_path.write_text(json.dumps(parsed, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return True, attempt, parsed
        except Exception as e:
            last_err = str(e)
            logger.warning("Attempt %d/%d failed for JSON %s: %s", attempt, max_retries, url, last_err)
            if attempt < max_retries:
                time.sleep(wait_seconds)

    return False, max_retries, None


def _build_expected_files(database: str, scheme_name: str) -> List[str]:
    expected: List[str] = ["profiles.list", "local/profiles_local.list"]

    if scheme_name in ("cgMLST_v2", "cgMLST"):
        sent = REQUIRED_SENTINELS_CGMLST.get(database, [])
    else:
        sent = REQUIRED_SENTINELS_MLST.get(database, [])

    for locus in sent:
        expected.append(f"{locus}.fasta")
        # makeblastdb nucl outputs (we validate at least one index artifact)
        expected.append(f"{locus}.fasta.nsq")

    if scheme_name == "MLST_Achtman":
        expected.append("all_allels.fasta")

    return expected


def _gunzip_to_file(*, src_gz: Path, dest: Path) -> None:
    """
    Decompress src_gz to dest. This is used for stable checksums, because .gz
    bytes may vary between requests even if decompressed content is identical.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src_gz, "rb") as fin:
        data = fin.read()
    dest.write_bytes(data)


@click.command()
@click.option("--workspace", type=str, default=None, help="Workspace path used in report metadata.")
@click.option("--run_id", type=str, default=None, help="Unique run ID (defaults to generated).")
@click.option("--container_image", type=str, default="unknown", help="Container image name (report metadata).")
@click.option("--report_file", type=str, default=None, help="Report JSON file name (defaults to {run_id}.json).")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, default=None, help="User name (report metadata).")
@click.option("--host", type=str, default=None, help="Host name (report metadata).")
@click.option(
    "-d",
    "--database",
    help="[REQUIRED] Genus-specific name of the database in EnteroBase",
    type=click.Choice(["senterica", "ecoli"]),
    required=True,
)
@click.option(
    "-s",
    "--scheme_name",
    help="[REQUIRED] Name of the scheme in EnteroBase",
    type=click.Choice(["cgMLST_v2", "cgMLST", "MLST_Achtman"]),
    required=True,
)
@click.option(
    "-r",
    "--scheme_dir",
    help="[REQUIRED] Scheme directory in EnteroBase",
    type=click.Choice(
        ["Salmonella.cgMLSTv2", "Escherichia.cgMLSTv1", "Escherichia.Achtman7GeneMLST", "Salmonella.Achtman7GeneMLST"]
    ),
    required=True,
)
@click.option("-c", "--cpus", default=4, show_default=True, help="Number of CPUs to use for BLAST indexing.")
@click.option(
    "-t",
    "--credentials_file",
    default="/home/update/credentials.txt",
    show_default=True,
    type=click.Path(),
    help="Path to key=value credentials file (must contain enterobase_token).",
)
@click.option("-o", "--output_dir", help="[REQUIRED] Output directory.", required=True, type=click.Path())
def main(
    workspace: Optional[str],
    run_id: Optional[str],
    container_image: str,
    report_file: Optional[str],
    log_file: str,
    user: Optional[str],
    host: Optional[str],
    database: str,
    scheme_name: str,
    scheme_dir: str,
    cpus: int,
    credentials_file: str,
    output_dir: str,
) -> None:

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not run_id:
        run_id = generate_run_id("enterobase_schema_data")

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
        "workspace": f"{workspace}/enterobase_schema" if workspace else str(out_dir),
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    database_identity = {"name": "enterobase_schema_data", "category": "typing"}

    source = {
        "source_type": "https",
        "reference": "https://enterobase.warwick.ac.uk",
        # Update decision is based on stable content checksums:
        # - canonicalized loci.json
        # - decompressed profiles.list (not profiles.list.gz)
        "expected_raw_files": ["loci.json", "profiles.list"],
        "expected_processed_files": _build_expected_files(database, scheme_name),
    }

    remaining_steps = list(ALL_STEPS)

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=database_identity,
        execution_context=execution_context,
        run_id=run_id,
        source=source,
        log_file=f"{execution_context['workspace']}/logs/{log_file}",
    )

    def skip_remaining_steps(steps: List[str], reason: str) -> None:
        for s in steps:
            rb.add_skipped(s, reason)

    credentials = parse_credentials_file(Path(credentials_file), logger)
    auth = get_enterobase_auth(credentials, logger)
    if auth is None:
        skip_remaining_steps(remaining_steps, "Skipped: missing EnteroBase API token.")
        rb.fail(code="AUTH_TOKEN_MISSING", message=f"No valid enterobase_token in credentials file: {credentials_file}", retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    api_url = f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/{scheme_name}/loci?limit=10000&scheme={scheme_name}&offset=0"
    scheme_base_url = f"https://enterobase.warwick.ac.uk/schemes/{scheme_dir}/"
    profiles_url = f"{scheme_base_url.rstrip('/')}/profiles.list.gz"

    loci_json_path = out_dir / "loci.json"
    profiles_gz_path = out_dir / "profiles.list.gz"
    profiles_list_path = out_dir / "profiles.list"
    manifest_path = out_dir / "enterobase_md5.json"

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
    started_at = get_timestamp()
    api_av = check_url_available(api_url, retries=3, interval=30, logger=logger, auth=auth)
    scheme_av = check_url_available(scheme_base_url, retries=3, interval=30, logger=logger)

    db_avail = {
        "status": StatusType.PASSED.value if (api_av["status"] == StatusType.PASSED.value and scheme_av["status"] == StatusType.PASSED.value) else StatusType.FAILED.value,
        "message": "All required endpoints reachable" if (api_av["status"] == StatusType.PASSED.value and scheme_av["status"] == StatusType.PASSED.value) else "One or more endpoints unreachable",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": max(int(api_av.get("attempts", 1) or 1), int(scheme_av.get("attempts", 1) or 1)),
        "retryable": True,
        "metrics": {
            "checks": {
                api_url: {"status": api_av.get("status"), "message": api_av.get("message"), "metrics": api_av.get("metrics", {})},
                scheme_base_url: {"status": scheme_av.get("status"), "message": scheme_av.get("message"), "metrics": scheme_av.get("metrics", {})},
            }
        },
    }

    rb.add_named_milestone("DATABASE_AVAILABILITY", db_avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")

    if db_avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=db_avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 3) REMOTE_FILES_DOWNLOAD_STATUS (metadata artifacts)
    # -----------------------------
    started_at = get_timestamp()
    ok_json, attempts_json, parsed_json = _download_json_with_retry(
        url=api_url,
        output_path=loci_json_path,
        logger=logger,
        auth=auth,
        max_retries=3,
        wait_seconds=30,
        timeout_s=60,
    )

    ok_profiles, attempts_profiles = _download_file_with_retry(
        url=profiles_url,
        output_path=profiles_gz_path,
        logger=logger,
        max_retries=3,
        wait_seconds=60,
        timeout_s=60,
    )

    # Decompress profiles for stable checksums (gzip headers may change between requests)
    ok_profiles_list = False
    if ok_profiles and profiles_gz_path.exists():
        try:
            _gunzip_to_file(src_gz=profiles_gz_path, dest=profiles_list_path)
            ok_profiles_list = True
        except Exception as e:
            logger.warning("Failed to decompress profiles.list.gz to profiles.list: %s", e)

    finished_at = get_timestamp()
    if not ok_json or not ok_profiles or not ok_profiles_list:
        rb.add_named_milestone(
            "REMOTE_FILES_DOWNLOAD_STATUS",
            {
                "status": StatusType.FAILED.value,
                "message": "Failed to download required metadata artifacts (loci.json and/or profiles.list.gz) or failed to decompress profiles.list.",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": max(attempts_json, attempts_profiles),
                "retryable": True,
                "metrics": {
                    "loci_json_ok": ok_json,
                    "profiles_gz_ok": ok_profiles,
                    "profiles_list_ok": ok_profiles_list,
                    "loci_json_path": str(loci_json_path),
                    "profiles_gz_path": str(profiles_gz_path),
                    "profiles_list_path": str(profiles_list_path),
                },
            },
        )
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download metadata artifacts.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    loci_count = 0
    try:
        loci_count = len((parsed_json or {}).get("loci", []))
    except Exception:
        loci_count = 0

    rb.add_named_milestone(
        "REMOTE_FILES_DOWNLOAD_STATUS",
        {
            "status": StatusType.PASSED.value,
            "message": "Metadata artifacts downloaded successfully.",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": max(attempts_json, attempts_profiles),
            "retryable": True,
            "metrics": {
                "loci_count": loci_count,
                "loci_json_bytes": int(loci_json_path.stat().st_size) if loci_json_path.exists() else 0,
                "profiles_gz_bytes": int(profiles_gz_path.stat().st_size) if profiles_gz_path.exists() else 0,
                "profiles_list_bytes": int(profiles_list_path.stat().st_size) if profiles_list_path.exists() else 0,
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

    new_md5: Dict[str, str] = {
        # loci.json is canonicalized on write
        "loci.json": file_md5sum(str(loci_json_path)),
        # Use decompressed profiles.list for stable checksum
        "profiles.list": file_md5sum(str(profiles_list_path)),
    }

    changed = [k for k, v in new_md5.items() if old_md5.get(k, "") != v]
    expected_files = _build_expected_files(database, scheme_name)
    required_present = all((out_dir / rel).exists() for rel in expected_files)

    update_required = (not manifest_path.exists()) or bool(changed) or (not required_present)

    if update_required:
        msg = "No previous manifest: treating as first build." if first_build else ("Update required: checksum change detected." if changed else "Update required: expected files missing locally.")
        remove_old_workspace(
            out_dir,
            keep=("logs", "reports", manifest_path.name, loci_json_path.name, profiles_gz_path.name, profiles_list_path.name),
            logger=logger,
        )
        # Persist new baseline immediately (same approach as VFDB)
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

    # -----------------------------
    # 5) PROCESSING_STATUS
    # -----------------------------
    started_at = get_timestamp()

    # parse loci from saved JSON (use parsed_json if available)
    if parsed_json is None:
        parsed_json = json.loads(loci_json_path.read_text(encoding="utf-8"))
    loci_entries = (parsed_json or {}).get("loci", [])
    loci: List[str] = []
    for item in loci_entries:
        if isinstance(item, dict) and item.get("locus"):
            loci.append(str(item["locus"]))

    if not loci:
        proc = {
            "status": StatusType.FAILED.value,
            "message": "No loci found in loci.json; cannot proceed.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"loci_json": str(loci_json_path)},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Download loci files (gz)
    downloaded = 0
    failed: List[str] = []
    attempts_used_max = 1

    for i, locus in enumerate(loci):
        if i > 0 and i % 100 == 0:
            # throttle to be gentle on EnteroBase
            time.sleep(2)
        url = f"{scheme_base_url.rstrip('/')}/{locus}.fasta.gz"
        dest = out_dir / f"{locus}.fasta.gz"
        ok, attempts_used = _download_file_with_retry(
            url=url,
            output_path=dest,
            logger=logger,
            max_retries=3,
            wait_seconds=60,
            timeout_s=120,
        )
        attempts_used_max = max(attempts_used_max, attempts_used)
        if not ok:
            failed.append(locus)
            break
        downloaded += 1

    if failed:
        proc = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download locus FASTA.gz for: {failed[0]}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {"downloaded": downloaded, "failed_locus": failed[0], "loci_total": len(loci)},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=proc["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Decompress profiles + loci gz
    # profiles.list is produced during metadata download; keep the gz as optional reference.
    if not profiles_list_path.exists():
        try:
            _gunzip_to_file(src_gz=profiles_gz_path, dest=profiles_list_path)
        except Exception:
            ok_profiles_gunzip = _execute_command(f"gunzip -f {profiles_gz_path}", logger=logger)
            if not ok_profiles_gunzip:
                proc = {
                    "status": StatusType.FAILED.value,
                    "message": "Failed to produce profiles.list from profiles.list.gz",
                    "started_at": started_at,
                    "finished_at": get_timestamp(),
                    "attempts": 1,
                    "retryable": False,
                    "metrics": {"file": str(profiles_gz_path)},
                }
                rb.add_named_milestone("PROCESSING_STATUS", proc)
                remaining_steps.remove("PROCESSING_STATUS")
                skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
                rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
                rb.finalize("FAIL")
                rb.write(str(report_dir / report_file))
                return

    for locus in loci:
        gz = out_dir / f"{locus}.fasta.gz"
        if not gz.exists():
            continue
        ok = _execute_command(f"gunzip -f {gz}", logger=logger)
        if not ok:
            proc = {
                "status": StatusType.FAILED.value,
                "message": f"Failed to gunzip {gz.name}",
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "attempts": 1,
                "retryable": False,
                "metrics": {"file": str(gz)},
            }
            rb.add_named_milestone("PROCESSING_STATUS", proc)
            remaining_steps.remove("PROCESSING_STATUS")
            skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
            rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
            rb.finalize("FAIL")
            rb.write(str(report_dir / report_file))
            return

    # BLAST indexing
    fasta_files = [str(out_dir / f"{locus}.fasta") for locus in loci if (out_dir / f"{locus}.fasta").exists()]
    if not fasta_files:
        proc = {
            "status": StatusType.FAILED.value,
            "message": "No loci FASTA files found after decompression.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"loci_total": len(loci)},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # 7-gene MLST: no need for heavy parallelism, but keep it safe
    threads = max(1, int(cpus or 1))
    if scheme_name == "MLST_Achtman":
        threads = 1

    def _index_one(path_str: str) -> Tuple[str, bool]:
        ok, _m = run_makeblastdb(Path(path_str), dbtype="nucl", logger=logger)
        return path_str, ok

    with ThreadPool(threads) as pool:
        results = pool.map(_index_one, fasta_files)

    failed_index = [p for (p, ok) in results if not ok]
    if failed_index:
        proc = {
            "status": StatusType.FAILED.value,
            "message": f"makeblastdb failed for {len(failed_index)} file(s).",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"failed": failed_index[:20], "failed_truncated": len(failed_index) > 20},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    if scheme_name == "MLST_Achtman":
        all_allels_path = out_dir / "all_allels.fasta"
        logger.info("Merging loci fastas to %s ...", all_allels_path)
        _concat_fastas_in_dir(src_dir=out_dir, out_path=all_allels_path)

    proc = {
        "status": StatusType.PASSED.value,
        "message": "Downloaded loci, decompressed files, and created BLAST indices.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {"loci_total": len(loci), "downloaded": downloaded, "indexed": len(fasta_files), "threads": threads},
    }
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # -----------------------------
    # 6) FINAL_STATUS
    # -----------------------------
    # Ensure profiles.list exists and create local stub
    try:
        _write_profiles_local_stub(output_dir=out_dir, logger=logger)
    except Exception as e:
        final_payload = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to create local profiles stub: {e}",
            "started_at": get_timestamp(),
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {},
        }
        rb.add_named_milestone("FINAL_STATUS", final_payload)
        remaining_steps.remove("FINAL_STATUS")
        rb.fail(code="FINAL_STATUS_FAILED", message=final_payload["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    final = verify_expected_files(base_dir=out_dir, expected_files=_build_expected_files(database, scheme_name))
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
