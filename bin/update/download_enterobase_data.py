#!/usr/bin/env python3

from __future__ import annotations

import getpass
import json
import socket
import time
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import click
import numpy as np
import requests

from utils.net import HEADERS, StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import file_md5sum
from utils.validation import get_timestamp, verify_expected_files

try:
    from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

    HAS_PYDANTIC = True
except Exception:
    HAS_PYDANTIC = False
    BaseModel = object  # type: ignore[assignment]
    ConfigDict = dict  # type: ignore[assignment]
    ValidationError = Exception  # type: ignore[assignment]

    def field_validator(*_args, **_kwargs):  # type: ignore[no-redef]
        def _decorator(func):
            return func

        return _decorator


DATABASE = {"name": "enterobase_historical_data", "category": "typing"}
SOURCE = {
    "source_type": "https",
    "reference": "https://enterobase.warwick.ac.uk",
    "expected_raw_files": ["strains_api_payload"],
    "expected_processed_files": ["strains_table.npy", "straindata_table.npy", "sts_table.npy"],
}


if HAS_PYDANTIC:

    class STSchemeModel(BaseModel):
        model_config = ConfigDict(extra="allow")
        st_id: int

        @field_validator("st_id", mode="before")
        @classmethod
        def _coerce_st_id(cls, v: Any) -> int:
            return int(v)

    class StrainDataEntryModel(BaseModel):
        model_config = ConfigDict(extra="allow")
        sts: List[STSchemeModel] = []


def _basic_auth_tuple(api_token: str) -> Tuple[str, str]:
    return api_token, ""


def _read_first_line(path: Path) -> str:
    with path.open("rt", encoding="utf-8", errors="replace") as f:
        return (f.readline() or "").strip()


def _fetch_json_with_retry(
    *,
    url: str,
    logger,
    auth: Tuple[str, str],
    max_retries: int = 3,
    wait_seconds: int = 30,
    timeout_s: int = 120,
) -> Tuple[bool, int, Optional[Dict[str, Any]], str]:
    err = ""
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout_s, auth=auth)
            if r.status_code != 200:
                err = f"HTTP {r.status_code}"
                raise RuntimeError(err)
            payload = r.json()
            if not isinstance(payload, dict):
                err = "Invalid JSON payload shape"
                raise RuntimeError(err)
            return True, attempt, payload, ""
        except Exception as e:
            err = str(e)
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, url, err)
            if attempt < max_retries:
                time.sleep(wait_seconds)
    return False, max_retries, None, err


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _load_checksum_list(*, base_dir: Path, rel_files: List[str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for rel in rel_files:
        p = base_dir / rel
        if p.exists():
            out.append({"file_name": rel, "checksum": file_md5sum(str(p))})
    return out


def _backup_paths(paths: List[Path], logger) -> List[Tuple[Path, Path]]:
    backups: List[Tuple[Path, Path]] = []
    for p in paths:
        if not p.exists():
            continue
        bak = p.with_name(p.name + ".old")
        if bak.exists():
            bak.unlink()
        logger.info("Backing up existing file: %s -> %s", p, bak)
        os.replace(p, bak)
        backups.append((p, bak))
    return backups


def _restore_backups(backups: List[Tuple[Path, Path]], logger) -> None:
    for original, backup in backups:
        try:
            if original.exists():
                original.unlink()
        except Exception:
            pass
        if backup.exists():
            logger.info("Restoring backup: %s -> %s", backup, original)
            os.replace(backup, original)


def _remove_backup_files(backups: List[Tuple[Path, Path]], logger) -> None:
    for _original, backup in backups:
        try:
            if backup.exists():
                logger.info("Removing backup file: %s", backup)
                backup.unlink()
        except Exception as e:
            logger.warning("Failed to remove backup %s: %s", backup, e)


def _write_md5_manifest(*, out_dir: Path, checksums: List[Dict[str, str]]) -> Path:
    files: Dict[str, Dict[str, Any]] = {}
    for row in checksums:
        file_name = row["file_name"]
        p = out_dir / file_name
        files[file_name] = {
            "md5": row["checksum"],
            "bytes": int(p.stat().st_size) if p.exists() else 0,
        }
    payload = {
        "generated_at": get_timestamp(),
        "files": files,
    }
    manifest_path = out_dir / "enterobase_md5.json"
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return manifest_path


def _extract_st_ids_for_scheme(*, straindata: Dict[str, Dict[str, Any]], cgname: str) -> Tuple[List[int], int]:
    known_sts: set[int] = set()
    missing_sts_field = 0
    for _barcode, value in straindata.items():
        if not isinstance(value, dict):
            continue
        schemes = value.get("sts")
        if not isinstance(schemes, list):
            missing_sts_field += 1
            continue
        for scheme in schemes:
            if not isinstance(scheme, dict):
                continue
            if cgname not in scheme.values():
                continue
            try:
                st_id = int(scheme.get("st_id", -1))
            except Exception:
                continue
            if st_id >= 1:
                known_sts.add(st_id)
    return sorted(known_sts), missing_sts_field


def _validate_straindata_entry(value: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if not HAS_PYDANTIC:
        return True, None
    try:
        StrainDataEntryModel.model_validate(value)  # type: ignore[attr-defined]
        return True, None
    except ValidationError as e:
        return False, str(e)


def _download_straindata(
    *,
    database: str,
    barcodes: List[str],
    auth: Tuple[str, str],
    logger,
    step: int = 160,
    sleep_seconds: int = 10,
    total_remote_strains: Optional[int] = None,
) -> Tuple[bool, Dict[str, Dict[str, Any]], Dict[str, Any], str]:
    started_at = get_timestamp()
    attempts_used_max = 1
    out: Dict[str, Dict[str, Any]] = {}
    invalid_records = 0
    invalid_examples: List[str] = []

    if not barcodes:
        return True, out, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "chunks_total": 0,
            "chunks_ok": 0,
            "chunks_failed": 0,
            "invalid_records": 0,
        }, ""

    chunks_ok = 0
    chunks_failed = 0
    processed_barcodes = 0
    selected_total = len(barcodes)
    for idx, chunk in enumerate(_chunked(barcodes, step), start=1):
        params: List[Tuple[str, str]] = [
            ("limit", str(step)),
            ("sortorder", "asc"),
        ]
        for barcode in chunk:
            params.append(("barcode", barcode))
        params.append(("offset", "0"))
        url = f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/straindata?{urlencode(params)}"
        ok, attempts_used, payload, err = _fetch_json_with_retry(url=url, logger=logger, auth=auth)
        attempts_used_max = max(attempts_used_max, attempts_used)
        if not ok:
            chunks_failed += 1
            return False, out, {
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "chunks_total": int(np.ceil(len(barcodes) / step)),
                "chunks_ok": chunks_ok,
                "chunks_failed": chunks_failed,
                "failed_chunk_index": idx,
            }, err
        chunk_data = (payload or {}).get("straindata", {})
        if not isinstance(chunk_data, dict):
            chunks_failed += 1
            return False, out, {
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "chunks_total": int(np.ceil(len(barcodes) / step)),
                "chunks_ok": chunks_ok,
                "chunks_failed": chunks_failed,
                "failed_chunk_index": idx,
            }, "Invalid 'straindata' payload"

        for barcode, entry in chunk_data.items():
            if not isinstance(entry, dict):
                continue
            valid, reason = _validate_straindata_entry(entry)
            if not valid:
                invalid_records += 1
                if len(invalid_examples) < 5:
                    invalid_examples.append(f"{barcode}: {reason}")
                continue
            normalized = dict(entry)
            if not isinstance(normalized.get("sts"), list):
                normalized["sts"] = []
            normalized.setdefault("country", None)
            normalized.setdefault("collection_year", None)
            out[barcode] = normalized
        chunks_ok += 1
        processed_barcodes += len(chunk)
        if total_remote_strains is not None:
            logger.info(
                "Downloaded straindata for %d/%d selected strains (remote total: %d)",
                min(processed_barcodes, selected_total),
                selected_total,
                total_remote_strains,
            )
        else:
            logger.info(
                "Downloaded straindata for %d/%d selected strains",
                min(processed_barcodes, selected_total),
                selected_total,
            )
        if idx * step < len(barcodes):
            time.sleep(sleep_seconds)

    return True, out, {
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "chunks_total": int(np.ceil(len(barcodes) / step)),
        "chunks_ok": chunks_ok,
        "chunks_failed": chunks_failed,
        "attempts_max": attempts_used_max,
        "invalid_records": invalid_records,
        "invalid_examples": invalid_examples,
    }, ""


def _download_sts(
    *,
    database: str,
    cgname: str,
    st_ids: List[int],
    auth: Tuple[str, str],
    logger,
    step: int = 400,
    sleep_seconds: int = 10,
    total_remote_sts: Optional[int] = None,
) -> Tuple[bool, Dict[str, Dict[str, Any]], Dict[str, Any], str]:
    started_at = get_timestamp()
    out: Dict[str, Dict[str, Any]] = {}
    attempts_used_max = 1
    chunks_ok = 0
    chunks_failed = 0
    processed_sts = 0
    selected_total = len(st_ids)

    if not st_ids:
        return True, out, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "chunks_total": 0,
            "chunks_ok": 0,
            "chunks_failed": 0,
        }, ""

    for idx, chunk in enumerate(_chunked(st_ids, step), start=1):
        st_arg = ",".join(str(x) for x in chunk)
        url = (
            f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/{cgname}/sts"
            f"?limit=500&scheme={cgname}&st_id={st_arg}&offset=0"
        )
        ok, attempts_used, payload, err = _fetch_json_with_retry(url=url, logger=logger, auth=auth)
        attempts_used_max = max(attempts_used_max, attempts_used)
        if not ok:
            chunks_failed += 1
            return False, out, {
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "chunks_total": int(np.ceil(len(st_ids) / step)),
                "chunks_ok": chunks_ok,
                "chunks_failed": chunks_failed,
                "failed_chunk_index": idx,
            }, err

        items = (payload or {}).get("STs", [])
        if not isinstance(items, list):
            chunks_failed += 1
            return False, out, {
                "started_at": started_at,
                "finished_at": get_timestamp(),
                "chunks_total": int(np.ceil(len(st_ids) / step)),
                "chunks_ok": chunks_ok,
                "chunks_failed": chunks_failed,
                "failed_chunk_index": idx,
            }, "Invalid 'STs' payload"

        for item in items:
            if not isinstance(item, dict):
                continue
            st_key = str(item.get("ST_id", ""))
            info = item.get("info", {})
            if not st_key or not isinstance(info, dict):
                continue
            hiercc = info.get("hierCC", {})
            if isinstance(hiercc, dict):
                out[st_key] = hiercc
        chunks_ok += 1
        processed_sts += len(chunk)
        if total_remote_sts is not None:
            logger.info(
                "Downloaded ST batches for %d/%d selected STs (candidate STs from selected strains: %d)",
                min(processed_sts, selected_total),
                selected_total,
                total_remote_sts,
            )
        else:
            logger.info(
                "Downloaded ST batches for %d/%d selected STs",
                min(processed_sts, selected_total),
                selected_total,
            )
        if idx * step < len(st_ids):
            time.sleep(sleep_seconds)

    return True, out, {
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "chunks_total": int(np.ceil(len(st_ids) / step)),
        "chunks_ok": chunks_ok,
        "chunks_failed": chunks_failed,
        "attempts_max": attempts_used_max,
    }, ""


@click.command()
@click.option("--workspace", type=str, default=None, help="Workspace path used in report metadata.")
@click.option("--run_id", type=str, default=None, help="Unique run ID (defaults to generated).")
@click.option("--container_image", type=str, default="unknown", help="Container image name (report metadata).")
@click.option("--report_file", type=str, default=None, help="Report JSON file name (defaults to {run_id}.json).")
@click.option("--log_file", type=str, default="enterobase.log", help="Log file name.")
@click.option("--user", type=str, default=None, help="User name (report metadata).")
@click.option("--host", type=str, default=None, help="Host name (report metadata).")
@click.option("-d", "--database", type=click.Choice(["senterica", "ecoli"]), required=True)
@click.option("-g", "--cgname", type=click.Choice(["cgMLST_v2", "cgMLST"]), required=True)
@click.option(
    "-t",
    "--api_token_file",
    default="/home/update/enterobase_api.txt",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Path to EnteroBase API token file.",
)
@click.option("-o", "--output_dir", required=True, type=click.Path(path_type=Path), help="Output directory.")
@click.option(
    "--limit_first_n",
    type=int,
    default=None,
    help="Optional test mode: process only first N strains from remote list.",
)
def main(
    workspace: Optional[str],
    run_id: Optional[str],
    container_image: str,
    report_file: Optional[str],
    log_file: str,
    user: Optional[str],
    host: Optional[str],
    database: str,
    cgname: str,
    api_token_file: Path,
    output_dir: Path,
    limit_first_n: Optional[int],
) -> None:
    out_dir = output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not run_id:
        run_id = generate_run_id("enterobase_historical")

    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)

    if report_file is None:
        report_file = f"{run_id}.json"
    report_dir = out_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    execution_context = {
        "workspace": f"{workspace}/enterobase_historical" if workspace else str(out_dir),
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=SOURCE,
        log_file=f"{execution_context['workspace']}/logs/{log_file}",
    )

    remaining_steps = list(ALL_STEPS)

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

    api_token = _read_first_line(api_token_file)
    if not api_token:
        skip_remaining_steps(remaining_steps, "Skipped: missing EnteroBase API token.")
        rb.fail(code="AUTH_TOKEN_MISSING", message=f"Token file is empty: {api_token_file}", retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return
    auth = _basic_auth_tuple(api_token)

    strains_probe_url = f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/strains?limit=1&offset=0"
    strains_download_url = (
        f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/strains"
        "?my_strains=false&sortorder=asc&return_all=true&offset=0"
    )
    st_probe_url = f"https://enterobase.warwick.ac.uk/api/v2.0/{database}/{cgname}/sts?limit=1&scheme={cgname}&offset=0"

    # 2) DATABASE_AVAILABILITY
    started_at = get_timestamp()
    strains_av = check_url_available(strains_probe_url, retries=3, interval=30, logger=logger, auth=auth)
    sts_av = check_url_available(st_probe_url, retries=3, interval=30, logger=logger, auth=auth)
    db_ok = strains_av["status"] == StatusType.PASSED.value and sts_av["status"] == StatusType.PASSED.value
    db_avail = {
        "status": StatusType.PASSED.value if db_ok else StatusType.FAILED.value,
        "message": "All required endpoints reachable" if db_ok else "One or more endpoints unreachable",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": max(int(strains_av.get("attempts", 1) or 1), int(sts_av.get("attempts", 1) or 1)),
        "retryable": True,
        "metrics": {
            "checks": {
                "strains_probe_url": strains_av,
                "sts_probe_url": sts_av,
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

    # 3) REMOTE_FILES_DOWNLOAD_STATUS
    started_at = get_timestamp()
    ok, attempts_used, payload, err = _fetch_json_with_retry(
        url=strains_download_url,
        logger=logger,
        auth=auth,
        max_retries=3,
        wait_seconds=30,
        timeout_s=900,
    )
    if not ok:
        rem = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download strain list: {err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": attempts_used,
            "retryable": True,
            "metrics": {},
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", rem)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download remote files.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=rem["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    strain_table = (payload or {}).get("Strains", [])
    if not isinstance(strain_table, list):
        strain_table = []
    total_remote = len(strain_table)

    selected = strain_table
    if limit_first_n is not None and limit_first_n >= 0:
        selected = strain_table[:limit_first_n]
    elif limit_first_n is not None and limit_first_n < 0:
        selected = []

    existing_strains_path = out_dir / "strains_table.npy"
    existing_straindata_path = out_dir / "straindata_table.npy"
    existing_sts_path = out_dir / "sts_table.npy"

    existing_strain_rows: List[Dict[str, Any]] = []
    existing_straindata: Dict[str, Dict[str, Any]] = {}
    existing_sts_data: Dict[str, Dict[str, Any]] = {}

    if existing_strains_path.exists():
        try:
            loaded = np.load(existing_strains_path, allow_pickle=True)
            if isinstance(loaded, np.ndarray):
                existing_strain_rows = [x for x in loaded.tolist() if isinstance(x, dict)]
        except Exception as e:
            logger.warning("Failed to load existing strains_table.npy: %s", e)
    if existing_straindata_path.exists():
        try:
            loaded = np.load(existing_straindata_path, allow_pickle=True)
            loaded_obj = loaded.item()
            if isinstance(loaded_obj, dict):
                existing_straindata = {str(k): v for k, v in loaded_obj.items() if isinstance(v, dict)}
        except Exception as e:
            logger.warning("Failed to load existing straindata_table.npy: %s", e)
    if existing_sts_path.exists():
        try:
            loaded = np.load(existing_sts_path, allow_pickle=True)
            loaded_obj = loaded.item()
            if isinstance(loaded_obj, dict):
                existing_sts_data = {str(k): v for k, v in loaded_obj.items() if isinstance(v, dict)}
        except Exception as e:
            logger.warning("Failed to load existing sts_table.npy: %s", e)

    existing_barcodes = {
        str(row.get("strain_barcode", ""))
        for row in existing_strain_rows
        if isinstance(row, dict) and row.get("strain_barcode")
    }
    selected_rows = [row for row in selected if isinstance(row, dict)]
    selected_barcodes = [str(row.get("strain_barcode", "")) for row in selected_rows if row.get("strain_barcode")]
    selected_barcodes = list(dict.fromkeys(selected_barcodes))

    novel_rows = [row for row in selected_rows if str(row.get("strain_barcode", "")) not in existing_barcodes]
    barcodes = [str(row.get("strain_barcode", "")) for row in novel_rows if row.get("strain_barcode")]
    barcodes = list(dict.fromkeys(barcodes))
    logger.info(
        "Remote strains total: %d; selected scope: %d; local known: %d; novel to download: %d (limit_first_n=%s)",
        total_remote,
        len(selected_barcodes),
        len(existing_barcodes),
        len(barcodes),
        str(limit_first_n),
    )

    straindata_ok, straindata, straindata_metrics, err = _download_straindata(
        database=database,
        barcodes=barcodes,
        auth=auth,
        logger=logger,
        total_remote_strains=total_remote,
    )
    if not straindata_ok:
        rem = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download straindata: {err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": int(straindata_metrics.get("attempts_max", 1) or 1),
            "retryable": True,
            "metrics": {
                "total_remote_strains": total_remote,
                "requested_limit_first_n": limit_first_n,
                "effective_selected_strains": len(barcodes),
                "straindata": straindata_metrics,
            },
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", rem)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download remote files.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=rem["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    merged_straindata = dict(existing_straindata)
    merged_straindata.update(straindata)

    known_sts_all, missing_sts_field = _extract_st_ids_for_scheme(straindata=merged_straindata, cgname=cgname)
    missing_st_ids = [st for st in known_sts_all if str(st) not in existing_sts_data]
    logger.info(
        "pHierCC/ST summary: candidate ST IDs=%d, already local=%d, novel to download=%d",
        len(known_sts_all),
        len(existing_sts_data),
        len(missing_st_ids),
    )
    if not missing_st_ids:
        logger.info("No new pHierCC/ST IDs to download; existing sts_table.npy is already up to date for current scope.")
    else:
        logger.info("Will download pHierCC/ST data for %d novel ST IDs.", len(missing_st_ids))
    sts_ok, sts_data, sts_metrics, err = _download_sts(
        database=database,
        cgname=cgname,
        st_ids=missing_st_ids,
        auth=auth,
        logger=logger,
        total_remote_sts=len(known_sts_all),
    )
    if not sts_ok:
        rem = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download ST data: {err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": int(sts_metrics.get("attempts_max", 1) or 1),
            "retryable": True,
            "metrics": {
                "total_remote_strains": total_remote,
                "requested_limit_first_n": limit_first_n,
                "effective_selected_strains": len(barcodes),
                "known_st_candidates_total": len(known_sts_all),
                "novel_st_ids_to_download": len(missing_st_ids),
                "sts": sts_metrics,
            },
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", rem)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download remote files.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=rem["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    merged_sts_data = dict(existing_sts_data)
    merged_sts_data.update(sts_data)

    strains_by_barcode: Dict[str, Dict[str, Any]] = {}
    for row in existing_strain_rows:
        barcode = str(row.get("strain_barcode", "")) if isinstance(row, dict) else ""
        if barcode:
            strains_by_barcode[barcode] = row
    for row in selected_rows:
        barcode = str(row.get("strain_barcode", "")) if isinstance(row, dict) else ""
        if barcode:
            strains_by_barcode[barcode] = row
    merged_strain_rows = [strains_by_barcode[k] for k in sorted(strains_by_barcode.keys())]

    rem = {
        "status": StatusType.PASSED.value,
        "message": "Downloaded strains, straindata and ST tables.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": max(int(straindata_metrics.get("attempts_max", 1) or 1), int(sts_metrics.get("attempts_max", 1) or 1)),
        "retryable": True,
        "metrics": {
            "total_remote_strains": total_remote,
            "requested_limit_first_n": limit_first_n,
            "effective_selected_scope": len(selected_barcodes),
            "effective_novel_strains_downloaded": len(barcodes),
            "missing_sts_field_count": missing_sts_field,
            "known_st_candidates_total": len(known_sts_all),
            "novel_st_ids_to_download": len(missing_st_ids),
            "local_existing_strains": len(existing_barcodes),
            "local_existing_sts": len(existing_sts_data),
            "pydantic_enabled": HAS_PYDANTIC,
            "selection_policy": "stable_first_n",
            "straindata": straindata_metrics,
            "sts": sts_metrics,
        },
    }
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", rem)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")

    # Prepare temp outputs before update decision
    tmp_dir = out_dir / f".tmp_enterobase_{run_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_strains = tmp_dir / "strains_table.npy"
    tmp_straindata = tmp_dir / "straindata_table.npy"
    tmp_sts = tmp_dir / "sts_table.npy"

    np.save(tmp_strains, merged_strain_rows, allow_pickle=True)
    np.save(tmp_straindata, merged_straindata, allow_pickle=True)
    np.save(tmp_sts, merged_sts_data, allow_pickle=True)

    expected = list(SOURCE["expected_processed_files"])
    checksums_before = _load_checksum_list(base_dir=out_dir, rel_files=expected)
    checksums_after_tmp = [
        {"file_name": "strains_table.npy", "checksum": file_md5sum(str(tmp_strains))},
        {"file_name": "straindata_table.npy", "checksum": file_md5sum(str(tmp_straindata))},
        {"file_name": "sts_table.npy", "checksum": file_md5sum(str(tmp_sts))},
    ]
    before_map = {x["file_name"]: x["checksum"] for x in checksums_before}
    changed_files = [
        item["file_name"] for item in checksums_after_tmp if before_map.get(item["file_name"], "") != item["checksum"]
    ]
    decision_changed = bool(changed_files) or len(checksums_before) != len(checksums_after_tmp)

    # 4) UPDATE_STATUS (always rebuild, report-level changed/no_change)
    started_at = get_timestamp()
    update_message = (
        "Always rebuild policy executed; content changed."
        if decision_changed
        else "Always rebuild policy executed; content unchanged."
    )
    upd = {
        "status": StatusType.PASSED.value,
        "message": update_message,
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "update_required": True,
            "update_decision_label": "changed" if decision_changed else "no_change",
            "changed_files": changed_files,
            "requested_limit_first_n": limit_first_n,
            "effective_selected_scope": len(selected_barcodes),
            "effective_novel_strains_downloaded": len(barcodes),
            "total_remote_strains": total_remote,
            "processed_st_count": len(merged_sts_data),
        },
    }
    rb.add_named_milestone("UPDATE_STATUS", upd)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(
        mode="always_rebuild",
        result="updated" if decision_changed else "latest_version_present",
        message=f"{update_message} decision={'changed' if decision_changed else 'no_change'}",
        first_build=len(checksums_before) == 0,
        checksums_before=checksums_before,
        checksums_after=checksums_after_tmp,
    )

    # 5) PROCESSING_STATUS
    started_at = get_timestamp()
    targets = [
        out_dir / "strains_table.npy",
        out_dir / "straindata_table.npy",
        out_dir / "sts_table.npy",
    ]
    backups: List[Tuple[Path, Path]] = []
    try:
        backups = _backup_paths(targets, logger=logger)
        (tmp_strains).replace(out_dir / "strains_table.npy")
        (tmp_straindata).replace(out_dir / "straindata_table.npy")
        (tmp_sts).replace(out_dir / "sts_table.npy")
    except Exception as e:
        _restore_backups(backups, logger=logger)
        proc = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to replace final files (restored backups): {e}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"tmp_dir": str(tmp_dir), "backup_count": len(backups)},
        }
        rb.add_named_milestone("PROCESSING_STATUS", proc)
        remaining_steps.remove("PROCESSING_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return
    finally:
        for path in [tmp_strains, tmp_straindata, tmp_sts]:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        try:
            if tmp_dir.exists():
                tmp_dir.rmdir()
        except Exception:
            pass

    proc = {
        "status": StatusType.PASSED.value,
        "message": "Saved EnteroBase numpy tables.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "strains_rows": len(merged_strain_rows),
            "straindata_rows": len(merged_straindata),
            "sts_rows": len(merged_sts_data),
            "backup_count": len(backups),
        },
    }
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # 6) FINAL_STATUS
    final = verify_expected_files(base_dir=out_dir, expected_files=expected)
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")
    if final["status"] != StatusType.PASSED.value:
        _restore_backups(backups, logger=logger)
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    checksums_after_final = _load_checksum_list(base_dir=out_dir, rel_files=expected)
    manifest_path = _write_md5_manifest(out_dir=out_dir, checksums=checksums_after_final)
    logger.info("Wrote checksum manifest: %s", manifest_path)
    _remove_backup_files(backups, logger=logger)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()
