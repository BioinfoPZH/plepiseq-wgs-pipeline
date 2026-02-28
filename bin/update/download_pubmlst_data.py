#!/usr/bin/env python3
"""
Download isolate provenance and HierCC clustering data from PubMLST.

This script fetches Campylobacter isolate metadata (straindata) and cgMLST
classification-scheme profiles (HierCC-equivalent) from the PubMLST REST API,
merging new records into existing local numpy tables so that the server is not
re-queried for data already present.

Rate-limiting policy
--------------------
PubMLST enforces a maximum of **4 simultaneous connections** (since Oct 2023).
Additional connections are automatically rejected.  Workers in this script are
hard-capped at 4 to comply.  A short sleep is inserted every 1000 individual
isolate requests to avoid sustained high-frequency traffic.

Data-access policy (effective Jan 2025)
---------------------------------------
Post-2024 records require OAuth1 authentication.  Pass
``--credentials_file`` pointing to a key=value file with at least
``client_id`` and ``client_secret``.
See: https://pubmlst.org/change-data-access-policy
"""

from __future__ import annotations

import getpass
import json
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import click
import numpy as np
import requests

from utils.net import HEADERS, StatusType, check_url_available
from utils.report import ALL_STEPS, SCHEMA_VERSION, ReportBuilder
from utils.run_id import generate_run_id
from utils.setup_logging import _setup_logging
from utils.updates_helpers import (
    composite_availability_check,
    file_md5sum,
    get_pubmlst_oauth,
    parse_credentials_file,
)
from utils.validation import get_timestamp, verify_expected_files

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class HierCCEntryModel(BaseModel):
    """Validates a single HierCC clustering entry."""
    model_config = ConfigDict(extra="allow")
    d0: Union[int, str] = "unk"
    d5: Union[int, str] = "unk"
    d10: Union[int, str] = "unk"
    d25: Union[int, str] = "unk"
    d50: Union[int, str] = "unk"
    d100: Union[int, str] = "unk"
    d200: Union[int, str] = "unk"


class STSchemeModel(BaseModel):
    """Validates a single ST scheme assignment."""
    model_config = ConfigDict(extra="allow")
    scheme_name: str
    st_id: Union[int, str]


class IsolateEntryModel(BaseModel):
    """Validates a fully-processed isolate record."""
    model_config = ConfigDict(extra="allow")
    isolate_id: str
    country: str = "Unknown"
    date_entered: str = ""
    year: str = ""
    sequencing: str = "No data"
    sts: List[STSchemeModel] = []
    hiercc: Optional[HierCCEntryModel] = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PUBMLST_BASE = "https://rest.pubmlst.org"

DATABASE = {"name": "pubmlst_isolate_data", "category": "typing"}
SOURCE = {
    "source_type": "https",
    "reference": PUBMLST_BASE,
    "expected_raw_files": ["timestamp"],
    "expected_processed_files": ["straindata_table.npy", "sts_table.npy", "timestamp"],
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _get_json(
    url: str,
    *,
    logger,
    auth: Any = None,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    wait_seconds: int = 30,
    timeout_s: int = 120,
) -> Tuple[bool, int, Optional[Any], str]:
    """GET *url*, parse JSON, retry on transient errors.  Returns ``(ok, attempts, payload, err)``."""
    err = ""
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout_s, auth=auth)
            if r.status_code == 401 or r.status_code == 403:
                # OAuth rejected – retry once without auth later
                err = f"HTTP {r.status_code}"
                logger.warning("HTTP %s for %s; may need valid OAuth credentials.", r.status_code, url)
                break
            if r.status_code != 200:
                err = f"HTTP {r.status_code}"
                raise RuntimeError(err)
            return True, attempt, r.json(), ""
        except Exception as e:
            err = str(e)
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, url, err)
            if attempt < max_retries:
                time.sleep(wait_seconds)
    return False, max_retries, None, err


# ---------------------------------------------------------------------------
# Profile (HierCC / sts_table) download
# ---------------------------------------------------------------------------


def _download_profiles(
    *,
    scheme_link: str,
    previous_update: str,
    logger,
    auth: Any = None,
    limit_first_n: Optional[int] = None,
) -> Tuple[bool, Dict[str, Dict[str, Any]], Dict[str, Any], str]:
    """
    Download cgMLST profiles updated since *previous_update* from PubMLST.

    Each profile yields a dict of ``{dN: group}`` entries keyed by ``str(cgST)``.
    When *limit_first_n* is set, only the first N profile URIs are processed
    (useful for testing without downloading the full dataset).
    """
    started_at = get_timestamp()

    # First request: discover how many records are available
    ok, attempts, payload, err = _get_json(
        scheme_link + "/profiles",
        logger=logger,
        auth=auth,
        params={"updated_after": previous_update},
    )
    if not ok or payload is None:
        return False, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "error": err,
        }, err

    record_count = int(payload.get("records", 0))
    logger.info("Profiles available since %s: %d", previous_update, record_count)

    if record_count == 0:
        return True, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "profiles_available": 0,
            "profiles_downloaded": 0,
        }, ""

    # Second request: fetch all profile URIs (with page_size = record_count)
    ok, attempts, payload, err = _get_json(
        scheme_link + "/profiles",
        logger=logger,
        auth=auth,
        params={"page_size": record_count, "updated_after": previous_update},
    )
    if not ok or payload is None:
        return False, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "error": err,
        }, err

    profile_uris: List[str] = payload.get("profiles", [])
    total_available = len(profile_uris)

    if limit_first_n is not None and limit_first_n >= 0:
        profile_uris = profile_uris[:limit_first_n]
        logger.info(
            "Test mode: limiting profiles to first %d of %d available (limit_first_n=%d)",
            len(profile_uris), total_available, limit_first_n,
        )

    out: Dict[str, Dict[str, Any]] = {}
    failed_profiles: List[str] = []
    i = 0

    for profile_uri in profile_uris:
        if i > 0 and i % 1000 == 0:
            logger.info("Downloaded %d/%d profiles", i, len(profile_uris))
            time.sleep(4)
        i += 1

        ok_p, _a, profile_data, err_p = _get_json(profile_uri, logger=logger, auth=auth, timeout_s=60)
        if not ok_p or profile_data is None:
            failed_profiles.append(profile_uri)
            continue

        try:
            cgst_key = str(profile_data["cgST"])
            entry: Dict[str, Any] = {}
            for level, level_data in profile_data.get("classification_schemes", {}).items():
                level_number = level.split("_")[-1]
                level_str = f"d{level_number}"
                entry[level_str] = level_data["group"]["group"]
            out[cgst_key] = entry
        except Exception as exc:
            failed_profiles.append(profile_uri)
            logger.warning("Failed to parse profile %s: %s", profile_uri, exc)

    finished_at = get_timestamp()
    metrics: Dict[str, Any] = {
        "started_at": started_at,
        "finished_at": finished_at,
        "profiles_available": record_count,
        "profiles_selected": len(profile_uris),
        "profiles_downloaded": len(out),
        "profiles_failed": len(failed_profiles),
        "limit_first_n": limit_first_n,
    }
    if failed_profiles:
        logger.warning("%d profile(s) failed to download.", len(failed_profiles))

    return True, out, metrics, ""


# ---------------------------------------------------------------------------
# Isolate (straindata_table) download
# ---------------------------------------------------------------------------


def _parse_single_isolate(
    isolate_info: Dict[str, Any],
    cgmlst_scheme_name: str,
    mlst_scheme_name: str,
) -> Dict[str, Any]:
    """
    Extract provenance, ST and HierCC data from a single isolate API response.
    Returns the processed dict for one isolate.
    """
    prov = isolate_info.get("provenance", {})

    isolate_id = prov.get("isolate", "")
    country = prov.get("country", "Unknown")
    date_entered = prov.get("date_entered", "")
    year = str(prov.get("year", "")) or date_entered.split("-")[0] if date_entered else ""

    entry: Dict[str, Any] = {
        "isolate_id": isolate_id,
        "country": country,
        "date_entered": date_entered,
        "year": year,
    }

    # Sequencing / biosample accession (optional)
    try:
        entry["sequencing"] = isolate_info["isolate_info"]["biosample_accession"][1]
    except (KeyError, IndexError, TypeError):
        entry["sequencing"] = "No data"

    # Scheme assignments
    entry["sts"] = []
    mlst_found = False
    cgmlst_found = False
    hiercc_found = False

    for scheme in isolate_info.get("schemes", []):
        desc = scheme.get("description", "")

        # MLST
        if desc == mlst_scheme_name:
            fields = scheme.get("fields", {})
            if "ST" in fields:
                mlst_found = True
                entry["sts"].append({"scheme_name": mlst_scheme_name, "st_id": fields["ST"]})

        # cgMLST + HierCC
        elif desc == cgmlst_scheme_name:
            level_0: Union[int, str] = "unk"
            fields = scheme.get("fields", {})
            if "cgST" in fields:
                cgst_val = fields["cgST"]
                if isinstance(cgst_val, list):
                    cgst_val = cgst_val[0]
                if isinstance(cgst_val, (int, str)):
                    cgmlst_found = True
                    entry["sts"].append({"scheme_name": cgmlst_scheme_name, "st_id": cgst_val})
                    level_0 = cgst_val

            if "classification_schemes" in scheme:
                cs = scheme["classification_schemes"]
                hiercc: Dict[str, Any] = {"d0": level_0}
                try:
                    for prefix, key in [
                        ("d5", "Cjc_cgc2_5"),
                        ("d10", "Cjc_cgc2_10"),
                        ("d25", "Cjc_cgc2_25"),
                        ("d50", "Cjc_cgc2_50"),
                        ("d100", "Cjc_cgc2_100"),
                        ("d200", "Cjc_cgc2_200"),
                    ]:
                        if key in cs:
                            hiercc[prefix] = cs[key]["groups"][0]["group"]
                        else:
                            hiercc[prefix] = "unk"
                    entry["hiercc"] = hiercc
                    hiercc_found = True
                except Exception:
                    pass

    # Fill missing scheme data with sentinel values
    if not mlst_found:
        entry["sts"].append({"scheme_name": mlst_scheme_name, "st_id": "unk"})
    if not cgmlst_found:
        entry["sts"].append({"scheme_name": cgmlst_scheme_name, "st_id": "unk"})
    if not hiercc_found:
        entry["hiercc"] = {
            "d0": "unk", "d5": "unk", "d10": "unk",
            "d25": "unk", "d50": "unk", "d100": "unk", "d200": "unk",
        }

    return entry


def _validate_isolate_entry(value: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a processed isolate record through Pydantic when available."""
    try:
        IsolateEntryModel.model_validate(value)
        return True, None
    except ValidationError as e:
        return False, str(e)


def _download_isolates(
    *,
    isolates_db: str,
    cgmlst_scheme_name: str,
    mlst_scheme_name: str,
    previous_update: str,
    logger,
    auth: Any = None,
    limit_first_n: Optional[int] = None,
) -> Tuple[bool, Dict[str, Dict[str, Any]], Dict[str, Any], str]:
    """
    Download isolate records updated since *previous_update*.

    Returns ``(ok, isolates_dict, metrics, err)`` where *isolates_dict* is
    keyed by the PubMLST isolate numeric ID (as string).
    When *limit_first_n* is set, only the first N isolate URIs are processed
    (useful for testing without downloading the full dataset).
    """
    started_at = get_timestamp()

    base_url = f"{PUBMLST_BASE}/db/{isolates_db}/isolates"

    # Discover record count
    ok, attempts, payload, err = _get_json(
        base_url,
        logger=logger,
        auth=auth,
        params={"updated_after": previous_update},
    )
    if not ok or payload is None:
        return False, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "error": err,
        }, err

    total_count = int(payload.get("records", 0))
    logger.info("Isolates available since %s: %d", previous_update, total_count)

    if total_count == 0:
        return True, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "isolates_available": 0,
            "isolates_downloaded": 0,
        }, ""

    # Fetch full list of isolate URIs
    ok, attempts, payload, err = _get_json(
        base_url,
        logger=logger,
        auth=auth,
        params={"page_size": total_count, "updated_after": previous_update},
    )
    if not ok or payload is None:
        return False, {}, {
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "error": err,
        }, err

    isolate_uris: List[str] = payload.get("isolates", [])
    total_available = len(isolate_uris)

    if limit_first_n is not None and limit_first_n >= 0:
        isolate_uris = isolate_uris[:limit_first_n]
        logger.info(
            "Test mode: limiting isolates to first %d of %d available (limit_first_n=%d)",
            len(isolate_uris), total_available, limit_first_n,
        )

    out: Dict[str, Dict[str, Any]] = {}
    failed_uris: List[str] = []
    invalid_records = 0
    invalid_examples: List[str] = []
    i = 0

    for uri in isolate_uris:
        if i > 0 and i % 1000 == 0:
            logger.info("Downloaded %d/%d isolates", i, len(isolate_uris))
            # Throttle to avoid overwhelming PubMLST (max 4 simultaneous connections)
            time.sleep(4)
        i += 1

        ok_i, _a, isolate_info, err_i = _get_json(uri, logger=logger, auth=auth, timeout_s=60)
        if not ok_i or isolate_info is None:
            failed_uris.append(uri)
            continue

        try:
            isolate_key = str(isolate_info["provenance"]["id"])
            entry = _parse_single_isolate(isolate_info, cgmlst_scheme_name, mlst_scheme_name)

            # Pydantic validation (when available)
            valid, reason = _validate_isolate_entry(entry)
            if not valid:
                invalid_records += 1
                if len(invalid_examples) < 5:
                    invalid_examples.append(f"{isolate_key}: {reason}")
                # Still store the record – validation is advisory
                logger.warning("Validation issue for isolate %s: %s", isolate_key, reason)

            out[isolate_key] = entry
        except Exception as exc:
            failed_uris.append(uri)
            logger.warning("Error processing isolate %s: %s", uri, exc)

    # Retry failed isolates once after a pause
    if failed_uris:
        logger.info("Retrying %d failed isolate(s) after 60s pause...", len(failed_uris))
        time.sleep(60)
        retry_remaining: List[str] = []
        for uri in failed_uris:
            ok_r, _a, isolate_info, _ = _get_json(uri, logger=logger, auth=auth, timeout_s=60)
            if not ok_r or isolate_info is None:
                retry_remaining.append(uri)
                continue
            try:
                isolate_key = str(isolate_info["provenance"]["id"])
                entry = _parse_single_isolate(isolate_info, cgmlst_scheme_name, mlst_scheme_name)
                out[isolate_key] = entry
            except Exception:
                retry_remaining.append(uri)
        if retry_remaining:
            logger.warning("%d isolate(s) still failed after retry.", len(retry_remaining))
        failed_uris = retry_remaining

    finished_at = get_timestamp()
    metrics: Dict[str, Any] = {
        "started_at": started_at,
        "finished_at": finished_at,
        "isolates_available": total_count,
        "isolates_selected": len(isolate_uris),
        "isolates_downloaded": len(out),
        "isolates_failed": len(failed_uris),
        "invalid_records": invalid_records,
        "invalid_examples": invalid_examples,
        "pydantic_enabled": True,
        "limit_first_n": limit_first_n,
    }
    return True, out, metrics, ""


# ---------------------------------------------------------------------------
# Backup / restore helpers (same pattern as download_enterobase_data.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Checksum manifest helpers
# ---------------------------------------------------------------------------


def _load_checksum_list(*, base_dir: Path, rel_files: List[str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for rel in rel_files:
        p = base_dir / rel
        if p.exists():
            out.append({"file_name": rel, "checksum": file_md5sum(str(p))})
    return out


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
    manifest_path = out_dir / "pubmlst_isolatedata_md5.json"
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return manifest_path


# ---------------------------------------------------------------------------
# Existing .npy loading (numpy 2.x compatible)
# ---------------------------------------------------------------------------


def _load_npy_dict(path: Path, logger) -> Dict[str, Any]:
    """Load a dict that was saved as ``np.save(path, dict_obj, allow_pickle=True)``."""
    if not path.exists():
        return {}
    try:
        loaded = np.load(path, allow_pickle=True)
        obj = loaded.item()
        if isinstance(obj, dict):
            return {str(k): v for k, v in obj.items()}
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
    return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.option("--workspace", type=str, default=None, help="Workspace path used in report metadata.")
@click.option("--run_id", type=str, default=None, help="Unique run ID (defaults to generated).")
@click.option("--container_image", type=str, default="unknown", help="Container image name (report metadata).")
@click.option("--report_file", type=str, default=None, help="Report JSON file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, default=None, help="User name (report metadata).")
@click.option("--host", type=str, default=None, help="Host name (report metadata).")
@click.option("--credentials_file", type=click.Path(), default="/home/update/credentials.txt", show_default=True, help="Path to key=value credentials file (PubMLST keys: client_id, client_secret).")
@click.option("--download_workers", type=int, default=4, show_default=True, help="Max concurrent workers (capped at 4).")
@click.option("-o", "--output_dir", required=True, type=click.Path(), help="Output directory.")
@click.option(
    "-d",
    "--isolates_database",
    default="pubmlst_campylobacter_isolates",
    show_default=True,
    help="PubMLST isolates database name.",
)
@click.option(
    "-s",
    "--seqdef_database",
    default="pubmlst_campylobacter_seqdef",
    show_default=True,
    help="PubMLST sequence definition database name.",
)
@click.option(
    "--cgmlst_scheme_name",
    default="C. jejuni / C. coli cgMLST v2",
    show_default=True,
    help="Name of the cgMLST scheme in PubMLST.",
)
@click.option(
    "--mlst_scheme_name",
    default="MLST",
    show_default=True,
    help="Name of the MLST scheme in PubMLST.",
)
@click.option(
    "--limit_first_n",
    type=int,
    default=None,
    help="Optional test mode: process only first N profiles and first N isolates from remote list.",
)
def main(
    workspace: Optional[str],
    run_id: Optional[str],
    container_image: str,
    report_file: Optional[str],
    log_file: str,
    user: Optional[str],
    host: Optional[str],
    credentials_file: str,
    download_workers: int,
    output_dir: str,
    isolates_database: str,
    seqdef_database: str,
    cgmlst_scheme_name: str,
    mlst_scheme_name: str,
    limit_first_n: Optional[int],
) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    # Logging
    log_dir = out_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix
    logger = _setup_logging(output_dir=log_dir, filename=log_file)
    logger.info("Starting PubMLST isolate data downloader")
    if limit_first_n is not None:
        logger.info("TEST MODE: limit_first_n=%d — only first %d profiles and isolates will be downloaded.", limit_first_n, limit_first_n)

    # Report setup
    if report_file is None or run_id not in (report_file or ""):
        report_file = f"{run_id}.json"
    report_dir = out_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    execution_context = {
        "workspace": f"{workspace}/{DATABASE['name']}" if workspace else str(out_dir),
        "user": user or getpass.getuser(),
        "host": host or socket.gethostname(),
        "container_image": container_image,
    }

    remaining_steps = list(ALL_STEPS)
    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=SOURCE,
        log_file=f"{execution_context['workspace']}/logs/{log_file}",
    )

    def skip_remaining_steps(steps: List[str], reason: str) -> None:
        for s in steps:
            rb.add_skipped(s, reason)

    credentials = parse_credentials_file(Path(credentials_file), logger)
    oauth_auth = get_pubmlst_oauth(credentials, logger)

    # Hard cap at 4 per PubMLST policy
    if download_workers < 1:
        download_workers = 1
    if download_workers > 4:
        logger.warning("download_workers=%d exceeds PubMLST guidance; capping to 4.", download_workers)
        download_workers = 4

    current_date = time.strftime("%Y-%m-%d", time.gmtime())

    # Paths
    straindata_path = out_dir / "straindata_table.npy"
    sts_path = out_dir / "sts_table.npy"
    timestamp_path = out_dir / "timestamp"

    schemes_url = f"{PUBMLST_BASE}/db/{seqdef_database}/schemes"
    isolates_url = f"{PUBMLST_BASE}/db/{isolates_database}/isolates"

    # -----------------------------------------------------------------------
    # 1) PREFLIGHT_CONNECTIVITY
    # -----------------------------------------------------------------------
    pre = check_url_available("https://www.google.com", retries=3, interval=30, logger=logger)
    rb.add_named_milestone("PREFLIGHT_CONNECTIVITY", pre)
    remaining_steps.remove("PREFLIGHT_CONNECTIVITY")
    if pre["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed preflight connectivity.")
        rb.fail(code="NO_INTERNET", message=pre.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------------------------------------------------
    # 2) DATABASE_AVAILABILITY
    # -----------------------------------------------------------------------
    db_avail = composite_availability_check(
        [PUBMLST_BASE, schemes_url, isolates_url],
        logger,
        retries=3,
        interval=30,
    )
    rb.add_named_milestone("DATABASE_AVAILABILITY", db_avail)
    remaining_steps.remove("DATABASE_AVAILABILITY")
    if db_avail["status"] != StatusType.PASSED.value:
        skip_remaining_steps(remaining_steps, "Skipped due to failed database availability check.")
        rb.fail(code="DATABASE_UNAVAILABLE", message=db_avail.get("message", ""), retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------------------------------------------------
    # 3) REMOTE_FILES_DOWNLOAD_STATUS
    # -----------------------------------------------------------------------
    started_at = get_timestamp()

    # Read previous update timestamp (for incremental download)
    if timestamp_path.exists():
        previous_update = timestamp_path.read_text(encoding="utf-8").strip()
    else:
        previous_update = "1990-01-01"
    logger.info("Previous update timestamp: %s", previous_update)

    # Find scheme link for cgMLST
    ok, _a, schemes_payload, err = _get_json(schemes_url, logger=logger, auth=oauth_auth)
    if not ok or schemes_payload is None:
        dl_fail = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to fetch schemes list: {err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": True,
            "metrics": {"url": schemes_url},
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_fail)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download metadata.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=dl_fail["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    scheme_link = ""
    for scheme in schemes_payload.get("schemes", []):
        if scheme.get("description") == cgmlst_scheme_name:
            scheme_link = scheme.get("scheme", "")
            break

    if not scheme_link:
        dl_fail = {
            "status": StatusType.FAILED.value,
            "message": f"Could not find scheme link for '{cgmlst_scheme_name}'",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"scheme_name": cgmlst_scheme_name},
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_fail)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: scheme not found.")
        rb.fail(code="SCHEME_NOT_FOUND", message=dl_fail["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Load existing data
    existing_sts = _load_npy_dict(sts_path, logger)
    existing_straindata = _load_npy_dict(straindata_path, logger)
    logger.info(
        "Loaded existing data: %d sts entries, %d straindata entries",
        len(existing_sts),
        len(existing_straindata),
    )

    # Download profiles (HierCC / sts_table)
    profiles_ok, new_profiles, profiles_metrics, profiles_err = _download_profiles(
        scheme_link=scheme_link,
        previous_update=previous_update,
        logger=logger,
        auth=oauth_auth,
        limit_first_n=limit_first_n,
    )
    if not profiles_ok:
        dl_fail = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download profiles: {profiles_err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": True,
            "metrics": {"profiles": profiles_metrics},
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_fail)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download profiles.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=dl_fail["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Merge profiles
    merged_sts = dict(existing_sts)
    merged_sts.update(new_profiles)

    # Download isolates (straindata_table)
    isolates_ok, new_isolates, isolates_metrics, isolates_err = _download_isolates(
        isolates_db=isolates_database,
        cgmlst_scheme_name=cgmlst_scheme_name,
        mlst_scheme_name=mlst_scheme_name,
        previous_update=previous_update,
        logger=logger,
        auth=oauth_auth,
        limit_first_n=limit_first_n,
    )
    if not isolates_ok:
        dl_fail = {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download isolates: {isolates_err}",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": True,
            "metrics": {"profiles": profiles_metrics, "isolates": isolates_metrics},
        }
        rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", dl_fail)
        remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")
        skip_remaining_steps(remaining_steps, "Skipped: failed to download isolates.")
        rb.fail(code="REMOTE_FILES_DOWNLOAD_FAILED", message=dl_fail["message"], retry_recommended=True)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Merge isolates
    merged_straindata = dict(existing_straindata)
    merged_straindata.update(new_isolates)

    rem = {
        "status": StatusType.PASSED.value,
        "message": "Downloaded profiles and isolates from PubMLST.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": True,
        "metrics": {
            "previous_update": previous_update,
            "current_date": current_date,
            "limit_first_n": limit_first_n,
            "existing_sts_count": len(existing_sts),
            "existing_straindata_count": len(existing_straindata),
            "new_profiles_count": len(new_profiles),
            "new_isolates_count": len(new_isolates),
            "merged_sts_count": len(merged_sts),
            "merged_straindata_count": len(merged_straindata),
            "pydantic_enabled": True,
            "profiles": profiles_metrics,
            "isolates": isolates_metrics,
        },
    }
    rb.add_named_milestone("REMOTE_FILES_DOWNLOAD_STATUS", rem)
    remaining_steps.remove("REMOTE_FILES_DOWNLOAD_STATUS")

    # -----------------------------------------------------------------------
    # 4) UPDATE_STATUS
    # -----------------------------------------------------------------------
    started_at = get_timestamp()

    # Detect whether limit_first_n caused records to be skipped.
    # When the limit truncated the available records we must NOT advance the
    # timestamp — otherwise the skipped records fall behind the new timestamp
    # and are permanently lost (they won't appear in future updated_after
    # queries).
    profiles_available = int(profiles_metrics.get("profiles_available", 0))
    profiles_selected = int(profiles_metrics.get("profiles_selected", profiles_available))
    isolates_available = int(isolates_metrics.get("isolates_available", 0))
    isolates_selected = int(isolates_metrics.get("isolates_selected", isolates_available))

    profiles_skipped = profiles_available - profiles_selected
    isolates_skipped = isolates_available - isolates_selected
    limit_active = limit_first_n is not None
    records_truncated = (profiles_skipped > 0) or (isolates_skipped > 0)

    # Decide whether to advance the timestamp
    # Only advance when ALL available records were downloaded (no truncation)
    advance_timestamp = not records_truncated

    if records_truncated:
        logger.warning(
            "limit_first_n=%s caused truncation: %d/%d profiles and %d/%d isolates downloaded. "
            "Timestamp will NOT be advanced so remaining records are picked up on the next run.",
            limit_first_n, profiles_selected, profiles_available,
            isolates_selected, isolates_available,
        )

    # Save merged data to temp files for checksum comparison
    tmp_dir = out_dir / f".tmp_pubmlst_{run_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_straindata = tmp_dir / "straindata_table.npy"
    tmp_sts = tmp_dir / "sts_table.npy"

    np.save(tmp_straindata, merged_straindata, allow_pickle=True)
    np.save(tmp_sts, merged_sts, allow_pickle=True)

    expected = ["straindata_table.npy", "sts_table.npy"]
    checksums_before = _load_checksum_list(base_dir=out_dir, rel_files=expected)
    checksums_after_tmp = [
        {"file_name": "straindata_table.npy", "checksum": file_md5sum(str(tmp_straindata))},
        {"file_name": "sts_table.npy", "checksum": file_md5sum(str(tmp_sts))},
    ]
    before_map = {x["file_name"]: x["checksum"] for x in checksums_before}
    changed_files = [
        item["file_name"]
        for item in checksums_after_tmp
        if before_map.get(item["file_name"], "") != item["checksum"]
    ]
    first_build = len(checksums_before) == 0
    decision_changed = bool(changed_files) or first_build

    effective_timestamp = current_date if advance_timestamp else previous_update

    if records_truncated:
        update_message = (
            f"Incremental update with limit_first_n={limit_first_n}: "
            f"{profiles_selected}/{profiles_available} profiles and "
            f"{isolates_selected}/{isolates_available} isolates downloaded. "
            f"Timestamp NOT advanced (remains {previous_update}); "
            f"remaining records will be available on next run."
        )
    elif decision_changed:
        update_message = (
            f"Incremental update from timestamp {previous_update}: "
            f"content changed ({len(new_profiles)} new profiles, {len(new_isolates)} new isolates). "
            f"Timestamp advanced to {current_date}."
        )
    else:
        update_message = (
            f"Incremental update from timestamp {previous_update}: content unchanged."
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
            "first_build": first_build,
            "new_profiles_count": len(new_profiles),
            "new_isolates_count": len(new_isolates),
            "limit_first_n": limit_first_n,
            "limit_active": limit_active,
            "records_truncated": records_truncated,
            "profiles_available": profiles_available,
            "profiles_selected": profiles_selected,
            "profiles_skipped": profiles_skipped,
            "isolates_available": isolates_available,
            "isolates_selected": isolates_selected,
            "isolates_skipped": isolates_skipped,
            "timestamp_previous": previous_update,
            "timestamp_effective": effective_timestamp,
            "timestamp_advanced": advance_timestamp,
        },
    }
    rb.add_named_milestone("UPDATE_STATUS", upd)
    remaining_steps.remove("UPDATE_STATUS")
    rb.set_update_decision(
        mode="timestamp",
        result="updated" if decision_changed else "latest_version_present",
        message=update_message,
        first_build=first_build,
        checksums_before=checksums_before,
        checksums_after=checksums_after_tmp,
        timestamp_local=previous_update,
        timestamp_remote=effective_timestamp,
    )

    # -----------------------------------------------------------------------
    # 5) PROCESSING_STATUS
    # -----------------------------------------------------------------------
    started_at = get_timestamp()

    targets = [straindata_path, sts_path]
    backups: List[Tuple[Path, Path]] = []
    try:
        backups = _backup_paths(targets, logger=logger)
        tmp_straindata.replace(straindata_path)
        tmp_sts.replace(sts_path)

        # Write timestamp — only advance when all available records were
        # consumed.  When limit_first_n truncated the download, keep the old
        # timestamp so the remaining records appear on the next run.
        timestamp_path.write_text(effective_timestamp, encoding="utf-8")
        if advance_timestamp:
            logger.info("Timestamp advanced to %s", effective_timestamp)
        else:
            logger.info(
                "Timestamp intentionally kept at %s (limit_first_n truncation in effect).",
                effective_timestamp,
            )
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
        # Clean up temp files
        for path in [tmp_straindata, tmp_sts]:
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
        "message": (
            "Saved PubMLST numpy tables."
            + (f" Timestamp advanced to {effective_timestamp}." if advance_timestamp
               else f" Timestamp kept at {effective_timestamp} (limit_first_n truncation).")
        ),
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "straindata_rows": len(merged_straindata),
            "sts_rows": len(merged_sts),
            "timestamp_written": effective_timestamp,
            "timestamp_advanced": advance_timestamp,
            "backup_count": len(backups),
        },
    }
    rb.add_named_milestone("PROCESSING_STATUS", proc)
    remaining_steps.remove("PROCESSING_STATUS")

    # -----------------------------------------------------------------------
    # 6) FINAL_STATUS
    # -----------------------------------------------------------------------
    final = verify_expected_files(
        base_dir=out_dir,
        expected_files=SOURCE["expected_processed_files"],
    )
    rb.add_named_milestone("FINAL_STATUS", final)
    remaining_steps.remove("FINAL_STATUS")

    if final["status"] != StatusType.PASSED.value:
        _restore_backups(backups, logger=logger)
        rb.fail(code="FINAL_STATUS_FAILED", message=final.get("message", ""), retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # Persist checksum manifest and clean up backups
    checksums_after_final = _load_checksum_list(base_dir=out_dir, rel_files=expected)
    manifest_path = _write_md5_manifest(out_dir=out_dir, checksums=checksums_after_final)
    logger.info("Wrote checksum manifest: %s", manifest_path)
    _remove_backup_files(backups, logger=logger)

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))


if __name__ == "__main__":
    main()
