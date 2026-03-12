import os
import tarfile
import logging
from typing import List, Optional
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from utils.updates_helpers import file_md5sum
from datetime import datetime
from utils.generic_helpers import get_timestamp
import json

def s3_client_unsigned():
    """Create an unsigned boto3 S3 client (public buckets)."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))

def check_s3_connectivity(bucket: str, prefix: str, attempts: int = 3, interval_sec: int = 120, logger: Optional[logging.Logger] = None) -> dict:
    """
    Try listing one object under prefix to validate access. Returns a milestone-like dict.
    """
    started = None

    started = get_timestamp()
    cli = s3_client_unsigned()
    for attempt in range(1, attempts + 1):
        try:
            if logger:
                logger.info(f"Checking S3 connectivity (attempt {attempt}/{attempts})...")
            resp = cli.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            finished = get_timestamp()
            return {
                "status": "PASSED",
                "message": "S3 prefix reachable",
                "started_at": started,
                "finished_at": finished,
                "attempts": attempt,
                "retryable": True,
                "metrics": {"bucket": bucket, "prefix": prefix, "listed": bool(resp.get("Contents"))},
            }
        except (BotoCoreError, ClientError) as e:
            if logger:
                logger.warning("S3 connectivity check failed: %s", e)
            if attempt < attempts:
                import time
                time.sleep(interval_sec)
            else:
                finished = datetime.utcnow().isoformat() + "Z"
                return {
                    "status": "FAILED",
                    "message": str(e),
                    "started_at": started,
                    "finished_at": finished,
                    "attempts": attempts,
                    "retryable": True,
                    "metrics": {"bucket": bucket, "prefix": prefix},
                }

def list_available_databases(bucket_name: str, prefix: str) -> List[str]:
    cli = s3_client_unsigned()
    paginator = cli.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def find_latest_database(databases: List[str], db_name_regexp) -> Optional[str]:
    """Find the latest database key by extracting a YYYYMMDD date group named 'date' from the key."""

    latest_date = None
    latest_database = None
    for database in databases:
        match = db_name_regexp.match(database)
        if match:
            date_str = match.group("date")
            try:
                date = datetime.strptime(date_str, "%Y%m%d")
            except Exception:
                continue
            if latest_date is None or date > latest_date:
                latest_date = date
                latest_database = database
    return latest_database

def download_remote_md5(bucket: str, md5_key: str, target_path: str):
    """
    Download remote md5 file from S3 into target_path and return parsed mapping {tarball_name: md5}.
    """
    cli = s3_client_unsigned()
    cli.download_file(bucket, md5_key, target_path)
    parsed = {}
    with open(target_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue
            # assume format: <md5> [other cols] <filename>
            fname = parts[-1]
            md5 = parts[0]
            parsed[fname] = md5
    return parsed

def download_file_and_extract_s3(bucket: str, key: str, dest_tar_path: str, extract_to: Optional[str] = None, logger: Optional[logging.Logger] = None):
    """
    Download tar.gz from S3 to dest_tar_path, extract into extract_to (or dest_tar_path's dir),
    compute md5 of the downloaded file and write current_md5.txt into extract dir.
    Returns dict with metrics.
    """
    cli = s3_client_unsigned()
    os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
    cli.download_file(bucket, key, dest_tar_path)
    if logger:
        logger.info("Downloaded %s to %s", key, dest_tar_path)
    if extract_to is None:
        extract_to = os.path.dirname(dest_tar_path)
    # extract tar.gz
    try:
        with tarfile.open(dest_tar_path, "r:gz") as tf:
            tf.extractall(path=extract_to)
    except Exception:
        raise
    # compute md5

    md5 = file_md5sum(dest_tar_path)
    # write current_md5.txt
    tar_name = os.path.basename(dest_tar_path)
    new_md5 = {tar_name: md5}
    with open(os.path.join(extract_to, "current_md5.json"), "w", encoding="utf-8") as f:
        json.dump(new_md5, f, indent=2)

    # remove tar
    try:
        os.remove(dest_tar_path)
    except OSError:
        pass
    return {"tar_name": os.path.basename(dest_tar_path), "md5": md5, "extracted_to": extract_to}

