import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from requests_oauthlib import OAuth1

from utils.generic_helpers import get_timestamp
from utils.net import StatusType, check_url_available

def file_md5sum(path: str):
    """Compute md5 checksum of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def composite_availability_check(
    urls: List[str],
    logger,
    *,
    retries: int = 3,
    interval: int = 10,
    auth: Optional[Tuple[str, str]] = None,
) -> Dict[str, Any]:
    """
    Create a single schema-shaped milestone for availability of multiple endpoints.

    Returns a milestone payload dict (without 'name' – caller should set it via ReportBuilder).
    """
    started_at = get_timestamp()
    attempts_used_max = 1

    per_url: Dict[str, Any] = {}
    for u in urls:
        res = check_url_available(u, retries=retries, interval=interval, logger=logger, auth=auth)
        attempts_used_max = max(attempts_used_max, int(res.get("attempts", 1) or 1))
        per_url[u] = {"status": res.get("status"), "message": res.get("message"), "metrics": res.get("metrics", {})}
        if res.get("status") != StatusType.PASSED.value:
            finished_at = get_timestamp()
            return {
                "status": StatusType.FAILED.value,
                "message": f"One or more endpoints unreachable (first failure: {u})",
                "started_at": started_at,
                "finished_at": finished_at,
                "attempts": attempts_used_max,
                "retryable": True,
                "metrics": {"checks": per_url},
            }

    finished_at = get_timestamp()
    return {
        "status": StatusType.PASSED.value,
        "message": "All required endpoints reachable",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": attempts_used_max,
        "retryable": True,
        "metrics": {"checks": per_url},
    }


def parse_credentials_file(path: Path, logger) -> Dict[str, str]:
    """Parse a ``key=value`` credentials file.

    Ignores blank lines and ``#`` comments.  Returns an empty dict when the
    file is missing or unreadable (a warning is logged in that case).
    """
    out: Dict[str, str] = {}
    try:
        if not path.exists():
            logger.warning("Credentials file not found: %s", path)
            return out
        raw = path.read_text(encoding="utf-8", errors="replace")
        for line in raw.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            out[k.strip()] = v.strip()
    except Exception as e:
        logger.warning("Failed to read credentials file (%s): %s", path, e)
    return out


def get_enterobase_auth(
    credentials: Dict[str, str], logger
) -> Optional[Tuple[str, str]]:
    """Extract EnteroBase API token from parsed credentials.

    Returns a ``(token, "")`` Basic-auth tuple or ``None`` when the
    ``enterobase_token`` key is missing / empty.
    """
    token = credentials.get("enterobase_token", "").strip()
    if not token:
        logger.warning(
            "Credentials file does not contain a valid 'enterobase_token' key."
        )
        return None
    logger.info("EnteroBase API token loaded from credentials file.")
    return (token, "")


def get_pubmlst_oauth(credentials: Dict[str, str], logger) -> Optional[OAuth1]:
    """Build an OAuth1 object from PubMLST keys in *credentials*.

    Looks for ``client_id`` (or ``client_key``), ``client_secret``, and
    optionally ``access_token`` / ``access_token_secret`` (with aliases
    ``resource_owner_key`` / ``resource_owner_secret``).

    Returns ``None`` when the minimum required ``client_id`` is missing,
    allowing scripts to proceed unauthenticated.
    """
    client_id = (
        credentials.get("client_id")
        or credentials.get("client_key")
        or ""
    ).strip()
    client_secret = credentials.get("client_secret", "").strip()
    access_token = (
        credentials.get("access_token")
        or credentials.get("resource_owner_key")
        or ""
    ).strip()
    access_token_secret = (
        credentials.get("access_token_secret")
        or credentials.get("resource_owner_secret")
        or ""
    ).strip()

    if not client_id:
        logger.warning(
            "PubMLST client_id not found in credentials file; "
            "proceeding unauthenticated."
        )
        return None

    if access_token and access_token_secret:
        logger.info(
            "Using OAuth1 (client + access token) for PubMLST requests."
        )
        return OAuth1(client_id, client_secret, access_token, access_token_secret)

    logger.info("Using OAuth1 (client credentials only) for PubMLST requests.")
    return OAuth1(client_id, client_secret)


def parse_md5_text(content: str) -> Optional[Tuple[str, str]]:
    """
    Parse a simple md5 file content like:
      '<md5>  <filename>'
    Returns (filename, md5) or None if parsing fails.
    """
    line = ""
    for raw in (content or "").splitlines():
        raw = raw.strip()
        if raw:
            line = raw
            break
    if not line:
        return None
    parts = line.split()
    if len(parts) < 2:
        return None
    md5 = parts[0].strip().lower()
    filename = parts[-1].strip()
    if not re.fullmatch(r"[a-f0-9]{32}", md5):
        return None
    return filename, md5

