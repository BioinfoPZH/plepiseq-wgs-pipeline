import hashlib
from typing import Any, Dict, List

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
) -> Dict[str, Any]:
    """
    Create a single schema-shaped milestone for availability of multiple endpoints.

    Returns a milestone payload dict (without 'name' – caller should set it via ReportBuilder).
    """
    started_at = get_timestamp()
    attempts_used_max = 1

    per_url: Dict[str, Any] = {}
    for u in urls:
        res = check_url_available(u, retries=retries, interval=interval, logger=logger)
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

