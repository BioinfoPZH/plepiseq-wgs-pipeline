from dataclasses import dataclass
from typing import Optional, Tuple, Any

import requests
from datetime import datetime, timezone
import time
from enum import Enum
import logging

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
           'accept' : '*/*'}


class StatusType(Enum):
    PASSED = 'PASSED'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'


# metrics is a placeholder for optional keys for a milestone, not relevant for checking connection
@dataclass
class UrlAvailabilityResult:
    status: StatusType = StatusType.SKIPPED
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    message: Optional[str] = None
    attempts: int = 1
    retryable: bool = True
    metrics: Optional[dict[str, Any]] = None

    def to_dict(self):
        return {
            "status": self.status.value,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "message": self.message,
            "attempts": self.attempts,
            "retryable": self.retryable,
            "metrics": self.metrics or {}
        }



def _is_retryable_http_status(status_code: int) -> bool:
    # 429 Too Many Requests, 5xx server errors are usually transient
    return status_code == 429 or (500 <= status_code <= 599)


def _check_connection(session: requests.Session,
                      url: str,
                      timeout_s: int = 10) -> Tuple[StatusType, str, Optional[int]]:
    """
    Function checks connection. By a default function asks for HEAD of the webpage, and as a fallback full body
    Returns status , message, http_status
    """
    # HEAD first
    try:
        r = session.head(url, headers=HEADERS , timeout=timeout_s, allow_redirects=True)
        if r.status_code < 400:
            return StatusType.PASSED, "Endpoint reachable", r.status_code
        if _is_retryable_http_status(r.status_code):
            return StatusType.FAILED, "Endpoint returned retryable error", r.status_code
        # Non-retryable error (e.g. 404): do not retry by default
        return StatusType.FAILED, "Endpoint returned non-success", r.status_code
    except requests.RequestException:
        # HEAD failed we try again with GET
        pass

    # GET fallback
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
        if r.status_code < 400:
            return StatusType.PASSED, "Endpoint reachable", r.status_code
        if _is_retryable_http_status(r.status_code):
            return StatusType.FAILED, "Endpoint returned retryable error", r.status_code
        return StatusType.FAILED, "Endpoint returned non-success", r.status_code
    except requests.RequestException as e:
        return StatusType.FAILED, f"Endpoint unreachable: {e}", None
    # In case we ask

def check_url_available(
    url: str,
    retries: int = 3,
    interval: int = 30,
    logger: Optional[logging.Logger] = None
) -> dict[str, Any]:
    """
    Checks whether a remote URL is reachable. By default, 3 attempts are made with 30s interval between attempts. Each
    attempt has 20s timeout

    Returns UrlAvailabilityResult which is identical to the Milestone definition in json schema
    """
    session = requests.Session()
    timeout_s: int = 20
    exit_code = -1
    # let initiate defaults in casa  code below fails

    status = StatusType.FAILED
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


    for attempt in range(1, retries + 1):
        if logger:
            logger.info(f"Attempt {attempt}/{retries}: Checking connection to {url}")
        status, msg, exit_code = _check_connection(session, url, timeout_s=timeout_s)

        # Success → return immediately
        if StatusType.PASSED == status:
            results =  UrlAvailabilityResult(
                status = status,
                message=msg,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                attempts=attempt,
                retryable=True,
                metrics={'http_status' : exit_code},
            )
            return results.to_dict()

        # Retry with backoff
        if attempt < retries:
            time.sleep(interval)


    results = UrlAvailabilityResult(
        status = status,
        message=f"Failed after {retries} attempt(s)",
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        attempts=retries,
        retryable=True,
        metrics={'http_status': exit_code}
    )
    return results.to_dict()

