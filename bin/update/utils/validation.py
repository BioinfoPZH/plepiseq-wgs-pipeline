from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from utils.net import StatusType


def get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def verify_expected_files(
    *,
    base_dir: Path,
    expected_files: List[str],
    max_missing_list: int = 50,
) -> Dict[str, Any]:
    """
    Verify that all expected files exist under base_dir.

    Returns a milestone payload (without 'name' – add that via ReportBuilder.add_named_milestone).
    """
    started_at = get_timestamp()

    missing: List[str] = []
    present = 0

    for rel in expected_files:
        p = base_dir / rel
        if p.exists():
            present += 1
        else:
            missing.append(rel)

    finished_at = get_timestamp()

    if missing:
        missing_files_out = missing[:max_missing_list]
        truncated = len(missing) > len(missing_files_out)
        msg = f"Missing {len(missing)}/{len(expected_files)} expected processed files."
        return {
            "status": StatusType.FAILED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "expected_total": len(expected_files),
                "present": present,
                "missing": len(missing),
                "missing_files": missing_files_out,
                "missing_files_truncated": truncated,
            },
        }

    msg = f"All {len(expected_files)} expected processed files are present."
    return {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "expected_total": len(expected_files),
            "present": present,
            "missing": 0,
        },
    }
