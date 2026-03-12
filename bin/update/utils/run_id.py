from __future__ import annotations
from datetime import datetime, timezone
import uuid


def generate_run_id(db_name: str) -> str:
    """
    Create a stable, sortable unique run ID.
    Example: 2026-01-21T132036Z_vfdb_7f3a2c1b
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    short = uuid.uuid4().hex[:8]
    return f"{ts}_{db_name}_{short}"

