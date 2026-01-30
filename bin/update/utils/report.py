# utils/report.py
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import json
import time
from utils.net import StatusType
from pathlib import Path

ALL_STEPS = [
    "PREFLIGHT_CONNECTIVITY",
    "DATABASE_AVAILABILITY",
    "REMOTE_FILES_DOWNLOAD_STATUS",
    "UPDATE_STATUS",
    "PROCESSING_STATUS",
    "FINAL_STATUS",
]

SCHEMA_VERSION = "1.1.1"

def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ReportBuilder:
    report: Dict[str, Any]
    _t0: float = field(default_factory=time.time)

    @classmethod
    def start(
        cls,
        *,
        schema_version: str,
        database: Dict[str, Any],
        execution_context: Dict[str, Any],
        run_id: str,
        source: Dict[str, Any],
        log_file: str,
    ) -> "ReportBuilder":
        rep = {
            "schema_version": schema_version,
            "database": database,
            "execution_context": execution_context,
            "run_id": run_id,
            "started_at": utc_now_z(),
            "finished_at": None,
            "runtime_seconds": 0,
            "run_status": "FAIL",
            "update_decision": {
                "mode": "always_rebuild",
                "result": "error",
                "message": "Not set",
                "first_build": True
            },
            "source": source,
            "log_file": Path(log_file).name,
            "milestones": []
        }
        return cls(report=rep)

    def add_milestone(self, milestone: Dict[str, Any]) -> None:
        self.report["milestones"].append(milestone)

    def add_named_milestone(self, name: str, payload: Dict[str, Any]) -> None:
        """
        Adds milestone dict returned by utils.net.check_url_available(),
        attaching 'name' to match the schema.
        """
        m = dict(payload)
        m["name"] = name
        self.add_milestone(m)

    def add_skipped(self, name: str, message: str) -> None:
        """
        Adds a schema-compatible SKIPPED milestone.
        """
        self.add_milestone({
            "name": name,
            "status": StatusType.SKIPPED.value,
            "attempts": 1,
            "retryable": False,
            "message": message,
            "metrics": {},
        })

    def set_update_decision(
        self,
        *,
        mode: str,
        result: str,
        message: str,
        first_build: bool = False,
        checksums_before: Optional[List[Dict[str, str]]] = None,
        checksums_after: Optional[List[Dict[str, str]]] = None,
        version_local: Optional[str] = None,
        version_remote: Optional[str] = None,
        timestamp_local: Optional[str] = None,
        timestamp_remote: Optional[str] = None,
    ) -> None:
        d: Dict[str, Any] = {"mode": mode, "result": result, "message": message, "first_build": first_build}
        if checksums_before is not None:
            d["checksums_before"] = checksums_before
        if checksums_after is not None:
            d["checksums_after"] = checksums_after
        if version_local is not None:
            d["version_local"] = version_local
        if version_remote is not None:
            d["version_remote"] = version_remote
        if timestamp_local is not None:
            d["timestamp_local"] = timestamp_local
        if timestamp_remote is not None:
            d["timestamp_remote"] = timestamp_remote

        self.report["update_decision"] = d

    def fail(self, *, code: str, message: str, retry_recommended: bool = True) -> None:
        self.report["error"] = {
            "code": code,
            "message": message,
            "retry_recommended": retry_recommended
        }

    def finalize(self, run_status: str) -> None:
        self.report["run_status"] = run_status
        self.report["finished_at"] = utc_now_z()
        self.report["runtime_seconds"] = int(time.time() - self._t0)

    def write(self, path: str) -> None:
        # just in case directory to path file does not exists
        if not os.path.exists(Path(path).parent):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)
