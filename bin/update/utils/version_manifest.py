from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict


def read_version_manifest(path: Path) -> Dict[str, str]:
    """
    Read a small JSON manifest containing version identifiers.

    Returns {} if the file does not exist or is invalid.
    """
    try:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in parsed.items():
            out[str(k)] = str(v)
        return out
    except Exception:
        return {}


def write_version_manifest(path: Path, data: Dict[str, str]) -> None:
    """
    Atomically write a version manifest JSON.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
