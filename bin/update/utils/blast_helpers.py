from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def is_fasta_and_dbtype(path: Path) -> Tuple[bool, Optional[str]]:
    """
    Detect whether a file appears to be a FASTA.

    Returns:
      (is_fasta, dbtype) where dbtype is "nucl" or "prot" when is_fasta=True.
    """
    try:
        with path.open("rt", encoding="utf-8", errors="replace") as f:
            first = f.readline()
            if not first.startswith(">"):
                return False, None

            # Peek a second line to guess type. If it's empty, default to nucl.
            second = (f.readline() or "").strip()
            if not second:
                return True, "nucl"

            allowed_nucl = set("ATGCatgcNn")
            dbtype = "nucl" if set(second) <= allowed_nucl else "prot"
            return True, dbtype
    except Exception:
        return False, None


def run_makeblastdb(path: Path, dbtype: str, logger=None) -> Tuple[bool, Dict[str, Any]]:
    """
    Run makeblastdb for a FASTA file. Returns (ok, metrics).
    """
    cmd = ["makeblastdb", "-in", str(path), "-dbtype", dbtype]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        ok = p.returncode == 0
        if logger is not None:
            if ok:
                logger.debug("makeblastdb ok: %s", " ".join(cmd))
            else:
                logger.warning("makeblastdb failed (rc=%s): %s", p.returncode, " ".join(cmd))
        return ok, {
            "cmd": cmd,
            "rc": int(p.returncode),
            "stdout_snippet": (p.stdout or "")[:2000],
            "stderr_snippet": (p.stderr or "")[:2000],
            "dbtype": dbtype,
            "file": str(path),
        }
    except Exception as e:
        if logger is not None:
            logger.warning("makeblastdb exception for %s: %s", path, e)
        return False, {"cmd": cmd, "error": str(e), "dbtype": dbtype, "file": str(path)}


def index_if_fasta(path: Path, logger=None) -> Tuple[bool, Dict[str, Any]]:
    """
    If file is FASTA, run makeblastdb with inferred dbtype.
    If not FASTA, returns ok=True with skipped=True.
    """
    is_fa, dbtype = is_fasta_and_dbtype(path)
    if not is_fa:
        return True, {"skipped": True, "reason": "not_fasta", "file": str(path)}
    ok, metrics = run_makeblastdb(path, dbtype=dbtype or "nucl", logger=logger)
    metrics["skipped"] = False
    return ok, metrics

