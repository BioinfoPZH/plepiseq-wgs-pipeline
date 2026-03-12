from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple

import requests


def get_github_head_sha(
    *,
    owner: str,
    repo: str,
    ref: str = "main",
    token_env: str = "GITHUB_TOKEN",
    timeout_s: int = 20,
    logger: Any = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch the HEAD commit SHA for a GitHub repo ref (branch/tag/SHA).

    Uses GitHub REST API:
      GET https://api.github.com/repos/{owner}/{repo}/commits/{ref}

    If an optional token is present in `token_env`, it will be used to increase
    rate limits.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{ref}"

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "pzh_pipeline_viral-updater",
    }

    token = os.environ.get(token_env, "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    if logger is not None:
        logger.info("Fetching GitHub commit SHA: %s", url)

    r = requests.get(url, headers=headers, timeout=timeout_s)
    http_status = r.status_code

    metrics: Dict[str, Any] = {
        "url": url,
        "http_status": http_status,
        "token_used": bool(token),
    }

    # Surface rate-limit headers when present (helpful for diagnostics)
    for k in ("x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"):
        if k in r.headers:
            metrics[k] = r.headers.get(k)

    if http_status >= 400:
        # Try to include a small error snippet (avoid dumping huge bodies)
        snippet = ""
        try:
            snippet = (r.text or "")[:500]
        except Exception:
            pass
        raise RuntimeError(f"GitHub API error {http_status} for {url}. Body: {snippet}")

    data: Dict[str, Any] = r.json()
    sha = str(data.get("sha", "")).strip()
    if not sha:
        raise RuntimeError(f"GitHub API response missing 'sha' for {url}")

    return sha, metrics


def build_version_string(shas: Dict[str, str]) -> str:
    """
    Build a deterministic, schema-friendly version string from multiple SHA entries.
    Example: 'Freyja-barcodes=abc...;Freyja-data=def...'
    """
    parts = [f"{k}={v}" for k, v in sorted(shas.items(), key=lambda kv: kv[0])]
    return ";".join(parts)
