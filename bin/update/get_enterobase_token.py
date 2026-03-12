#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import click
import requests


def _extract_token(payload: Any) -> Optional[str]:
    """Extract token string from EnteroBase login response payload."""
    if isinstance(payload, str) and payload.strip():
        return payload.strip()

    if isinstance(payload, dict):
        # Prefer known token-like keys first.
        for key in ("api_token", "token", "access_token"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        # Fallback: find first non-empty string value behind a key containing "token".
        for key, value in payload.items():
            if "token" in str(key).lower() and isinstance(value, str) and value.strip():
                return value.strip()

    return None


@click.command()
@click.option("--username", required=True, type=str, help="EnteroBase username.")
@click.option("--password", required=True, type=str, help="EnteroBase password.")
@click.option(
    "--output-file",
    "output_file",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
    help="Path where the token will be written.",
)
@click.option(
    "--server",
    default="https://enterobase.warwick.ac.uk",
    show_default=True,
    help="EnteroBase server URL.",
)
@click.option(
    "--timeout",
    default=30,
    show_default=True,
    type=int,
    help="HTTP timeout in seconds.",
)
def main(username: str, password: str, output_file: Path, server: str, timeout: int) -> None:
    """
    Renew EnteroBase API token and save it to --output-file.

    The output file will contain only the token string.
    """
    login_url = f"{server.rstrip('/')}/api/v2.0/login"
    params = {"username": username, "password": password}

    try:
        response = requests.get(login_url, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise click.ClickException(f"Request to EnteroBase login endpoint failed: {exc}") from exc

    if response.status_code != 200:
        raise click.ClickException(
            f"EnteroBase login failed with HTTP {response.status_code}: {response.text.strip()}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise click.ClickException("EnteroBase response is not valid JSON.") from exc

    token = _extract_token(payload)
    if not token:
        raise click.ClickException(
            "Could not extract token from EnteroBase response. "
            "Inspect response JSON shape and adjust token key mapping."
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(token, encoding="utf-8")
    click.echo(f"Token saved to: {output_file}")


if __name__ == "__main__":
    main()
