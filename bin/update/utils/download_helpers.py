import requests
import time
import logging
from pathlib import Path
from typing import Optional, Tuple
import logging


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
           'accept' : '*/*'}

def _download_file_with_retry(
    url: str,
    output_path: Path,
    logger: logging.Logger,
    max_retries: int = 3,
    wait_seconds: int = 300,
    timeout_s: int = 60,
    auth: Optional[Tuple[str, str]] = None,
) -> Tuple[bool, int]:
    """Download a file with retries. Returns (success, attempts_used)."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        logger.info("Attempt %d/%d...", attempt, max_retries)
        try:
            # remove partial file from previous attempt
            if output_path.exists():
                output_path.unlink()

            with requests.get(url, stream=True, timeout=timeout_s, headers=HEADERS, auth=auth) as response:
                if response.status_code == 200:
                    logger.info("Downloading %s...", output_path.name)
                    with open(output_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    return True, attempt

                logger.warning(
                    "Attempt %d/%d failed for %s with HTTP %d",
                    attempt, max_retries, url, response.status_code
                )

        except Exception as e:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, url, str(e))

        if attempt < max_retries:
            logger.info("Waiting %d seconds before retry...", wait_seconds)
            time.sleep(wait_seconds)

    logger.error("Failed to download %s after %d attempts.", url, max_retries)
    return False, max_retries