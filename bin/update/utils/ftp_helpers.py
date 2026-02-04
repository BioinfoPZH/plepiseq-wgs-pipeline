from __future__ import annotations

import io
import os
import time
import logging
from ftplib import FTP, error_perm
from pathlib import Path
from typing import Any, Dict, List, Optional


def ftp_connect(
    host: str,
    directory: str,
    timeout_s: int = 60,
    logger: Optional[logging.Logger] = None,
    *,
    retries: int = 1,
    interval: int = 0,
) -> FTP:
    """
    Connect to FTP host, anonymous login, and cwd into directory.
    """
    last_err: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        if logger is not None:
            logger.info("FTP: connecting to %s (attempt %d/%d)", host, attempt, max(1, retries))
        ftp = None
        try:
            ftp = FTP(host, timeout=timeout_s)
            ftp.login()
            ftp.cwd(directory)
            if logger is not None:
                try:
                    pwd = ftp.pwd()
                except Exception:
                    pwd = ""
                logger.info("FTP: connected and cwd to %s", pwd or directory)
            return ftp
        except Exception as e:
            last_err = str(e)
            try:
                if ftp is not None:
                    ftp.close()
            except Exception:
                pass
            if attempt < max(1, retries) and interval > 0:
                if logger is not None:
                    logger.info("FTP: connect failed (%s). Retrying in %ds.", last_err, interval)
                time.sleep(interval)

    raise RuntimeError(f"FTP connect failed after {max(1, retries)} attempt(s): {last_err}")


def ftp_read_text(
    ftp: FTP,
    filename: str,
    encoding: str = "utf-8",
    logger: Optional[logging.Logger] = None,
    *,
    retries: int = 1,
    interval: int = 0,
) -> str:
    """
    Read a small remote text file into memory.
    """
    last_err: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        if logger is not None:
            logger.info("FTP: reading text file %s (attempt %d/%d)", filename, attempt, max(1, retries))
        try:
            buf = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", buf.write)
            out = buf.getvalue().decode(encoding, errors="replace").strip()
            if logger is not None:
                logger.info("FTP: read %s (%d bytes)", filename, len(out.encode(encoding, errors="replace")))
            return out
        except Exception as e:
            last_err = str(e)
            if attempt < max(1, retries) and interval > 0:
                if logger is not None:
                    logger.info("FTP: read failed (%s). Retrying in %ds.", last_err, interval)
                time.sleep(interval)

    raise RuntimeError(f"FTP read failed after {max(1, retries)} attempt(s) for {filename}: {last_err}")


def ftp_is_dir(ftp: FTP, name: str) -> bool:
    """
    Return True if `name` exists on the current FTP path and is a directory.
    """
    cur = ftp.pwd()
    try:
        ftp.cwd(name)
        ftp.cwd(cur)
        return True
    except error_perm:
        return False


def ftp_list_regular_files(
    ftp: FTP,
    logger: Optional[logging.Logger] = None,
    *,
    retries: int = 1,
    interval: int = 0,
) -> List[str]:
    """
    List only regular files (skip directories) in the current FTP directory.
    """
    last_err: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        if logger is not None:
            logger.info("FTP: listing files (attempt %d/%d)", attempt, max(1, retries))
        try:
            names = ftp.nlst()
            break
        except error_perm as e:
            last_err = str(e)
            names = []
            if attempt < max(1, retries) and interval > 0:
                if logger is not None:
                    logger.info("FTP: list failed (%s). Retrying in %ds.", last_err, interval)
                time.sleep(interval)
    else:
        names = []

    files: List[str] = []
    for n in names:
        if n in (".", ".."):
            continue
        if ftp_is_dir(ftp, n):
            continue
        files.append(n)
    if logger is not None:
        logger.info("FTP: found %d regular file(s)", len(files))
    return files


def ftp_download_file_atomic(
    ftp: FTP,
    remote_name: str,
    dest_path: Path,
    *,
    blocksize: int = 64 * 1024,
    logger: Optional[logging.Logger] = None,
    retries: int = 1,
    interval: int = 0,
) -> Dict[str, Any]:
    """
    Download `remote_name` to `dest_path` atomically (tmp + replace).

    Returns metrics (bytes_written, tmp_path, dest_path, remote_name).
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_name(dest_path.name + ".tmp")

    last_err: Optional[str] = None

    for attempt in range(1, max(1, retries) + 1):
        bytes_written = 0
        t0 = time.time()

        # Ensure old tmp doesn't confuse recovery.
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

        if logger is not None:
            logger.info("FTP: downloading %s (attempt %d/%d)", remote_name, attempt, max(1, retries))

        try:
            with open(tmp_path, "wb") as fh:
                def _write(chunk: bytes) -> None:
                    nonlocal bytes_written
                    fh.write(chunk)
                    bytes_written += len(chunk)

                ftp.retrbinary(f"RETR {remote_name}", _write, blocksize=blocksize)

            os.replace(tmp_path, dest_path)

            elapsed_s = time.time() - t0
            if logger is not None:
                logger.info(
                    "FTP: downloaded %s -> %s (%d bytes in %.1fs)",
                    remote_name,
                    dest_path.name,
                    bytes_written,
                    elapsed_s,
                )

            return {
                "remote_name": remote_name,
                "dest_path": str(dest_path),
                "tmp_path": str(tmp_path),
                "bytes_written": bytes_written,
                "blocksize": blocksize,
                "elapsed_seconds": elapsed_s,
                "attempts_used": attempt,
            }
        except Exception as e:
            last_err = str(e)
            # cleanup partial temp file
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

            if attempt < max(1, retries) and interval > 0:
                if logger is not None:
                    logger.info("FTP: download failed (%s). Retrying in %ds.", last_err, interval)
                time.sleep(interval)

    raise RuntimeError(
        f"FTP download failed after {max(1, retries)} attempt(s) for {remote_name}: {last_err}"
    )

