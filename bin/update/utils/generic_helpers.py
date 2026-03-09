import shutil
from pathlib import Path
import logging
import subprocess
import os
from typing import List, Tuple, Union, Sequence
from datetime import datetime, timezone

def get_timestamp():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _dir_removal(directory: Path, keep_dirs: tuple[str, ...], logger: logging.Logger):
    logger.info(f"Removing old directories in {directory} except: {keep_dirs}")
    for d in directory.iterdir():
        if d.is_dir() and d.name not in keep_dirs:
            shutil.rmtree(d, ignore_errors=True)

def remove_old_workspace(directory: Path, keep: tuple[str, ...], logger: logging.Logger) -> None:
    logger.info("Removing old files and directories in %s except: %s", directory, keep)
    for p in directory.iterdir():
        if p.name in keep:
            continue
        try:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("Failed to remove %s: %s", p, e)

def backup_paths(paths: List[Path], logger: logging.Logger) -> List[Tuple[Path, Path]]:
    """Rename existing files to *.old, returning (original, backup) pairs for restoration."""
    backups: List[Tuple[Path, Path]] = []
    for p in paths:
        if not p.exists():
            continue
        bak = Path(str(p) + ".old")
        try:
            if bak.exists():
                bak.unlink()
        except Exception:
            pass
        logger.info("Backing up existing file: %s -> %s", p, bak)
        os.replace(p, bak)
        backups.append((p, bak))
    return backups


def restore_backups(backups: List[Tuple[Path, Path]], logger: logging.Logger) -> None:
    """Restore original files from *.old backups."""
    for orig, bak in backups:
        try:
            if orig.exists():
                orig.unlink()
        except Exception:
            pass
        if bak.exists():
            logger.info("Restoring backup: %s -> %s", bak, orig)
            os.replace(bak, orig)


def remove_backup_files(backups: List[Tuple[Path, Path]], logger: logging.Logger) -> None:
    """Remove *.old backup files after a successful update."""
    for _orig, bak in backups:
        try:
            if bak.exists():
                logger.info("Removing backup file: %s", bak)
                bak.unlink()
        except Exception as e:
            logger.warning("Failed to remove backup %s: %s", bak, e)


def _execute_command(cmd: Union[str, Sequence[str]], logger: logging.Logger | None = None) -> bool:
    if isinstance(cmd, str):
        process = subprocess.Popen(
            cmd,
            shell=True,  # <-- key change
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    else:
        process = subprocess.Popen(
            list(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    stdout, stderr = process.communicate()

    if logger is not None:
        if stdout:
            logger.debug(stdout.rstrip())
        if stderr:
            logger.debug(stderr.rstrip())

    if process.returncode != 0:
        if logger is not None:
            logger.warning("Command failed (rc=%d): %s", process.returncode, cmd)
        return False
    return True