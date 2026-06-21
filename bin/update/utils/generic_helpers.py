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


def backup_paths_to_dir(paths: List[Path], backup_dir: Path, logger: logging.Logger) -> List[Tuple[Path, Path]]:
    """Move each existing path (file or directory) into backup_dir, preserving its name.

    Returns (original, backup) pairs so the originals can be restored later. Unlike
    backup_paths() (which renames files to *.old in place), this handles directories and
    collects everything under a single backup directory, which is convenient for an
    all-or-nothing rollback of a whole output tree.
    """
    backup_dir.mkdir(parents=True, exist_ok=True)
    pairs: List[Tuple[Path, Path]] = []
    for p in paths:
        if not p.exists():
            continue
        dest = backup_dir / p.name
        try:
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest, ignore_errors=True)
                else:
                    dest.unlink()
        except Exception:
            pass
        logger.info("Backing up %s -> %s", p, dest)
        os.replace(p, dest)
        pairs.append((p, dest))
    return pairs


def restore_paths_from_dir(backups: List[Tuple[Path, Path]], logger: logging.Logger) -> None:
    """Restore originals (files or directories) from a backup created by backup_paths_to_dir()."""
    for orig, bak in backups:
        try:
            if orig.exists():
                if orig.is_dir():
                    shutil.rmtree(orig, ignore_errors=True)
                else:
                    orig.unlink()
        except Exception:
            pass
        if bak.exists():
            logger.info("Restoring backup: %s -> %s", bak, orig)
            os.replace(bak, orig)


def remove_backup_dir(backup_dir: Path, logger: logging.Logger) -> None:
    """Remove the temporary backup directory (best-effort) after a successful update."""
    if backup_dir.exists():
        try:
            logger.info("Removing backup directory: %s", backup_dir)
            shutil.rmtree(backup_dir, ignore_errors=True)
        except Exception as e:
            logger.warning("Failed to remove backup dir %s: %s", backup_dir, e)


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