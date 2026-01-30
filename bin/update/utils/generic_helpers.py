import shutil
from pathlib import Path
import logging
import subprocess
import os
from typing import Union, Sequence

def _dir_removal(directory: Path, keep_dirs: tuple[str, ...], logger: logging.Logger):
    logger.info(f"Removing old directories in {directory} except: {keep_dirs}")
    for d in directory.iterdir():
        if d.is_dir() and d.name not in keep_dirs:
            shutil.rmtree(d, ignore_errors=True)


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