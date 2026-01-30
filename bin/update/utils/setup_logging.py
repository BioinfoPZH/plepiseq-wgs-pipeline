import logging
import sys
from pathlib import Path


def _setup_logging(output_dir: Path, filename: str = "log.log") -> logging.Logger:
    """
    Configurate logging file. Function returns a logger object
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = str(output_dir / filename)

    handlers = [
        logging.FileHandler(log_file_path, mode="w"),
        logging.StreamHandler(sys.stdout),
    ]

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Replace handlers to avoid duplicates across multiple scripts/tests
    root.handlers = handlers

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    for h in handlers:
        h.setFormatter(formatter)

    logging.info("Logging initialized. Output log: %s", log_file_path)
    return logging.getLogger()
