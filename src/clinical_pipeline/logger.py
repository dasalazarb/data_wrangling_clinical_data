from __future__ import annotations

import logging
from pathlib import Path

from rich.logging import RichHandler

from .utils import ensure_dir


def build_logger(log_dir: str | Path, run_name: str, level: str = "INFO") -> logging.Logger:
    ensure_dir(log_dir)
    logger = logging.getLogger(run_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(Path(log_dir) / f"{run_name}.log", encoding="utf-8")
    fh.setFormatter(fmt)

    rh = RichHandler(rich_tracebacks=True, markup=False)
    rh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(rh)
    return logger
