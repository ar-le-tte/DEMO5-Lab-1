"""
- Console + file logging
- Rotating log files to avoid huge log sizes
- Log level controlled via LOG_LEVEL env var (default: INFO)
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(
    name: str = "tmdb_pipeline",
    log_dir: str = "logs",
    log_file: str = "pipeline.log",
) -> logging.Logger:
    """
    Create (or return) a configured logger.

    Environment variables:
        LOG_LEVEL: DEBUG, INFO, WARNING, ERROR (default INFO)
        LOG_TO_FILE: 1/0 (default 1)
    """
    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False 

    # If already configured, return it
    if getattr(logger, "_configured", False):
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler (rotating)
    log_to_file = os.getenv("LOG_TO_FILE", "1") not in ("0", "false", "False")
    if log_to_file:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fp = Path(log_dir) / log_file
        fh = RotatingFileHandler(
            fp,
            maxBytes=5_000_000,  # 5MB
            backupCount=3,       # keep last 3 files
            encoding="utf-8",
        )
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    logger._configured = True
    return logger