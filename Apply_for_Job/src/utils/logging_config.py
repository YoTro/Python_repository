"""
logging_config.py - Centralized logging setup for the whole project.

Call `setup_logging()` once at process startup (in main.py).
All other modules just use `logging.getLogger(__name__)` as usual.

Level resolution order (highest priority first):
  1. LOG_LEVEL environment variable
  2. `level` argument to setup_logging()
  3. Default: INFO

Log output:
  - Console : INFO and above (coloured on TTY via optional colorlog)
  - File    : DEBUG and above → logs/app.log (rotating, 5 × 10 MB)
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

_DEFAULT_LEVEL = logging.INFO
_LOG_DIR       = Path(__file__).resolve().parents[2] / "logs"
_LOG_FILE      = _LOG_DIR / "app.log"
_FMT           = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FMT      = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int | str | None = None) -> None:
    """
    Configure root logger once.
    Safe to call multiple times — subsequent calls are no-ops.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured

    # Resolve level
    env_level = os.environ.get("LOG_LEVEL", "").upper()
    resolved  = (
        getattr(logging, env_level, None)
        or (level if isinstance(level, int) else getattr(logging, str(level).upper(), None))
        or _DEFAULT_LEVEL
    )
    root.setLevel(logging.DEBUG)   # root catches everything; handlers filter

    formatter = logging.Formatter(_FMT, datefmt=_DATE_FMT)

    # ── Console handler ───────────────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(resolved)
    try:
        import colorlog
        console.setFormatter(colorlog.ColoredFormatter(
            "%(log_color)s" + _FMT,
            datefmt=_DATE_FMT,
            log_colors={
                "DEBUG":    "cyan",
                "INFO":     "green",
                "WARNING":  "yellow",
                "ERROR":    "red",
                "CRITICAL": "bold_red",
            },
        ))
    except ImportError:
        console.setFormatter(formatter)
    root.addHandler(console)

    # ── Rotating file handler ─────────────────────────────────────────
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_h = logging.handlers.RotatingFileHandler(
            _LOG_FILE,
            maxBytes=10 * 1024 * 1024,   # 10 MB per file
            backupCount=5,
            encoding="utf-8",
        )
        file_h.setLevel(logging.DEBUG)
        file_h.setFormatter(formatter)
        root.addHandler(file_h)
    except OSError as e:
        root.warning("Cannot open log file %s: %s", _LOG_FILE, e)

    # Silence noisy third-party loggers
    for noisy in ("urllib3", "DrissionPage", "openai._base_client", "httpcore", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root.debug("Logging initialised — level=%s  file=%s",
               logging.getLevelName(resolved), _LOG_FILE)
