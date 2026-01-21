#!/usr/bin/env python3
"""
shared_log.py

Shared Logging Module

Provides a unified logging interface for pipeline components, ensuring
consistent formatting, timestamping, and output handling (console/file).
This module exposes a small, well-documented API used across the codebase:

- Logging : class -- configure application-wide logging (console + optional file)
- get_logger() -> logging.Logger -- retrieve the configured logger (creates minimal console
  logger if not initialized)
- get_adapter(context) -> logging.LoggerAdapter -- logger with bound contextual fields
- log_header(metadata) -> datetime -- log start-of-job metadata and return a timestamp
- log_footer(start_time, success=True, error_message=None, extra=None) -- log completion

The module supports optional JSON-formatted logs (useful when sending logs to CloudWatch/ELK),
rotating file logs, and structured context via LoggerAdapter.

Examples:
    # Initialize logging at program start
    Logging(basename="myjob", foldername="./logs", console=True, json=False)

    # In code
    logger = get_logger()
    logger.info("Doing work", extra={"step": "download"})

    # Add contextual fields
    adapter = get_adapter({"run_id": "R0001"})
    adapter.info("Started run")

    # Use header/footer helpers
    start = log_header({"RunID": "R0001", "Input": "/data/foo.fastq"})
    # ... work ...
    log_footer(start, success=True, extra={"objects_uploaded": 12})
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Module-level logger reference (initialized by Logging())
_LOGGER: Optional[logging.Logger] = None


# ----------------------------
# Utilities / Formatters
# ----------------------------
class JsonFormatter(logging.Formatter):
    """
    Simple JSON formatter for logging records.

    The formatter serializes a limited set of record attributes plus any `extra`
    fields stored in the record.__dict__ into a single JSON object string.

    This is intentionally small and dependency-free to avoid pulling in external libraries.
    """

    def format(self, record: logging.LogRecord) -> str:
        # Base record fields we always include
        base = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
            "logger": record.name,
            "lineno": record.lineno,
            "funcName": record.funcName,
        }

        # Extract extras (anything not part of the standard LogRecord attributes)
        std_attrs = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module", "exc_info",
            "exc_text", "stack_info", "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process"
        }
        extras = {k: v for k, v in record.__dict__.items() if k not in std_attrs}
        if extras:
            base["extra"] = extras

        # If exception info present, append it (string form)
        if record.exc_info:
            base["exception"] = self.formatException(record.exc_info)

        return json.dumps(base, default=str, ensure_ascii=False)


class HumanFormatter(logging.Formatter):
    """
    Human-readable formatter with ISO-8601 UTC timestamps.

    Example format:
        2026-01-21T12:34:56Z — INFO — module — Message text
    """

    def __init__(self) -> None:
        super().__init__("%(asctime)s — %(levelname)s — %(module)s — %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        # Always use UTC ISO timestamp ending with 'Z'
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


# ----------------------------
# Public API
# ----------------------------
class Logging:
    """
    Configure the global logger for the application.

    This class intentionally mirrors your prior `Logging` initializer but adds:
      - Optional JSON output (json=True)
      - Optional rotating file handler to prevent unbounded log growth
      - Consistent UTC ISO timestamps
      - Type hints and robust validation

    Args:
        basename: Prefix for logfile names (the date and suffix will be appended).
        foldername: Directory to store log files. If None, file logging is disabled.
        console: If True, logs are emitted to stdout as well.
        level: Logging level (e.g. 'INFO', 'DEBUG').
        logger_key: Optional explicit logger name (useful to prevent collisions).
        json: If True, output logs as JSON (structured). Otherwise, human-readable text.
        rotate: If True and foldername is provided, use RotatingFileHandler.
        max_bytes: Max bytes for rotation (default 10 MB).
        backup_count: Number of rotated files to keep (default 7).
    """

    def __init__(
        self,
        basename: str = "log",
        foldername: Optional[str] = None,
        console: bool = True,
        level: str = "INFO",
        logger_key: Optional[str] = None,
        json: bool = False,
        rotate: bool = True,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 7,
    ) -> None:
        global _LOGGER

        self.basename = Path(basename).stem
        self.foldername = Path(foldername) if foldername else None
        self.console = bool(console)
        self.level = (level or "INFO").upper()
        self.json = bool(json)
        self.rotate = bool(rotate)
        self.max_bytes = int(max_bytes)
        self.backup_count = int(backup_count)

        # Build a stable logger name to prevent collisions in multi-process environments
        logger_name = logger_key or (f"{self.basename}" + (f"|{self.foldername}" if self.foldername else "|console"))

        # Ensure folder exists if needed
        if self.foldername:
            self.foldername.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, self.level, logging.INFO))
        logger.propagate = False

        # Remove existing handlers to avoid duplicate messages when re-initializing
        if logger.handlers:
            for h in list(logger.handlers):
                logger.removeHandler(h)

        # Choose formatter
        formatter = JsonFormatter() if self.json else HumanFormatter()

        # Console handler
        if self.console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(getattr(logging, self.level, logging.INFO))
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        # File handler (optionally rotating)
        if self.foldername:
            filename = f"{self.basename}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"
            file_path = self.foldername / filename
            if self.rotate:
                fh = logging.handlers.RotatingFileHandler(str(file_path), maxBytes=self.max_bytes, backupCount=self.backup_count)
            else:
                fh = logging.FileHandler(str(file_path))
            fh.setLevel(getattr(logging, self.level, logging.INFO))
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        _LOGGER = logger

    @staticmethod
    def get_logger() -> logging.Logger:
        """
        Return the configured logger instance.

        If logging has not been configured via Logging(...), a minimal console logger
        is created and returned to avoid None checks across the codebase.
        """
        return get_logger()


def get_logger() -> logging.Logger:
    """
    Retrieve the module's configured logger. If not configured, create a minimal console logger.

    Returns:
        logging.Logger
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    # Fallback: create minimal console logger to avoid None checks
    fallback = logging.getLogger("shared_log_fallback")
    fallback.setLevel(logging.INFO)
    fallback.propagate = False
    if not fallback.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(HumanFormatter())
        fallback.addHandler(ch)
    _LOGGER = fallback
    return _LOGGER


def get_adapter(context: Optional[Dict[str, Any]] = None) -> logging.LoggerAdapter:
    """
    Return a LoggerAdapter bound with the provided context dict under the 'extra' namespace.

    Args:
        context: dict of contextual fields (e.g., {"run_id": "R0001", "user": "alice"}).

    Returns:
        logging.LoggerAdapter
    """
    logger = get_logger()
    return logging.LoggerAdapter(logger, context or {})


def _iso_now() -> str:
    """Return current UTC time as ISO string with 'Z' suffix."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log_header(metadata: Dict[str, Any]) -> datetime:
    """
    Log the start of a process with structured metadata.

    Args:
        metadata: Mapping of key -> value describing the job (e.g., RunID, FileSize).

    Returns:
        datetime: UTC start timestamp (timezone-aware).
    """
    logger = get_logger()
    start = datetime.now(timezone.utc)
    # Attach a start_time into metadata for convenience
    metadata_with_ts = dict(metadata)
    metadata_with_ts.setdefault("Start_Time", start.isoformat().replace("+00:00", "Z"))
    logger.info("PROCESS STARTED", extra={"metadata": metadata_with_ts})
    # Also emit each metadata key for easy human readability (keeps parity with prior behavior)
    for k in sorted(metadata_with_ts.keys()):
        logger.info("%s: %s", k, metadata_with_ts[k])
    return start


def log_footer(start_time: datetime, success: bool = True, error_message: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Log the completion of a process with duration metrics and status.

    Args:
        start_time: datetime returned by `log_header`.
        success: True if the job completed successfully; False otherwise.
        error_message: Optional error message to record when success is False.
        extra: Optional additional fields to include under the `extra` key.
    """
    logger = get_logger()
    end = datetime.now(timezone.utc)
    duration = end - start_time
    payload: Dict[str, Any] = {
        "Start_Time": start_time.isoformat().replace("+00:00", "Z"),
        "End_Time": end.isoformat().replace("+00:00", "Z"),
        "Duration_seconds": round(duration.total_seconds(), 3),
    }
    if extra:
        payload.update(extra)

    if success:
        logger.info("STATUS: COMPLETED SUCCESSFULLY", extra={"metadata": payload})
    else:
        payload["error"] = error_message
        logger.error("STATUS: FAILED", extra={"metadata": payload})


# Backwards-compatible aliases for earlier code that imported module-level functions
log_start = log_header
log_end = log_footer
