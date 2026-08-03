"""Structured logging (mirrors a conventional structlog setup).

A single processor chain gives every log line a consistent shape so Symba logs
drop straight into a standard ELK/Fluentbit pipeline. Set
SYMBA_LOG__FILE_PATH=/var/log/app/symba.log for file logging.

This is the ONLY module allowed to `import logging` (enforced by
tools/check_imports.py). Import the module-level `logger` everywhere else:

    from symba.observability.logging import logger
    logger = logger.bind(service="claim_service", context="engine/services")

Config is read from env at import (SYMBA_LOG__*, SYMBA_APP__*) with defaults, so
importing `logger` never requires the pydantic config object to be built first,
keeping the module-level logger ergonomic to use anywhere in the codebase.
"""

import logging
import os
import re
import socket
import sys
import traceback
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import structlog
from structlog.typing import EventDict, WrappedLogger

from symba.observability.tracing import get_trace_id


def _level_to_int(name: str) -> int:
    """Map a level name to its int without the deprecated getLevelName(str)."""
    return getattr(logging, name.upper(), logging.INFO)


log_level = os.getenv("SYMBA_LOG__LEVEL", "INFO")
log_file_path = os.getenv("SYMBA_LOG__FILE_PATH", "")
enable_stdout = os.getenv("SYMBA_LOG__ENABLE_STDOUT", "true").lower() != "false"

APP_NAME = os.getenv("SYMBA_APP__NAME", "symba")
MAJOR_VERSION = os.getenv("SYMBA_APP__VERSION_MAJOR", "0")
MINOR_VERSION = os.getenv("SYMBA_APP__VERSION_MINOR", "1")
PATCH_VERSION = os.getenv("SYMBA_APP__VERSION_PATCH", "0")
ENVIRONMENT = os.getenv("SYMBA_APP__ENVIRONMENT", "development")


def _resolve_worker_name() -> str:
    """Resolve a stable name for this process for log correlation.

    Precedence: SYMBA_WORKER_NAME -> WORKER_NAME -> HOSTNAME ->
    "<hostname>-<short-uuid>" so multiple local processes stay distinguishable.
    """
    for env_var in ("SYMBA_WORKER_NAME", "WORKER_NAME", "HOSTNAME"):
        value = (os.getenv(env_var) or "").strip()
        if value:
            return value
    try:
        host = socket.gethostname() or "worker"
    except OSError:
        host = "worker"
    return f"{host}-{uuid.uuid4().hex[:8]}"


WORKER_NAME = _resolve_worker_name()


def add_context(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add application context to logs."""
    event_dict["app_name"] = APP_NAME
    event_dict["app_version"] = f"{MAJOR_VERSION}.{MINOR_VERSION}.{PATCH_VERSION}"
    event_dict["environment"] = ENVIRONMENT
    event_dict["worker_name"] = WORKER_NAME
    return event_dict


def add_trace_id(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add correlation/trace ID to logs."""
    event_dict["trace_id"] = get_trace_id()
    return event_dict


def add_error_info(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Expand an `error=<exc>` key into structured exception info."""
    exc = event_dict.get("error")
    if isinstance(exc, BaseException):
        try:
            stack: str | None = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        except Exception:
            stack = None
        event_dict["exception"] = {
            "name": type(exc).__name__,
            "message": str(exc),
            "stack": stack,
        }
        del event_dict["error"]
    return event_dict


def parse_uvicorn_access_log(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Structure Uvicorn access log lines into fields."""
    log_msg = str(event_dict.get("event", ""))
    match = re.match(
        r'(?P<ip>[^\s]+):\d+ - "(?P<method>[A-Z]+) (?P<url>[^\s]+) [^\s]+" (?P<status>\d+)',
        log_msg,
    )
    if match:
        log_data: dict[str, Any] = dict(match.groupdict())
        log_data["status"] = int(log_data["status"])
        log_data["service"] = "uvicorn"
        log_data["context"] = "api"
        event_dict.update(log_data)
        del event_dict["event"]
    return event_dict


_handlers: list[logging.Handler] = []

if enable_stdout:
    _handlers.append(logging.StreamHandler(stream=sys.stdout))

if log_file_path:
    log_path = Path(log_file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _handlers.append(RotatingFileHandler(log_path, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"))

if not _handlers:
    _handlers.append(logging.StreamHandler(stream=sys.stdout))

logging.basicConfig(format="", level=_level_to_int(log_level), handlers=_handlers)

# Suppress verbose third-party logs.
for _noisy, _lvl in (
    ("grpc", logging.WARNING),
    ("asyncpg", logging.WARNING),
    ("httpx", logging.WARNING),
    ("httpcore", logging.WARNING),
    ("urllib3", logging.WARNING),
):
    logging.getLogger(_noisy).setLevel(_lvl)

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        parse_uvicorn_access_log,
        add_error_info,
        add_context,
        add_trace_id,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(_level_to_int(log_level)),
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()


class StructlogHandler(logging.Handler):
    """Route stdlib log records (e.g. Uvicorn) through structlog."""

    def emit(self, record: logging.LogRecord) -> None:
        logger.msg(self.format(record))


uvicorn_log_config: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"format": "%(message)s"}},
    "handlers": {
        "structlog_handler": {"()": StructlogHandler, "formatter": "default"},
    },
    "loggers": {
        "uvicorn.error": {"handlers": ["structlog_handler"], "level": "INFO", "propagate": True},
        "uvicorn.access": {"handlers": ["structlog_handler"], "level": "INFO", "propagate": False},
    },
}

__all__ = ["logger", "uvicorn_log_config", "WORKER_NAME"]
