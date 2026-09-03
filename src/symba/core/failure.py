"""Safe worker-failure records for durable job history.

Provider exceptions often include HTTP response bodies, headers, prompts, and
credentials in ``str(exc)``. The engine therefore persists a message only when
a current SDK explicitly marks it as safely serialized, applies a second
redaction pass, and accepts only a small allow-list of diagnostic fields.
"""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from typing import Any

_MESSAGE_CAP = 2048
_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")
_HEX_RE = re.compile(r"^[a-fA-F0-9]{1,64}$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:/-]{1,128}$")
_SAFE_MODULE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
_SECRET_TOKEN_RE = re.compile(r"(?i)\b(?:sk|pk|api)-[A-Za-z0-9_-]{8,}\b")
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{6,}")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|secret|password)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_STRUCTURED_BODY_MARKERS = (
    "response body",
    "response_body",
    "response content",
    "response_content",
    "'headers':",
    '"headers":',
    "headers=",
    "'body':",
    '"body":',
    "body=",
    "{'error':",
    '{"error":',
    "<html",
    "<!doctype",
)


def build_error_entry(
    error_type: str,
    message: str,
    stack_hash: str,
    retryable: bool,
    *,
    message_safe: bool = False,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the only worker-failure shape allowed into ``error_history``."""

    entry: dict[str, Any] = {
        "type": error_type if _TYPE_RE.fullmatch(error_type) else "WorkerError",
        "message": _safe_message(message, approved=message_safe),
        "stack_hash": stack_hash if _HEX_RE.fullmatch(stack_hash) else "",
        "retryable": bool(retryable),
        "at": datetime.now(UTC).isoformat(),
    }
    safe_metadata = sanitize_error_metadata(metadata)
    if safe_metadata:
        entry["metadata"] = safe_metadata
    return entry


def sanitize_error_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Keep only bounded scalar diagnostics with known-safe semantics."""

    if not isinstance(metadata, dict):
        return {}

    safe: dict[str, Any] = {}
    status_code = metadata.get("status_code")
    if isinstance(status_code, int) and not isinstance(status_code, bool) and 100 <= status_code <= 599:
        safe["status_code"] = status_code

    for key in ("error_code", "request_id"):
        value = metadata.get(key)
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            text = str(value)
            if _SAFE_IDENTIFIER_RE.fullmatch(text):
                safe[key] = text

    module = metadata.get("module")
    if isinstance(module, str) and _SAFE_MODULE_RE.fullmatch(module):
        safe["module"] = module

    retry_after_s = normalize_retry_after(metadata.get("retry_after_s"))
    if retry_after_s is not None:
        safe["retry_after_s"] = retry_after_s
    return safe


def normalize_retry_after(value: Any) -> float | None:
    """Validate a downstream retry hint without accepting NaN or huge delays."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    delay = float(value)
    if not math.isfinite(delay) or delay < 0 or delay > 86_400:
        return None
    return delay


def _safe_message(message: str, *, approved: bool) -> str:
    if not approved:
        return "Worker task failed"

    compact = " ".join(message.split())
    compact = _BEARER_RE.sub("Bearer [redacted]", compact)
    compact = _SECRET_TOKEN_RE.sub("[redacted]", compact)
    compact = _SECRET_ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}[redacted]", compact)
    lowered = compact.casefold()
    if (
        not compact
        or any(marker in lowered for marker in _STRUCTURED_BODY_MARKERS)
        or compact.startswith(("{", "["))
        or "http://" in lowered
        or "https://" in lowered
    ):
        return "Worker task failed"
    return compact[:_MESSAGE_CAP]


__all__ = ["build_error_entry", "normalize_retry_after", "sanitize_error_metadata"]
