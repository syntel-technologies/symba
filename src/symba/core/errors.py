"""Engine error taxonomy.

These are the engine-internal exceptions raised by services and mapped to gRPC
status codes / HTTP responses by the transport layer. The SDK-facing handler
exceptions (RetryableError/FatalError raised BY handlers) live in the separate
SDK package; the engine only receives their classification over the wire via
FailRequest.retryable.

The base class carries a consistent shape (status_code / error_code / message /
trace_id / to_dict) so HTTP responses and log records are consistent, while
keeping Symba's `context` kwarg and `grpc_code` for the gRPC transport.

    core/errors.py MUST stay import-pure (no symba.observability, no transports).
    It lazily reads the trace_id to avoid a config/logging import cycle.
"""

from __future__ import annotations

from typing import Any


class SymbaError(Exception):
    """Base engine error."""

    error_code: str = "symba_error"
    status_code: int = 500  # HTTP mapping used by the control-plane transport
    grpc_code: str = "INTERNAL"  # gRPC status name used by the data-plane transport

    def __init__(self, message: str | None = None, **context: Any):
        self.message = message or self.__class__.__doc__ or self.error_code
        self.context = context
        self.trace_id = _current_trace_id()
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status_code": self.status_code,
            "error_code": self.error_code,
            "message": self.message,
            "trace_id": self.trace_id,
            "context": self.context,
        }


def _current_trace_id() -> str | None:
    """Best-effort correlation ID; None if tracing isn't wired (e.g. pure tests)."""
    try:
        from symba.observability.tracing import get_trace_id
    except Exception:
        return None
    try:
        return get_trace_id()
    except Exception:
        return None


class StaleLease(SymbaError):
    """The lease token does not match; another attempt owns this job."""

    error_code = "stale_lease"
    status_code = 409
    grpc_code = "ABORTED"


class NotFound(SymbaError):
    """Unknown job, tenant, or task."""

    error_code = "not_found"
    status_code = 404
    grpc_code = "NOT_FOUND"


class ResultTooLarge(SymbaError):
    """Result exceeds the 64KB cap: store a reference, not the data."""

    error_code = "result_too_large"
    status_code = 413
    grpc_code = "INVALID_ARGUMENT"


class PayloadTooLarge(SymbaError):
    """Payload exceeds the 256KB cap."""

    error_code = "payload_too_large"
    status_code = 413
    grpc_code = "INVALID_ARGUMENT"


class ChainTooLong(SymbaError):
    """Chain exceeds the configured maximum; use fan-out/deps instead."""

    error_code = "chain_too_long"
    status_code = 400
    grpc_code = "INVALID_ARGUMENT"


class FanOutTooLarge(SymbaError):
    """Fan-out exceeds the configured child limit."""

    error_code = "fanout_too_large"
    status_code = 400
    grpc_code = "INVALID_ARGUMENT"


class UpstreamTooLarge(SymbaError):
    """Inline upstream results exceed the per-assignment cap."""

    error_code = "upstream_too_large"
    status_code = 413
    grpc_code = "INVALID_ARGUMENT"


class TenantQuotaExceeded(SymbaError):
    """Tenant queued-jobs cap reached; retryable by contract."""

    error_code = "tenant_quota_exceeded"
    status_code = 429
    grpc_code = "RESOURCE_EXHAUSTED"


class ValidationError(SymbaError):
    """A request field failed validation (e.g. a malformed cron expression)."""

    error_code = "validation_error"
    status_code = 422
    grpc_code = "INVALID_ARGUMENT"


class Unauthenticated(SymbaError):
    """Missing or invalid credentials."""

    error_code = "unauthenticated"
    status_code = 401
    grpc_code = "UNAUTHENTICATED"


class PermissionDenied(SymbaError):
    """Authenticated but not authorized for this tenant/resource."""

    error_code = "permission_denied"
    status_code = 403
    grpc_code = "PERMISSION_DENIED"


class WaitKeyAlreadyConsumed(SymbaError):
    """A resumed handler re-waited on a key whose signal was already delivered.

    On re-entry the handler re-runs from the top; a same-key wait returns the consumed
    payload WITHOUT re-parking. Reusing the key for a NEW wait after it was consumed is
    a contract violation — the second wait would block forever.
    """

    error_code = "wait_key_already_consumed"
    status_code = 409
    grpc_code = "FAILED_PRECONDITION"


class ProtocolVersionUnsupported(SymbaError):
    """The client's protocol/SDK version is outside the engine's supported range."""

    error_code = "protocol_unsupported"
    status_code = 426
    grpc_code = "FAILED_PRECONDITION"
