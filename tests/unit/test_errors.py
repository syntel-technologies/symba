"""L1: engine error taxonomy.

Covers the shared error base (`SymbaError`): default vs custom message,
context capture, best-effort trace_id (including both lazy-lookup fallbacks), the
`to_dict()` HTTP projection the control plane serializes, and that every subclass
carries the status_code / grpc_code / error_code mapping the transports rely on.
"""

from __future__ import annotations

import sys

import pytest

from symba.core import errors as e
from symba.core.errors import (
    NotFound,
    PayloadTooLarge,
    StaleLease,
    SymbaError,
    TenantQuotaExceeded,
)

pytestmark = pytest.mark.l1


def test_default_message_falls_back_to_docstring():
    # No explicit message -> the class docstring is used as the default.
    err = StaleLease()
    assert err.message == StaleLease.__doc__
    assert str(err) == StaleLease.__doc__


def test_default_message_uses_error_code_when_no_docstring():
    class Bare(SymbaError):
        error_code = "bare"

    Bare.__doc__ = None
    assert Bare().message == "bare"


def test_custom_message_and_context_captured():
    err = NotFound("job gone", job_id="j1", tenant="acme")
    assert err.message == "job gone"
    assert err.context == {"job_id": "j1", "tenant": "acme"}


def test_trace_id_is_populated_when_tracing_available():
    # trace_id_context has a default UUID, so the happy lookup path returns a str.
    assert isinstance(SymbaError("x").trace_id, str)


def test_to_dict_is_the_transport_projection():
    err = PayloadTooLarge("too big", size=999)
    body = err.to_dict()
    assert body == {
        "status_code": 413,
        "error_code": "payload_too_large",
        "message": "too big",
        "trace_id": err.trace_id,
        "context": {"size": 999},
    }


@pytest.mark.parametrize(
    ("exc", "status_code", "grpc_code", "error_code"),
    [
        (StaleLease, 409, "ABORTED", "stale_lease"),
        (NotFound, 404, "NOT_FOUND", "not_found"),
        (PayloadTooLarge, 413, "INVALID_ARGUMENT", "payload_too_large"),
        (TenantQuotaExceeded, 429, "RESOURCE_EXHAUSTED", "tenant_quota_exceeded"),
    ],
)
def test_subclass_mappings(exc: type[SymbaError], status_code: int, grpc_code: str, error_code: str):
    err = exc()
    assert err.status_code == status_code
    assert err.grpc_code == grpc_code
    assert err.error_code == error_code
    assert isinstance(err, SymbaError)


# --------------------------------------------------------------------------- #
# _current_trace_id fallbacks (import-purity contract: errors must not hard-depend
# on observability). Both failure branches degrade to None, never raise.
# --------------------------------------------------------------------------- #


def test_trace_id_none_when_tracing_import_fails(monkeypatch: pytest.MonkeyPatch):
    # Simulate the observability module being unavailable (pure-test/import cycle).
    monkeypatch.setitem(sys.modules, "symba.observability.tracing", None)
    assert e._current_trace_id() is None


def test_trace_id_none_when_get_trace_id_raises(monkeypatch: pytest.MonkeyPatch):
    import symba.observability.tracing as tracing

    def _boom() -> str:
        raise RuntimeError("no context")

    monkeypatch.setattr(tracing, "get_trace_id", _boom)
    assert e._current_trace_id() is None
