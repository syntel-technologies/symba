# pyright: reportDeprecated=false
"""Trace-ID propagation.

A per-request/per-task contextvar carries a correlation ID that the logging
pipeline stamps onto every line. The gRPC/HTTP transports set it from an inbound
header (or generate a UUID4); background loops (dispatcher, sweeper, cron) set it
per pass. OpenTelemetry span tracing (optional, OTLP) is configured separately by
`configure_tracing`.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

# NOTE: this module MUST NOT import symba.observability.logging at module scope —
# logging imports get_trace_id from here (services/tracing <- config/logging
# split). configure_tracing binds a logger lazily.

# Correlation ID for the current request/task. Read by shared logging processor.
trace_id_context: ContextVar[str] = ContextVar("trace_id", default=str(uuid4()))

_INBOUND_REQUEST_ID_HEADER = "x-symba-request-id"
_MAX_INBOUND_REQUEST_ID_LEN = 256


def get_trace_id() -> str:
    """Return the current correlation ID."""
    return trace_id_context.get()


def set_trace_id(trace_id: str) -> None:
    trace_id_context.set(trace_id)


def new_trace_id() -> str:
    """Generate + install a fresh correlation ID (used per background-loop pass)."""
    tid = str(uuid4())
    trace_id_context.set(tid)
    return tid


def _read_inbound_trace_id(request: Request) -> str | None:
    raw = request.headers.get(_INBOUND_REQUEST_ID_HEADER)
    if raw:
        value = raw.strip()
        if value and len(value) <= _MAX_INBOUND_REQUEST_ID_LEN:
            return value
    qs = request.query_params.get("trace_id")
    if qs and qs.strip():
        return qs.strip()
    return None


class TraceIDMiddleware:
    """Attach a stable correlation ID to every HTTP request.

    Resolution order: inbound X-Symba-Request-Id header -> ?trace_id= query ->
    fresh UUID4. Exposed on trace_id_context and request.state.trace_id.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        trace_id = _read_inbound_trace_id(request) or str(uuid4())
        token = trace_id_context.set(trace_id)
        request.state.trace_id = trace_id
        try:
            await self.app(scope, receive, send)
        finally:
            trace_id_context.reset(token)


def configure_tracing(otlp_endpoint: str) -> bool:
    """Enable OTLP span tracing if an endpoint is configured."""
    from symba.observability.logging import logger

    log = logger.bind(service="tracing", context="engine/observability")
    if not otlp_endpoint:
        log.debug("[configure_tracing] Disabled (no otlp_endpoint)")
        return False

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": "symba"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)
    log.info("[configure_tracing] Enabled", endpoint=otlp_endpoint)
    return True


# ── span model ────────────────────────────────────────────────────────────────
#
#   submit (control plane)  ──link──►  claim/execute (worker, 1 span/attempt)
#                                              │
#                                              ▼
#                                      complete (engine)
#
# The engine emits the `submit`, `claim`, and `complete` spans. Every span carries
# `ctx_id` (the join key to the app's own traces — including LLM traces on the app
# side; the engine records nothing LLM-specific per the boundary decision) plus the
# job_id and task. Cross-process linkage to the worker's `execute` span via a
# `traceparent` in job metadata is a worker-SDK concern (separate repo) and is NOT
# wired here; the engine spans are self-contained and joined app-side on ctx_id.
#
# When no OTLP endpoint is configured, get_tracer_provider() returns the API's
# no-op provider, so `engine_span(...)` is a cheap no-op — safe to leave on the hot
# path unconditionally.


@contextmanager
def engine_span(
    name: str, *, ctx_id: str | None, job_id: str | None, task: str | None = None, **attrs: Any
) -> Iterator[None]:
    """Open one engine-side span (submit|claim|complete) with the standard attributes.

    A no-op when tracing is disabled/unavailable. The underlying span context
    manager is entered EXACTLY once; attribute-setting is guarded so an SDK/exporter
    failure can never fail a job transition (observability is advisory), while
    exceptions from the wrapped body always propagate."""
    try:
        from opentelemetry import trace

        tracer = trace.get_tracer("symba.engine")
        with tracer.start_as_current_span(name) as span:
            try:
                if ctx_id is not None:
                    span.set_attribute("symba.ctx_id", ctx_id)
                if job_id is not None:
                    span.set_attribute("symba.job_id", job_id)
                if task is not None:
                    span.set_attribute("symba.task", task)
                for key, value in attrs.items():
                    span.set_attribute(f"symba.{key}", value)
            except Exception:  # attribute failure must not affect the body
                pass
            yield
    except ImportError:
        yield
