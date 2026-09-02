"""Typed records for the hot-path repository.

RORO boundary between services and the repository: services pass a *Spec/*Args
object and receive a typed result, never raw asyncpg Records. Keeping these here
(not in the repository module) lets services import the shapes without importing
asyncpg.

    why dataclasses and not pydantic here
    -------------------------------------
    These cross the in-process service<->repo boundary only; they are never
    validated from untrusted input (that is the transport/DTO layer's job). Plain
    slotted dataclasses keep the hot path allocation-cheap and dependency-light.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def _empty_payload() -> dict[str, Any]:
    """Return a precisely typed payload for dataclass default factories."""
    return {}


@dataclass(slots=True)
class SubmitSpec:
    """One job to insert. Mirrors the submit.sql column order."""

    task_name: str
    payload: dict[str, Any]
    tenant: str = "default"
    pipeline: str | None = None
    stage: str | None = None
    ctx_id: str | None = None
    state: str = "submitted"
    priority: int = 0
    group_key: str | None = None
    max_concurrent_per_group: int | None = None
    dedup_key: str | None = None
    runs_on: list[str] = field(default_factory=lambda: [])
    rate_class: str | None = None
    on_success: str | None = None
    chain_tail: list[str] = field(default_factory=lambda: [])
    on_failure: dict[str, Any] | None = None
    remaining_deps: int = 0
    # depends_on edges: (upstream_job_id, alias) pairs. remaining_deps is
    # derived from len(depends_on) at submit unless the caller set it explicitly;
    # alias is the key under ctx.output (None -> resolve by producer task_name).
    depends_on: list[tuple[str, str | None]] = field(default_factory=lambda: [])
    max_attempts: int = 5
    backoff: dict[str, Any] | None = None
    timeout_s: int = 600
    run_at: datetime | None = None
    lease_ttl_s: int = 60
    parent_gate_id: str | None = None


@dataclass(slots=True)
class SubmitResult:
    job_id: str | None  # None -> deduplicated (ON CONFLICT hit)
    deduplicated: bool


@dataclass(slots=True)
class ClaimedJob:
    """A row returned by claim.sql (RETURNING j.*), narrowed to what the matcher
    and the assignment builder need. `raw` keeps the full record for the
    transport to build a JobAssignment without a second read."""

    id: str
    task_name: str
    tenant: str
    group_key: str | None
    max_concurrent_per_group: int | None
    priority: int
    lease_token: str
    lease_ttl_s: int
    payload: dict[str, Any]
    attempt: int
    raw: dict[str, Any]


@dataclass(slots=True)
class ExhaustedLease:
    """An abandoned execution whose stored retry budget is exhausted."""

    job_id: str
    lease_token: str


@dataclass(slots=True)
class InlineUpstream:
    """One producer in the Job.upstream inline tier (chain pred or depends_on)."""

    asking_job_id: str
    key: str
    producer_job_id: str
    result: dict[str, Any] | None


@dataclass(slots=True)
class TerminalRow:
    """Post-archive projection shared by complete/fail_die/cancel."""

    id: str
    tenant: str
    ctx_id: str | None
    task_name: str
    group_key: str | None
    max_concurrent_per_group: int | None
    on_success: str | None = None
    chain_tail: list[str] = field(default_factory=lambda: [])
    on_failure: dict[str, Any] | None = None
    parent_gate_id: str | None = None
    prior_state: str | None = None
    # Inherited by a chain continuation so the next link
    # stays in the same pipeline lineage and routing lane. Defaulted so cancel/fail
    # callers that don't SELECT them still construct a valid row.
    pipeline: str | None = None
    stage: str | None = None
    priority: int = 0
    runs_on: list[str] = field(default_factory=lambda: [])
    rate_class: str | None = None
    lease_ttl_s: int = 60
    # Timing for the latency histograms (complete.sql only selects these; fail/cancel
    # callers leave them None). queue-wait = started - created, exec = finished - started.
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True)
class ResultRow:
    id: str
    state: str
    result: dict[str, Any] | None
    error_history: list[dict[str, Any]]


@dataclass(slots=True)
class CronDue:
    """A schedule due to fire this tick, read through cron_due.sql."""

    schedule_id: str
    cron_expr: str
    task_name: str
    payload: dict[str, Any]
    tenant: str
    last_fire: datetime | None
    next_fire: datetime | None


@dataclass(slots=True)
class JobRow:
    """Single-job read projection for GET /v1/jobs/{id}, read through
    the jobs_all view so it works for live and archived jobs alike."""

    id: str
    tenant: str
    task_name: str
    state: str
    attempt: int
    priority: int
    group_key: str | None
    ctx_id: str | None
    # The worker currently/last holding the job. Real worker_id once the
    # matcher re-stamps it post-assignment; may still be the batch "engine:{tags}"
    # placeholder in the brief window before re-stamp, or NULL before first claim.
    claimed_by: str | None
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error_history: list[dict[str, Any]]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


# ── UI read/stats surface ─────────────────────────────────────────────────────


@dataclass(slots=True)
class JobListItem:
    """One row of the UI job list. Lighter than JobRow — the list omits
    payload/result bodies (fetched lazily on job-detail) but carries error_history
    so the DLQ view can group by stack_hash client-side."""

    id: str
    tenant: str
    task_name: str
    pipeline: str | None
    stage: str | None
    state: str
    attempt: int
    priority: int
    group_key: str | None
    ctx_id: str | None
    wait_key: str | None
    claimed_by: str | None
    error_history: list[dict[str, Any]]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(slots=True)
class QueueStat:
    """One (task, rate_class) lane's depth + head-of-line age (Queues view)."""

    task_name: str
    rate_class: str | None
    depth: int
    oldest_age_s: int


@dataclass(slots=True)
class WorkerRow:
    """One fleet-view worker row (Fleet view)."""

    worker_id: str
    tags: list[str]
    labels: dict[str, Any]
    slots: int
    slots_busy: int
    last_seen: datetime
    stale: bool


@dataclass(slots=True)
class CronRow:
    """One cron-view schedule row (Cron view)."""

    schedule_id: str
    cron_expr: str
    task_name: str
    tenant: str
    enabled: bool
    last_fire: datetime | None
    next_fire: datetime | None
    created_at: datetime
    payload: dict[str, Any] = field(default_factory=_empty_payload)


@dataclass(slots=True)
class EventRow:
    """One audit-ledger row for the job-detail timeline and the SSE stream."""

    id: int
    job_id: str
    tenant: str
    ctx_id: str | None
    event: str
    at: datetime
    detail: dict[str, Any] | None


@dataclass(slots=True)
class TreeEdge:
    """One dependency edge in a ctx pipeline DAG (Pipeline view)."""

    upstream: str
    downstream: str
    alias: str | None
