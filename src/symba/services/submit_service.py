# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Control-plane submit + read service.

Owns the business rules the control plane needs; the REST and gRPC transports are
thin adapters over it (workspace rule: no logic in transports).

    submit
    ----------------------
    1. validate each spec's payload against the 256KB cap (PayloadTooLarge).
    2. compute the INITIAL state (core.readiness): a spec with declared deps or a
       future run_at starts 'submitted'; otherwise 'queued' (immediately claimable).
    3. insert all specs in ONE transaction (all-or-nothing batch submit).
    4. dedup hits are idempotent success (deduplicated=true), NOT errors.
    5. wake the dispatcher so a locally-submitted queued job is matched this tick
       instead of waiting for the idle-decay timer.

The wake is an optimization, never required for correctness: the dispatcher polls
regardless (no NOTIFY).
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime

from symba.config import SymbaConfig
from symba.core.errors import ChainTooLong, NotFound, PayloadTooLarge, TenantQuotaExceeded
from symba.core.states import JobState
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import JobRow, SubmitSpec
from symba.observability import metrics
from symba.observability.logging import logger
from symba.observability.tracing import engine_span
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="submit_service", context="engine/services")


@dataclass(slots=True)
class SubmitOutcome:
    job_ids: list[str]  # per spec; a dedup-hit slot carries the EXISTING job's id
    deduplicated: list[bool]


class SubmitService:
    def __init__(self, pools: Pools, registry: WorkerRegistry, config: SymbaConfig) -> None:
        self._pools = pools
        self._registry = registry
        self._max_payload_bytes = config.limits.max_payload_kb * 1024
        self._max_chain_len = config.limits.max_chain_len
        self._tenant_queued_cap = config.limits.tenant_queued_cap  # 0 = unlimited
        # Deterministic backoff hint surfaced as gRPC/REST Retry-After.
        self._quota_retry_after_s = config.sweeper.interval_s

    async def submit(self, specs: list[SubmitSpec]) -> SubmitOutcome:
        if not specs:
            return SubmitOutcome(job_ids=[], deduplicated=[])

        for spec in specs:
            self._prepare(spec)

        job_ids: list[str] = []
        deduplicated: list[bool] = []
        any_queued = False
        async with self._pools.general.acquire() as conn, conn.transaction():
            # Per-tenant queue cap: enforce INSIDE the tx so the
            # count and the inserts share one snapshot. Checked once per tenant against
            # the whole batch's contribution — an all-or-nothing batch must not push a
            # tenant past its cap. cap=0 disables (the common case, no extra round trip).
            if self._tenant_queued_cap > 0:
                await self._enforce_tenant_caps(conn, specs)
            for spec in specs:
                # `submit` span: root of the per-job trace, keyed on ctx_id
                # (the app-side join key). No-op unless an OTLP endpoint is set.
                with engine_span("symba.submit", ctx_id=spec.ctx_id, job_id=None, task=spec.task_name):
                    result = await repo.submit(conn, spec)
                job_ids.append(result.job_id or "")
                deduplicated.append(result.deduplicated)
                if result.deduplicated or result.job_id is None:
                    continue
                if spec.state == JobState.QUEUED:
                    any_queued = True
                # Wire depends_on edges in the SAME tx so a crash never
                # leaves a job that thinks it has deps with no edges to satisfy them.
                for upstream_id, alias in spec.depends_on:
                    await repo.insert_dependency(conn, job_id=result.job_id, depends_on_job_id=upstream_id, alias=alias)

        if any_queued:
            self._registry.wake.set()  # local-submit wake source

        for spec, ded in zip(specs, deduplicated, strict=True):
            state = "deduplicated" if ded else "submitted"
            metrics.jobs_total.labels(tenant=spec.tenant, task=spec.task_name, state=state).inc()
        logger.info("[submit] Batch submitted", count=len(specs), deduplicated=sum(deduplicated))
        return SubmitOutcome(job_ids=job_ids, deduplicated=deduplicated)

    async def _enforce_tenant_caps(self, conn: object, specs: list[SubmitSpec]) -> None:
        """Reject the batch if any tenant would exceed tenant_queued_cap.

        Counts once per distinct tenant in the batch (current live + this batch's
        additions for that tenant) so N specs for one tenant cost one COUNT, not N.
        Raises TenantQuotaExceeded (429 / RESOURCE_EXHAUSTED) with a Retry-After hint;
        retryable by contract, so the client backs off and re-submits."""
        batch_by_tenant = Counter(spec.tenant for spec in specs)
        for tenant, adding in batch_by_tenant.items():
            current = await repo.count_tenant_live(conn, tenant=tenant)  # type: ignore[arg-type]
            if current + adding > self._tenant_queued_cap:
                logger.warning(
                    "[submit] Tenant queue cap exceeded",
                    tenant=tenant,
                    current=current,
                    adding=adding,
                    cap=self._tenant_queued_cap,
                )
                raise TenantQuotaExceeded(
                    f"tenant '{tenant}' queue cap reached",
                    tenant=tenant,
                    current=current,
                    cap=self._tenant_queued_cap,
                    retry_after_s=self._quota_retry_after_s,
                )

    async def get_job(self, *, job_id: str, tenant: str) -> JobRow:
        # A syntactically invalid id can never match a row; treat it as NotFound
        # (404 / NOT_FOUND) rather than letting the UUID cast raise a DB DataError
        # that leaks as a 500 / gRPC UNKNOWN. This is the path the SDK doctor probe
        # exercises with a sentinel id (ENG-1).
        try:
            uuid.UUID(job_id)
        except (ValueError, AttributeError, TypeError):
            raise NotFound(job_id=job_id, tenant=tenant) from None
        async with self._pools.general.acquire() as conn:
            row = await repo.get_job(conn, job_id=job_id, tenant=tenant)
        if row is None:
            raise NotFound(job_id=job_id, tenant=tenant)
        return row

    def _prepare(self, spec: SubmitSpec) -> None:
        """Validate the backpressure caps and set the initial state in place."""
        size = len(json.dumps(spec.payload).encode())
        if size > self._max_payload_bytes:
            raise PayloadTooLarge(task_name=spec.task_name, size_bytes=size, cap_bytes=self._max_payload_bytes)
        # chain length = this job + its remaining chain_tail (a longer chain is a
        # design smell -> use fan-out/deps). on_success is the next hop;
        # chain_tail is everything after it, so total hops = 1 (on_success) + tail.
        chain_len = (1 if spec.on_success else 0) + len(spec.chain_tail)
        if chain_len > self._max_chain_len:
            raise ChainTooLong(task_name=spec.task_name, chain_len=chain_len, cap=self._max_chain_len)
        # remaining_deps is the transactional readiness counter: derive it
        # from the declared edges unless the caller pre-set it (e.g. fan-out gates
        # that manage the counter themselves).
        if spec.depends_on and spec.remaining_deps == 0:
            spec.remaining_deps = len(spec.depends_on)
        spec.state = _initial_state(spec)


def _initial_state(spec: SubmitSpec) -> str:
    """queued iff no deps AND run_at already due; otherwise submitted."""
    has_deps = spec.remaining_deps > 0
    is_deferred = spec.run_at is not None and spec.run_at > datetime.now(UTC)
    if has_deps or is_deferred:
        return JobState.SUBMITTED
    return JobState.QUEUED
