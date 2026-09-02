# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnusedFunction=false
"""HTTP control-plane server.

FastAPI app, lifespan-managed, served by uvicorn on the HTTP port. At M0 it
exposes only the operational surface needed for the container health gate:

    GET /healthz   liveness  — process is up and the event loop is turning
    GET /readyz    readiness — hot pool can round-trip `SELECT 1` to Postgres
    GET /metrics   Prometheus exposition

The REST job mirror is the 1:1 curl-able / UI-facing counterpart to the gRPC
ClientService; the same service classes back both transports. The transport
stays dumb — it decodes, calls a service, encodes; engine errors propagate to
the exception handler which maps SymbaError.status_code.

    endpoints (M1 slice)
    --------------------
    POST /v1/jobs          submit 1..n specs in one tx -> job ids + dedup flags
    GET  /v1/jobs/{id}     read one job across hot + archive (tenant-scoped)

    liveness vs readiness
    ---------------------
    /healthz -> 200 as soon as the app is serving (restart signal for the LB)
    /readyz  -> 200 only when Postgres is reachable (traffic-gating signal);
                503 during a PG blip so the LB drains this instance instead of
                sending it claims it cannot serve.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from symba.core.errors import SymbaError, Unauthenticated
from symba.db.records import (
    CronRow,
    EventRow,
    JobListItem,
    JobRow,
    QueueStat,
    SubmitSpec,
    TreeEdge,
    WorkerRow,
)
from symba.observability.logging import logger, uvicorn_log_config
from symba.observability.metrics import REGISTRY
from symba.observability.tracing import TraceIDMiddleware
from symba.transport.auth import Principal
from symba.transport.state import EngineState

logger = logger.bind(service="http_server", context="engine/transport")


class JobSpecDTO(BaseModel):
    task_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    group_key: str | None = None
    max_concurrent_per_group: int | None = None
    dedup_key: str | None = None
    runs_on: list[str] = Field(default_factory=list)
    rate_class: str | None = None
    max_attempts: int = 5
    timeout_s: int = 600
    lease_ttl_s: int = 60
    # Chain: on_success is the next task after this one; chain_tail is the rest.
    # The engine advances the linked list on each success.
    on_success: str | None = None
    chain_tail: list[str] = Field(default_factory=list)


class SubmitRequestDTO(BaseModel):
    tenant: str = "default"
    specs: list[JobSpecDTO] = Field(min_length=1)


class SubmitResponseDTO(BaseModel):
    job_ids: list[str]
    deduplicated: list[bool]


class FanOutRequestDTO(BaseModel):
    tenant: str = "default"
    ctx_id: str | None = None
    children: list[JobSpecDTO] = Field(min_length=1)
    on_complete: JobSpecDTO
    gate_policy: str = "all_success"  # all_success | all_terminal | quorum(n)


class FanOutResponseDTO(BaseModel):
    child_job_ids: list[str]
    gate_id: str


class CancelResponseDTO(BaseModel):
    cancelled: bool
    was_running: bool
    cascaded: list[str]
    note: str


class SignalRequestDTO(BaseModel):
    tenant: str = "default"
    wait_key: str
    payload: dict[str, Any] | None = None
    signaled_by: str | None = None


class SignalResponseDTO(BaseModel):
    delivered: bool  # True: woke a waiting job; False: parked for a future waiter
    job_id: str | None


class ResubmitResponseDTO(BaseModel):
    new_job_id: str
    resubmitted_from: str


class JobResponseDTO(BaseModel):
    id: str
    tenant: str
    task_name: str
    state: str
    attempt: int
    priority: int
    group_key: str | None
    ctx_id: str | None
    # The worker holding/that last held the job. Surfaced so the job detail can
    # link back to the fleet drill-in; the "engine:{tags}" placeholder
    # (pre-attribution / claimed-but-unassigned) is normalized to None here.
    worker: str | None
    # Full bodies for the job-detail view (list endpoints omit these — see JobListItemDTO).
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error_history: list[dict[str, Any]]
    created_at: str
    started_at: str | None
    finished_at: str | None

    @classmethod
    def from_row(cls, row: JobRow) -> JobResponseDTO:
        return cls(
            id=row.id,
            tenant=row.tenant,
            task_name=row.task_name,
            state=row.state,
            attempt=row.attempt,
            priority=row.priority,
            group_key=row.group_key,
            ctx_id=row.ctx_id,
            worker=_real_worker(row.claimed_by),
            payload=row.payload,
            result=row.result,
            error_history=row.error_history,
            created_at=row.created_at.isoformat(),
            started_at=row.started_at.isoformat() if row.started_at else None,
            finished_at=row.finished_at.isoformat() if row.finished_at else None,
        )


# ── UI read/stats DTOs ────────────────────────────────────────────────────────


class JobListItemDTO(BaseModel):
    id: str
    tenant: str
    task_name: str
    state: str
    attempt: int
    priority: int
    group_key: str | None
    ctx_id: str | None
    wait_key: str | None
    worker: str | None
    error_history: list[dict[str, Any]]
    created_at: str
    started_at: str | None
    finished_at: str | None

    @classmethod
    def from_row(cls, r: JobListItem) -> JobListItemDTO:
        return cls(
            id=r.id,
            tenant=r.tenant,
            task_name=r.task_name,
            state=r.state,
            attempt=r.attempt,
            priority=r.priority,
            group_key=r.group_key,
            ctx_id=r.ctx_id,
            wait_key=r.wait_key,
            worker=_real_worker(r.claimed_by),
            error_history=r.error_history,
            created_at=r.created_at.isoformat(),
            started_at=r.started_at.isoformat() if r.started_at else None,
            finished_at=r.finished_at.isoformat() if r.finished_at else None,
        )


class JobListDTO(BaseModel):
    jobs: list[JobListItemDTO]


class BoardDTO(BaseModel):
    counts: dict[str, int]  # state -> count


class QueueStatDTO(BaseModel):
    task_name: str
    rate_class: str | None
    depth: int
    oldest_age_s: int

    @classmethod
    def from_row(cls, r: QueueStat) -> QueueStatDTO:
        return cls(task_name=r.task_name, rate_class=r.rate_class, depth=r.depth, oldest_age_s=r.oldest_age_s)


class QueuesDTO(BaseModel):
    queues: list[QueueStatDTO]


class WorkerDTO(BaseModel):
    worker_id: str
    tags: list[str]
    labels: dict[str, Any]
    slots: int
    slots_busy: int
    last_seen: str
    stale: bool

    @classmethod
    def from_row(cls, r: WorkerRow) -> WorkerDTO:
        return cls(
            worker_id=r.worker_id,
            tags=r.tags,
            labels=r.labels,
            slots=r.slots,
            slots_busy=r.slots_busy,
            last_seen=r.last_seen.isoformat(),
            stale=r.stale,
        )


class WorkersDTO(BaseModel):
    workers: list[WorkerDTO]


class CronDTO(BaseModel):
    schedule_id: str
    cron_expr: str
    task_name: str
    tenant: str
    enabled: bool
    last_fire: str | None
    next_fire: str | None
    created_at: str
    payload: dict[str, Any]

    @classmethod
    def from_row(cls, r: CronRow) -> CronDTO:
        return cls(
            schedule_id=r.schedule_id,
            cron_expr=r.cron_expr,
            task_name=r.task_name,
            tenant=r.tenant,
            enabled=r.enabled,
            last_fire=r.last_fire.isoformat() if r.last_fire else None,
            next_fire=r.next_fire.isoformat() if r.next_fire else None,
            created_at=r.created_at.isoformat(),
            payload=r.payload or {},
        )


class CronListDTO(BaseModel):
    schedules: list[CronDTO]


class CronToggleDTO(BaseModel):
    enabled: bool


class CronUpsertDTO(BaseModel):
    schedule_id: str
    cron_expr: str
    task_name: str
    payload: dict[str, Any] | None = None
    enabled: bool = True


class EventDTO(BaseModel):
    event: str
    at: str
    detail: dict[str, Any] | None

    @classmethod
    def from_row(cls, r: EventRow) -> EventDTO:
        return cls(event=r.event, at=r.at.isoformat(), detail=r.detail)


class EventsDTO(BaseModel):
    events: list[EventDTO]


class TreeEdgeDTO(BaseModel):
    upstream: str
    downstream: str
    alias: str | None

    @classmethod
    def from_row(cls, r: TreeEdge) -> TreeEdgeDTO:
        return cls(upstream=r.upstream, downstream=r.downstream, alias=r.alias)


class TreeDTO(BaseModel):
    edges: list[TreeEdgeDTO]


def _real_worker(claimed_by: str | None) -> str | None:
    """Normalize claimed_by to a real worker_id for the UI.

    claim.sql stamps a batch placeholder "engine:{tags}" before the matcher re-stamps
    the real worker_id; a job claimed-but-not-yet-assigned (rare, all slots full) can
    linger with the placeholder. The fleet drill-in only wants real workers, so the
    placeholder — and NULL (never claimed) — both surface as None.
    """
    if claimed_by is None or claimed_by.startswith("engine:"):
        return None
    return claimed_by


def _to_spec(tenant: str, dto: JobSpecDTO) -> SubmitSpec:
    return SubmitSpec(
        task_name=dto.task_name,
        payload=dto.payload,
        tenant=tenant,
        priority=dto.priority,
        group_key=dto.group_key,
        max_concurrent_per_group=dto.max_concurrent_per_group,
        dedup_key=dto.dedup_key,
        runs_on=dto.runs_on,
        rate_class=dto.rate_class,
        max_attempts=dto.max_attempts,
        timeout_s=dto.timeout_s,
        lease_ttl_s=dto.lease_ttl_s,
        on_success=dto.on_success,
        chain_tail=dto.chain_tail,
    )


def build_app(state: EngineState) -> FastAPI:
    app = FastAPI(title="Symba", docs_url="/docs", openapi_url="/openapi.json")
    authenticator = state.authenticator

    # Unauthenticated surface: liveness/readiness probes and the metrics scrape must
    # work before any credential is presented (the LB and Prometheus are trusted
    # infra, not tenants). Docs/openapi are served for the generated client + humans.
    _PUBLIC_PATHS = frozenset({"/healthz", "/readyz", "/metrics", "/docs", "/openapi.json"})

    app.add_middleware(TraceIDMiddleware)

    @app.middleware("http")
    async def _auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        # Fail-closed: every /v1 path requires a Principal; anything else on the public
        # list is exempt. The resolved principal rides on request.state for the
        # require_principal dependency (which enforces tenant scoping per handler).
        if request.url.path in _PUBLIC_PATHS or not request.url.path.startswith("/v1"):
            return await call_next(request)
        peer_ip = request.client.host if request.client else None
        try:
            principal = authenticator.authenticate(
                authorization=request.headers.get("authorization"),
                tenant_header=request.headers.get("x-symba-tenant"),
                peer_ip=peer_ip,
            )
        except SymbaError as exc:
            return JSONResponse(status_code=exc.status_code, content=exc.to_dict())
        request.state.principal = principal
        return await call_next(request)

    @app.exception_handler(SymbaError)
    async def _symba_error_handler(_request: Request, exc: SymbaError) -> JSONResponse:
        # Transports never catch engine errors in handlers (workspace rule); this
        # one middleware maps the whole SymbaError hierarchy to its HTTP shape.
        # A retryable-by-contract error (e.g. tenant queue cap) carries a
        # retry_after_s hint in its context -> surface it as the Retry-After header so
        # clients back off deterministically instead of hammering.
        headers: dict[str, str] | None = None
        retry_after = exc.context.get("retry_after_s")
        if retry_after is not None:
            headers = {"Retry-After": str(int(retry_after))}
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict(), headers=headers)

    def require_principal(request: Request) -> Principal:
        # The middleware set this for every /v1 path; its absence means the request
        # slipped past auth (should be impossible) — refuse rather than guess a tenant.
        principal = getattr(request.state, "principal", None)
        if principal is None:
            raise Unauthenticated("no authenticated principal on request")
        return principal

    @app.post("/v1/jobs", response_model=SubmitResponseDTO)
    async def submit_jobs(
        request: SubmitRequestDTO, principal: Principal = Depends(require_principal)
    ) -> SubmitResponseDTO:
        # Tenant is authoritative from the credential: a caller cannot submit into
        # another tenant by setting the body field.
        specs = [_to_spec(principal.tenant, dto) for dto in request.specs]
        outcome = await state.submit.submit(specs)
        return SubmitResponseDTO(job_ids=outcome.job_ids, deduplicated=outcome.deduplicated)

    @app.post("/v1/fanout", response_model=FanOutResponseDTO)
    async def fan_out(
        request: FanOutRequestDTO, principal: Principal = Depends(require_principal)
    ) -> FanOutResponseDTO:
        outcome = await state.fanout.fan_out(
            tenant=principal.tenant,
            ctx_id=request.ctx_id,
            children=[_to_spec(principal.tenant, c) for c in request.children],
            on_complete=_to_spec(principal.tenant, request.on_complete),
            policy=request.gate_policy,
        )
        return FanOutResponseDTO(child_job_ids=outcome.child_job_ids, gate_id=outcome.gate_id)

    @app.post("/v1/jobs/{job_id}/cancel", response_model=CancelResponseDTO)
    async def cancel_job(
        job_id: str, cascade: bool = False, principal: Principal = Depends(require_principal)
    ) -> CancelResponseDTO:
        # Tenant isolation: confirm the job belongs to the caller's tenant before
        # acting (get_job raises NotFound for another tenant's id -> 404, so an id
        # from a foreign tenant is indistinguishable from a nonexistent one).
        await state.submit.get_job(job_id=job_id, tenant=principal.tenant)
        outcome = await state.cancel.cancel(job_id=job_id, cascade=cascade)
        return CancelResponseDTO(
            cancelled=outcome.cancelled,
            was_running=outcome.was_running,
            cascaded=outcome.cascaded,
            note=outcome.note,
        )

    @app.post("/v1/signals", response_model=SignalResponseDTO)
    async def signal(request: SignalRequestDTO, principal: Principal = Depends(require_principal)) -> SignalResponseDTO:
        outcome = await state.signals.signal(
            tenant=principal.tenant,
            wait_key=request.wait_key,
            payload=request.payload,
            signaled_by=request.signaled_by,
        )
        return SignalResponseDTO(delivered=outcome.delivered, job_id=outcome.job_id)

    @app.post("/v1/jobs/{job_id}/resubmit", response_model=ResubmitResponseDTO)
    async def resubmit_job(job_id: str, principal: Principal = Depends(require_principal)) -> ResubmitResponseDTO:
        outcome = await state.resubmit.resubmit(job_id=job_id, tenant=principal.tenant)
        return ResubmitResponseDTO(new_job_id=outcome.new_job_id, resubmitted_from=outcome.resubmitted_from)

    @app.get("/v1/jobs/{job_id}", response_model=JobResponseDTO)
    async def get_job(job_id: str, principal: Principal = Depends(require_principal)) -> JobResponseDTO:
        row = await state.submit.get_job(job_id=job_id, tenant=principal.tenant)
        return JobResponseDTO.from_row(row)

    # ── UI read/stats surface ───────────────────────────────────────────────────

    @app.get("/v1/jobs", response_model=JobListDTO)
    async def list_jobs(
        principal: Principal = Depends(require_principal),
        state_filter: str | None = None,
        task_name: str | None = None,
        ctx_id: str | None = None,
        worker: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> JobListDTO:
        rows = await state.query.jobs(
            tenant=principal.tenant,
            state=state_filter,
            task_name=task_name,
            ctx_id=ctx_id,
            worker=worker,
            limit=limit,
            offset=offset,
        )
        return JobListDTO(jobs=[JobListItemDTO.from_row(r) for r in rows])

    @app.get("/v1/stats/board", response_model=BoardDTO)
    async def stats_board(principal: Principal = Depends(require_principal)) -> BoardDTO:
        return BoardDTO(counts=await state.query.board(tenant=principal.tenant))

    @app.get("/v1/stats/queues", response_model=QueuesDTO)
    async def stats_queues(principal: Principal = Depends(require_principal)) -> QueuesDTO:
        rows = await state.query.queues(tenant=principal.tenant)
        return QueuesDTO(queues=[QueueStatDTO.from_row(r) for r in rows])

    @app.get("/v1/workers", response_model=WorkersDTO)
    async def list_workers(_principal: Principal = Depends(require_principal)) -> WorkersDTO:
        # Fleet is engine-global (workers serve all tenants); auth still required so
        # only authenticated operators can enumerate the fleet.
        rows = await state.query.workers()
        return WorkersDTO(workers=[WorkerDTO.from_row(r) for r in rows])

    @app.get("/v1/cron", response_model=CronListDTO)
    async def list_cron(principal: Principal = Depends(require_principal)) -> CronListDTO:
        rows = await state.query.cron(tenant=principal.tenant)
        return CronListDTO(schedules=[CronDTO.from_row(r) for r in rows])

    @app.put("/v1/cron/{schedule_id}")
    async def set_cron_enabled(
        schedule_id: str,
        request: CronToggleDTO,
        principal: Principal = Depends(require_principal),
    ) -> dict[str, bool]:
        await state.query.set_cron_enabled(schedule_id=schedule_id, tenant=principal.tenant, enabled=request.enabled)
        return {"enabled": request.enabled}

    @app.post("/v1/cron", response_model=CronDTO)
    async def upsert_cron(
        request: CronUpsertDTO,
        principal: Principal = Depends(require_principal),
    ) -> CronDTO:
        row = await state.query.upsert_cron(
            schedule_id=request.schedule_id,
            cron_expr=request.cron_expr,
            task_name=request.task_name,
            payload=request.payload,
            tenant=principal.tenant,
            enabled=request.enabled,
        )
        return CronDTO.from_row(row)

    @app.delete("/v1/cron/{schedule_id}")
    async def delete_cron(
        schedule_id: str,
        principal: Principal = Depends(require_principal),
    ) -> dict[str, bool]:
        await state.query.delete_cron(schedule_id=schedule_id, tenant=principal.tenant)
        return {"deleted": True}

    @app.get("/v1/jobs/{job_id}/events", response_model=EventsDTO)
    async def job_events(job_id: str, principal: Principal = Depends(require_principal)) -> EventsDTO:
        rows = await state.query.events(job_id=job_id, tenant=principal.tenant)
        return EventsDTO(events=[EventDTO.from_row(r) for r in rows])

    @app.get("/v1/jobs/{job_id}/tree", response_model=TreeDTO)
    async def job_tree(job_id: str, principal: Principal = Depends(require_principal)) -> TreeDTO:
        # The DAG is scoped by ctx_id; resolve it from the job first (get_job raises
        # NotFound for an unknown/foreign-tenant job, which the middleware maps to 404).
        job = await state.submit.get_job(job_id=job_id, tenant=principal.tenant)
        if job.ctx_id is None:
            return TreeDTO(edges=[])
        rows = await state.query.tree(ctx_id=job.ctx_id, tenant=principal.tenant)
        return TreeDTO(edges=[TreeEdgeDTO.from_row(r) for r in rows])

    @app.get("/v1/jobs/{job_id}/checkpoints")
    async def job_checkpoint(job_id: str, principal: Principal = Depends(require_principal)) -> dict[str, Any]:
        # Confirm ownership before returning checkpoint bytes (they can hold arbitrary
        # handler state); a foreign-tenant id 404s via get_job.
        await state.submit.get_job(job_id=job_id, tenant=principal.tenant)
        data = await state.checkpoints.get(job_id=job_id, tenant=principal.tenant)
        return {"job_id": job_id, "checkpoint": data}

    @app.get("/v1/events/stream")
    async def events_stream(request: Request, principal: Principal = Depends(require_principal)) -> StreamingResponse:
        # Server-Sent Events: one long-lived text/event-stream fed by the engine's
        # in-process ledger fan-out. A keepalive comment every 15s holds the
        # connection through proxies; the fetch-based UI client auto-reconnects.
        async def _gen() -> AsyncIterator[bytes]:
            async with state.events.subscribe(tenant=principal.tenant) as queue:
                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        row: EventRow = await asyncio.wait_for(queue.get(), timeout=15.0)
                    except TimeoutError:
                        yield b": keepalive\n\n"
                        continue
                    payload = json.dumps(
                        {
                            "job_id": row.job_id,
                            "ctx_id": row.ctx_id,
                            "event": row.event,
                            "at": row.at.isoformat(),
                            "detail": row.detail,
                        }
                    )
                    yield f"event: job_event\ndata: {payload}\n\n".encode()

        return StreamingResponse(_gen(), media_type="text/event-stream")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(response: Response) -> dict[str, str]:
        if not state.serving:
            response.status_code = 503
            return {"status": "starting"}
        try:
            async with state.pools.hot.acquire() as conn:
                await conn.fetchval("SELECT 1")
        except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
            logger.warning("[readyz] Postgres unreachable", error=str(exc))
            response.status_code = 503
            return {"status": "degraded"}
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    return app


async def serve_http(state: EngineState) -> None:
    """Run uvicorn in-process on the shared event loop."""
    import uvicorn

    cfg = state.config
    server = uvicorn.Server(
        uvicorn.Config(
            build_app(state),
            host="0.0.0.0",  # noqa: S104 - container-internal; LB terminates ingress
            port=cfg.server.http_port,
            log_config=uvicorn_log_config,
            access_log=True,
            lifespan="on",
        )
    )
    logger.info("[serve_http] Starting HTTP server", port=cfg.server.http_port)
    await server.serve()
