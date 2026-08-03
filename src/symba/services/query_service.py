# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Read-only query service backing the operator UI.

Every UI view and every button is the public REST API (rule: "the UI has
zero private endpoints — anything an operator can click, they can script"). This
service owns the read/stats/list business rules; the HTTP transport is a thin
adapter over it (workspace rule: no logic in transports).

    view                 service method
    ---------------      --------------
    Live board       ->  board(tenant)
    Queues           ->  queues(tenant)
    Fleet            ->  workers()
    Cron             ->  cron(tenant) / set_cron_enabled(...)
    Failed / DLQ     ->  jobs(state='dead', ...)      (client groups by stack_hash)
    Waiting          ->  jobs(state='waiting', ...)
    Pipeline         ->  jobs(ctx_id=...) + tree(ctx_id)
    Job detail       ->  events(job_id)               (+ submit.get_job / checkpoints)

All reads use the GENERAL pool (never the hot pool): UI queries must never contend
with the claim path (hot pool is reserved for the dispatcher/worker RPCs). Page
sizes are hard-capped so a huge archive can never stream unbounded rows.
"""

from __future__ import annotations

from croniter import croniter

from symba.core.errors import NotFound, PermissionDenied, ValidationError
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import CronRow, EventRow, JobListItem, QueueStat, TreeEdge, WorkerRow
from symba.observability.logging import logger

logger = logger.bind(service="query_service", context="engine/services")

_MAX_PAGE = 500  # hard cap: the console never pulls more than this per request


class QueryService:
    def __init__(self, pools: Pools) -> None:
        self._pools = pools

    async def jobs(
        self,
        *,
        tenant: str = "default",
        state: str | None = None,
        task_name: str | None = None,
        ctx_id: str | None = None,
        worker: str | None = None,
        parent_gate_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[JobListItem]:
        capped = max(1, min(limit, _MAX_PAGE))
        async with self._pools.general.acquire() as conn:
            return await repo.list_jobs(
                conn,
                tenant=tenant,
                state=state,
                task_name=task_name,
                ctx_id=ctx_id,
                claimed_by=worker,
                parent_gate_id=parent_gate_id,
                limit=capped,
                offset=max(0, offset),
            )

    async def board(self, *, tenant: str = "default") -> dict[str, int]:
        async with self._pools.general.acquire() as conn:
            return await repo.stats_board(conn, tenant=tenant)

    async def queues(self, *, tenant: str = "default") -> list[QueueStat]:
        async with self._pools.general.acquire() as conn:
            return await repo.stats_queues(conn, tenant=tenant)

    async def workers(self) -> list[WorkerRow]:
        async with self._pools.general.acquire() as conn:
            return await repo.list_workers(conn)

    async def cron(self, *, tenant: str = "default") -> list[CronRow]:
        async with self._pools.general.acquire() as conn:
            return await repo.list_cron(conn, tenant=tenant)

    async def set_cron_enabled(self, *, schedule_id: str, tenant: str = "default", enabled: bool) -> CronRow:
        """Enable/disable a schedule. Raises NotFound for an unknown/foreign schedule.

        Returns the resulting schedule row so the gRPC AdminService can echo the
        updated CronSchedule (the RPC returns CronSchedule, not a bare bool).
        """
        async with self._pools.general.acquire() as conn:
            ok = await repo.cron_set_enabled(conn, schedule_id=schedule_id, tenant=tenant, enabled=enabled)
            if not ok:
                raise NotFound(job_id=schedule_id, tenant=tenant)
            rows = await repo.list_cron(conn, tenant=tenant)
        logger.info("[cron] Toggled schedule", schedule_id=schedule_id, enabled=enabled, tenant=tenant)
        row = next((r for r in rows if r.schedule_id == schedule_id), None)
        if row is None:
            raise NotFound(job_id=schedule_id, tenant=tenant)
        return row

    async def upsert_cron(
        self,
        *,
        schedule_id: str,
        cron_expr: str,
        task_name: str,
        payload: dict[str, object] | None = None,
        tenant: str = "default",
        enabled: bool = True,
    ) -> CronRow:
        """Create/update a schedule. Validates the cron expression at the boundary.

        A malformed cron_expr is rejected here so no poison row reaches the loop.
        A schedule_id owned by another tenant raises PermissionDenied (the tenant-
        guarded upsert matches zero rows and returns None).
        """
        if not croniter.is_valid(cron_expr):
            raise ValidationError(f"invalid cron expression: {cron_expr!r}", cron_expr=cron_expr)
        async with self._pools.general.acquire() as conn:
            row = await repo.cron_upsert(
                conn,
                schedule_id=schedule_id,
                cron_expr=cron_expr,
                task_name=task_name,
                payload=payload or {},
                tenant=tenant,
                enabled=enabled,
            )
        if row is None:
            raise PermissionDenied(f"schedule {schedule_id!r} is owned by another tenant", schedule_id=schedule_id)
        logger.info("[cron] Upserted schedule", schedule_id=schedule_id, task_name=task_name, tenant=tenant)
        return row

    async def delete_cron(self, *, schedule_id: str, tenant: str = "default") -> None:
        """Delete a schedule. Raises NotFound for an unknown/foreign schedule."""
        async with self._pools.general.acquire() as conn:
            ok = await repo.cron_delete(conn, schedule_id=schedule_id, tenant=tenant)
        if not ok:
            raise NotFound(job_id=schedule_id, tenant=tenant)
        logger.info("[cron] Deleted schedule", schedule_id=schedule_id, tenant=tenant)

    async def events(self, *, job_id: str, tenant: str = "default") -> list[EventRow]:
        async with self._pools.general.acquire() as conn:
            return await repo.job_events(conn, job_id=job_id, tenant=tenant)

    async def tree(self, *, ctx_id: str, tenant: str = "default") -> list[TreeEdge]:
        async with self._pools.general.acquire() as conn:
            return await repo.job_tree_edges(conn, tenant=tenant, ctx_id=ctx_id)
