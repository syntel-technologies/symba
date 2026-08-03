# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Cancel service — the per-state cancel matrix.

Cancel is state-dependent:

    state            action
    ---------------  --------------------------------------------------------
    submitted/queued/waiting   direct archive -> 'cancelled' (cancel_immediate)
    running                    set cancel_requested; the worker learns via the
                               next HeartbeatResponse.cancelled and aborts
                               cooperatively; the lease is the backstop.
    terminal (archived)        idempotent no-op (nothing to do)

With cascade=true, cancelling a job also archives its whole LIVE dependent cone as
'cancelled' (cascade_cancel), because those dependents can never become ready once
their upstream is gone. This reuses the exact recursive CTE the fail-die path uses.

    cancel(cascade=true)
    --------------------
        target ─cancel─►  archived 'cancelled'
           └─dependents─► archived 'cancelled' (transitive), each audited
"""

from __future__ import annotations

from dataclasses import dataclass

from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability.logging import logger
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="cancel_service", context="engine/services")


@dataclass(slots=True)
class CancelOutcome:
    cancelled: bool  # True if the target moved to cancelled OR was flagged running
    was_running: bool  # True -> cooperative cancel in flight (not yet terminal)
    cascaded: list[str]  # dependent ids archived as cancelled (cascade=true only)
    note: str


class CancelService:
    def __init__(self, pools: Pools, registry: WorkerRegistry | None = None) -> None:
        self._pools = pools
        self._registry = registry

    async def cancel(self, *, job_id: str, cascade: bool = False) -> CancelOutcome:
        cascaded: list[str] = []
        async with self._pools.general.acquire() as conn, conn.transaction():
            # Non-running states archive directly. cancel_immediate returns the
            # terminal row (or None if the job is running / already terminal).
            term = await repo.cancel_immediate(conn, job_id=job_id)
            if term is not None:
                await repo.record_event(conn, job_id=job_id, event="cancelled")
                if cascade:
                    cascaded = await self._cascade(conn, root_job_id=job_id)
                logger.info("[cancel] Cancelled", job_id=job_id, cascaded=len(cascaded))
                return CancelOutcome(cancelled=True, was_running=False, cascaded=cascaded, note="archived")

            # Running: flag it. The worker aborts on the next heartbeat; the lease
            # backstops a worker that ignores the flag.
            claimed_by = await repo.cancel_running(conn, job_id=job_id)
            if claimed_by is not None:
                await repo.record_event(conn, job_id=job_id, event="cancel_requested", detail={"worker": claimed_by})
                if cascade:
                    cascaded = await self._cascade(conn, root_job_id=job_id)
                logger.info("[cancel] Cancel requested (running)", job_id=job_id, worker=claimed_by)
                return CancelOutcome(cancelled=True, was_running=True, cascaded=cascaded, note="cancel_requested")

        # Neither path matched: already terminal / unknown -> idempotent no-op.
        logger.debug("[cancel] No-op (terminal/unknown)", job_id=job_id)
        return CancelOutcome(cancelled=False, was_running=False, cascaded=[], note="noop")

    async def _cascade(self, conn: object, *, root_job_id: str) -> list[str]:
        cancelled = await repo.cascade_cancel(conn, root_job_id=root_job_id)
        for dep_id in cancelled:
            await repo.record_event(
                conn, job_id=dep_id, event="dependency_cancelled", detail={"root_cause": root_job_id}
            )
        return cancelled
