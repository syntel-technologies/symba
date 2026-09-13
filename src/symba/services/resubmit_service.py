# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Resubmit / DLQ-replay service.

The ops surface for the dead-letter queue: a DEAD job is never auto-pruned (the DLQ
retention contract), so an operator triages `jobs_all WHERE final_state='dead'`
grouped by stack_hash and replays the ones that were transient failures.

    original (archived, DEAD)          fresh row (hot, QUEUED)
    -------------------------          -----------------------
    id = A, final_state='dead'    ──►  id = B (new uuidv7)
                                       resubmitted_from = A   (lineage)
                                       attempt = 0            (clean slate)
                                       dedup_key = NULL       (replay must not collide)

The original stays archived as the audit record; resubmit is NOT an in-place retry.
Each replay is one row insert — a bulk replay is just many single resubmits so a
malformed one id does not roll back the whole triage batch (ops predictability).
"""

from __future__ import annotations

from dataclasses import dataclass

from symba.core.errors import NotFound
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability import metrics
from symba.observability.logging import logger
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="resubmit_service", context="engine/services")


@dataclass(slots=True)
class ResubmitOutcome:
    """One replayed job: the fresh id and the original it descends from."""

    new_job_id: str
    resubmitted_from: str


class ResubmitService:
    def __init__(self, pools: Pools, registry: WorkerRegistry | None = None) -> None:
        self._pools = pools
        self._registry = registry

    async def resubmit(self, *, job_id: str, tenant: str = "default") -> ResubmitOutcome:
        """Replay one terminal job as a fresh queued attempt. Raises NotFound if the
        id is not a terminal job in this tenant."""
        async with self._pools.general.acquire() as conn:
            new_id = await repo.resubmit(conn, job_id=job_id, tenant=tenant)
        if new_id is None:
            raise NotFound(job_id=job_id, tenant=tenant)

        if self._registry is not None:
            self._registry.wake.set()  # a freshly-queued job exists
        metrics.resubmits_total.inc()
        logger.info("[resubmit] Replayed", resubmitted_from=job_id, new_job_id=new_id, tenant=tenant)
        return ResubmitOutcome(new_job_id=str(new_id), resubmitted_from=job_id)

    async def resubmit_many(self, *, job_ids: list[str], tenant: str = "default") -> list[ResubmitOutcome]:
        """Bulk DLQ replay. Each id is resubmitted independently so one bad id skips
        (logged) rather than failing the whole batch — ops replays should be robust."""
        outcomes: list[ResubmitOutcome] = []
        for job_id in job_ids:
            try:
                outcomes.append(await self.resubmit(job_id=job_id, tenant=tenant))
            except NotFound:
                logger.warning("[resubmit] Skipped non-terminal/unknown job", job_id=job_id, tenant=tenant)
        return outcomes
