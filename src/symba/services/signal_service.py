# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Signal / WAITING rendezvous service.

The human-in-the-loop primitive: a running handler can `wait_for_event(key)` to park
until an external `signal(key, payload)` arrives. The hard part is the RACE — the signal
can arrive before OR after the wait — and both orders must resume the job EXACTLY once.

    two orders, one outcome (resume-once)
    -------------------------------------
    wait-first:                       signal-first:
      Wait: consume? none  ─┐           Signal: wake? none ─┐
            park WAITING     │                 park signal   │
      Signal: wake WAITING ──┘           Wait: consume it ───┘
            -> QUEUED (payload)                -> return payload, never parks

Each direction is ONE transaction so the "wake vs park" decision is atomic:
  * signal(): try to wake a waiter (signal.sql). If 0 rows, park the signal
    (signal_insert.sql) in the SAME tx — exactly one of the two happens.
  * wait():   try to consume a pending signal (wait_consume.sql). If a row comes back,
    the job resumes immediately without ever entering WAITING. If not, park the running
    job into WAITING (wait_park.sql), lease-guarded.

Re-entry: a resumed handler re-runs from the top and re-calls wait() on the same
key. The signal it is resuming on was already consumed, so a fresh consume finds nothing
AND the job is no longer 'running' (it is the re-claimed 'running' of a new attempt) — we
detect the already-delivered case via the event ledger and return the remembered payload
instead of parking again. Reusing the key for a genuinely NEW wait raises
WaitKeyAlreadyConsumed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from symba.core.errors import StaleLease
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability import metrics
from symba.observability.logging import logger
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="signal_service", context="engine/services")


@dataclass(slots=True)
class WaitOutcome:
    """The result of a worker Wait RPC."""

    resumed_immediately: bool  # True => a pending signal was consumed, job never parked
    payload: dict[str, Any] | None  # the signal payload when resumed_immediately


@dataclass(slots=True)
class SignalOutcome:
    """The result of a control-plane signal."""

    delivered: bool  # True => woke a waiting job; False => parked for a future waiter
    job_id: str | None


class SignalService:
    def __init__(self, pools: Pools, registry: WorkerRegistry | None = None) -> None:
        self._pools = pools
        self._registry = registry

    async def signal(
        self,
        *,
        tenant: str,
        wait_key: str,
        payload: dict[str, Any] | None,
        signaled_by: str | None = None,
    ) -> SignalOutcome:
        """Fire a signal: wake the oldest waiter, or park it for a future wait.

        Both branches are one transaction so the wake-or-park choice is atomic against
        a concurrent wait() picking the other branch.
        """
        async with self._pools.hot.acquire() as conn, conn.transaction():
            woken = await repo.signal_wake(conn, tenant=tenant, wait_key=wait_key, payload=payload or {})
            if woken is not None:
                await repo.record_event(
                    conn, job_id=woken["id"], event="signal_received", detail={"wait_key": wait_key}
                )
                job_id = str(woken["id"])
                delivered = True
            else:
                await repo.signal_park(
                    conn, tenant=tenant, wait_key=wait_key, payload=payload or {}, signaled_by=signaled_by
                )
                job_id = None
                delivered = False

        if delivered and self._registry is not None:
            self._registry.wake.set()  # a newly-queued job exists
        metrics.signals_total.labels(outcome="delivered" if delivered else "parked").inc()
        logger.info(
            "[signal] Signal handled",
            wait_key=wait_key,
            tenant=tenant,
            delivered=delivered,
            job_id=job_id,
        )
        return SignalOutcome(delivered=delivered, job_id=job_id)

    async def wait(
        self,
        *,
        job_id: str,
        lease_token: str,
        tenant: str,
        wait_key: str,
        timeout_s: int,
    ) -> WaitOutcome:
        """Park a running job until a signal arrives, or resume now if one is pending.

        One transaction: consume-or-park. If a pending signal is consumed the job never
        parks and the caller keeps running with the returned payload. Otherwise the job
        moves RUNNING -> WAITING (lease-guarded) and the worker frees the slot.
        """
        async with self._pools.hot.acquire() as conn, conn.transaction():
            payload = await repo.wait_consume(conn, tenant=tenant, wait_key=wait_key)
            if payload is not None:
                # signal-first: resume immediately, no parking.
                await repo.record_event(
                    conn, job_id=job_id, event="signal_consumed", detail={"wait_key": wait_key}
                )
                metrics.signals_total.labels(outcome="consumed").inc()
                logger.info("[wait] Pending signal consumed; not parking", job_id=job_id, wait_key=wait_key)
                return WaitOutcome(resumed_immediately=True, payload=payload)

            parked = await repo.wait_park(
                conn, job_id=job_id, lease_token=lease_token, wait_key=wait_key, timeout_s=timeout_s
            )
            if parked is None:
                raise StaleLease(job_id=job_id)
            await repo.record_event(conn, job_id=job_id, event="waiting", detail={"wait_key": wait_key})

        metrics.jobs_total.labels(tenant=tenant, task="", state="waiting").inc()
        logger.info("[wait] Job parked WAITING", job_id=job_id, wait_key=wait_key, timeout_s=timeout_s)
        return WaitOutcome(resumed_immediately=False, payload=None)
