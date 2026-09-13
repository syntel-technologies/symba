# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""In-process SSE fan-out of the audit ledger (live updates).

One background poller per engine tails `job_events` (id-cursor, via events_since.sql)
and pushes each new row to every connected SSE client. This is the deliberate simple
mechanism: in-process pubsub, where multi-engine UIs are eventually consistent within
a second via PG — NOT LISTEN/NOTIFY (which does not survive a failover and silently
drops under load).

    poller (1 per engine)              subscribers (1 per SSE connection)
    ---------------------              ----------------------------------
    events_since(after_id) ──every──►  fan-out ──► asyncio.Queue ──► SSE client A
       advance after_id     ~500ms                └─► asyncio.Queue ──► SSE client B

    backpressure: each subscriber has a BOUNDED queue. A slow client that
    fills its queue drops the OLDEST buffered event (put_nowait after a get_nowait)
    rather than blocking the poller for everyone — the UI is a live view, not a
    guaranteed-delivery bus (the ledger itself is the durable record). A dropped
    event is at worst a missed intermediate render; the next board refresh reconciles.

    lifecycle: start()/stop() bracket the poller task in the engine TaskGroup. A
    subscribe() context manager registers/deregisters a queue so a disconnected
    client is cleaned up deterministically (no leaked queues).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from dataclasses import dataclass

from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import EventRow
from symba.observability.logging import logger

logger = logger.bind(service="event_stream", context="engine/services")

_POLL_INTERVAL_S = 0.5  # UI liveness target: ~sub-second
_BATCH = 500  # page cap per poll so one busy tenant cannot flood the stream
_QUEUE_MAX = 256  # per-subscriber buffer; overflow drops oldest (see module docstring)


@dataclass(slots=True, eq=False)
class _Subscriber:
    # eq=False -> identity hashing: each connection is a distinct object in the
    # subscriber set even if two share a tenant (a Queue is unhashable anyway).
    tenant: str
    queue: asyncio.Queue[EventRow]


class EventStream:
    """Single-poller ledger tail with per-connection bounded fan-out queues."""

    def __init__(self, pools: Pools) -> None:
        self._pools = pools
        self._subscribers: set[_Subscriber] = set()
        self._after_id = 0
        self._stop = asyncio.Event()

    async def start(self) -> None:
        """Run the poll loop until stop(); contained like the periodic loops."""
        logger.info("[event_stream] Starting SSE poller")
        # Start from the current head so a fresh engine does not replay history.
        async with self._pools.general.acquire() as conn:
            self._after_id = await repo.max_event_id(conn, tenant="default")
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except Exception:
                # A PG blip must not kill the stream; the next tick re-reads from the
                # same cursor so no row is lost (id > after_id is idempotent).
                logger.error("[event_stream] Poll failed", exc_info=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=_POLL_INTERVAL_S)

    async def _poll_once(self) -> None:
        if not self._subscribers:
            # No listeners: keep the cursor at the head so we don't buffer a backlog
            # to deliver to a client that connects later (a fresh client starts "now").
            async with self._pools.general.acquire() as conn:
                self._after_id = await repo.max_event_id(conn, tenant="default")
            return
        async with self._pools.general.acquire() as conn:
            rows = await repo.events_since(conn, after_id=self._after_id, tenant="default", limit=_BATCH)
        if not rows:
            return
        self._after_id = rows[-1].id
        for row in rows:
            self._fan_out(row)

    def _fan_out(self, row: EventRow) -> None:
        for sub in self._subscribers:
            if sub.tenant != row.tenant:
                continue
            try:
                sub.queue.put_nowait(row)
            except asyncio.QueueFull:
                # Drop the oldest to make room (live view, not a durable bus).
                with contextlib.suppress(asyncio.QueueEmpty):
                    sub.queue.get_nowait()
                with contextlib.suppress(asyncio.QueueFull):
                    sub.queue.put_nowait(row)

    async def snapshot_for_ctx(
        self, *, tenant: str, ctx_id: str, after_id: int = 0, limit: int = _BATCH
    ) -> list[EventRow]:
        """Read one page of the persisted ledger for a ctx (bounded snapshot read).

        Backs StreamEvents(snapshot=true): unlike the live tail, this returns already
        written rows ordered by id so the caller can page from 0 and stop on a short
        page. Read-only on the general pool; no subscription/queue involved.
        """
        async with self._pools.general.acquire() as conn:
            return await repo.events_for_ctx(conn, tenant=tenant, ctx_id=ctx_id, after_id=after_id, limit=limit)

    @contextlib.asynccontextmanager
    async def subscribe(self, *, tenant: str = "default") -> AsyncGenerator[asyncio.Queue[EventRow]]:
        """Register a bounded queue for the life of one SSE connection."""
        sub = _Subscriber(tenant=tenant, queue=asyncio.Queue(maxsize=_QUEUE_MAX))
        self._subscribers.add(sub)
        logger.debug("[event_stream] Subscriber added", tenant=tenant, total=len(self._subscribers))
        try:
            yield sub.queue
        finally:
            self._subscribers.discard(sub)
            logger.debug("[event_stream] Subscriber removed", tenant=tenant, total=len(self._subscribers))

    def stop(self) -> None:
        self._stop.set()
