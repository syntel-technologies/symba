"""In-memory worker registry.

    worker_id -> WorkerConn(tags, free_slots, assignment queue, labels)

The matcher reads a snapshot of connected workers with free_slots > 0, claims
jobs for them, and pushes JobAssignments onto each worker's queue; the Claim
stream handler drains that queue to the client. The registry is process-local
(no shared worker state across engines — each engine matches for the
workers connected to IT), guarded by a single asyncio.Lock since all mutation
happens on the one event loop but snapshots must be consistent.

    lifecycle
    ---------
    Claim stream opens  -> register(conn)         (free_slots from first frame)
    ClaimRequest frames -> update_slots(...)       (worker-driven flow control)
    matcher assigns     -> reserve_slots()/enqueue (decrement local free_slots)
    Claim stream closes -> unregister(worker_id)   (in-flight leases recovered
                                                     by the sweeper via lease TTL)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from symba.v1 import data_plane_pb2 as dp

# Persistence hooks: the registry stays the in-memory source of truth for
# matching, but mirrors lifecycle changes into the `workers` read model so the
# fleet view and connected-worker count survive across engines/restarts.
# Injected so the registry keeps its pool-free dependency surface.
#   upsert(worker_id, tags, labels, slots_total, slots_busy)
#   delete(worker_id)
UpsertHook = Callable[[str, list[str], dict[str, str], int, int], Awaitable[None]]
DeleteHook = Callable[[str], Awaitable[None]]


@dataclass(slots=True)
class WorkerConn:
    worker_id: str
    tags: frozenset[str]
    free_slots: int
    labels: dict[str, str]
    # Total advertised capacity, fixed for the connection. free_slots decreases as
    # the matcher assigns; slots_busy = slots_total - free_slots. Captured from the
    # first Claim frame (at connect free_slots == total), so the fleet view can show
    # a real busy/total gauge instead of only "currently free".
    slots_total: int = 0
    # Bounded so a stalled worker cannot let assignments pile up unboundedly;
    # the matcher only assigns up to free_slots, so this never blocks in practice.
    queue: asyncio.Queue[dp.JobAssignment] = field(default_factory=lambda: asyncio.Queue(maxsize=1024))


class WorkerRegistry:
    def __init__(self, on_upsert: UpsertHook | None = None, on_delete: DeleteHook | None = None) -> None:
        self._workers: dict[str, WorkerConn] = {}
        # A Claim stream may reconnect while jobs are still executing, so its
        # first free_slots value is not necessarily the worker's total capacity.
        # Retain the high-water mark across stream churn as a compatibility
        # fallback for SDKs that do not send the reserved total-slots label.
        self._known_slots_total: dict[str, int] = {}
        self._lock = asyncio.Lock()
        # Set by the matcher-adjacent code to short-circuit the dispatcher sleep
        # when slots free up (wake source; optimization, not correctness).
        self.wake = asyncio.Event()
        # Optional read-model persistence. Fired OUTSIDE the lock so
        # a slow DB write never stalls matching; a failed write is contained (logged)
        # since the in-memory registry — not this table — is authoritative for claims.
        self._on_upsert = on_upsert
        self._on_delete = on_delete

    async def register(self, conn: WorkerConn) -> None:
        async with self._lock:
            conn.slots_total = max(
                conn.slots_total,
                conn.free_slots,
                self._known_slots_total.get(conn.worker_id, 0),
            )
            self._known_slots_total[conn.worker_id] = conn.slots_total
            self._workers[conn.worker_id] = conn
        self.wake.set()
        await self._persist_upsert(conn)

    async def unregister(
        self,
        worker_id: str,
        *,
        expected: WorkerConn | None = None,
    ) -> bool:
        """Remove one worker connection without deleting a newer generation."""
        async with self._lock:
            current = self._workers.get(worker_id)
            if current is None or (expected is not None and current is not expected):
                return False
            self._workers.pop(worker_id)
        if self._on_delete is not None:
            await self._on_delete(worker_id)
        return True

    async def update_slots(
        self,
        worker_id: str,
        free_slots: int,
        tags: frozenset[str] | None = None,
        *,
        expected: WorkerConn | None = None,
    ) -> bool:
        async with self._lock:
            conn = self._workers.get(worker_id)
            if conn is None or (expected is not None and conn is not expected):
                return False
            was_idle = conn.free_slots == 0
            conn.free_slots = free_slots
            # A worker may advertise more capacity than at connect (config reload);
            # slots_total is the high-water mark so slots_busy never goes negative.
            if free_slots > conn.slots_total:
                conn.slots_total = free_slots
                self._known_slots_total[worker_id] = conn.slots_total
            if tags is not None:
                conn.tags = tags
            snapshot = conn
        if was_idle and free_slots > 0:
            self.wake.set()
        # Slot frames double as heartbeats: refresh the read model so last_seen
        # tracks liveness and the busy/total gauge stays live.
        await self._persist_upsert(snapshot)
        return True

    async def _persist_upsert(self, conn: WorkerConn) -> None:
        if self._on_upsert is None:
            return
        slots_busy = max(0, conn.slots_total - conn.free_slots)
        await self._on_upsert(conn.worker_id, sorted(conn.tags), dict(conn.labels), conn.slots_total, slots_busy)

    async def snapshot_available(self) -> list[WorkerConn]:
        """Workers with capacity right now. Returns the live objects (single loop,
        so the matcher mutates free_slots in place as it assigns)."""
        async with self._lock:
            return [w for w in self._workers.values() if w.free_slots > 0]

    async def count(self) -> int:
        async with self._lock:
            return len(self._workers)
