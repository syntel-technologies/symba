from __future__ import annotations

import asyncio

import pytest

from symba import PROTOCOL_VERSION
from symba.services.registry import WorkerConn, WorkerRegistry
from symba.transport.worker_service import WorkerServicer, _slots_total
from symba.v1 import data_plane_pb2 as dp


async def test_reconnect_preserves_total_slot_high_water() -> None:
    registry = WorkerRegistry()
    first = WorkerConn(
        worker_id="w1",
        tags=frozenset(),
        free_slots=16,
        labels={},
        slots_total=16,
    )
    await registry.register(first)
    await registry.unregister("w1")

    reconnect = WorkerConn(
        worker_id="w1",
        tags=frozenset(),
        free_slots=9,
        labels={},
    )
    await registry.register(reconnect)

    assert reconnect.slots_total == 16


async def test_old_stream_cannot_mutate_or_unregister_replacement() -> None:
    deleted: list[str] = []

    async def on_delete(worker_id: str) -> None:
        deleted.append(worker_id)

    registry = WorkerRegistry(on_delete=on_delete)
    old = WorkerConn(
        worker_id="w1",
        tags=frozenset({"old"}),
        free_slots=4,
        labels={},
        registered_tasks=frozenset({"old.task"}),
    )
    replacement = WorkerConn(
        worker_id="w1",
        tags=frozenset({"new"}),
        free_slots=8,
        labels={},
        registered_tasks=frozenset({"new.task"}),
    )
    await registry.register(old)
    await registry.register(replacement)

    assert not await registry.update_slots(
        "w1",
        1,
        frozenset({"late-old-frame"}),
        frozenset({"late.old.task"}),
        expected=old,
    )
    assert not await registry.unregister("w1", expected=old)
    assert await registry.snapshot_available() == [replacement]
    assert replacement.free_slots == 8
    assert replacement.tags == frozenset({"new"})
    assert replacement.registered_tasks == frozenset({"new.task"})
    assert deleted == []

    assert await registry.unregister("w1", expected=replacement)
    assert deleted == ["w1"]


async def test_claim_exits_when_worker_request_stream_ends() -> None:
    registry = WorkerRegistry()
    servicer = WorkerServicer(registry, None, None, None)  # type: ignore[arg-type]

    async def frames():
        yield dp.ClaimRequest(
            worker_id="w1",
            free_slots=4,
            sdk_version=PROTOCOL_VERSION,
        )

    stream = servicer.Claim(frames(), None)  # type: ignore[arg-type]
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(stream), timeout=0.25)

    assert await registry.count() == 0


async def test_claim_persists_registered_tasks_separately_from_tags() -> None:
    observed: list[tuple[list[str], list[str]]] = []

    async def on_upsert(
        worker_id: str,
        tags: list[str],
        registered_tasks: list[str],
        labels: dict[str, str],
        slots: int,
        slots_busy: int,
    ) -> None:
        del worker_id, labels, slots, slots_busy
        observed.append((tags, registered_tasks))

    registry = WorkerRegistry(on_upsert=on_upsert)
    servicer = WorkerServicer(registry, None, None, None)  # type: ignore[arg-type]

    async def frames():
        yield dp.ClaimRequest(
            worker_id="w-capabilities",
            tags=["ingest"],
            registered_tasks=["parse.document", "embed.batch"],
            free_slots=4,
            sdk_version=PROTOCOL_VERSION,
        )

    stream = servicer.Claim(frames(), None)  # type: ignore[arg-type]
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(stream), timeout=0.25)

    assert observed == [(["ingest"], ["embed.batch", "parse.document"])]


def test_claim_reserved_label_reports_total_capacity() -> None:
    frame = dp.ClaimRequest(
        worker_id="w1",
        free_slots=9,
        labels={"symba.slots_total": "16"},
    )
    assert _slots_total(frame) == 16


def test_claim_bad_total_label_falls_back_to_free_slots() -> None:
    frame = dp.ClaimRequest(
        worker_id="w1",
        free_slots=9,
        labels={"symba.slots_total": "invalid"},
    )
    assert _slots_total(frame) == 9


async def test_slot_bursts_keep_capacity_live_without_per_frame_database_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0
    monkeypatch.setattr("symba.services.registry.monotonic", lambda: now)
    writes: list[int] = []

    async def persist(
        worker_id: str, tags: list[str], tasks: list[str], labels: dict[str, str], total: int, busy: int
    ) -> None:
        writes.append(busy)

    registry = WorkerRegistry(on_upsert=persist)
    conn = WorkerConn(worker_id="burst", tags=frozenset(), free_slots=64, labels={})
    await registry.register(conn)
    for _ in range(100):
        await registry.update_slots("burst", 32, expected=conn)
    assert conn.free_slots == 32
    assert writes == [0]

    now += 1.0
    await registry.update_slots("burst", 16, expected=conn)
    assert writes == [0, 48]
    await registry.update_slots("burst", 8, registered_tasks=frozenset({"new.task"}), expected=conn)
    assert writes == [0, 48, 56]  # metadata changes are immediate
    await registry.update_slots("burst", 64, expected=conn)
    assert writes == [0, 48, 56, 0]  # completion of the burst immediately shows idle
    now += 5.0
    await registry.update_slots("burst", 64, expected=conn)
    assert writes == [0, 48, 56, 0, 0]  # idle heartbeat still refreshes liveness
