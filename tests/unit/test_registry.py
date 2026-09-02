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
    )
    replacement = WorkerConn(
        worker_id="w1",
        tags=frozenset({"new"}),
        free_slots=8,
        labels={},
    )
    await registry.register(old)
    await registry.register(replacement)

    assert not await registry.update_slots(
        "w1",
        1,
        frozenset({"late-old-frame"}),
        expected=old,
    )
    assert not await registry.unregister("w1", expected=old)
    assert await registry.snapshot_available() == [replacement]
    assert replacement.free_slots == 8
    assert replacement.tags == frozenset({"new"})
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
