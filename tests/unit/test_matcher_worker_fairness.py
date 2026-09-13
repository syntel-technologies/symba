"""Worker fairness must survive low arrival rates, reconnects and saturation."""

from collections import Counter
from typing import cast

import pytest

from symba.config import load_config
from symba.db.pool import Pools
from symba.db.records import ClaimedJob
from symba.services.matcher import _MAX_WORKER_CURSORS, Matcher
from symba.services.registry import WorkerConn, WorkerRegistry

pytestmark = [pytest.mark.l1, pytest.mark.asyncio]


def _matcher() -> Matcher:
    return Matcher(cast(Pools, None), WorkerRegistry(), load_config())


def _worker(worker_id: str, tag: str = "ingest") -> WorkerConn:
    return WorkerConn(worker_id=worker_id, tags=frozenset({tag}), free_slots=4, labels={})


def _job(number: int) -> ClaimedJob:
    return ClaimedJob(
        id=f"job-{number}", task_name="parse", tenant="default", group_key=None,
        max_concurrent_per_group=None, priority=5, lease_token=f"lease-{number}",
        lease_ttl_s=60, payload={}, attempt=1, raw={},
    )


async def test_two_thousand_single_job_batches_use_all_eight_workers_equally() -> None:
    matcher = _matcher()
    workers = [_worker(f"s{host}-w{replica}") for host in (3, 4) for replica in range(4)]
    counts: Counter[str] = Counter()
    for number in range(2000):
        # Simulate completion between ticks. The old idx=0 loop put every job
        # on the first worker even though all eight had available capacity.
        for worker in workers:
            worker.free_slots = 4
        _, attribution = await matcher._assign_round_robin([_job(number)], workers)
        counts[attribution[0][1]] += 1
    assert set(counts.values()) == {250}
    assert len(counts) == 8


async def test_full_batch_consumes_all_slots_without_over_assignment() -> None:
    matcher = _matcher()
    workers = [_worker(f"w{i}") for i in range(8)]
    assigned, attribution = await matcher._assign_round_robin([_job(i) for i in range(33)], workers)
    assert assigned == len(attribution) == 32
    assert all(worker.free_slots == 0 and worker.queue.qsize() == 4 for worker in workers)


async def test_cursor_survives_missing_saturated_worker_and_reconnect_order() -> None:
    matcher = _matcher()
    a, b, c = [_worker(name) for name in ("a", "b", "c")]
    assert (await matcher._assign_round_robin([_job(0)], [a, b, c]))[1][0][1] == "a"
    # a disappeared from the available snapshot. Reconnected workers are now
    # in a different registry insertion order; the next choice is still b.
    assert (await matcher._assign_round_robin([_job(1)], [c, b]))[1][0][1] == "b"
    reconnected_a = _worker("a")
    assert (await matcher._assign_round_robin([_job(2)], [reconnected_a, c]))[1][0][1] == "c"
    assert (await matcher._assign_round_robin([_job(3)], [c, b, reconnected_a]))[1][0][1] == "a"


async def test_authoritative_capacity_cannot_be_reopened_by_advisory_slots() -> None:
    matcher = _matcher()
    a, b = [_worker(name) for name in ("a", "b")]
    capacities = {"a": 0, "b": 1}
    assigned, attribution = await matcher._assign_round_robin(
        [_job(0), _job(1)], [a, b], capacity_by_worker=capacities,
    )
    assert assigned == 1
    assert attribution[0][1] == "b"
    assert a.queue.empty() and b.queue.qsize() == 1
    assert capacities == {"a": 0, "b": 1}
    assert await matcher._assign_round_robin([_job(2)], [a, b], capacity_by_worker={}) == (0, [])


async def test_tag_groups_have_independent_cursors() -> None:
    matcher = _matcher()
    ingest = [_worker(name) for name in ("a", "b")]
    other = [_worker(name, "other") for name in ("a", "b")]
    selected = []
    for number, workers in enumerate((ingest, other, ingest, other)):
        selected.append((await matcher._assign_round_robin([_job(number)], workers))[1][0][1])
    assert selected == ["a", "a", "b", "b"]


async def test_cursor_memory_is_bounded_and_empty_batches_do_not_reset_it() -> None:
    matcher = _matcher()
    for number in range(_MAX_WORKER_CURSORS + 5):
        await matcher._assign_round_robin([_job(number)], [_worker("a", str(number))])
    assert len(matcher._last_worker_by_tags) == _MAX_WORKER_CURSORS
    assert ("0",) not in matcher._last_worker_by_tags
    before = dict(matcher._last_worker_by_tags)
    assert await matcher._assign_round_robin([], [_worker("b")]) == (0, [])
    assert await matcher._assign_round_robin([_job(0)], []) == (0, [])
    assert dict(matcher._last_worker_by_tags) == before
