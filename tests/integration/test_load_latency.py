# pyright: reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownVariableType=false
"""L6: the dispatcher latency floor.

what this pins
---------------
This promises p95 queue->claim < 150ms WITHOUT a NOTIFY — pure adaptive polling.
The risk it guards: the polling tick silently degrading that latency target. Two
deterministic assertions, both driving the REAL Dispatcher.run() loop over PG18:

  idle floor   submit ONE job into an idle system -> claimed within
               max_tick_ms + margin (the worst case is a full idle-decayed tick;
               the local-submit wake should actually beat that).
  burst p95    submit a burst, drive the loop, and assert the ready_to_claim
               histogram p95 < 150ms — the promised number, measured from DB
               clocks (run_at -> started_at) so it is skew-free.

k6 counterpart (tests/load/ready_to_claim.js) runs the same assertion against a
compose-up engine nightly; this L6 test is the CI-fast, worker-free proof.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from symba.config import load_config
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.observability import metrics
from symba.services.loops import Dispatcher
from symba.services.registry import WorkerConn
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l6, pytest.mark.asyncio(loop_scope="session")]

_MAX_TICK_MS = 250
_IDLE_MARGIN_MS = 250  # scheduler/PG slack on a shared CI box


@pytest_asyncio.fixture(loop_scope="session")
async def engine(migrated_pool: asyncpg.Pool) -> AsyncIterator[EngineState]:
    async with migrated_pool.acquire() as conn:
        await conn.execute("TRUNCATE jobs, jobs_archive, job_events RESTART IDENTITY CASCADE")
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    config = load_config()
    config.dispatcher.min_tick_ms = 10
    config.dispatcher.max_tick_ms = _MAX_TICK_MS
    state = EngineState.build(config, pools)
    state.serving = True
    yield state


async def _register_worker(state: EngineState, slots: int) -> WorkerConn:
    conn = WorkerConn(worker_id="w-load", tags=frozenset(), free_slots=slots, labels={})
    await state.registry.register(conn)
    return conn


async def test_gap6_idle_floor_claim_within_max_tick(db: asyncpg.Connection, engine: EngineState) -> None:
    worker = await _register_worker(engine, slots=1)
    dispatcher = Dispatcher(engine)
    loop_task = asyncio.create_task(dispatcher.run())
    try:
        # Let the loop reach its idle-decayed state so we test the WORST case, not a
        # lucky fast first tick.
        await asyncio.sleep(0.4)
        started = time.perf_counter()
        await engine.submit.submit([SubmitSpec(task_name="t.echo", payload={"n": 1})])
        # The local-submit wake should short-circuit the idle sleep; wait for the
        # assignment to land on the worker's queue.
        assignment = await asyncio.wait_for(worker.queue.get(), timeout=2.0)
        elapsed_ms = (time.perf_counter() - started) * 1000
        assert assignment.job.spec.task_name == "t.echo"
        assert elapsed_ms < _MAX_TICK_MS + _IDLE_MARGIN_MS, f"idle claim took {elapsed_ms:.0f}ms"
    finally:
        dispatcher.stop()
        engine.registry.wake.set()  # unblock the loop's wait so it can exit
        loop_task.cancel()
        with pytest.raises((asyncio.CancelledError, TimeoutError)):
            await asyncio.wait_for(loop_task, timeout=1.0)


async def test_gap6_burst_ready_to_claim_p95_under_150ms(db: asyncpg.Connection, engine: EngineState) -> None:
    # ready_to_claim_ms is an unlabeled Histogram (no .clear()); snapshot the
    # cumulative buckets before the burst and diff after, so a prior test's samples
    # (e.g. the idle-floor claim) don't pollute this p95.
    before = _hist_buckets(metrics.ready_to_claim_ms)
    burst = 200
    worker = await _register_worker(engine, slots=burst)
    await engine.submit.submit([SubmitSpec(task_name="t.echo", payload={"i": i}) for i in range(burst)])

    dispatcher = Dispatcher(engine)
    loop_task = asyncio.create_task(dispatcher.run())
    try:
        # Drain the burst: wait until the worker's queue has all assignments.
        deadline = time.perf_counter() + 5.0
        while worker.queue.qsize() < burst and time.perf_counter() < deadline:
            await asyncio.sleep(0.02)
        assert worker.queue.qsize() == burst, f"only {worker.queue.qsize()}/{burst} claimed"
    finally:
        dispatcher.stop()
        engine.registry.wake.set()
        loop_task.cancel()
        with pytest.raises((asyncio.CancelledError, TimeoutError)):
            await asyncio.wait_for(loop_task, timeout=1.0)

    # Diff the buckets so p95 is over THIS burst only (DB-clock derived, so it is the
    # pure dispatcher latency: run_at -> started_at).
    after = _hist_buckets(metrics.ready_to_claim_ms)
    delta = {le: after[le] - before.get(le, 0.0) for le in after}
    p95 = _bucket_quantile(delta, 0.95)
    assert p95 is not None, "no ready_to_claim samples recorded for the burst"
    assert p95 < 150.0, f"ready_to_claim p95 = {p95:.1f}ms exceeds latency floor (150ms)"


def _hist_buckets(hist: object) -> dict[str, float]:
    """Cumulative `le -> count` buckets off a prometheus_client Histogram (no deps)."""
    buckets: dict[str, float] = {}
    for metric in hist.collect():  # type: ignore[attr-defined]
        for sample in metric.samples:
            if sample.name.endswith("_bucket"):
                buckets[sample.labels["le"]] = sample.value
    return buckets


def _bucket_quantile(buckets: dict[str, float], q: float) -> float | None:
    """qth quantile from cumulative `le -> count` buckets — the coarse upper-bound
    estimate Prometheus' histogram_quantile gives, sufficient for a floor assertion."""
    total = buckets.get("+Inf", 0.0)
    if total == 0:
        return None
    target = q * total

    def _le_key(le: str) -> float:
        return float("inf") if le == "+Inf" else float(le)

    for le in sorted(buckets, key=_le_key):
        if buckets[le] >= target:
            return _le_key(le)
    return None
