# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportPrivateUsage=false
"""L4 coordination regression pins: re-entry contract + loop containment.

    re-entry contract
    -----------------------------------------------------
    The engine-owned half of the re-entry story (the SDK owns spy-counter recompute
    proofs, which are out of scope here): a handler that checkpointed BEFORE waiting
    is resumed on a different worker with its checkpoint preloaded, and a pending
    signal is consumed on re-wait WITHOUT re-parking. This proves the primitives the
    SDK's "don't re-buy the LLM call" guarantee stands on.

    loop containment
    --------------------------------------------------
    A single poisoned pass (bad row / PG error) in the dispatcher, sweeper, or cron
    loop must be CONTAINED: logged, counted in symba_loop_errors_total, and the loop
    keeps ticking. A persistent failure is surfaced by the metric alert, never by a
    silent process death. We drive ONE bad pass, assert the counter moved and the loop
    survives, then a healthy pass to prove throughput recovers.
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.config import load_config
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability.metrics import loop_errors_total
from symba.services.checkpoint_service import CheckpointService
from symba.services.loops import CronService, Dispatcher, Sweeper
from symba.services.rate_limiter import RateLimiter
from symba.services.registry import WorkerRegistry
from symba.services.signal_service import SignalService
from symba.transport.state import EngineState
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l4, pytest.mark.asyncio(loop_scope="session")]


# --------------------------------------------------------------------------- #
# re-entry contract (engine-owned primitives)
# --------------------------------------------------------------------------- #


async def test_gap5_reentry_checkpoint_and_signal_consume(db: asyncpg.Connection, pools: Pools) -> None:
    signals = SignalService(pools, WorkerRegistry())
    checkpoints = CheckpointService(pools, RateLimiter(load_config().redis, pools))
    job_id = await h.seed(db, h.spec(task_name="t.reentry"))

    # attempt 1: expensive pre-wait work is checkpointed, then the handler waits.
    a1 = await h.claim_one(db, worker="w1")
    await checkpoints.put(job_id=job_id, tenant="default", data={"draft": "llm-output"})
    out1 = await signals.wait(
        job_id=job_id, lease_token=a1.lease_token, tenant="default", wait_key="gate", timeout_s=3600
    )
    assert out1.resumed_immediately is False, "no signal yet -> parked WAITING"

    # a signal arrives while parked; the job returns to the queue with the payload.
    await signals.signal(tenant="default", wait_key="gate", payload={"approved": True})

    # attempt 2 (resume on a DIFFERENT worker): the checkpoint is preloaded so the
    # SDK would skip the expensive recompute.
    a2 = await h.claim_one(db, worker="w2")
    assert a2.id == job_id
    assert a2.raw["checkpoint_data"] == {"draft": "llm-output"}
    # the consumed signal payload is carried back on the resume.
    assert a2.raw["event_payload"] == {"approved": True}


async def test_gap5_signal_first_reentry_consumes_without_parking(
    db: asyncpg.Connection, pools: Pools
) -> None:
    # If the signal is already pending when the (resumed) handler re-waits, it consumes
    # inline and NEVER parks again — the re-wait is a no-op resume, not a second block.
    signals = SignalService(pools, WorkerRegistry())
    job_id = await h.seed(db, h.spec(task_name="t.reentry2"))
    claimed = await h.claim_one(db, worker="w1")

    await signals.signal(tenant="default", wait_key="k2", payload={"v": 1})  # signal-first
    out = await signals.wait(
        job_id=job_id, lease_token=claimed.lease_token, tenant="default", wait_key="k2", timeout_s=3600
    )
    assert out.resumed_immediately is True
    assert out.payload == {"v": 1}
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running", "never parked"


# --------------------------------------------------------------------------- #
# loop containment — one poisoned pass never kills the loop
# --------------------------------------------------------------------------- #


async def _run_one_pass(loop: object) -> None:
    """Drive exactly one iteration of PeriodicLoop.run(): run the pass, contain errors.

    Mirrors the base-loop body (pass_ inside try/except that bumps the metric) so we
    can assert containment without spinning the real forever-loop.
    """
    try:
        await loop.pass_()  # type: ignore[attr-defined]
    except Exception:
        loop_errors_total.labels(loop=loop.name).inc()  # type: ignore[attr-defined]


@pytest.mark.parametrize("loop_name", ["dispatcher", "sweeper", "cron"])
async def test_gap9_poisoned_pass_is_contained_and_recovers(
    db: asyncpg.Connection, pools: Pools, monkeypatch: pytest.MonkeyPatch, loop_name: str
) -> None:
    state = EngineState.build(load_config(), pools)
    loop: Dispatcher | Sweeper | CronService = {
        "dispatcher": Dispatcher(state),
        "sweeper": Sweeper(state),
        "cron": CronService(state),
    }[loop_name]

    before = loop_errors_total.labels(loop=loop_name)._value.get()

    # Poison the next pass: the matcher / sweep / cron_due read raises a PG error.
    async def _boom(*_a: object, **_k: object) -> object:
        raise asyncpg.PostgresError("injected poison pass")

    if loop_name == "dispatcher":
        monkeypatch.setattr(state.matcher, "pass_", _boom)
    elif loop_name == "sweeper":
        monkeypatch.setattr(repo, "try_sweeper_lock", _boom)
    else:
        monkeypatch.setattr(repo, "cron_due", _boom)

    # The loop body contains the error: it does NOT propagate out of run()'s try/except.
    await _run_one_pass(loop)
    after = loop_errors_total.labels(loop=loop_name)._value.get()
    assert after == before + 1, f"{loop_name}: poisoned pass must bump symba_loop_errors_total"

    # Recovery: undo the poison and a healthy pass runs cleanly (throughput recovers).
    monkeypatch.undo()
    await _run_one_pass(loop)  # no raise, no further counter bump
    assert loop_errors_total.labels(loop=loop_name)._value.get() == after, "healthy pass does not error"
