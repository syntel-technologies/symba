# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 signal / WAITING rendezvous.

Proves both orderings of the human-in-the-loop primitive resume a job EXACTLY once,
plus the timeout backstop and the re-entry contract.

    wait-first                         signal-first
    ----------                         ------------
    running --wait--> WAITING           signal (no waiter) --> parked in `signals`
    signal --> QUEUED (payload)         running --wait--> consume parked --> keep running

The service owns the transaction; these tests drive it against real PG18 and assert on
the resulting job state, the delivered payload, and the audit ledger.
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.core.errors import StaleLease
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.registry import WorkerRegistry
from symba.services.signal_service import SignalService
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _svc(pools: Pools) -> SignalService:
    return SignalService(pools, WorkerRegistry())


async def _run(db: asyncpg.Connection, job_id: str, worker: str = "w1") -> str:
    """Claim `job_id` so it is RUNNING, returning its lease token."""
    claimed = await h.claim_one(db, worker=worker)
    assert claimed.id == job_id
    return claimed.lease_token


async def _events(db: asyncpg.Connection, job_id: str) -> list[str]:
    rows = await db.fetch("SELECT event FROM job_events WHERE job_id = $1 ORDER BY at", job_id)
    return [r["event"] for r in rows]


# ── wait-first: the job parks, then a signal wakes it ─────────────────────────


async def test_wait_first_parks_then_signal_resumes_once(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="approval"))
    lease = await _run(db, job_id)

    # The handler waits: no signal pending -> the job parks WAITING (slot freed).
    out = await svc.wait(job_id=job_id, lease_token=lease, tenant="default", wait_key="approve-42", timeout_s=3600)
    assert out.resumed_immediately is False
    row = await db.fetchrow("SELECT state, wait_key, lease_token FROM jobs WHERE id = $1", job_id)
    assert row["state"] == "waiting" and row["wait_key"] == "approve-42"
    assert row["lease_token"] is None, "parking frees the lease so the slot is released"

    # A signal for that key wakes the job with the payload, back to QUEUED.
    sig = await svc.signal(tenant="default", wait_key="approve-42", payload={"decision": "yes"}, signaled_by="ops")
    assert sig.delivered is True and sig.job_id == job_id
    woke = await db.fetchrow("SELECT state, event_payload, wait_key FROM jobs WHERE id = $1", job_id)
    assert woke["state"] == "queued"
    assert woke["event_payload"] == {"decision": "yes"}
    assert woke["wait_key"] is None
    assert "waiting" in await _events(db, job_id) and "signal_received" in await _events(db, job_id)


# ── signal-first: the signal is parked, then the wait consumes it ─────────────


async def test_signal_first_parks_then_wait_consumes_without_parking(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    # The signal arrives BEFORE anyone waits -> parked in `signals`, no job to wake.
    sig = await svc.signal(tenant="default", wait_key="webhook-7", payload={"ok": True})
    assert sig.delivered is False and sig.job_id is None
    parked = await db.fetchval("SELECT count(*) FROM signals WHERE wait_key='webhook-7' AND consumed_at IS NULL")
    assert parked == 1

    # Now a job runs and waits on that key: it consumes the parked signal and NEVER parks.
    job_id = await h.seed(db, h.spec(task_name="approval"))
    lease = await _run(db, job_id)
    out = await svc.wait(job_id=job_id, lease_token=lease, tenant="default", wait_key="webhook-7", timeout_s=3600)
    assert out.resumed_immediately is True
    assert out.payload == {"ok": True}
    # The job is still RUNNING (it kept its lease; it never transitioned to WAITING).
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"
    # The signal is now consumed (single delivery).
    remaining = await db.fetchval("SELECT count(*) FROM signals WHERE wait_key='webhook-7' AND consumed_at IS NULL")
    assert remaining == 0
    assert "signal_consumed" in await _events(db, job_id)


# ── one signal wakes exactly one of several waiters ───────────────────────────


async def test_signal_wakes_exactly_one_waiter(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    a = await h.seed(db, h.spec(task_name="approval"))
    b = await h.seed(db, h.spec(task_name="approval"))
    la = (await h.claim_one(db, worker="wa")).lease_token
    lb = (await h.claim_one(db, worker="wb")).lease_token
    # Two jobs park on the same key.
    await svc.wait(job_id=a, lease_token=la, tenant="default", wait_key="k", timeout_s=3600)
    await svc.wait(job_id=b, lease_token=lb, tenant="default", wait_key="k", timeout_s=3600)

    # One signal wakes exactly ONE (the oldest waiter); the other stays parked.
    await svc.signal(tenant="default", wait_key="k", payload={"n": 1})
    states = {
        a: await db.fetchval("SELECT state FROM jobs WHERE id=$1", a),
        b: await db.fetchval("SELECT state FROM jobs WHERE id=$1", b),
    }
    assert sorted(states.values()) == ["queued", "waiting"], f"exactly one woken: {states}"

    # A second signal wakes the remaining one.
    await svc.signal(tenant="default", wait_key="k", payload={"n": 2})
    assert await db.fetchval("SELECT state FROM jobs WHERE id=$1", a) == "queued"
    assert await db.fetchval("SELECT state FROM jobs WHERE id=$1", b) == "queued"


# ── timeout backstop: a wait that no signal answers is released by the sweeper ─


async def test_wait_timeout_is_released_with_event(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="approval"))
    lease = await _run(db, job_id)
    await svc.wait(job_id=job_id, lease_token=lease, tenant="default", wait_key="never", timeout_s=3600)

    # Backdate the deadline: the sweeper releases it with a synthetic timeout payload.
    await db.execute("UPDATE jobs SET wait_expires_at = now() - interval '1 s' WHERE id = $1", job_id)
    released = await repo.sweep_waits(db, limit=100)
    assert released == [job_id]
    # The sweeper records the ledger event (mirrors Sweeper.pass_()).
    for jid in released:
        await repo.record_event(db, job_id=jid, event="wait_timed_out")

    row = await db.fetchrow("SELECT state, event_payload FROM jobs WHERE id = $1", job_id)
    assert row["state"] == "queued"
    assert row["event_payload"] == {"__timeout__": True}
    assert "wait_timed_out" in await _events(db, job_id)


# ── stale lease cannot park a job it no longer owns ───────────────────────────


async def test_wait_with_stale_lease_is_rejected(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="approval"))
    await _run(db, job_id)  # a valid worker owns it

    with pytest.raises(StaleLease):
        await svc.wait(job_id=job_id, lease_token="not-the-real-token", tenant="default", wait_key="k", timeout_s=60)
    # The job is untouched — still running under the real lease.
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"
