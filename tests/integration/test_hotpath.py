"""L2 hot-path SQL integration tests against real Postgres 18.

Each test seeds rows via the repository, exercises one hot-path operation, and
asserts the row moved to the correct table/state. The key regression pins
(reclaim predicate, sweeper scope, dedup-not-error) are covered explicitly.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from symba.db import repository as repo
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


# --------------------------------------------------------------------------- #
# submit + dedup
# --------------------------------------------------------------------------- #


async def test_submit_inserts_and_returns_id(db: asyncpg.Connection) -> None:
    res = await repo.submit(db, h.spec())
    assert res.deduplicated is False
    assert res.job_id is not None
    row_state = await db.fetchval("SELECT state FROM jobs WHERE id = $1", res.job_id)
    assert row_state == "queued"


async def test_submit_dedup_is_ok_not_error(db: asyncpg.Connection) -> None:
    first = await repo.submit(db, h.spec(dedup_key="k1"))
    second = await repo.submit(db, h.spec(dedup_key="k1"))
    assert first.deduplicated is False and first.job_id is not None
    # A duplicate submit is idempotent success, NOT an error. It echoes the EXISTING
    # job's id (SDK contract: h2.id == h1.id) — the caller treats a dedup hit as a
    # reference to the canonical job, not a new one.
    assert second.deduplicated is True
    assert second.job_id == first.job_id
    count = await db.fetchval("SELECT count(*) FROM jobs WHERE dedup_key = 'k1'")
    assert count == 1


async def test_submit_json_payload_round_trips(db: asyncpg.Connection) -> None:
    payload = {"nested": {"a": [1, 2, 3]}, "flag": True}
    res = await repo.submit(db, h.spec(payload=payload))
    stored = await db.fetchval("SELECT payload FROM jobs WHERE id = $1", res.job_id)
    assert stored == payload


# --------------------------------------------------------------------------- #
# claim: tags, priority, group ceiling, no double-assign
# --------------------------------------------------------------------------- #


async def test_claim_moves_queued_to_running(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    assert claimed.id == job_id
    assert claimed.lease_token
    assert claimed.attempt == 1  # incremented at claim
    state = await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id)
    assert state == "running"


async def test_claim_skips_future_run_at(db: asyncpg.Connection) -> None:
    # X1: run_at in the future must not be claimable this tick.
    await h.seed(db, h.spec())
    await db.execute("UPDATE jobs SET run_at = now() + interval '1 hour'")
    got = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1000)
    assert got == []


async def test_claim_respects_runs_on_tags(db: asyncpg.Connection) -> None:
    await h.seed(db, h.spec(runs_on=["gpu"]))
    # A worker without the gpu tag cannot cover runs_on.
    none = await repo.claim(
        db, worker_tags=["cpu"], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1000
    )
    assert none == []
    got = await repo.claim(
        db, worker_tags=["cpu", "gpu"], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1000
    )
    assert len(got) == 1


async def test_claim_respects_group_ceiling_across_batches(db: asyncpg.Connection) -> None:
    # Two jobs in the same group with cap=1. The SQL guard (group_running <
    # cap) enforces the ceiling ACROSS batches: batch 1 claims one, batch 2 is
    # blocked because group_running is now 1. Per-batch exactness (below) is a
    # separate mechanism (the matcher's per_group_cap), so here we pass a large
    # per_group_cap and rely on the counter guard between batches.
    await h.seed(db, h.spec(group_key="g", cap=1))
    await h.seed(db, h.spec(group_key="g", cap=1))
    first = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1)
    assert len(first) == 1
    # Second claim in a fresh tx sees group_running=1 and the cap blocks it.
    second = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w2", per_group_cap=1)
    assert second == []
    assert await h.group_running(db, "default", "g", "t.echo") == 1


async def test_claim_per_group_cap_limits_one_batch(db: asyncpg.Connection) -> None:
    # per_group_cap ($5, rn_in_group <= $5) bounds how many rows of ONE
    # group_key a single batch may take, giving cap=1 exactness on a single
    # engine even when the group has headroom. Two claimable jobs, one group,
    # per_group_cap=1 -> exactly one claimed in this batch.
    await h.seed(db, h.spec(group_key="g", cap=5))
    await h.seed(db, h.spec(group_key="g", cap=5))
    got = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1)
    assert len(got) == 1
    still_queued = await db.fetchval("SELECT count(*) FROM jobs WHERE state = 'queued' AND group_key = 'g'")
    assert still_queued == 1


async def test_claim_exhausted_rate_class_skipped(db: asyncpg.Connection) -> None:
    await h.seed(db, h.spec(rate_class="slow"))
    blocked = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=["slow"], limit=10, claimed_by="w1", per_group_cap=1000
    )
    assert blocked == []
    ok = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1000)
    assert len(ok) == 1


async def test_claim_enforces_partial_rate_quota_exactly(db: asyncpg.Connection) -> None:
    for _ in range(5):
        await h.seed(db, h.spec(rate_class="slow"))

    got = await repo.claim(
        db,
        worker_tags=[],
        exhausted_rate_classes=[],
        limit=10,
        claimed_by="w1",
        per_group_cap=1000,
        rate_quotas={"slow": 2},
    )

    assert len(got) == 2
    assert {job.raw.get("rate_class") for job in got} == {"slow"}
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE state = 'queued' AND rate_class = 'slow'") == 3


async def test_claim_enforces_each_rate_quota_with_unlimited_jobs(
    db: asyncpg.Connection,
) -> None:
    for _ in range(3):
        await h.seed(db, h.spec(rate_class="slow"))
        await h.seed(db, h.spec(rate_class="fast"))
    for _ in range(2):
        await h.seed(db, h.spec())

    got = await repo.claim(
        db,
        worker_tags=[],
        exhausted_rate_classes=[],
        limit=10,
        claimed_by="w1",
        per_group_cap=1000,
        rate_quotas={"slow": 1, "fast": 2},
    )

    by_class: dict[str | None, int] = {}
    for job in got:
        rate_class = job.raw.get("rate_class")
        by_class[rate_class] = by_class.get(rate_class, 0) + 1
    assert by_class == {None: 2, "fast": 2, "slow": 1}


async def test_rate_quota_skips_group_blocked_rows_when_ranking(
    db: asyncpg.Connection,
) -> None:
    running_id = await h.seed(db, h.spec(group_key="blocked", cap=1, rate_class="slow", priority=20))
    assert (await h.claim_one(db)).id == running_id
    await h.seed(db, h.spec(group_key="blocked", cap=1, rate_class="slow", priority=10))
    eligible_id = await h.seed(db, h.spec(rate_class="slow"))

    got = await repo.claim(
        db,
        worker_tags=[],
        exhausted_rate_classes=[],
        limit=10,
        claimed_by="w2",
        per_group_cap=1000,
        rate_quotas={"slow": 1},
    )

    assert [job.id for job in got] == [eligible_id]


async def test_claim_priority_order(db: asyncpg.Connection) -> None:
    low = await h.seed(db, h.spec(priority=0))
    high = await h.seed(db, h.spec(priority=9))
    got = await repo.claim(db, worker_tags=[], exhausted_rate_classes=[], limit=1, claimed_by="w1", per_group_cap=1000)
    assert [j.id for j in got] == [high]
    assert low  # low stays queued


# --------------------------------------------------------------------------- #
# complete: archive move + lease guard
# --------------------------------------------------------------------------- #


async def test_complete_moves_to_archive_succeeded(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    term = await repo.complete_move(db, job_id=job_id, lease_token=claimed.lease_token, result={"ok": True})
    assert term is not None and term.id == job_id
    # Row left the hot table entirely and landed in archive as succeeded.
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id = $1", job_id) == 0
    assert await h.state_of(db, job_id) == "succeeded"
    res = await repo.get_result(db, job_id=job_id, tenant="default")
    assert res is not None and res.result == {"ok": True}


async def test_complete_stale_lease_returns_none(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    term = await repo.complete_move(db, job_id=job_id, lease_token="wrong-token", result={})
    assert term is None
    # Job is untouched and still running.
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"


async def test_complete_decrements_group_running(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(group_key="g", cap=2))
    claimed = await h.claim_one(db)
    assert await h.group_running(db, "default", "g", "t.echo") == 1
    async with db.transaction():
        term = await repo.complete_move(db, job_id=job_id, lease_token=claimed.lease_token, result={})
        assert term is not None
        await repo.decrement_group(db, tenant=term.tenant, group_key=term.group_key, task_name=term.task_name)
    assert await h.group_running(db, "default", "g", "t.echo") == 0


# --------------------------------------------------------------------------- #
# fail: retry-with-backoff and die-to-archive
# --------------------------------------------------------------------------- #


async def test_fail_retry_requeues_with_backoff(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    next_run = datetime.now(UTC) + timedelta(seconds=30)
    row = await repo.fail_retry(
        db, job_id=job_id, lease_token=claimed.lease_token, next_run_at=next_run, error_entry={"type": "boom"}
    )
    assert row is not None
    requeued = await db.fetchrow("SELECT state, lease_token FROM jobs WHERE id = $1", job_id)
    assert requeued is not None
    assert requeued["state"] == "queued" and requeued["lease_token"] is None


async def test_fail_die_moves_to_dead(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    term = await repo.fail_die(db, job_id=job_id, lease_token=claimed.lease_token, error_entry={"type": "fatal"})
    assert term is not None
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id = $1", job_id) == 0
    assert await h.state_of(db, job_id) == "dead"


async def test_fail_die_stale_lease_returns_none(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    term = await repo.fail_die(db, job_id=job_id, lease_token="nope", error_entry={})
    assert term is None
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"


# --------------------------------------------------------------------------- #
# cancel: per-state matrix
# --------------------------------------------------------------------------- #


async def test_cancel_queued_moves_to_cancelled(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(state="queued"))
    term = await repo.cancel_immediate(db, job_id=job_id)
    assert term is not None and term.prior_state == "queued"
    assert await h.state_of(db, job_id) == "cancelled"


async def test_cancel_running_sets_flag_only(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec())
    await h.claim_one(db, worker="w7")
    # Immediate cancel does NOT touch a running row...
    assert await repo.cancel_immediate(db, job_id=job_id) is None
    # ...the cooperative path flags it instead.
    who = await repo.cancel_running(db, job_id=job_id)
    assert who == "w7"
    flag = await db.fetchval("SELECT cancel_requested FROM jobs WHERE id = $1", job_id)
    assert flag is True


# --------------------------------------------------------------------------- #
# sweeper: lease reclaim + wait expiry
# --------------------------------------------------------------------------- #


async def test_sweep_leases_reclaims_only_expired(db: asyncpg.Connection) -> None:
    # A healthy lease with a recent heartbeat must survive the sweep untouched.
    expired_id = await h.seed(db, h.spec())
    healthy_id = await h.seed(db, h.spec())
    await h.claim_one(db, worker="w1")  # claims one of them
    # Claim the second explicitly so both are running.
    await h.claim_one(db, worker="w2")
    # Age exactly one lease into the past.
    await db.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1", expired_id)
    await db.execute("UPDATE jobs SET lease_expires_at = now() + interval '1 hour' WHERE id = $1", healthy_id)

    reclaimed = await repo.sweep_leases(db, limit=100)
    assert reclaimed == 1
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", expired_id) == "queued"
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", healthy_id) == "running"


async def test_sweep_leases_reclaims_missing_worker_after_grace(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(lease_ttl_s=1800))
    await h.claim_one(db, worker="w-gone")
    await db.execute(
        "UPDATE jobs SET last_heartbeat_at = now() - interval '1 minute' WHERE id = $1",
        job_id,
    )

    reclaimed = await repo.sweep_leases(db, limit=100, stale_worker_grace_s=45)

    assert reclaimed == 1
    row = await db.fetchrow(
        "SELECT state, claimed_by, lease_expires_at, error_history FROM jobs WHERE id = $1",
        job_id,
    )
    assert row is not None
    assert row["state"] == "queued"
    assert row["claimed_by"] is None
    assert row["lease_expires_at"] is None
    assert row["error_history"][-1]["type"] == "lease_expired"


async def test_sweep_leases_does_not_requeue_exhausted_attempt(db: asyncpg.Connection) -> None:
    spec = h.spec(lease_ttl_s=1800)
    spec.max_attempts = 1
    job_id = await h.seed(db, spec)
    await h.claim_one(db, worker="w-gone")
    await db.execute(
        "UPDATE jobs SET last_heartbeat_at = now() - interval '1 minute' WHERE id = $1",
        job_id,
    )

    candidates = await repo.list_exhausted_leases(db, limit=100, stale_worker_grace_s=45)
    reclaimed = await repo.sweep_leases(db, limit=100, stale_worker_grace_s=45)

    assert [(row.job_id, row.lease_token) for row in candidates] == [
        (job_id, await db.fetchval("SELECT lease_token FROM jobs WHERE id=$1", job_id))
    ]
    assert reclaimed == 0
    assert await db.fetchval("SELECT state FROM jobs WHERE id=$1", job_id) == "running"


async def test_sweep_leases_archives_cancelled_job_after_worker_loss(
    db: asyncpg.Connection,
) -> None:
    job_id = await h.seed(db, h.spec(lease_ttl_s=1800))
    await h.claim_one(db, worker="w-cancelled")
    assert await repo.cancel_running(db, job_id=job_id) == "w-cancelled"
    await db.execute(
        "UPDATE jobs SET last_heartbeat_at = now() - interval '1 minute' WHERE id = $1",
        job_id,
    )

    reclaimed = await repo.sweep_leases(
        db,
        limit=100,
        stale_worker_grace_s=45,
    )

    assert reclaimed == 1
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id = $1", job_id) == 0
    assert (
        await db.fetchval(
            "SELECT final_state FROM jobs_archive WHERE id = $1",
            job_id,
        )
        == "cancelled"
    )


async def test_sweep_waits_expires_only_past_deadline(db: asyncpg.Connection) -> None:
    # regression pin: a waiting job past wait_expires_at is released with a timeout marker;
    # one still within its deadline survives (scope pin).
    past_id = await h.seed(db, h.spec(state="waiting"))
    future_id = await h.seed(db, h.spec(state="waiting"))
    await db.execute("UPDATE jobs SET wait_key='k', wait_expires_at=now() - interval '1 s' WHERE id=$1", past_id)
    await db.execute("UPDATE jobs SET wait_key='k', wait_expires_at=now() + interval '1 h' WHERE id=$1", future_id)

    expired = await repo.sweep_waits(db, limit=100)
    assert expired == [past_id], "only the past-deadline job is released, and its id is returned"
    past_row = await db.fetchrow("SELECT state, event_payload FROM jobs WHERE id = $1", past_id)
    assert past_row is not None
    assert past_row["state"] == "queued"
    assert past_row["event_payload"] == {"__timeout__": True}
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", future_id) == "waiting"


async def test_reconcile_group_running_heals_drift(db: asyncpg.Connection) -> None:
    await h.seed(db, h.spec(group_key="g", cap=5))
    await h.claim_one(db)
    assert await h.group_running(db, "default", "g", "t.echo") == 1
    # Simulate counter drift (a crash left it too high).
    await db.execute("UPDATE group_running SET running = 9 WHERE group_key = 'g'")
    corrected = await repo.reconcile_group_running(db)
    assert corrected == 1
    assert await h.group_running(db, "default", "g", "t.echo") == 1


# --------------------------------------------------------------------------- #
# signal rendezvous
# --------------------------------------------------------------------------- #


async def test_signal_wakes_oldest_waiting_job(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(state="waiting"))
    await db.execute("UPDATE jobs SET wait_key = 'ready' WHERE id = $1", job_id)
    woken = await repo.signal_wake(db, tenant="default", wait_key="ready", payload={"v": 42})
    assert woken is not None and str(woken["id"]) == job_id
    woke_row = await db.fetchrow("SELECT state, event_payload FROM jobs WHERE id = $1", job_id)
    assert woke_row is not None
    assert woke_row["state"] == "queued" and woke_row["event_payload"] == {"v": 42}


async def test_signal_no_waiter_returns_none(db: asyncpg.Connection) -> None:
    woken = await repo.signal_wake(db, tenant="default", wait_key="absent", payload={})
    assert woken is None


# --------------------------------------------------------------------------- #
# get_result: hot + archive read-through
# --------------------------------------------------------------------------- #


async def test_get_result_unknown_job_returns_none(db: asyncpg.Connection) -> None:
    res = await repo.get_result(db, job_id=str(uuid.uuid4()), tenant="default")
    assert res is None


async def test_get_result_tenant_scoped(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(tenant="acme"))
    # Same id but a different tenant must not see the row.
    assert await repo.get_result(db, job_id=job_id, tenant="other") is None
    assert await repo.get_result(db, job_id=job_id, tenant="acme") is not None
