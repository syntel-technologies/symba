"""L2 engine tests: matcher pass + JobService round-trips.

These exercise the in-process wiring above the raw SQL layer:
  - the matcher claims queued jobs for a registered worker and enqueues real
    JobAssignment protobufs onto that worker's queue (worker-driven flow control);
  - JobService.complete/fail/heartbeat drive the correct multi-statement tx and
    lease-guard behavior against a real Postgres 18.

The service acquires its own connection from a Pools built over the shared
migrated pool, so writes it commits are visible to the `db` connection used for
seeding and assertions (both draw from the same pool).
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.config import load_config
from symba.db.pool import Pools
from symba.services.job_service import JobService
from symba.services.loops import Sweeper
from symba.services.matcher import Matcher
from symba.services.registry import WorkerConn, WorkerRegistry
from symba.transport.state import EngineState
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


# --------------------------------------------------------------------------- #
# matcher: snapshot -> claim -> shape -> enqueue
# --------------------------------------------------------------------------- #


async def test_matcher_assigns_queued_job_to_worker(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec())
    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w1", tags=frozenset(), free_slots=4, labels={})
    await registry.register(conn)

    matcher = Matcher(pools, registry, load_config())
    assigned = await matcher.pass_()

    assert assigned == 1
    assert conn.free_slots == 3  # one slot consumed
    assignment = conn.queue.get_nowait()
    assert assignment.job.id == job_id
    assert assignment.lease_token
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"


async def test_matcher_no_workers_claims_nothing(db: asyncpg.Connection, pools: Pools) -> None:
    await h.seed(db, h.spec())
    matcher = Matcher(pools, WorkerRegistry(), load_config())
    assert await matcher.pass_() == 0
    assert await db.fetchval("SELECT state FROM jobs WHERE state = 'queued'") == "queued"


async def test_matcher_respects_free_slots(db: asyncpg.Connection, pools: Pools) -> None:
    for _ in range(5):
        await h.seed(db, h.spec())
    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w1", tags=frozenset(), free_slots=2, labels={})
    await registry.register(conn)

    assigned = await Matcher(pools, registry, load_config()).pass_()
    assert assigned == 2  # never assigns beyond announced slots
    assert conn.free_slots == 0
    running = await db.fetchval("SELECT count(*) FROM jobs WHERE state = 'running'")
    assert running == 2


async def test_matcher_caps_stale_slot_frame_by_attributed_running_jobs(
    db: asyncpg.Connection,
    pools: Pools,
) -> None:
    for _ in range(4):
        await h.seed(db, h.spec())
    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w1", tags=frozenset(), free_slots=2, labels={})
    await registry.register(conn)
    matcher = Matcher(pools, registry, load_config())

    assert await matcher.pass_() == 2
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE claimed_by = 'w1'") == 2

    # This frame was produced while the first assignment batch was still in
    # flight. It must not reopen capacity already consumed by those jobs.
    await registry.update_slots("w1", free_slots=2)
    assert await matcher.pass_() == 0
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE state = 'queued'") == 2


async def test_matcher_tag_routing(db: asyncpg.Connection, pools: Pools) -> None:
    gpu_id = await h.seed(db, h.spec(runs_on=["gpu"]))
    await h.seed(db, h.spec(runs_on=["fpga"]))  # no worker covers this
    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w1", tags=frozenset({"gpu", "cpu"}), free_slots=4, labels={})
    await registry.register(conn)

    assigned = await Matcher(pools, registry, load_config()).pass_()
    assert assigned == 1
    assert conn.queue.get_nowait().job.id == gpu_id


# --------------------------------------------------------------------------- #
# JobService.complete
# --------------------------------------------------------------------------- #


async def test_service_complete_archives_and_decrements(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec(group_key="g", cap=2))
    claimed = await h.claim_one(db)
    assert await h.group_running(db, "default", "g", "t.echo") == 1

    svc = JobService(pools, load_config())
    accepted = await svc.complete(job_id=job_id, lease_token=claimed.lease_token, result={"ok": 1})

    assert accepted is True
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id = $1", job_id) == 0
    assert await h.state_of(db, job_id) == "succeeded"
    assert await h.group_running(db, "default", "g", "t.echo") == 0


async def test_service_complete_stale_lease_raises(db: asyncpg.Connection, pools: Pools) -> None:
    from symba.core.errors import StaleLease

    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    svc = JobService(pools, load_config())
    with pytest.raises(StaleLease):
        await svc.complete(job_id=job_id, lease_token="wrong", result={})
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"


# --------------------------------------------------------------------------- #
# JobService.fail: retry-or-die decision
# --------------------------------------------------------------------------- #


async def test_service_fail_retries_when_attempts_remain(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec())  # max_attempts default 5
    claimed = await h.claim_one(db)  # attempt -> 1

    svc = JobService(pools, load_config())
    outcome = await svc.fail(
        job_id=job_id,
        lease_token=claimed.lease_token,
        error_type="ValueError",
        error_message="boom",
        stack_hash="abc123",
        retryable=True,
    )
    assert outcome.will_retry is True
    row = await db.fetchrow("SELECT state, lease_token FROM jobs WHERE id = $1", job_id)
    assert row is not None and row["state"] == "queued" and row["lease_token"] is None


async def test_service_fail_dies_when_not_retryable(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec(group_key="g", cap=1))
    claimed = await h.claim_one(db)

    svc = JobService(pools, load_config())
    outcome = await svc.fail(
        job_id=job_id,
        lease_token=claimed.lease_token,
        error_type="Fatal",
        error_message="nope",
        stack_hash="deadbeef",
        retryable=False,
    )
    assert outcome.will_retry is False
    assert await h.state_of(db, job_id) == "dead"
    assert await h.group_running(db, "default", "g", "t.echo") == 0


async def test_service_fail_dies_when_attempts_exhausted(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec())
    # Pin max_attempts=1 so the first (and only) attempt exhausts the budget.
    await db.execute("UPDATE jobs SET max_attempts = 1 WHERE id = $1", job_id)
    claimed = await h.claim_one(db)  # attempt -> 1, == max_attempts

    svc = JobService(pools, load_config())
    outcome = await svc.fail(
        job_id=job_id,
        lease_token=claimed.lease_token,
        error_type="ValueError",
        error_message="boom",
        stack_hash="abc",
        retryable=True,  # retryable, but no attempts left
    )
    assert outcome.will_retry is False
    assert await h.state_of(db, job_id) == "dead"


async def test_sweeper_terminalizes_exhausted_lease_through_job_service(
    db: asyncpg.Connection,
    pools: Pools,
) -> None:
    spec = h.spec(group_key="g", cap=1, lease_ttl_s=1800)
    spec.max_attempts = 1
    spec.on_failure = {"task_name": "t.recover", "payload": {"source": "lease"}}
    job_id = await h.seed(db, spec)
    await h.claim_one(db, worker="w-gone")
    await db.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id=$1",
        job_id,
    )

    state = EngineState.build(load_config(), pools)
    processed = await Sweeper(state).pass_()

    assert processed >= 1
    assert await h.state_of(db, job_id) == "dead"
    assert await h.group_running(db, "default", "g", "t.echo") == 0
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE task_name='t.recover' AND state='queued'") == 1
    error_history = await db.fetchval("SELECT error_history FROM jobs_archive WHERE id=$1", job_id)
    assert error_history[-1]["type"] == "LeaseAttemptsExhausted"


# --------------------------------------------------------------------------- #
# JobService.heartbeat
# --------------------------------------------------------------------------- #


async def test_service_heartbeat_extends_lease(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    before = await db.fetchval("SELECT lease_expires_at FROM jobs WHERE id = $1", job_id)

    svc = JobService(pools, load_config())
    expires_at, cancelled = await svc.heartbeat(job_id=job_id, lease_token=claimed.lease_token)
    assert cancelled is False
    assert expires_at >= before


async def test_service_heartbeat_reports_cancel_flag(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)
    await db.execute("UPDATE jobs SET cancel_requested = true WHERE id = $1", job_id)

    svc = JobService(pools, load_config())
    _, cancelled = await svc.heartbeat(job_id=job_id, lease_token=claimed.lease_token)
    assert cancelled is True


async def test_service_heartbeat_stale_lease_raises(db: asyncpg.Connection, pools: Pools) -> None:
    from symba.core.errors import StaleLease

    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    svc = JobService(pools, load_config())
    with pytest.raises(StaleLease):
        await svc.heartbeat(job_id=job_id, lease_token="wrong")


# --------------------------------------------------------------------------- #
# Sweeper.pass_(): advisory-lock election + maintenance statements
# --------------------------------------------------------------------------- #


async def test_sweeper_pass_reclaims_expired_lease(db: asyncpg.Connection, pools: Pools) -> None:
    from symba.services.loops import Sweeper
    from symba.transport.state import EngineState

    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    # Force the lease into the past so the sweeper presumes the worker dead.
    await db.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 hour' WHERE id = $1", job_id)

    sweeper = Sweeper(EngineState.build(load_config(), pools))
    moved = await sweeper.pass_()

    assert moved >= 1
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "queued"
    assert await db.fetchval("SELECT lease_token FROM jobs WHERE id = $1", job_id) is None


async def test_sweeper_pass_skips_when_election_lost(db: asyncpg.Connection, pools: Pools) -> None:
    from symba.db import repository as repo
    from symba.services.loops import Sweeper
    from symba.transport.state import EngineState

    job_id = await h.seed(db, h.spec())
    await h.claim_one(db)
    await db.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 hour' WHERE id = $1", job_id)

    # Hold the sweeper election on a separate session so the loop must skip.
    async with pools.general.acquire() as holder:
        assert await repo.try_sweeper_lock(holder) is True
        try:
            sweeper = Sweeper(EngineState.build(load_config(), pools))
            moved = await sweeper.pass_()
        finally:
            await repo.unlock_sweeper(holder)

    assert moved == 0
    # The loser did NOT reclaim: the job is still running under its expired lease.
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"
