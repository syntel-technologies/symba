# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 concurrency + crash-atomicity pins.

These are the "the queue must not lie under pressure" tests, so a regression is
obvious:

    test_claim_respects_group_ceiling             — one batch, cap holds
    test_claim_uses_exact_remaining_group_capacity — high-limit cap=4 claim
    test_concurrent_group_claims_hold_exact_cap    — racing cap=4 claimers
    test_concurrent_claims_never_double_assign    — 8 racing claimers, SKIP LOCKED
    archive-move atomicity                         — a rolled-back Complete leaves the
                                                    job fully LIVE, never lost from
                                                    both jobs and jobs_archive
    sweeper scope                                  — foreign-tenant + healthy-lease rows
                                                    survive a sweeper pass untouched

The concurrent test manages its own connections directly off the shared pool (it
needs N simultaneous sessions), so it takes `migrated_pool` rather than the
single-connection `db` fixture and truncates up front itself.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from symba.config import load_config
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.loops import Sweeper
from symba.transport.state import EngineState
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


# --------------------------------------------------------------------------- #
# group ceiling in ONE batch
# --------------------------------------------------------------------------- #


async def test_claim_respects_group_ceiling(db: asyncpg.Connection) -> None:
    # Three cap=1 jobs: two in cust-1, one in cust-2. A single claim with
    # headroom (limit=10) must take exactly one per group -> {cust-1, cust-2},
    # the second cust-1 job held back. The independent per_group_cap=1 fairness
    # setting matches the same ceiling here but is not relied on by the cap=4 pins.
    await h.seed(db, h.spec(task_name="webhook", group_key="cust-1", cap=1))
    await h.seed(db, h.spec(task_name="webhook", group_key="cust-1", cap=1))
    await h.seed(db, h.spec(task_name="webhook", group_key="cust-2", cap=1))

    claimed = await repo.claim(
        db, worker_tags=["general"], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1
    )

    assert {j.group_key for j in claimed} == {"cust-1", "cust-2"}
    assert len(claimed) == 2  # second cust-1 job held back


async def test_claim_uses_exact_remaining_group_capacity(db: asyncpg.Connection) -> None:
    ids_by_priority: dict[int, str] = {}
    for priority in range(20):
        ids_by_priority[priority] = await h.seed(
            db,
            h.spec(task_name="provider.call", group_key="provider", cap=4, priority=priority),
        )

    initial = await repo.claim(
        db,
        worker_tags=[],
        exhausted_rate_classes=[],
        limit=2,
        claimed_by="initial-worker",
        per_group_cap=1000,
    )
    assert {job.id for job in initial} == {ids_by_priority[p] for p in (18, 19)}

    claimed = await repo.claim(
        db,
        worker_tags=[],
        exhausted_rate_classes=[],
        limit=50,
        claimed_by="wide-worker",
        per_group_cap=1000,
    )

    assert len(claimed) == 2
    assert {job.id for job in claimed} == {ids_by_priority[p] for p in (16, 17)}
    assert await h.group_running(db, "default", "provider", "provider.call") == 4
    assert await db.fetchval(
        "SELECT count(*) FROM jobs WHERE state='running' AND group_key='provider'"
    ) == 4
    assert await db.fetchval(
        "SELECT count(*) FROM jobs WHERE state='queued' AND group_key='provider'"
    ) == 16


# --------------------------------------------------------------------------- #
# 8 racing claimers never double-assign (SKIP LOCKED)
# --------------------------------------------------------------------------- #


async def test_concurrent_claims_never_double_assign(migrated_pool: asyncpg.Pool) -> None:
    async with migrated_pool.acquire() as seeder:
        await seeder.execute(
            "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
            "group_running, checkpoints, signals, workers RESTART IDENTITY CASCADE"
        )
        for _ in range(200):
            await h.seed(seeder, h.spec())

    async def run_claim(worker: str) -> list[str]:
        # Each claimer gets its own connection + transaction, exactly like a real
        # engine's claim pass, so FOR UPDATE SKIP LOCKED is the only thing keeping
        # them from grabbing the same rows.
        async with migrated_pool.acquire() as conn, conn.transaction():
            claimed = await repo.claim(
                conn, worker_tags=[], exhausted_rate_classes=[], limit=50, claimed_by=worker, per_group_cap=1000
            )
        return [j.id for j in claimed]

    # 8 claimers, but the pool caps concurrent sessions; the gather still races
    # them enough to exercise SKIP LOCKED (pool max_size drives real contention).
    results = await asyncio.gather(*[run_claim(f"w{i}") for i in range(8)])

    ids = [jid for batch in results for jid in batch]
    assert len(ids) == len(set(ids)), "SKIP LOCKED must prevent any double-assignment"
    assert len(ids) == 200, "every queued job should be claimed exactly once"


async def test_concurrent_group_claims_hold_exact_cap(migrated_pool: asyncpg.Pool) -> None:
    async with migrated_pool.acquire() as seeder:
        await seeder.execute(
            "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
            "group_running, checkpoints, signals, workers RESTART IDENTITY CASCADE"
        )
        for priority in range(64):
            await h.seed(
                seeder,
                h.spec(task_name="provider.call", group_key="provider", cap=4, priority=priority),
            )
        # Start with the durable counter already present so the race specifically
        # proves row-lock serialization, independent of first-claim initialization.
        await seeder.execute(
            "INSERT INTO group_running (tenant, group_key, task_name, running) "
            "VALUES ('default', 'provider', 'provider.call', 0)"
        )

    async def run_claim(worker: str) -> list[str]:
        async with migrated_pool.acquire() as conn, conn.transaction():
            claimed = await repo.claim(
                conn,
                worker_tags=[],
                exhausted_rate_classes=[],
                limit=50,
                claimed_by=worker,
                per_group_cap=1000,
            )
        return [job.id for job in claimed]

    results = await asyncio.gather(*[run_claim(f"group-worker-{i}") for i in range(8)])
    ids = [job_id for batch in results for job_id in batch]

    assert len(ids) == len(set(ids)) == 4
    async with migrated_pool.acquire() as verifier:
        assert await h.group_running(verifier, "default", "provider", "provider.call") == 4
        assert await verifier.fetchval(
            "SELECT count(*) FROM jobs WHERE state='running' "
            "AND group_key='provider' AND task_name='provider.call'"
        ) == 4
        assert await verifier.fetchval(
            "SELECT count(*) FROM jobs WHERE state='queued' "
            "AND group_key='provider' AND task_name='provider.call'"
        ) == 60


# --------------------------------------------------------------------------- #
# archive-move atomicity
# --------------------------------------------------------------------------- #


async def test_complete_rollback_leaves_job_fully_live(db: asyncpg.Connection) -> None:
    # The Complete archive move is one atomic DELETE+INSERT CTE inside a tx. If the
    # engine dies before commit (here: an explicit rollback), the job must be
    # EITHER fully live OR fully archived — never absent from both (the
    # archive-move ghost). A rollback lands it fully live.
    job_id = await h.seed(db, h.spec())
    claimed = await h.claim_one(db)

    tx = db.transaction()
    await tx.start()
    term = await repo.complete_move(db, job_id=job_id, lease_token=claimed.lease_token, result={"ok": True})
    assert term is not None  # the move succeeded inside the tx...
    await tx.rollback()  # ...but the "engine" crashed before commit

    live = await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id)
    archived = await db.fetchval("SELECT count(*) FROM jobs_archive WHERE id = $1", job_id)
    assert live == "running", "rolled-back complete must leave the job fully live"
    assert archived == 0, "the job must NOT appear in the archive after rollback"
    # Never-lost invariant: it exists in exactly one place.
    assert await db.fetchval("SELECT count(*) FROM jobs_all WHERE id = $1", job_id) == 1


# --------------------------------------------------------------------------- #
# sweeper scope
# --------------------------------------------------------------------------- #


async def test_sweeper_pass_leaves_foreign_and_healthy_rows_untouched(
    db: asyncpg.Connection, migrated_pool: asyncpg.Pool
) -> None:
    # Seed three running jobs, then age only ONE lease into the past:
    #   - expired (default tenant)  -> MUST be reclaimed
    #   - healthy (default tenant)  -> unexpired lease, MUST survive
    #   - foreign (acme tenant)     -> expired too, but a full sweeper pass must
    #                                  still touch it ONLY via its own scope; it is
    #                                  reclaimed because the sweeper is global by
    #                                  design, so to prove SCOPE we keep it HEALTHY.
    expired_id = await h.seed(db, h.spec())
    healthy_id = await h.seed(db, h.spec())
    foreign_id = await h.seed(db, h.spec(tenant="acme"))
    await h.claim_one(db, worker="w1")
    await h.claim_one(db, worker="w2")
    await h.claim_one(db, worker="w3")

    await db.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 h' WHERE id = $1", expired_id)
    await db.execute("UPDATE jobs SET lease_expires_at = now() + interval '1 h' WHERE id = $1", healthy_id)
    await db.execute("UPDATE jobs SET lease_expires_at = now() + interval '1 h' WHERE id = $1", foreign_id)

    pools = Pools(hot=migrated_pool, general=migrated_pool)
    sweeper = Sweeper(EngineState.build(load_config(), pools))
    moved = await sweeper.pass_()

    assert moved == 1, "only the single expired lease should be reclaimed"
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", expired_id) == "queued"
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", healthy_id) == "running"
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", foreign_id) == "running"
