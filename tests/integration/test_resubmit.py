# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 resubmit / DLQ replay.

Proves the DLQ contract against real PG18:

    dead job (archived)        resubmit()             fresh row (hot)
    -------------------        ----------             --------------
    id=A final_state=dead  →   INSERT fresh       →   id=B state=queued
    original stays archived    resubmitted_from=A     attempt=0, dedup_key NULL

The original is NEVER mutated (it is the audit record); the replay is a brand-new
claimable job that carries the lineage pointer back to the failure it descends from.
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.core.errors import NotFound
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.registry import WorkerRegistry
from symba.services.resubmit_service import ResubmitService
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _svc(pools: Pools) -> ResubmitService:
    return ResubmitService(pools, WorkerRegistry())


async def _kill(db: asyncpg.Connection, *, dedup: str | None = None) -> str:
    """Seed a job, claim it, and fail it fatally so it archives as DEAD. Returns its id."""
    job_id = await h.seed(db, h.spec(task_name="t.flaky", dedup_key=dedup))
    claimed = await h.claim_one(db)
    term = await repo.fail_die(db, job_id=job_id, lease_token=claimed.lease_token, error_entry={"type": "boom"})
    assert term is not None
    return job_id


# ── a dead job replays as a fresh queued job with lineage ─────────────────────


async def test_resubmit_creates_fresh_queued_job_with_lineage(db: asyncpg.Connection, pools: Pools) -> None:
    dead_id = await _kill(db)
    assert await db.fetchval("SELECT final_state FROM jobs_all WHERE id=$1", dead_id) == "dead"

    outcome = await _svc(pools).resubmit(job_id=dead_id, tenant="default")
    assert outcome.resubmitted_from == dead_id
    assert outcome.new_job_id != dead_id

    fresh = await db.fetchrow(
        "SELECT state, attempt, resubmitted_from, task_name FROM jobs WHERE id=$1", outcome.new_job_id
    )
    assert fresh["state"] == "queued"
    assert fresh["attempt"] == 0
    assert str(fresh["resubmitted_from"]) == dead_id
    assert fresh["task_name"] == "t.flaky"
    # The original DEAD row is untouched (still the audit record in the archive).
    assert await db.fetchval("SELECT final_state FROM jobs_all WHERE id=$1", dead_id) == "dead"


# ── the fresh job is actually claimable (dispatcher can pick it up) ───────────


async def test_resubmitted_job_is_claimable(db: asyncpg.Connection, pools: Pools) -> None:
    dead_id = await _kill(db)
    outcome = await _svc(pools).resubmit(job_id=dead_id)
    claimed = await h.claim_one(db, worker="w1")
    assert claimed.id == outcome.new_job_id


# ── dedup_key is dropped so a replay never collides with the spent window ─────


async def test_resubmit_clears_dedup_key(db: asyncpg.Connection, pools: Pools) -> None:
    dead_id = await _kill(db, dedup="once-only")
    outcome = await _svc(pools).resubmit(job_id=dead_id)
    dedup = await db.fetchval("SELECT dedup_key FROM jobs WHERE id=$1", outcome.new_job_id)
    assert dedup is None, "a spent dedup key must not block the forced replay"


# ── resubmitting an unknown / live job raises NotFound ────────────────────────


async def test_resubmit_unknown_job_raises(pools: Pools) -> None:
    with pytest.raises(NotFound):
        await _svc(pools).resubmit(job_id="00000000-0000-0000-0000-000000000000")


async def test_resubmit_live_job_raises(db: asyncpg.Connection, pools: Pools) -> None:
    # A queued (live) job is retried via the fail path, never resubmitted.
    live_id = await h.seed(db, h.spec(task_name="t.live"))
    with pytest.raises(NotFound):
        await _svc(pools).resubmit(job_id=live_id)


# ── bulk replay skips bad ids and returns one outcome per good id ─────────────


async def test_resubmit_many_skips_bad_ids(db: asyncpg.Connection, pools: Pools) -> None:
    a = await _kill(db)
    b = await _kill(db)
    bad = "00000000-0000-0000-0000-000000000000"

    outcomes = await _svc(pools).resubmit_many(job_ids=[a, bad, b])
    assert len(outcomes) == 2
    assert {o.resubmitted_from for o in outcomes} == {a, b}
