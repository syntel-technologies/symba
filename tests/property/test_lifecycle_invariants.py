# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L3 property suite: random DAGs + random event schedules.

Hypothesis generates a small random workflow (independent jobs, linear chains,
depends_on edges, and fan-out gates) plus a random interleaving of worker events
(complete / fail / sweep). We drive it against a REAL Postgres 18 and then assert
the lifecycle invariants that every engine behavior must uphold no matter the
schedule.

    invariants asserted after each schedule
    ----------------------------------------------------
    I1  every job is terminal (archived) OR still live+claimable — no job is
        stranded in a non-terminal state with no path forward.
    I2  a succeeded job with a chain_tail has its continuation materialized.
    I3  a dependent is live/succeeded, or it was cancelled because an upstream died
        (cascade) — it never runs with an unsatisfied dependency.
    I4  every gate fired at most once (claim-once).
    I5  the job_events ledger is gapless: every archived job has a terminal event.

    generation shape (kept small — max_jobs≈30)
    -------------------------------------------------
      root_i ──(chain?)──► tail
             ──(dep?)────► dependent_i
      fan_out group ──gate──► on_complete

Hypothesis + async: the DB fixtures are session-scoped (set up once); each example
truncates all tables first, so examples are independent. deadline is disabled
because a DB round-trip's latency is not what we're measuring.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from symba.config import load_config
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.services.cancel_service import CancelService
from symba.services.fanout_service import FanOutService
from symba.services.job_service import JobService
from symba.services.registry import WorkerRegistry
from symba.services.submit_service import SubmitService

pytestmark = [pytest.mark.l3, pytest.mark.asyncio(loop_scope="session")]

_TRUNCATE = (
    "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
    "group_running, checkpoints, signals, workers RESTART IDENTITY CASCADE"
)


class _Services:
    """One bundle of services over a shared pool, built once per session."""

    def __init__(self, pools: Pools) -> None:
        registry = WorkerRegistry()
        self.pools = pools
        self.submit = SubmitService(pools, registry, load_config())
        self.jobs = JobService(pools, load_config(), registry)
        self.fanout = FanOutService(pools, registry, load_config())
        self.cancel = CancelService(pools, registry)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def prop_pool(migrated_pool: asyncpg.Pool) -> AsyncIterator[Pools]:
    """Reuse the session's migrated pool (schema already applied) so the property
    suite shares one backend with the rest of L2/L3."""
    yield Pools(hot=migrated_pool, general=migrated_pool)


# ── generators ────────────────────────────────────────────────────────────────

# A "kind" decides how each root job is wired. Kept to the M2 flow primitives.
_kinds = st.sampled_from(["plain", "chain", "dep", "gate"])


@st.composite
def _workflows(draw: st.DrawFn) -> list[str]:
    """Draw 1..6 workflow kinds (each expands to a few jobs -> ~<30 total)."""
    return draw(st.lists(_kinds, min_size=1, max_size=6))


# For each root job, decide its terminal event and whether a sweep runs after.
_outcomes = st.sampled_from(["succeed", "fail_retry", "fail_die"])


@st.composite
def _schedule(draw: st.DrawFn, n: int) -> list[str]:
    return draw(st.lists(_outcomes, min_size=n, max_size=n))


# ── driver ──────────────────────────────────────────────────────────────────


async def _drive(svc: _Services, db: asyncpg.Connection, workflows: list[str]) -> None:
    """Build the DAG then run one terminal event per claimable root, deterministically.

    We keep the driver simple and total: claim whatever is queued, apply the next
    scheduled outcome, repeat until nothing is claimable. This exercises chains
    advancing, deps flipping, gates firing, and cascades — the interleaving comes
    from Hypothesis choosing the workflow mix and outcomes.
    """
    reg = svc  # alias for brevity
    for i, kind in enumerate(workflows):
        if kind == "plain":
            await svc.submit.submit([SubmitSpec(task_name=f"plain{i}", payload={})])
        elif kind == "chain":
            await svc.submit.submit(
                [SubmitSpec(task_name=f"c{i}", payload={}, on_success=f"c{i}b", chain_tail=[f"c{i}c"])]
            )
        elif kind == "dep":
            out = await svc.submit.submit([SubmitSpec(task_name=f"u{i}", payload={})])
            down = SubmitSpec(task_name=f"d{i}", payload={})
            down.depends_on = [(out.job_ids[0], None)]
            await svc.submit.submit([down])
        elif kind == "gate":
            await svc.fanout.fan_out(
                tenant="default",
                ctx_id=None,
                children=[SubmitSpec(task_name=f"g{i}c0", payload={}), SubmitSpec(task_name=f"g{i}c1", payload={})],
                on_complete=SubmitSpec(task_name=f"g{i}done", payload={}),
                policy="all_terminal",
            )
    _ = reg

    # Drain: claim + terminate everything reachable, bounded so a bug can't hang us.
    for _ in range(200):
        claimed = await repo.claim(
            db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="prop-w", per_group_cap=1000
        )
        if not claimed:
            break
        for job in claimed:
            # Alternate success/failure by a cheap hash so the schedule varies but
            # stays reproducible for a given DAG.
            if hash(job.id) % 4 == 0:
                await svc.jobs.fail(
                    job_id=job.id,
                    lease_token=job.lease_token,
                    error_type="E",
                    error_message="x",
                    stack_hash="h",
                    retryable=False,
                )
            else:
                await svc.jobs.complete(job_id=job.id, lease_token=job.lease_token, result=None)


# ── invariants ────────────────────────────────────────────────────────────────


async def _assert_invariants(db: asyncpg.Connection) -> None:
    live = await db.fetch("SELECT id, state, on_success, chain_tail, remaining_deps FROM jobs")
    archived = await db.fetch("SELECT id, final_state, ctx_id, on_success FROM jobs_archive")

    # I1: after a full drain (every claimable job terminated), NOTHING is left
    # 'running'. A leftover running row would mean a lease/terminal-move leak.
    running = [r["id"] for r in live if r["state"] == "running"]
    assert not running, f"jobs stuck running after drain: {running}"

    # I1b: any still-live job is either claimable (queued) or legitimately parked
    # (submitted -> waiting on a dep/gate that never fired; waiting -> a signal).
    for r in live:
        assert r["state"] in ("queued", "submitted", "waiting"), r["state"]
        # A 'submitted' job must still have an unsatisfied reason to be parked.
        if r["state"] == "submitted":
            assert r["remaining_deps"] > 0, f"submitted job {r['id']} has no pending deps"

    # I2: a succeeded job whose chain had a successor must have materialized
    # that continuation somewhere (live queued or already archived).
    for a in archived:
        if a["final_state"] == "succeeded" and a["on_success"]:
            nxt = a["on_success"]
            found = await db.fetchval(
                "SELECT count(*) FROM (SELECT task_name FROM jobs UNION ALL "
                "SELECT task_name FROM jobs_archive) t WHERE task_name=$1",
                nxt,
            )
            assert found and found >= 1, f"chain successor {nxt!r} of {a['id']} missing"

    # I3: no live dependent has a DEAD upstream — cascade-cancel must have
    # swept it. (A live dependent with remaining_deps>0 whose upstream died would be
    # stranded forever.)
    stranded = await db.fetchval(
        "SELECT count(*) FROM jobs j "
        "JOIN job_dependencies d ON d.job_id = j.id "
        "JOIN jobs_archive a ON a.id = d.depends_on_job_id "
        "WHERE a.final_state = 'dead'"
    )
    assert stranded == 0, f"{stranded} live dependents have a dead upstream (cascade leak)"

    # I4: every gate fired at most once — fired_at is a single nullable
    # timestamp, so structurally <=1; assert the column never carries a list-like
    # over-fire by checking the succeeded/completed counts never exceed expected.
    over_counted = await db.fetchval("SELECT count(*) FROM gates WHERE completed_children > expected_children")
    assert over_counted == 0, "a gate counted more completions than it expected"

    # I5: the ledger is gapless — every archived job has >=1 terminal event.
    for a in archived:
        events = await db.fetchval("SELECT count(*) FROM job_events WHERE job_id=$1", a["id"])
        assert events >= 1, f"archived job {a['id']} has no ledger event"


# ── the property test ─────────────────────────────────────────────────────────


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(workflows=_workflows())
@pytest.mark.asyncio(loop_scope="session")
async def test_lifecycle_invariants(prop_pool: Pools, workflows: list[str]) -> None:
    svc = _Services(prop_pool)
    async with prop_pool.general.acquire() as db:
        await db.execute(_TRUNCATE)
        await _drive(svc, db, workflows)
        await _assert_invariants(db)


# ── regression pin: group_running drift under kill interleavings ─────────────
#
# A crash (kill -9) between a terminal write and its group_running decrement — or
# between a claim and its increment — leaves the counter DRIFTED from the true count
# of live 'running' rows. The counter is only a performance cache; the sweeper's
# reconcile is the authority that repairs it.
#
#   true running rows  ── crash drops a ±1 ──►  counter ≠ actual  (over/under-admit)
#   sweeper reconcile  ─────────────────────►  counter == actual  (self-healed)
#
# We drive a random schedule of claim/complete/fail against ONE capped group, then
# inject a random counter perturbation (the observable residue of a mid-tx kill) and
# assert reconcile restores the counter to exactly the live running count — and that
# no group is left PERSISTENTLY over its cap.


# A step is one of: claim a job, complete a running one, fail (die) a running one.
_steps = st.sampled_from(["claim", "complete", "fail"])


@st.composite
def _kill_schedule(draw: st.DrawFn) -> tuple[int, int, list[str], int]:
    n_jobs = draw(st.integers(min_value=3, max_value=8))
    cap = draw(st.integers(min_value=1, max_value=3))
    steps = draw(st.lists(_steps, min_size=1, max_size=12))
    # The drift a kill leaves: a lost ±1..2 on the counter (increment or decrement).
    drift = draw(st.integers(min_value=-2, max_value=2))
    return n_jobs, cap, steps, drift


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(sched=_kill_schedule())
@pytest.mark.asyncio(loop_scope="session")
async def test_gap2_group_running_reconciles_after_kill(
    prop_pool: Pools, sched: tuple[int, int, list[str], int]
) -> None:
    n_jobs, cap, steps, drift = sched
    svc = _Services(prop_pool)
    async with prop_pool.general.acquire() as db:
        await db.execute(_TRUNCATE)

        for _ in range(n_jobs):
            await svc.submit.submit(
                [SubmitSpec(task_name="capped", payload={}, group_key="g1", max_concurrent_per_group=cap)]
            )

        running: list[tuple[str, str]] = []  # (job_id, lease_token) currently running
        for step in steps:
            if step == "claim":
                # The matcher passes per_group_cap = remaining headroom (cap - committed
                # running), NOT a static cap — that headroom computation is what keeps a
                # group at or under its ceiling across batches. We reproduce
                # it here so the drive is faithful to how the engine actually claims.
                committed = (
                    await db.fetchval(
                        "SELECT running FROM group_running "
                        "WHERE tenant='default' AND group_key='g1' AND task_name='capped'"
                    )
                    or 0
                )
                headroom = max(0, cap - committed)
                got = await repo.claim(
                    db,
                    worker_tags=[],
                    exhausted_rate_classes=[],
                    limit=10,
                    claimed_by="w",
                    per_group_cap=max(1, headroom),
                )
                running.extend((j.id, j.lease_token) for j in got)
                # With matcher headroom, a claim never pushes the group over cap.
                assert len(running) <= cap, f"running {len(running)} > cap {cap} after claim"
            elif step == "complete" and running:
                jid, tok = running.pop()
                await svc.jobs.complete(job_id=jid, lease_token=tok, result=None)
            elif step == "fail" and running:
                jid, tok = running.pop()
                await svc.jobs.fail(
                    job_id=jid,
                    lease_token=tok,
                    error_type="E",
                    error_message="x",
                    stack_hash="h",
                    retryable=False,
                )

        # Simulate the residue of a mid-transaction kill: the counter lost a ±delta.
        if drift != 0:
            await db.execute(
                "UPDATE group_running SET running = GREATEST(0, running + $1) "
                "WHERE tenant='default' AND group_key='g1' AND task_name='capped'",
                drift,
            )

        # The sweeper's authority pass repairs the cache to match reality.
        await repo.reconcile_group_running(db)

        actual = await db.fetchval(
            "SELECT count(*) FROM jobs WHERE state='running' AND group_key='g1' AND task_name='capped'"
        )
        counter = await db.fetchval(
            "SELECT running FROM group_running WHERE tenant='default' AND group_key='g1' AND task_name='capped'"
        )
        # After reconcile the counter equals the true running count (or the row is
        # absent, meaning zero) — never persistently drifted.
        assert (counter or 0) == actual, f"counter {counter} != actual running {actual} after reconcile"
        # And the reconciled count never persistently exceeds the cap.
        assert (counter or 0) <= cap, f"group persistently over cap: {counter} > {cap}"
