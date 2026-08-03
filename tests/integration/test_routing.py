# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 M3 — heterogeneity + budgets.

Proves the routing/budget behavior the claim SQL + matcher already implement, so a
regression in any of the four levers is obvious:

    runs_on routing          a job is claimable ONLY by a worker whose tags cover its
                             runs_on set (runs_on <@ worker_tags).
    group ceilings           max_concurrent_per_group holds ACROSS batches via the
                             group_running counter, not just within one claim.
    priority/fairness        higher priority wins the head of the batch; within one
                             priority band no single group_key monopolizes.
    rate classes             the PG-fallback token bucket grants -> exhausts -> refills
                             -> returns unused, and the admin upsert/drain take effect.

    routing predicate (claim.sql)
    -----------------------------
      job.runs_on ⊆ worker.tags   →  claimable
      job.runs_on ⊄ worker.tags   →  skipped (stays queued for a capable worker)
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.config import RedisConfig, load_config
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.job_service import JobService
from symba.services.rate_limiter import RateLimiter
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


# ── runs_on tag routing ────────────────────────────────────────────────────────


async def test_runs_on_routes_only_to_capable_worker(db: asyncpg.Connection) -> None:
    # A gpu job and a general job. A cpu-only worker may claim ONLY the general one.
    await h.seed(db, h.spec(task_name="train", runs_on=["gpu"]))
    await h.seed(db, h.spec(task_name="webhook", runs_on=[]))

    cpu_claim = await repo.claim(
        db, worker_tags=["cpu", "general"], exhausted_rate_classes=[], limit=10, claimed_by="cpu-w", per_group_cap=100
    )
    assert {j.task_name for j in cpu_claim} == {"webhook"}, "cpu worker must not get the gpu job"

    # The gpu job stays queued until a gpu-tagged worker asks.
    gpu_claim = await repo.claim(
        db, worker_tags=["gpu"], exhausted_rate_classes=[], limit=10, claimed_by="gpu-w", per_group_cap=100
    )
    assert {j.task_name for j in gpu_claim} == {"train"}


async def test_runs_on_requires_all_tags_covered(db: asyncpg.Connection) -> None:
    # runs_on is a SUBSET test: a job needing {gpu, cuda12} is NOT claimable by a
    # worker with only {gpu} — every required tag must be present.
    await h.seed(db, h.spec(task_name="cuda", runs_on=["gpu", "cuda12"]))

    partial = await repo.claim(
        db, worker_tags=["gpu"], exhausted_rate_classes=[], limit=10, claimed_by="w", per_group_cap=100
    )
    assert partial == [], "a partially-covering worker must not claim the job"

    full = await repo.claim(
        db, worker_tags=["gpu", "cuda12"], exhausted_rate_classes=[], limit=10, claimed_by="w", per_group_cap=100
    )
    assert {j.task_name for j in full} == {"cuda"}


# ── group ceilings ACROSS batches ─────────────────────────────────────────────


async def test_group_ceiling_cap1_is_exact_across_batches(db: asyncpg.Connection, pools: Pools) -> None:
    # cap=1 is the EXACT guarantee (matcher per-(group,task) serialization +
    # committed group_running counter). Three cust-1 jobs at cap=1: only ever ONE runs
    # at a time. This is the single-slot invariant that must never regress.
    for _ in range(3):
        await h.seed(db, h.spec(task_name="webhook", group_key="cust-1", cap=1))

    first = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=1
    )
    assert len(first) == 1, "cap=1 -> exactly one claimable"
    assert await h.group_running(db, "default", "cust-1", "webhook") == 1

    second = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w2", per_group_cap=1
    )
    assert second == [], "committed counter == cap -> the slot is taken"

    # Complete the running one; exactly one more becomes claimable (never two).
    svc = JobService(pools, load_config())
    await svc.complete(job_id=first[0].id, lease_token=first[0].lease_token, result=None)
    assert await h.group_running(db, "default", "cust-1", "webhook") == 0

    third = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w3", per_group_cap=1
    )
    assert len(third) == 1, "one freed slot -> exactly one more, never two"
    assert await h.group_running(db, "default", "cust-1", "webhook") == 1


async def test_group_ceiling_blocks_second_batch_at_cap(db: asyncpg.Connection) -> None:
    # The committed counter is the cross-batch guard: once group_running reaches the
    # cap, a fresh batch claims ZERO of that group (running < cap is false). This is
    # the counter-table property, independent of the in-batch $5 cap.
    for _ in range(4):
        await h.seed(db, h.spec(task_name="webhook", group_key="cust-9", cap=2))

    first = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w1", per_group_cap=2
    )
    assert len(first) == 2 and await h.group_running(db, "default", "cust-9", "webhook") == 2

    second = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=10, claimed_by="w2", per_group_cap=2
    )
    assert second == [], "counter at cap -> next batch is blocked from the group entirely"


# ── priority + fairness shaping ────────────────────────────────────────────────


async def test_priority_wins_claim_order(db: asyncpg.Connection) -> None:
    await h.seed(db, h.spec(task_name="low", priority=0))
    await h.seed(db, h.spec(task_name="high", priority=9))
    await h.seed(db, h.spec(task_name="mid", priority=5))

    claimed = await repo.claim(
        db, worker_tags=[], exhausted_rate_classes=[], limit=1, claimed_by="w", per_group_cap=100
    )
    assert [j.task_name for j in claimed] == ["high"], "highest priority claimed first"


async def test_gap3_fairness_under_monopoly(db: asyncpg.Connection) -> None:
    # regression pin: a giant group must not starve a tiny one at EQUAL priority.
    # Seed 5000 jobs in group "big" + 10 in group "small"; the window-function cap in
    # claim.sql ($5 = per-group ceiling per batch) must let every "small" job through
    # within a bounded number of batches instead of burying them behind the 5000.
    #
    #   big:   [######## ... 5000 ...] ─┐  equal priority
    #   small: [## 10 ##] ──────────────┘  MUST all claim within a few batches
    #
    # Scaled down from the spec's literal 5000 to 500 so the L2 suite stays fast; the
    # ratio (50:1) is what proves the window cap, not the absolute count.
    big, small = 500, 10
    for _ in range(big):
        await h.seed(db, h.spec(task_name="fan", group_key="big", priority=5))
    small_ids: set[str] = set()
    for _ in range(small):
        small_ids.add(await h.seed(db, h.spec(task_name="fan", group_key="small", priority=5)))

    # per_group_cap models the matcher's window cap = GREATEST(2, limit/8). With
    # limit=64 that is 8 per group per batch, so the 10 small jobs must all surface
    # within a handful of batches rather than after all 500 big ones.
    seen_small = 0
    batches = 0
    while batches < 20:
        batches += 1
        claimed = await repo.claim(
            db, worker_tags=[], exhausted_rate_classes=[], limit=64, claimed_by="w", per_group_cap=8
        )
        if not claimed:
            break
        seen_small += sum(1 for j in claimed if j.id in small_ids)
        if seen_small == small:
            break
        # No caps set -> group_running is a noop; the window cap alone shapes fairness.

    assert seen_small == small, f"small group starved: only {seen_small}/{small} seen"
    # Bound: with 8/group/batch, 10 small jobs need <=2 batches to fully appear. Allow
    # slack for interleaving but assert it is NOT proportional to the big group (500/64).
    assert batches <= 4, f"small group took {batches} batches — window cap not shaping"


# ── rate classes: PG-fallback bucket (degraded mode) ──────────────────────────


def _no_redis_limiter(pools: Pools) -> RateLimiter:
    """A limiter with Redis disabled -> every take/return hits the PG bucket."""
    return RateLimiter(RedisConfig(url=""), pools)


async def test_rate_bucket_grants_then_exhausts_then_refills(
    db: asyncpg.Connection, migrated_pool: asyncpg.Pool
) -> None:
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    rl = _no_redis_limiter(pools)
    # capacity 3, refill 0 (no time-based refill) so exhaustion is deterministic.
    await rl.upsert(name="email", capacity=3, refill_per_s=0.0)

    assert await rl.reserve("email", 2) == 2, "first take within capacity"
    assert await rl.reserve("email", 5) == 1, "only 1 token left -> grant 1, not 5"
    assert await rl.reserve("email", 1) == 0, "bucket empty -> exhausted (claim skips class)"

    # Return the 2 unused from the first over-reservation; bucket has tokens again.
    await rl.release("email", 2)
    assert await rl.reserve("email", 2) == 2, "returned tokens are re-grantable"


async def test_rate_unknown_class_is_unlimited(db: asyncpg.Connection, migrated_pool: asyncpg.Pool) -> None:
    rl = _no_redis_limiter(Pools(hot=migrated_pool, general=migrated_pool))
    # A class with no config row is unlimited by construction -> grants the full ask.
    assert await rl.reserve("no-such-class", 1000) == 1000


async def test_rate_drain_empties_bucket_engine_wide(db: asyncpg.Connection, migrated_pool: asyncpg.Pool) -> None:
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    rl = _no_redis_limiter(pools)
    await rl.upsert(name="sms", capacity=10, refill_per_s=0.0)
    assert await rl.reserve("sms", 5) == 5

    # 429 feedback: a worker reported a rate-limit failure -> empty the bucket at once.
    await rl.drain("sms")
    assert await rl.reserve("sms", 1) == 0, "drained bucket grants nothing until refill"
    assert await db.fetchval("SELECT tokens FROM rate_classes WHERE name='sms'") == 0


async def test_distinct_rate_classes_peek_respects_tags(db: asyncpg.Connection) -> None:
    # The matcher's step-2 peek only sees classes with READY work for the tags.
    await h.seed(db, h.spec(task_name="a", rate_class="email"))
    await h.seed(db, h.spec(task_name="b", rate_class="sms", runs_on=["gpu"]))

    seen_cpu = await repo.distinct_rate_classes(db, worker_tags=[])
    assert set(seen_cpu) == {"email"}, "the gpu-only sms job is invisible to an untagged worker"

    seen_gpu = await repo.distinct_rate_classes(db, worker_tags=["gpu"])
    assert set(seen_gpu) == {"email", "sms"}
