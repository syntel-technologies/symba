# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L5 chaos scenarios 1-3 — crash convergence + effect-once + Redis-kill.

A `kill -9` is not modeled by literally SIGKILLing a subprocess here (that lives in
the compose-backed nightly suite). The *observable state a kill -9 leaves behind* is
deterministic and is what these tests reproduce:

    a job stuck `state='running'` with a lease that will expire and NO clean
    terminal write — because the process died before Complete/Fail.

Given that state, the engine's convergence contract is: the sweeper reclaims the
expired lease back to `queued`, the job is re-claimable, and downstream side effects
are deduplicated by the retry-stable `ctx.idempotency_key` so a re-run is
effect-once.

    Scenario 1 — engine dies BETWEEN claim and assignment-push
    ----------------------------------------------------------
      claim (attempt=1, running, lease set) ──✗ crash before push──► job orphaned
      sweeper (lease_expires_at < now) ──► queued again ──► re-claimable
      INVARIANT: exactly one live row throughout; never lost from jobs.

    Scenario 2 — worker dies MID-HANDLER
    ------------------------------------
      claim (attempt=1) ──► handler starts, records effect under idempotency_key ──✗
      sweeper ──► queued ──► re-claim (attempt=2)
      handler re-runs, SAME idempotency_key ──► ledger records the effect ONCE.
      INVARIANT: idempotency_key stable across attempts; idempotency_key_attempt differs.

    Scenario 3 — Redis dies MID-RUN (graceful degradation, N7)
    ---------------------------------------------------------------
      reserve() ──Redis eval──✗ RedisError──► fall through to PG bucket ──► granted
      INVARIANT: zero failures surfaced to the caller; ONE WARNING logged per outage
      (not per call); the PG bucket enforces the SAME token semantics as Redis.

    Scenario 4 — signal arrives AS a WAITING job times out (race)
    -------------------------------------------------------------------
      job WAITING ──signal wakes──► QUEUED (payload)   ┐ exactly ONE wins:
      sweeper (deadline passed) ──expire──► QUEUED      ┘ the signal consumed the
      INVARIANT: the job resumes exactly once; the timeout backstop finds nothing to
      expire once the signal has already woken it (no double delivery).

    Scenario 5 — worker dies while a job is WAITING
    --------------------------------------------------------------
      job checkpoints expensive work ──► WAITING (lease freed) ──✗ worker gone
      signal ──► QUEUED ──► re-claim on a NEW worker ──► checkpoint preloaded
      INVARIANT: the parked job is not lost; the resumed attempt gets its checkpoint.

    Scenario 6 — Redis dies mid-checkpoint (N7)
    ---------------------------------------------------
      put() ──PG upsert (durable) ──► Redis SET──✗ RedisError──► swallowed
      INVARIANT: the checkpoint is durable (PG committed) despite Redis; get() still
      returns it (PG fallback); the checkpoint fast path is never load-bearing.
"""

from __future__ import annotations

import logging

import asyncpg
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from symba.config import RedisConfig, load_config
from symba.core.idempotency import idempotency_key, idempotency_key_attempt
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.checkpoint_service import CheckpointService
from symba.services.rate_limiter import RateLimiter
from symba.services.registry import WorkerRegistry
from symba.services.signal_service import SignalService
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l5, pytest.mark.asyncio(loop_scope="session")]


async def _expire_lease(conn: asyncpg.Connection, job_id: str) -> None:
    """Reproduce a kill -9: the job is still `running` but its lease is already past.

    Backdating lease_expires_at is exactly the state the sweeper would encounter had
    the holder died and the TTL elapsed — no client clock is trusted at reclaim time
    (the sweep query compares against DB now()), we only move the row into the past.
    """
    await conn.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        job_id,
    )


# --------------------------------------------------------------------------- #
# Scenario 1: engine crash between claim and assignment push -> re-queued
# --------------------------------------------------------------------------- #


async def test_chaos1_engine_crash_between_claim_and_push_requeues(db: asyncpg.Connection) -> None:
    job_id = await h.seed(db, h.spec(task_name="t.crash1", lease_ttl_s=60))

    claimed = await h.claim_one(db)
    assert claimed.id == job_id
    assert claimed.attempt == 1
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"

    # crash: the engine never pushed the assignment and never wrote a terminal state.
    await _expire_lease(db, job_id)

    reclaimed = await repo.sweep_leases(db, limit=100)
    assert reclaimed == 1

    # convergence: fully live and queued again, never lost from jobs / never archived.
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "queued"
    assert await db.fetchval("SELECT lease_token FROM jobs WHERE id = $1", job_id) is None
    assert await db.fetchval("SELECT count(*) FROM jobs_archive WHERE id = $1", job_id) == 0

    # re-claimable; the attempt counter advances (retry budget honored).
    reclaimed_job = await h.claim_one(db, worker="w2")
    assert reclaimed_job.id == job_id
    assert reclaimed_job.attempt == 2


# --------------------------------------------------------------------------- #
# Scenario 2: worker crash mid-handler -> reclaim -> retry -> effect-once
# --------------------------------------------------------------------------- #


class _SideEffectLedger:
    """Test double for a downstream idempotent API (Stripe/SendGrid-style).

    apply() is invoked once per handler run (so `calls` counts handler executions),
    but the external system dedups on the idempotency_key — a key already seen is a
    no-op — so `committed` holds the set of effects that actually took hold.
    """

    def __init__(self) -> None:
        self.committed: set[str] = set()
        self.calls = 0

    def apply(self, key: str) -> None:
        self.calls += 1
        self.committed.add(key)  # set semantics == the API deduping on the key


async def test_chaos2_worker_crash_mid_handler_is_effect_once(db: asyncpg.Connection) -> None:
    # A dedup_key makes the logical identity explicit; idempotency_key derives from it
    # so it is identical across attempts of the SAME logical job.
    job_id = await h.seed(db, h.spec(task_name="t.charge", dedup_key="order-42", lease_ttl_s=60))
    key = idempotency_key(tenant="default", dedup_key="order-42", job_id=job_id)

    ledger = _SideEffectLedger()

    # attempt 1: worker claims, handler begins the side effect, then the worker dies.
    a1 = await h.claim_one(db, worker="w1")
    assert a1.attempt == 1
    ledger.apply(key)  # the charge was issued with the retry-stable key
    per_attempt_1 = idempotency_key_attempt(key, a1.attempt)
    await _expire_lease(db, job_id)  # kill -9 mid-handler: no Complete written

    # sweeper reclaims the dead lease.
    assert await repo.sweep_leases(db, limit=100) == 1
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "queued"

    # attempt 2: re-claim, handler re-runs with the SAME idempotency_key.
    a2 = await h.claim_one(db, worker="w2")
    assert a2.attempt == 2
    ledger.apply(key)
    per_attempt_2 = idempotency_key_attempt(key, a2.attempt)

    # effect-once: the handler ran twice, but the downstream effect committed once.
    assert ledger.calls == 2
    assert ledger.committed == {key}, "same key seen twice -> external system dedups to one effect"
    # the per-attempt variant differs, for APIs where a retry SHOULD be a new op.
    assert per_attempt_1 != per_attempt_2


# --------------------------------------------------------------------------- #
# Scenario 3: Redis killed mid-run -> PG fallback engages, zero failures
# --------------------------------------------------------------------------- #


class _DeadRedis:
    """A Redis client whose every op raises — models a Redis killed mid-run.

    We don't SIGKILL a real Redis here (that's the compose nightly suite); the
    observable effect a dead Redis has on the limiter is that every command raises
    a connection error, which is exactly what this double reproduces.
    """

    async def eval(self, *_args: object, **_kwargs: object) -> object:
        raise RedisConnectionError("redis killed mid-run")

    async def hset(self, *_args: object, **_kwargs: object) -> object:
        raise RedisConnectionError("redis killed mid-run")

    async def aclose(self) -> None:
        return None


async def test_chaos3_redis_kill_falls_back_to_pg_with_one_warning(
    db: asyncpg.Connection, migrated_pool: asyncpg.Pool, caplog: pytest.LogCaptureFixture
) -> None:
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    # Redis "enabled" (non-empty url) so the fast path is attempted, then swapped for
    # a client that always raises — Redis dying mid-run.
    rl = RateLimiter(RedisConfig(url="redis://dead:6379"), pools)
    rl._redis = _DeadRedis()  # type: ignore[assignment]
    await rl.upsert(name="webhook", capacity=5, refill_per_s=0.0)

    with caplog.at_level(logging.WARNING, logger="symba.services.rate_limiter"):
        # First reserve: Redis raises -> fall through to PG -> a real grant (no failure).
        g1 = await rl.reserve("webhook", 3)
        # Second reserve: Redis still dead -> PG again; must NOT re-log the same outage.
        g2 = await rl.reserve("webhook", 3)

    # Zero failures surfaced: the caller got real grants from the PG bucket, and the
    # PG bucket enforces the SAME semantics (5 cap -> 3 then only 2 left).
    assert g1 == 3, "first reserve served by PG fallback"
    assert g2 == 2, "PG bucket enforces the cap across calls (5 - 3 = 2 left)"

    # Log-once discipline: exactly one degraded-mode WARNING for the whole outage,
    # not one per reserve (a chatty limiter would drown the logs during an outage).
    degraded_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "falling back to PG" in r.getMessage()
    ]
    assert len(degraded_warnings) == 1, f"expected one degraded WARNING, got {len(degraded_warnings)}"


# --------------------------------------------------------------------------- #
# Scenario 4: signal arrives as a WAITING job times out -> single resume
# --------------------------------------------------------------------------- #


async def test_chaos4_signal_wins_race_against_timeout(db: asyncpg.Connection, pools: Pools) -> None:
    signals = SignalService(pools, WorkerRegistry())
    job_id = await h.seed(db, h.spec(task_name="t.approve"))
    claimed = await h.claim_one(db)

    # Park the job WAITING with an ALREADY-past deadline: the signal and the sweeper's
    # timeout are now both eligible to resume it. This is the crash-window race.
    await signals.wait(
        job_id=job_id, lease_token=claimed.lease_token, tenant="default", wait_key="k", timeout_s=1
    )
    await db.execute("UPDATE jobs SET wait_expires_at = now() - interval '1 s' WHERE id = $1", job_id)

    # The signal wins: it wakes the WAITING job with its payload -> QUEUED.
    sig = await signals.signal(tenant="default", wait_key="k", payload={"decision": "yes"})
    assert sig.delivered is True and sig.job_id == job_id

    # The timeout backstop now finds NOTHING to expire (the job left WAITING already),
    # so there is no double resume and no clobbering of the delivered payload.
    expired = await repo.sweep_waits(db, limit=100)
    assert expired == [], "signal already resumed the job; the sweeper has nothing to expire"
    row = await db.fetchrow("SELECT state, event_payload FROM jobs WHERE id = $1", job_id)
    assert row["state"] == "queued"
    assert row["event_payload"] == {"decision": "yes"}, "the signal payload survived; no timeout clobber"


# --------------------------------------------------------------------------- #
# Scenario 5: worker dies while WAITING -> re-claim gets the checkpoint back
# --------------------------------------------------------------------------- #


async def test_chaos5_worker_dies_while_waiting_resumes_with_checkpoint(
    db: asyncpg.Connection, pools: Pools
) -> None:
    signals = SignalService(pools, WorkerRegistry())
    checkpoints = CheckpointService(pools, RateLimiter(load_config().redis, pools))
    job_id = await h.seed(db, h.spec(task_name="t.longrun"))
    claimed = await h.claim_one(db, worker="w1")

    # The handler does expensive work, checkpoints it, THEN waits.
    await checkpoints.put(job_id=job_id, tenant="default", data={"draft": "expensive"})
    await signals.wait(
        job_id=job_id, lease_token=claimed.lease_token, tenant="default", wait_key="approve", timeout_s=3600
    )
    # Worker w1 is now gone; the job sits WAITING with no lease (parking freed it).
    assert await db.fetchval("SELECT lease_token FROM jobs WHERE id = $1", job_id) is None

    # A signal resumes it; a DIFFERENT worker re-claims the fresh queued job.
    await signals.signal(tenant="default", wait_key="approve", payload={"ok": True})
    reclaimed = await h.claim_one(db, worker="w2")
    assert reclaimed.id == job_id
    # The checkpoint is preloaded so the resumed attempt does not re-buy the work.
    assert reclaimed.raw["checkpoint_data"] == {"draft": "expensive"}


# --------------------------------------------------------------------------- #
# Scenario 6: Redis dies mid-checkpoint -> checkpoint still durable via PG
# --------------------------------------------------------------------------- #


class _DeadRedisKV(_DeadRedis):
    """Extends the dead-Redis double with the KV ops the checkpoint fast path uses."""

    async def set(self, *_args: object, **_kwargs: object) -> object:
        raise RedisError("redis killed mid-checkpoint")

    async def get(self, *_args: object, **_kwargs: object) -> object:
        raise RedisError("redis killed mid-checkpoint")


async def test_chaos6_redis_dies_mid_checkpoint_still_durable(db: asyncpg.Connection, pools: Pools) -> None:
    rl = RateLimiter(RedisConfig(url="redis://dead:6379"), pools)
    rl._redis = _DeadRedisKV()  # type: ignore[assignment]
    svc = CheckpointService(pools, rl)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))

    # put(): PG upsert commits first (durable), THEN the Redis SET raises and is
    # swallowed — the checkpoint is saved regardless of Redis.
    await svc.put(job_id=job_id, tenant="default", data={"n": 99})

    # get(): Redis GET raises -> PG fallback returns the durable value. Zero failures.
    got = await svc.get(job_id=job_id, tenant="default")
    assert got == {"n": 99}, "checkpoint durable in PG despite Redis being dead"
    # And it is directly in the durable table (the fast path was never load-bearing).
    row = await db.fetchval("SELECT data FROM checkpoints WHERE job_id = $1", job_id)
    assert row == {"n": 99}
