# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Checkpoint service.

A handler checkpoints expensive intermediate work (an LLM draft, a partial upload)
so that if the attempt is reclaimed and re-run on another worker it does not re-buy
that work. Redis is the fast path; Postgres is the durable system of record that is
preloaded into JobAssignment.checkpoint_json on the next claim (claim.sql LEFT JOIN).

    put(job, data)   ── Redis SET (best-effort) ──►  PG upsert (durable)
    get(job)         ── Redis GET (fast) ──miss──►   PG read (fallback)

    degraded mode: if Redis is down, put/get transparently use PG only. Redis
    is a latency optimization, NEVER a correctness dependency — a checkpoint
    is durable the moment the PG upsert commits. A Redis failure is logged ONCE per
    outage (log-once), not per call, mirroring the RateLimiter fallback policy.

Not lease-guarded on purpose: checkpointing is advisory. A stale worker writing a
checkpoint is harmless — it is only READ on the next claim, and a reclaimed job
re-runs its pre-checkpoint code regardless.
"""

from __future__ import annotations

import json
from typing import Any

from redis.exceptions import RedisError

from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability import metrics
from symba.observability.logging import logger
from symba.services.rate_limiter import RateLimiter

logger = logger.bind(service="checkpoint_service", context="engine/services")

_TTL_S = 72 * 3600  # matches checkpoints retention (checkpoints_hours=72)


def _key(tenant: str, job_id: str) -> str:
    return f"ckpt:{tenant}:{job_id}"


class CheckpointService:
    """Redis fast path + Postgres write-behind. Reuses the RateLimiter's
    Redis client so the whole engine shares one connection pool and one degraded-mode
    signal, rather than opening a second Redis pool for checkpoints."""

    def __init__(self, pools: Pools, rate_limiter: RateLimiter) -> None:
        self._pools = pools
        self._rate_limiter = rate_limiter
        self._redis_degraded = False

    async def put(self, *, job_id: str, tenant: str, data: dict[str, Any]) -> None:
        # Durable first: the checkpoint is only "saved" once PG commits (correctness).
        async with self._pools.hot.acquire() as conn:
            await repo.checkpoint_put(conn, job_id=job_id, tenant=tenant, data=data)
        await self._redis_set(tenant, job_id, data)
        metrics.checkpoints_total.labels(op="put").inc()
        logger.debug("[checkpoint] Stored", job_id=job_id, tenant=tenant)

    async def get(self, *, job_id: str, tenant: str) -> dict[str, Any] | None:
        cached = await self._redis_get(tenant, job_id)
        if cached is not None:
            metrics.checkpoints_total.labels(op="get_hit").inc()
            return cached
        async with self._pools.hot.acquire() as conn:
            data = await repo.checkpoint_get(conn, job_id=job_id, tenant=tenant)
        metrics.checkpoints_total.labels(op="get_miss" if data is None else "get_pg").inc()
        return data

    async def _redis_set(self, tenant: str, job_id: str, data: dict[str, Any]) -> None:
        client = self._rate_limiter.redis
        if client is None:
            return
        try:
            await client.set(_key(tenant, job_id), json.dumps(data), ex=_TTL_S)
            self._redis_degraded = False
        except RedisError:
            self._log_degraded_once()

    async def _redis_get(self, tenant: str, job_id: str) -> dict[str, Any] | None:
        client = self._rate_limiter.redis
        if client is None:
            return None
        try:
            raw = await client.get(_key(tenant, job_id))
            self._redis_degraded = False
        except RedisError:
            self._log_degraded_once()
            return None
        return json.loads(raw) if raw is not None else None

    def _log_degraded_once(self) -> None:
        if self._redis_degraded:
            return
        self._redis_degraded = True
        logger.warning("[checkpoint] Redis unavailable; using Postgres only (durable, higher latency)")
