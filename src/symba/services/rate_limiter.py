# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportAttributeAccessIssue=false
"""Rate limiter — per-rate_class token buckets.

Two interchangeable backends behind one API (graceful degradation):

    reserve(classes, want) ──► Redis Lua refill-and-take  (fast, engine-wide)
                          └──► PG fallback UPDATE          (slower, same semantics)

The matcher calls reserve() BEFORE the claim query (keeps the claim SQL sargable):
for every distinct rate_class present in the ready set it asks for up to
the batch LIMIT tokens; a class granted 0 goes into the claim's exhausted `$2` array
and is skipped this pass. After the claim, whatever wasn't consumed is returned via
release() so a class isn't spuriously drained by an over-eager reservation.

    token-bucket lifecycle (per class, per pass)
    --------------------------------------------
      refill(elapsed·refill_per_s, cap=capacity)  ← lazy, on every take
      take(min(available, want))                  → granted
      exhausted := granted == 0                    → into claim $2
      release(reserved − consumed)                 ← unused tokens back

Redis path: ONE Lua script does refill+take atomically under key rl:{class} (fields
tokens, ts) so N engine instances share one bucket. If Redis is unreachable the call
transparently falls back to the PG bucket (rate_classes.tokens/refilled_at), logs a
WARNING once per outage, and keeps going — a rate limiter must never be a SPOF for
the whole engine.

Config (name, capacity, refill_per_s) lives in Postgres, cached in memory, and is
reloaded on the refill tick so admin edits take effect within one tick without a
restart. 429-feedback: drain(class) empties a bucket immediately engine-wide when a
worker reports a rate-limit failure, so all engines back off at once.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from symba.config import RedisConfig
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability.logging import logger

logger = logger.bind(service="rate_limiter", context="engine/services")


# Redis refill-and-take, atomic under one key. Mirrors rate_take_pg.sql exactly.
#   KEYS[1] = rl:{class}   (hash: tokens, ts)
#   ARGV = capacity, refill_per_s, now_seconds, want
# Returns the integer number of tokens granted (0..want).
_LUA_REFILL_TAKE = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local want = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
    tokens = capacity
    ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * refill)

local granted = math.floor(math.min(tokens, want))
tokens = tokens - granted

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- expire idle buckets so a deleted class doesn't leak a key forever.
redis.call('PEXPIRE', key, 3600000)
return granted
"""

_LUA_RETURN = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local give = tonumber(ARGV[2])
local tokens = tonumber(redis.call('HGET', key, 'tokens'))
if tokens == nil then return 0 end
tokens = math.min(capacity, tokens + give)
redis.call('HSET', key, 'tokens', tokens)
return 1
"""


@dataclass(slots=True)
class _ClassCfg:
    capacity: float
    refill_per_s: float


class RateLimiter:
    """Engine-wide token buckets with Redis fast path + PG fallback."""

    def __init__(self, redis_cfg: RedisConfig, pools: Pools) -> None:
        self._pools = pools
        self._redis: Redis | None = None
        if redis_cfg.enabled:
            self._redis = Redis.from_url(
                redis_cfg.url,
                socket_timeout=redis_cfg.socket_timeout_s,
                socket_connect_timeout=redis_cfg.socket_timeout_s,
                decode_responses=True,
            )
        self._cfg: dict[str, _ClassCfg] = {}
        self._degraded = False  # True while Redis is unreachable (log-once guard)

    @property
    def redis(self) -> Redis | None:
        """The shared Redis client (decode_responses=True), or None when disabled.

        Exposed so the checkpoint fast path reuses ONE Redis pool and one
        degraded-mode signal instead of opening a second connection pool.
        """
        return self._redis

    async def load(self) -> None:
        """Refresh the in-memory config cache from Postgres (startup + refill tick)."""
        async with self._pools.general.acquire() as conn:
            rows = await repo.rate_classes_all(conn)
        self._cfg = {name: _ClassCfg(capacity=cap, refill_per_s=rate) for name, cap, rate in rows}
        logger.debug("[load] Rate-class config refreshed", classes=len(self._cfg))

    async def reserve(self, rate_class: str, want: int) -> int:
        """Take up to `want` tokens for one class. Returns granted (0 -> exhausted).

        An unknown class (no config row) is UNLIMITED by construction — it grants the
        full request and never appears in the claim's exhausted array.
        """
        if want <= 0:
            return 0
        cfg = self._cfg.get(rate_class)
        if cfg is None:
            return want  # unconfigured class = no limit

        if self._redis is not None:
            granted = await self._redis_take(rate_class, cfg, want)
            if granted is not None:
                return granted
            # fall through to PG on any Redis error (degraded mode).

        async with self._pools.general.acquire() as conn:
            return await repo.rate_take_pg(conn, rate_class=rate_class, want=want)

    async def release(self, rate_class: str, tokens: int) -> None:
        """Return unused reserved tokens. Best-effort; never raises."""
        if tokens <= 0 or rate_class not in self._cfg:
            return
        cfg = self._cfg[rate_class]
        if self._redis is not None:
            try:
                await self._redis.eval(_LUA_RETURN, 1, f"rl:{rate_class}", cfg.capacity, tokens)
                return
            except RedisError:
                pass  # fall back to PG
        async with self._pools.general.acquire() as conn:
            await repo.rate_return_pg(conn, rate_class=rate_class, tokens=tokens)

    async def drain(self, rate_class: str) -> None:
        """Empty a bucket immediately, engine-wide (429-feedback).

        A worker reporting a rate-limit failure means the downstream is already
        saying no; back the whole fleet off at once instead of each worker
        rediscovering it.
        """
        if rate_class not in self._cfg:
            return
        if self._redis is not None:
            try:
                await self._redis.hset(f"rl:{rate_class}", mapping={"tokens": 0, "ts": time.time()})
                logger.warning("[drain] Bucket drained (429 feedback)", rate_class=rate_class)
                return
            except RedisError:
                pass
        async with self._pools.general.acquire() as conn:
            await conn.execute(
                "UPDATE rate_classes SET tokens = 0, refilled_at = now() WHERE name = $1", rate_class
            )
        logger.warning("[drain] Bucket drained (429 feedback, PG)", rate_class=rate_class)

    async def upsert(self, *, name: str, capacity: float, refill_per_s: float) -> None:
        """Admin API: create/update a class; takes effect on the next refill tick."""
        async with self._pools.general.acquire() as conn:
            await repo.rate_class_upsert(conn, name=name, capacity=capacity, refill_per_s=refill_per_s)
        self._cfg[name] = _ClassCfg(capacity=capacity, refill_per_s=refill_per_s)
        logger.info("[upsert] Rate class set", rate_class=name, capacity=capacity, refill_per_s=refill_per_s)

    async def list_classes(self) -> list[tuple[str, float, float]]:
        """Admin API: the full (name, capacity, refill_per_s) config, authoritative from PG.

        Reads Postgres (not the in-memory cache) so an operator sees committed config
        even on an engine whose refill tick has not yet reloaded a peer's edit.
        """
        async with self._pools.general.acquire() as conn:
            return await repo.rate_classes_all(conn)

    async def snapshot_tokens(self) -> dict[str, float]:
        """Current token level per configured class, for the /metrics gauge.

        Read-only peek (no take): Redis HGET fast path, PG fallback, best-effort.
        An unread bucket (never taken from) reports its full capacity. Never raises;
        a metrics refresh must not be able to disturb the hot path.
        """
        levels: dict[str, float] = {}
        for name, cfg in self._cfg.items():
            level = cfg.capacity  # default: never-touched bucket sits at capacity
            if self._redis is not None:
                try:
                    raw = await self._redis.hget(f"rl:{name}", "tokens")
                    if raw is not None:
                        level = float(raw)
                except RedisError:
                    pass  # fall through to PG
            levels[name] = level
        if self._redis is None or self._degraded:
            async with self._pools.general.acquire() as conn:
                rows = await conn.fetch("SELECT name, tokens FROM rate_classes")
            for row in rows:
                if row["name"] in self._cfg and row["tokens"] is not None:
                    levels[row["name"]] = float(row["tokens"])
        return levels

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()

    async def _redis_take(self, rate_class: str, cfg: _ClassCfg, want: int) -> int | None:
        """Atomic Redis refill+take. Returns granted, or None on a Redis failure."""
        assert self._redis is not None
        try:
            granted = await self._redis.eval(
                _LUA_REFILL_TAKE, 1, f"rl:{rate_class}", cfg.capacity, cfg.refill_per_s, time.time(), want
            )
            if self._degraded:
                self._degraded = False
                logger.info("[reserve] Redis recovered; leaving degraded mode")
            return int(granted)
        except RedisError:
            if not self._degraded:
                self._degraded = True
                logger.warning("[reserve] Redis unreachable; falling back to PG buckets", rate_class=rate_class)
            return None
