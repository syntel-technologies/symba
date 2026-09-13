# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnnecessaryComparison=false, reportDeprecated=false, reportReturnType=false
"""Two asyncpg pools with distinct command timeouts.

hot     -> claim, complete, fail, heartbeat, get_result (5s timeout: a slow
           statement here is a bug, fail fast).
general -> submit, query, UI, sweeper, cron, signals (30s timeout).

No LISTEN connection exists anywhere: dispatch is polling-only.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import asyncpg

from symba.config import PostgresConfig
from symba.observability import metrics
from symba.observability.logging import logger

logger = logger.bind(service="pool", context="engine/db")


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register JSON/JSONB codecs so dicts round-trip without manual dumps/loads.

    asyncpg treats json/jsonb as text by default; every hot-path query binds
    dict payloads/results, so we set the codec once per pooled connection.
    """
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename,
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )


@dataclass(slots=True)
class Pools:
    hot: asyncpg.Pool
    general: asyncpg.Pool

    @asynccontextmanager
    async def acquire_hot(self) -> AsyncGenerator[asyncpg.Connection]:
        """Timed hot-pool acquire feeding symba_pool_acquire_wait_seconds{pool=hot}.

        Pool-acquire wait is the leading indicator of hot-pool saturation: when it
        climbs, the claim path is queueing on connections, not on Postgres. Timing
        the acquire (not the whole `async with`) isolates the wait from the query.
        """
        start = time.perf_counter()
        async with self.hot.acquire() as conn:
            metrics.pool_acquire_wait_seconds.labels(pool="hot").observe(time.perf_counter() - start)
            yield conn

    async def close(self) -> None:
        await self.hot.close()
        await self.general.close()


async def create_pools(cfg: PostgresConfig) -> Pools:
    logger.info(
        "[pool] Creating connection pools",
        hot_size=cfg.hot_pool_size,
        general_size=cfg.general_pool_size,
    )
    # All engine objects live in the `symba` schema (Flyway-managed); pin the
    # search_path on every pooled connection so unqualified SQL resolves there.
    server_settings = {"search_path": cfg.schema_name}
    hot = await asyncpg.create_pool(
        dsn=cfg.resolved_dsn,
        min_size=2,
        max_size=cfg.hot_pool_size,
        command_timeout=cfg.hot_command_timeout_s,
        server_settings=server_settings,
        init=_init_connection,
    )
    general = await asyncpg.create_pool(
        dsn=cfg.resolved_dsn,
        min_size=2,
        max_size=cfg.general_pool_size,
        command_timeout=cfg.general_command_timeout_s,
        server_settings=server_settings,
        init=_init_connection,
    )
    if hot is None or general is None:  # pragma: no cover - defensive
        raise RuntimeError("failed to create asyncpg pools")
    return Pools(hot=hot, general=general)
