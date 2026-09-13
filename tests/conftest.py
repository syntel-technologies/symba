"""Shared test fixtures.

L1 tests need no fixtures (pure logic). L2+ tests get a real Postgres 18 via
testcontainers (or an externally provided SYMBA_POSTGRES__DSN in CI, where PG
runs as a service container). Migrations are applied once per session.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest_asyncio

# The repository-local .env is a deployment input, not a test fixture. Keep the
# default test engine unauthenticated and let auth-boundary tests opt into token
# or mTLS mode explicitly through their copied configuration.
os.environ["SYMBA_AUTH__MODE"] = "none"

from symba.db.migrate import apply_schema  # noqa: E402
from symba.db.pool import Pools, _init_connection  # noqa: E402


def _external_dsn() -> str | None:
    return os.environ.get("SYMBA_POSTGRES__DSN")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pg_dsn() -> AsyncIterator[str]:
    """Yield a DSN to a migrated Postgres 18.

    Uses the CI-provided service DB if SYMBA_POSTGRES__DSN is set, otherwise
    spins up a throwaway container via testcontainers.
    """
    external = _external_dsn()
    if external:
        yield external
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:18.1", username="symba", password="symba", dbname="symba") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        yield f"postgresql://symba:symba@{host}:{port}/symba"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def migrated_pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(
        dsn=pg_dsn,
        min_size=2,
        max_size=8,
        server_settings={"search_path": "symba"},
        init=_init_connection,
    )
    assert pool is not None
    await apply_schema(pool)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(loop_scope="session")
async def db(migrated_pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    """A clean connection with all live/archive tables truncated between tests."""
    async with migrated_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
            "group_running, checkpoints, signals, workers, rate_classes, cron_schedules "
            "RESTART IDENTITY CASCADE"
        )
        yield conn


@pytest_asyncio.fixture(loop_scope="session")
async def pools(migrated_pool: asyncpg.Pool) -> AsyncIterator[Pools]:
    """Services acquire their own connection from this Pools, backed by the shared
    migrated pool, so writes they commit are visible to the `db` fixture (one backend)."""
    yield Pools(hot=migrated_pool, general=migrated_pool)
