# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Test-only schema loader.

PRODUCTION MIGRATIONS ARE RUN BY FLYWAY (see flyway.toml + the `flyway` service
in docker-compose.yml). The engine container depends on
`flyway: service_completed_successfully` and does NOT self-migrate.

This helper exists solely so L2/L3 integration tests and the EXPLAIN gate can
apply the same `database/symba/V*.sql` files to a throwaway testcontainers
database without standing up a Flyway sidecar. It applies the versioned SQL in
filename order inside one transaction; it is NOT a migration framework and keeps
no version table.
"""

from __future__ import annotations

import re
from pathlib import Path

import asyncpg

from symba.observability.logging import logger

logger = logger.bind(service="schema_loader", context="engine/db")

_VERSION_RE = re.compile(r"^V(\d+)__")


def _database_dir() -> Path:
    """Locate the database/symba directory (ships in the repo, not the image)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "database" / "symba"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("database/symba directory not found")


def discover_versioned() -> list[tuple[int, str, str]]:
    """Return sorted (version, name, sql) for every V###__*.sql file."""
    out: list[tuple[int, str, str]] = []
    for path in sorted(_database_dir().glob("V*.sql")):
        match = _VERSION_RE.match(path.name)
        if not match:
            continue
        out.append((int(match.group(1)), path.name, path.read_text()))
    out.sort(key=lambda row: row[0])
    return out


async def apply_schema(pool: asyncpg.Pool, schema: str = "symba") -> int:
    """Apply all versioned SQL to a fresh test database. Returns count applied.

    Mirrors what Flyway does in production: creates the `symba` schema and runs
    every migration with search_path set to it, so objects land in the same place
    tests and prod expect.
    """
    migrations = discover_versioned()
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}"')
        for version, name, sql in migrations:
            logger.debug("[apply_schema] Applying", version=version, name=name)
            await conn.execute(sql)
    logger.info("[apply_schema] Test schema applied", count=len(migrations), schema=schema)
    return len(migrations)
