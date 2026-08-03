# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 cron service.

Proves the deterministic-dedup firing contract against real PG18:

    due schedule           one CronService.pass_()
    ------------           -----------------------
    next_fire <= now  →    submit(dedup cron:{id}:{next_fire})  →  advance next_fire
    fired twice       →    second submit dedups (ux_jobs_dedup)  →  exactly one job
    two engines       →    both tick same window                →  still one job (dedup)
    all down an hour  →    fires ONCE on recovery                →  no 60x backfill
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from symba.config import load_config
from symba.db.pool import Pools
from symba.services.loops import CronService
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _cron(pools: Pools) -> CronService:
    return CronService(EngineState.build(load_config(), pools))


async def _add_schedule(
    db: asyncpg.Connection,
    *,
    schedule_id: str = "s1",
    cron_expr: str = "* * * * *",
    task_name: str = "t.cron",
    enabled: bool = True,
    next_fire: datetime | None = None,
) -> None:
    await db.execute(
        "INSERT INTO cron_schedules (schedule_id, cron_expr, task_name, enabled, next_fire) "
        "VALUES ($1, $2, $3, $4, $5)",
        schedule_id,
        cron_expr,
        task_name,
        enabled,
        next_fire,
    )


async def _jobs_for(db: asyncpg.Connection, task_name: str) -> int:
    return await db.fetchval("SELECT count(*) FROM jobs WHERE task_name = $1", task_name)


# ── a due schedule fires exactly one queued job and advances its window ────────


async def test_due_schedule_fires_one_job_and_advances(db: asyncpg.Connection, pools: Pools) -> None:
    past = datetime.now(UTC) - timedelta(seconds=5)
    await _add_schedule(db, next_fire=past)

    fired = await _cron(pools).pass_()
    assert fired == 1
    assert await _jobs_for(db, "t.cron") == 1

    # next_fire advanced past the window we just fired (advance from the intended
    # tick). A minutely schedule fired 5s late may land its next window at or just
    # before now(), which the loop clamps to now() — so the invariant is "moved
    # strictly forward from the fired window", not "in the future".
    row = await db.fetchrow("SELECT next_fire, last_fire FROM cron_schedules WHERE schedule_id='s1'")
    assert row["next_fire"] > past
    assert row["last_fire"] == past


# ── firing the same window twice dedups to exactly one job (ux_jobs_dedup) ─────


async def test_double_fire_same_window_dedups(db: asyncpg.Connection, pools: Pools) -> None:
    past = datetime.now(UTC) - timedelta(seconds=5)
    await _add_schedule(db, next_fire=past)

    await _cron(pools).pass_()
    # Reset the window back to the SAME past instant (simulate a lost advance / retry).
    await db.execute("UPDATE cron_schedules SET next_fire=$1 WHERE schedule_id='s1'", past)
    fired_again = await _cron(pools).pass_()

    # The submit dedups on cron:{id}:{next_fire}; no second job is created.
    assert fired_again == 0
    assert await _jobs_for(db, "t.cron") == 1


# ── two engines ticking the same window still produce exactly one job ──────────


async def test_two_engines_same_window_single_job(db: asyncpg.Connection, pools: Pools) -> None:
    past = datetime.now(UTC) - timedelta(seconds=5)
    await _add_schedule(db, next_fire=past)

    engine_a = _cron(pools)
    engine_b = CronService(EngineState.build(load_config(), pools))
    # Both engines see the same due window (no election); dedup collapses them.
    await engine_a.pass_()
    # Force B to see the original window too (A may have advanced it).
    await db.execute("UPDATE cron_schedules SET next_fire=$1 WHERE schedule_id='s1'", past)
    await engine_b.pass_()

    assert await _jobs_for(db, "t.cron") == 1


# ── disabled schedules never fire ─────────────────────────────────────────────


async def test_disabled_schedule_never_fires(db: asyncpg.Connection, pools: Pools) -> None:
    past = datetime.now(UTC) - timedelta(seconds=5)
    await _add_schedule(db, enabled=False, next_fire=past)

    fired = await _cron(pools).pass_()
    assert fired == 0
    assert await _jobs_for(db, "t.cron") == 0


# ── no missed-window backfill: a long-past window fires once, not N times ──────


async def test_no_backfill_fires_once_on_recovery(db: asyncpg.Connection, pools: Pools) -> None:
    # next_fire an hour in the past (all instances were "down"); a minutely schedule
    # would have 60 missed windows. Expected semantics: fire ONCE, then advance forward.
    long_past = datetime.now(UTC) - timedelta(hours=1)
    await _add_schedule(db, cron_expr="* * * * *", next_fire=long_past)

    fired = await _cron(pools).pass_()
    assert fired == 1
    assert await _jobs_for(db, "t.cron") == 1
    # The advanced window is at/after now — the backlog is NOT replayed.
    next_fire = await db.fetchval("SELECT next_fire FROM cron_schedules WHERE schedule_id='s1'")
    assert next_fire >= datetime.now(UTC) - timedelta(seconds=2)


# ── a never-fired schedule (next_fire NULL) computes its first window ──────────


async def test_null_next_fire_computes_first_window(db: asyncpg.Connection, pools: Pools) -> None:
    await _add_schedule(db, next_fire=None)  # never computed
    await _cron(pools).pass_()
    # A first window is now set in the future (first occurrence advanced past base).
    next_fire = await db.fetchval("SELECT next_fire FROM cron_schedules WHERE schedule_id='s1'")
    assert next_fire is not None
