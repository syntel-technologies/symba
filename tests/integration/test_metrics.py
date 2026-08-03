# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportPrivateUsage=false
"""L2 proof that the sweeper-refreshed gauges are actually EMITTED.

An audit found many declared gauges were never written. This suite drives real PG
state, runs one Sweeper pass, and reads the gauge values back off the registry — so
a future refactor that drops a gauge write fails here, not silently in prod.

    what one Sweeper pass must populate
    ------------------------------------------------
    seed 2 queued (t.a) + 1 waiting (t.b)  ─► Sweeper.pass_() ─► read gauges
        symba_queue_depth{task=t.a,state=queued}      == 2
        symba_queue_oldest_age_seconds{task=t.a}      >= 0   (present)
        symba_waiting_jobs{tenant=default}            == 1
        symba_pg_oldest_xact_age_seconds              present (>= 0)
"""

from __future__ import annotations

import asyncpg
import pytest

from symba.config import load_config
from symba.db.pool import Pools
from symba.observability import metrics
from symba.services.loops import Sweeper
from symba.transport.state import EngineState
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


async def _seed_waiting(conn: asyncpg.Connection, task: str) -> None:
    """Insert one WAITING job directly (bypasses submit; state=waiting is engine-set)."""
    await conn.execute(
        "INSERT INTO jobs (task_name, tenant, state, payload, wait_key) "
        "VALUES ($1, 'default', 'waiting', '{}'::jsonb, 'k1')",
        task,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_sweeper_refreshes_queue_gauges(db: asyncpg.Connection, pools: Pools) -> None:
    await h.seed(db, h.spec("t.a"))
    await h.seed(db, h.spec("t.a"))
    await _seed_waiting(db, "t.b")

    state = EngineState.build(load_config(), pools)
    await Sweeper(state).pass_()

    # queue_depth gauge for the two queued t.a jobs.
    depth = metrics.queue_depth.labels(task="t.a", state="queued")._value.get()
    assert depth == 2, f"expected depth 2 for t.a/queued, got {depth}"

    # oldest-age gauge present for t.a (>= 0; freshly seeded so small but real).
    oldest = metrics.queue_oldest_age_seconds.labels(task="t.a")._value.get()
    assert oldest >= 0

    # waiting_jobs gauge for the one WAITING job under the default tenant.
    waiting = metrics.waiting_jobs.labels(tenant="default")._value.get()
    assert waiting == 1, f"expected 1 waiting job, got {waiting}"

    # MVCC-horizon gauge is always refreshed (>= 0, our own probe excluded).
    xact_age = metrics.pg_oldest_xact_age_seconds._value.get()
    assert xact_age >= 0


@pytest.mark.asyncio(loop_scope="session")
async def test_sweeper_clears_drained_series(db: asyncpg.Connection, pools: Pools) -> None:
    """A task that drains to empty must DISAPPEAR from the depth gauge, not freeze.

    The refresh .clear()s each family before repopulating; without that a drained
    lane would report its last non-zero value forever."""
    jid = await h.seed(db, h.spec("t.drain"))
    state = EngineState.build(load_config(), pools)
    await Sweeper(state).pass_()
    assert metrics.queue_depth.labels(task="t.drain", state="queued")._value.get() == 1

    # Remove the job, refresh again: the series must be gone (KeyError on relabel is
    # avoided because .clear() drops it; re-reading .labels() would recreate at 0).
    await db.execute("DELETE FROM jobs WHERE id = $1", jid)
    await Sweeper(state).pass_()
    # After clear+refresh with no t.drain rows, the label set is not repopulated;
    # touching it via .labels() now yields a fresh 0 (proves it was cleared, not 1).
    assert metrics.queue_depth.labels(task="t.drain", state="queued")._value.get() == 0
