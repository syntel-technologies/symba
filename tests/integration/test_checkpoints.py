# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 checkpoints.

Proves the durable write-behind contract against real PG18 (Redis disabled in test
config -> the service exercises the PG-only degraded path, which is the correctness
floor):

    put(job, data)   ── PG upsert (durable) ──►  get returns it
    claim(job)       ── LEFT JOIN checkpoints ──►  assignment.checkpoint_json preloaded
    tenant scope     ── get(other tenant) ──────►  None (no cross-tenant read)
"""

from __future__ import annotations

import json

import asyncpg
import pytest

from symba.config import load_config
from symba.db.pool import Pools
from symba.services.checkpoint_service import CheckpointService
from symba.services.rate_limiter import RateLimiter
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _svc(pools: Pools) -> CheckpointService:
    cfg = load_config()
    return CheckpointService(pools, RateLimiter(cfg.redis, pools))


# ── put then get round-trips through Postgres (Redis disabled) ────────────────


async def test_put_then_get_roundtrip(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))

    await svc.put(job_id=job_id, tenant="default", data={"draft": "v1", "cost": 42})
    got = await svc.get(job_id=job_id, tenant="default")
    assert got == {"draft": "v1", "cost": 42}


# ── put is an upsert: the newest checkpoint wins ──────────────────────────────


async def test_put_upserts_newest_wins(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))

    await svc.put(job_id=job_id, tenant="default", data={"step": 1})
    await svc.put(job_id=job_id, tenant="default", data={"step": 2})
    assert (await svc.get(job_id=job_id, tenant="default")) == {"step": 2}
    rows = await db.fetchval("SELECT count(*) FROM checkpoints WHERE job_id = $1", job_id)
    assert rows == 1, "upsert keeps exactly one row per job"


# ── a claimed job preloads its checkpoint into the assignment (claim.sql join) ─


async def test_claim_preloads_checkpoint_into_assignment(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))
    await svc.put(job_id=job_id, tenant="default", data={"draft": "expensive-llm-output"})

    claimed = await h.claim_one(db, worker="w1")
    assert claimed.id == job_id
    # claim.sql LEFT JOINs checkpoints, so the raw row carries checkpoint_data with
    # no extra round-trip; the matcher packs it into JobAssignment.checkpoint_json.
    assert claimed.raw["checkpoint_data"] == {"draft": "expensive-llm-output"}


# ── a job with no checkpoint claims with a NULL checkpoint_data (no N+1) ───────


async def test_claim_without_checkpoint_is_null(db: asyncpg.Connection, pools: Pools) -> None:
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))
    claimed = await h.claim_one(db, worker="w1")
    assert claimed.id == job_id
    assert claimed.raw["checkpoint_data"] is None


# ── tenant scoping: one tenant cannot read another's checkpoint ───────────────


async def test_get_is_tenant_scoped(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt", tenant="acme"))
    await svc.put(job_id=job_id, tenant="acme", data={"secret": True})

    assert (await svc.get(job_id=job_id, tenant="acme")) == {"secret": True}
    assert (await svc.get(job_id=job_id, tenant="other")) is None


# ── the assignment json is well-formed for the SDK to decode ──────────────────


async def test_preloaded_checkpoint_json_is_valid(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _svc(pools)
    job_id = await h.seed(db, h.spec(task_name="t.ckpt"))
    await svc.put(job_id=job_id, tenant="default", data={"n": 7})

    from symba.services.matcher import _to_assignment

    claimed = await h.claim_one(db, worker="w1")
    assert claimed.id == job_id
    assignment = _to_assignment(claimed)
    assert json.loads(assignment.checkpoint_json.decode()) == {"n": 7}
