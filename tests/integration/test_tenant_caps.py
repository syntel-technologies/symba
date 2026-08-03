# pyright: reportPrivateUsage=false
"""L2 proof of the per-tenant queue cap.

    the gate
    --------------------
    tenant_queued_cap = 0  -> unlimited (the default, no COUNT round trip)
    tenant_queued_cap = N  -> submit that would push a tenant's LIVE count past N is
                              rejected with TenantQuotaExceeded (429 / RESOURCE_EXHAUSTED),
                              retryable-by-contract, carrying a Retry-After hint.

    checked per DISTINCT tenant against the WHOLE batch's contribution, inside the
    submit tx, so an all-or-nothing batch cannot straddle the cap and one tenant's
    load never counts against another's.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from symba.config import load_config
from symba.core.errors import TenantQuotaExceeded
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.services.registry import WorkerRegistry
from symba.services.submit_service import SubmitService
from symba.transport.http_server import build_app
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _capped_service(pools: Pools, cap: int) -> SubmitService:
    config = load_config()
    config.limits.tenant_queued_cap = cap
    return SubmitService(pools, WorkerRegistry(), config)


@pytest_asyncio.fixture(loop_scope="session")
async def capped_engine(migrated_pool: asyncpg.Pool) -> AsyncIterator[EngineState]:
    """An EngineState whose SubmitService enforces a cap of 2, over the shared pool."""
    async with migrated_pool.acquire() as conn:
        await conn.execute("TRUNCATE jobs, jobs_archive, job_events RESTART IDENTITY CASCADE")
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    state = EngineState.build(load_config(), pools)
    state.submit = _capped_service(pools, cap=2)
    state.serving = True
    yield state


async def test_cap_zero_is_unlimited(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _capped_service(pools, cap=0)
    outcome = await svc.submit([SubmitSpec(task_name="t.echo", payload={"i": i}) for i in range(5)])
    assert len([j for j in outcome.job_ids if j]) == 5


async def test_cap_rejects_over_limit_single_submits(db: asyncpg.Connection, pools: Pools) -> None:
    svc = _capped_service(pools, cap=2)
    await svc.submit([SubmitSpec(task_name="t.a", payload={})])
    await svc.submit([SubmitSpec(task_name="t.a", payload={})])  # now at the cap (2 live)

    with pytest.raises(TenantQuotaExceeded) as ei:
        await svc.submit([SubmitSpec(task_name="t.a", payload={})])
    assert ei.value.context["cap"] == 2
    assert ei.value.context["retry_after_s"] >= 1  # a positive backoff hint


async def test_cap_rejects_whole_batch_atomically(db: asyncpg.Connection, pools: Pools) -> None:
    """A batch that would straddle the cap is rejected ENTIRELY (no partial insert)."""
    svc = _capped_service(pools, cap=2)
    with pytest.raises(TenantQuotaExceeded):
        await svc.submit([SubmitSpec(task_name="t.a", payload={"i": i}) for i in range(3)])
    # Nothing was inserted — the tx rolled back on the raise.
    live = await db.fetchval("SELECT count(*) FROM jobs WHERE tenant = 'default'")
    assert live == 0


async def test_cap_is_per_tenant(db: asyncpg.Connection, pools: Pools) -> None:
    """One tenant at its cap never blocks another tenant's submit."""
    svc = _capped_service(pools, cap=1)
    await svc.submit([SubmitSpec(task_name="t.a", payload={}, tenant="tenant-a")])
    # tenant-a is full; tenant-b is empty and must still succeed.
    outcome = await svc.submit([SubmitSpec(task_name="t.a", payload={}, tenant="tenant-b")])
    assert outcome.job_ids[0]
    with pytest.raises(TenantQuotaExceeded):
        await svc.submit([SubmitSpec(task_name="t.a", payload={}, tenant="tenant-a")])


async def test_rest_returns_429_with_retry_after(db: asyncpg.Connection, capped_engine: EngineState) -> None:
    """REST surface maps the cap to 429 + Retry-After."""
    transport = httpx.ASGITransport(app=build_app(capped_engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://engine") as client:
        body = {"tenant": "default", "specs": [{"task_name": "t.a", "payload": {}}]}
        r1 = await client.post("/v1/jobs", json=body)
        r2 = await client.post("/v1/jobs", json=body)
        assert r1.status_code == 200 and r2.status_code == 200  # 2 live == cap
        r3 = await client.post("/v1/jobs", json=body)
        assert r3.status_code == 429
        assert int(r3.headers["Retry-After"]) >= 1
        assert r3.json()["error_code"] == "tenant_quota_exceeded"
