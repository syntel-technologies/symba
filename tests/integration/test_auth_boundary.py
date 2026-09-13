# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 HTTP auth boundary + tenant scoping (N9).

Proves the security boundary end-to-end against a real app:

    1. token mode: /v1 requires a valid Bearer; missing/bad -> 401.
    2. public paths (/healthz /readyz /metrics) are reachable WITHOUT a credential.
    3. tenant scoping (N9): a token maps to ONE tenant; a caller cannot read another
       tenant's job even with its (unguessable) id — it 404s, not 200.
    4. submit tenant is authoritative from the credential, not the request body.

We drive httpx.ASGITransport, which reports no client host (peer_ip=None); token
mode ignores the peer entirely, so that is irrelevant here — the credential is the
gate. (none-mode/loopback behavior is covered in tests/unit/test_auth.py.)
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from symba.config import AuthConfig, load_config
from symba.db.pool import Pools
from symba.transport.http_server import build_app
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]

# Two tenants, two secrets. The map is the whole authz model for shared-secret mode.
_TOKENS = {"secret-a": "tenant-a", "secret-b": "tenant-b"}
_HDR_A = {"authorization": "Bearer secret-a"}
_HDR_B = {"authorization": "Bearer secret-b"}


@pytest_asyncio.fixture(loop_scope="session")
async def engine(migrated_pool: asyncpg.Pool) -> AsyncIterator[EngineState]:
    cfg = load_config()
    # Force token mode for this suite regardless of the ambient dev default (none).
    cfg = cfg.model_copy(update={"auth": AuthConfig(mode="token", tokens=_TOKENS)})
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    state = EngineState.build(cfg, pools)
    state.serving = True
    yield state
    # Hermetic: this suite submits QUEUED jobs into the shared session pool. Leaving
    # them behind lets a sibling suite's tenant-agnostic matcher.pass_() claim them
    # (flaky "assert 2 == 1"). Truncate on teardown so ordering never matters.
    async with migrated_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
            "group_running, checkpoints, signals, workers, rate_classes, cron_schedules "
            "RESTART IDENTITY CASCADE"
        )


def _client(engine: EngineState) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=build_app(engine))
    return httpx.AsyncClient(transport=transport, base_url="http://sim")


# ── public surface is open ──────────────────────────────────────────────────────


async def test_health_endpoints_need_no_auth(engine: EngineState) -> None:
    async with _client(engine) as c:
        assert (await c.get("/healthz")).status_code == 200
        # readyz needs PG; migrated_pool is live so it should be ready.
        assert (await c.get("/readyz")).status_code == 200
        assert (await c.get("/metrics")).status_code == 200


# ── /v1 is fail-closed ──────────────────────────────────────────────────────────


async def test_v1_without_credentials_is_401(engine: EngineState) -> None:
    async with _client(engine) as c:
        resp = await c.get("/v1/stats/board")
    assert resp.status_code == 401
    assert resp.json()["error_code"] == "unauthenticated"


async def test_v1_with_bad_token_is_401(engine: EngineState) -> None:
    async with _client(engine) as c:
        resp = await c.get(
            "/v1/stats/board", headers={"authorization": "Bearer wrong", "X-Symba-Request-Id": "failed-auth"}
        )
    assert resp.status_code == 401
    assert resp.json()["trace_id"] == "failed-auth"


async def test_v1_query_token_is_not_a_credential(engine: EngineState) -> None:
    # Credentials in URLs leak into access logs, histories, and referrers. Even the
    # SSE endpoint uses an Authorization header, so query tokens must fail closed.
    async with _client(engine) as c:
        resp = await c.get("/v1/stats/board", params={"access_token": "secret-a"})
    assert resp.status_code == 401
    assert resp.json()["error_code"] == "unauthenticated"


async def test_v1_with_valid_token_is_200(engine: EngineState) -> None:
    async with _client(engine) as c:
        resp = await c.get("/v1/stats/board", headers=_HDR_A)
    assert resp.status_code == 200


# ── tenant scoping (N9) ─────────────────────────────────────────────────────────


async def test_submit_tenant_is_from_credential_not_body(engine: EngineState) -> None:
    # Body claims tenant-b, but the credential is tenant-a: the job must land in
    # tenant-a and be invisible to tenant-b.
    async with _client(engine) as c:
        submit = await c.post(
            "/v1/jobs",
            headers=_HDR_A,
            json={"tenant": "tenant-b", "specs": [{"task_name": "t.scope", "payload": {}}]},
        )
        assert submit.status_code == 200
        job_id = submit.json()["job_ids"][0]

        # Owner (tenant-a) can read it.
        mine = await c.get(f"/v1/jobs/{job_id}", headers=_HDR_A)
        assert mine.status_code == 200

        # Other tenant (tenant-b) cannot — 404, not 200 (id is not a capability).
        theirs = await c.get(f"/v1/jobs/{job_id}", headers=_HDR_B)
        assert theirs.status_code == 404


async def test_list_jobs_is_tenant_isolated(engine: EngineState) -> None:
    async with _client(engine) as c:
        await c.post(
            "/v1/jobs",
            headers=_HDR_A,
            json={"specs": [{"task_name": "t.only_a", "payload": {}}]},
        )
        a_jobs = (await c.get("/v1/jobs?task_name=t.only_a", headers=_HDR_A)).json()["jobs"]
        b_jobs = (await c.get("/v1/jobs?task_name=t.only_a", headers=_HDR_B)).json()["jobs"]
    assert len(a_jobs) >= 1
    assert b_jobs == []


async def test_cross_tenant_cancel_is_404(engine: EngineState) -> None:
    async with _client(engine) as c:
        submit = await c.post(
            "/v1/jobs",
            headers=_HDR_A,
            json={"specs": [{"task_name": "t.cancel", "payload": {}}]},
        )
        job_id = submit.json()["job_ids"][0]
        # tenant-b tries to cancel tenant-a's job -> ownership check 404s it.
        resp = await c.post(f"/v1/jobs/{job_id}/cancel", headers=_HDR_B)
    assert resp.status_code == 404
