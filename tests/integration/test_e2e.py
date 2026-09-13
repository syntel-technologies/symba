"""L2 end-to-end: submit -> claim -> complete -> read.

Proves the full single-job lifecycle across every M1 layer against real PG18:

    REST POST /v1/jobs                 (SubmitService: validate, initial-state, insert)
        -> Matcher.pass_()             (claim for a registered worker)
        -> JobService.complete()       (archive move + group decrement + event)
        -> REST GET /v1/jobs/{id}       (read-through jobs_all -> 'succeeded' + result)

Two entry points are covered: the pure service path and the REST/HTTP path
(FastAPI app over an in-memory ASGI transport), sharing one EngineState so the
local-submit wake source is exercised too.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from symba.config import load_config
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.services.registry import WorkerConn
from symba.transport.http_server import build_app
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def engine(migrated_pool: asyncpg.Pool) -> AsyncIterator[EngineState]:
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    state = EngineState.build(load_config(), pools)
    state.serving = True
    yield state


async def _register_worker(state: EngineState, *, slots: int = 4, tags: frozenset[str] | None = None) -> WorkerConn:
    conn = WorkerConn(worker_id="w-e2e", tags=tags or frozenset(), free_slots=slots, labels={})
    await state.registry.register(conn)
    return conn


# --------------------------------------------------------------------------- #
# pure service path
# --------------------------------------------------------------------------- #


async def test_submit_claim_complete_via_services(db: asyncpg.Connection, engine: EngineState) -> None:
    # submit
    outcome = await engine.submit.submit([SubmitSpec(task_name="t.echo", payload={"n": 7})])
    job_id = outcome.job_ids[0]
    assert job_id and outcome.deduplicated == [False]
    assert engine.registry.wake.is_set()  # local-submit wake fired for a queued job
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "queued"

    # claim via the matcher
    worker = await _register_worker(engine)
    assigned = await engine.matcher.pass_()
    assert assigned == 1
    assignment = worker.queue.get_nowait()
    assert assignment.job.id == job_id
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"

    # complete
    accepted = await engine.jobs.complete(job_id=job_id, lease_token=assignment.lease_token, result={"doubled": 14})
    assert accepted is True

    # read-through: the job left the hot table and reads back as succeeded
    row = await engine.submit.get_job(job_id=job_id, tenant="default")
    assert row.state == "succeeded"
    assert row.result == {"doubled": 14}
    assert row.finished_at is not None


async def test_submit_dedup_batch_is_idempotent(engine: EngineState) -> None:
    specs = [
        SubmitSpec(task_name="t.echo", payload={"i": 1}, dedup_key="dk"),
        SubmitSpec(task_name="t.echo", payload={"i": 2}, dedup_key="dk"),
    ]
    outcome = await engine.submit.submit(specs)
    assert outcome.deduplicated == [False, True]
    # A dedup hit echoes the EXISTING job's id (SDK contract: the duplicate slot
    # references the canonical job, it is not a new row and not an empty id).
    assert outcome.job_ids[0]
    assert outcome.job_ids[1] == outcome.job_ids[0]


async def test_submit_deferred_job_starts_submitted(db: asyncpg.Connection, engine: EngineState) -> None:
    from datetime import UTC, datetime, timedelta

    spec = SubmitSpec(task_name="t.later", payload={}, run_at=datetime.now(UTC) + timedelta(hours=1))
    outcome = await engine.submit.submit([spec])
    # A future run_at is not yet claimable -> starts 'submitted', not 'queued'.
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", outcome.job_ids[0]) == "submitted"


async def test_submit_rejects_oversized_payload(engine: EngineState) -> None:
    from symba.core.errors import PayloadTooLarge

    huge = {"blob": "x" * (engine.config.limits.max_payload_kb * 1024 + 1)}
    with pytest.raises(PayloadTooLarge):
        await engine.submit.submit([SubmitSpec(task_name="t.big", payload=huge)])


async def test_submit_rejects_overlong_chain(engine: EngineState) -> None:
    # Chain length = on_success (1 hop) + chain_tail; over the cap is a
    # design smell -> ChainTooLong. One under the cap is accepted.
    from symba.core.errors import ChainTooLong

    cap = engine.config.limits.max_chain_len
    over = SubmitSpec(task_name="t.chain", payload={}, on_success="next", chain_tail=[f"s{i}" for i in range(cap)])
    with pytest.raises(ChainTooLong):
        await engine.submit.submit([over])

    at_cap = SubmitSpec(
        task_name="t.chain", payload={}, on_success="next", chain_tail=[f"s{i}" for i in range(cap - 1)]
    )
    outcome = await engine.submit.submit([at_cap])  # exactly at the cap is allowed
    assert outcome.job_ids[0]


async def test_complete_rejects_oversized_result(db: asyncpg.Connection, engine: EngineState) -> None:
    # A result over the 64KB cap is rejected at Complete BEFORE
    # the archive move; the job stays running (lease expiry / retry is the backstop).
    from symba.core.errors import ResultTooLarge

    outcome = await engine.submit.submit([SubmitSpec(task_name="t.big-result", payload={})])
    job_id = outcome.job_ids[0]
    worker = await _register_worker(engine)
    assert await engine.matcher.pass_() == 1
    assignment = worker.queue.get_nowait()

    huge = {"blob": "x" * (engine.config.limits.max_result_kb * 1024 + 1)}
    with pytest.raises(ResultTooLarge):
        await engine.jobs.complete(job_id=job_id, lease_token=assignment.lease_token, result=huge)
    # the job did NOT move to archive; it is still running under its lease
    assert await db.fetchval("SELECT state FROM jobs WHERE id = $1", job_id) == "running"


# --------------------------------------------------------------------------- #
# REST/HTTP path (FastAPI over in-memory ASGI transport)
# --------------------------------------------------------------------------- #


async def test_rest_submit_then_get(db: asyncpg.Connection, engine: EngineState) -> None:
    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        resp = await client.post(
            "/v1/jobs",
            json={"tenant": "default", "specs": [{"task_name": "t.rest", "payload": {"hello": "world"}}]},
        )
        assert resp.status_code == 200
        job_id = resp.json()["job_ids"][0]
        assert resp.json()["deduplicated"] == [False]

        # drive it to terminal so the read crosses the archive boundary
        worker = await _register_worker(engine)
        assert await engine.matcher.pass_() == 1
        assignment = worker.queue.get_nowait()
        await engine.jobs.complete(job_id=job_id, lease_token=assignment.lease_token, result={"ok": True})

        got = await client.get(f"/v1/jobs/{job_id}")
        assert got.status_code == 200
        body = got.json()
        assert body["id"] == job_id and body["state"] == "succeeded"
        assert body["result"] == {"ok": True}
        # Job-detail FE fields — omitting these crashes the SPA on .length / viewers.
        assert body["payload"] == {"hello": "world"}
        assert body["error_history"] == []
        assert "started_at" in body


async def test_rest_get_unknown_job_is_404(engine: EngineState) -> None:
    import uuid

    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        resp = await client.get(f"/v1/jobs/{uuid.uuid4()}")
        assert resp.status_code == 404
        assert resp.json()["error_code"] == "not_found"


async def test_rest_submit_empty_specs_rejected(engine: EngineState) -> None:
    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        resp = await client.post("/v1/jobs", json={"tenant": "default", "specs": []})
        assert resp.status_code == 422  # pydantic min_length=1


# --------------------------------------------------------------------------- #
# control-plane wiring: signal + resubmit over HTTP (M5)
# --------------------------------------------------------------------------- #


async def test_rest_signal_parks_for_future_waiter(engine: EngineState) -> None:
    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        # No waiter yet -> the signal is parked (delivered=False) for a future wait.
        resp = await client.post(
            "/v1/signals",
            json={"tenant": "default", "wait_key": "rest-key", "payload": {"go": True}, "signaled_by": "ops"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"delivered": False, "job_id": None}


async def test_rest_resubmit_dead_job(db: asyncpg.Connection, engine: EngineState) -> None:
    # Drive a job to DEAD via the service layer, then replay it over REST.
    spec = SubmitSpec(task_name="t.dlq", payload={"n": 1}, max_attempts=1)
    (await engine.submit.submit([spec]))
    worker = await _register_worker(engine)
    assert await engine.matcher.pass_() == 1
    assignment = worker.queue.get_nowait()
    dead_id = assignment.job.id
    await engine.jobs.fail(
        job_id=dead_id,
        lease_token=assignment.lease_token,
        error_type="boom",
        error_message="fatal",
        stack_hash="h1",
        retryable=False,
    )

    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        resp = await client.post(f"/v1/jobs/{dead_id}/resubmit")
        assert resp.status_code == 200
        body = resp.json()
        assert body["resubmitted_from"] == dead_id
        assert body["new_job_id"] != dead_id
        # The fresh job is readable and queued.
        got = await client.get(f"/v1/jobs/{body['new_job_id']}")
        assert got.json()["state"] == "queued"


async def test_rest_resubmit_unknown_is_404(engine: EngineState) -> None:
    import uuid

    app = build_app(engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://sim") as client:
        resp = await client.post(f"/v1/jobs/{uuid.uuid4()}/resubmit")
        assert resp.status_code == 404
        assert resp.json()["error_code"] == "not_found"
