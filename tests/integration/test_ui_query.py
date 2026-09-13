# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 UI read/stats surface.

Proves the operator-console endpoints against real PG18. Every UI view is the public
REST API, so these tests exercise the exact endpoints the SPA calls:

    /v1/jobs?state=          list + filters (DLQ, Waiting, Pipeline)
    /v1/stats/board          state -> count tiles
    /v1/stats/queues         per-lane depth + head-of-line age
    /v1/workers              fleet
    /v1/cron  + PUT          schedule list + enable/disable
    /v1/jobs/{id}/events     audit timeline
    /v1/jobs/{id}/tree       ctx dependency DAG edges
    /v1/events/stream        SSE ledger fan-out (via EventStream directly)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from symba.config import load_config
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.services.event_stream import EventStream
from symba.services.loops import Sweeper
from symba.services.registry import WorkerConn
from symba.transport.http_server import build_app
from symba.transport.state import EngineState

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def engine(migrated_pool: asyncpg.Pool) -> AsyncIterator[EngineState]:
    # Start clean: this suite calls matcher.pass_() (tenant-agnostic), which would
    # otherwise claim QUEUED jobs leaked by a sibling suite running earlier in the
    # session and skew counts (flaky "assert 2 == 1"). Truncate up front so ordering
    # never matters.
    async with migrated_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE jobs, jobs_archive, job_events, job_dependencies, gates, "
            "group_running, checkpoints, signals, workers, rate_classes, cron_schedules "
            "RESTART IDENTITY CASCADE"
        )
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    state = EngineState.build(load_config(), pools)
    state.serving = True
    yield state


def _client(engine: EngineState) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=build_app(engine))
    return httpx.AsyncClient(transport=transport, base_url="http://sim")


async def _drive_dead(engine: EngineState) -> str:
    """Submit a job and fail it fatally so it archives as DEAD. Returns its id."""
    (await engine.submit.submit([SubmitSpec(task_name="t.dlq", payload={}, max_attempts=1)]))
    worker = WorkerConn(worker_id="w-dlq", tags=frozenset(), free_slots=4, labels={})
    await engine.registry.register(worker)
    assert await engine.matcher.pass_() == 1
    a = worker.queue.get_nowait()
    await engine.jobs.fail(
        job_id=a.job.id,
        lease_token=a.lease_token,
        error_type="boom",
        error_message="fatal",
        stack_hash="h1",
        retryable=False,
    )
    return a.job.id


# ── job list + filters ────────────────────────────────────────────────────────


async def test_list_jobs_filters_by_state(engine: EngineState) -> None:
    dead_id = await _drive_dead(engine)
    await engine.submit.submit([SubmitSpec(task_name="t.live", payload={})])

    async with _client(engine) as client:
        dead = (await client.get("/v1/jobs", params={"state_filter": "dead"})).json()["jobs"]
        assert any(j["id"] == dead_id for j in dead)
        assert all(j["state"] == "dead" for j in dead)

        queued = (await client.get("/v1/jobs", params={"state_filter": "queued"})).json()["jobs"]
        assert all(j["state"] == "queued" for j in queued)
        assert dead_id not in {j["id"] for j in queued}


async def test_list_jobs_filters_by_ctx(engine: EngineState) -> None:
    await engine.submit.submit([SubmitSpec(task_name="t.a", payload={}, ctx_id="ctx-42")])
    await engine.submit.submit([SubmitSpec(task_name="t.b", payload={}, ctx_id="other")])
    async with _client(engine) as client:
        rows = (await client.get("/v1/jobs", params={"ctx_id": "ctx-42"})).json()["jobs"]
        assert rows and all(j["ctx_id"] == "ctx-42" for j in rows)


async def test_query_service_honors_native_pipeline_filters(engine: EngineState) -> None:
    await engine.submit.submit(
        [
            SubmitSpec(
                task_name="t.scope-a",
                payload={},
                pipeline="iknowledge:index:a",
                stage="embed",
                group_key="embed:a",
            ),
            SubmitSpec(
                task_name="t.scope-b",
                payload={},
                pipeline="iknowledge:index:b",
                stage="summarize",
                group_key="summarize:b",
            ),
        ]
    )

    rows = await engine.query.jobs(
        pipeline="iknowledge:index:a",
        stage="embed",
        group_key="embed:a",
        limit=50,
    )

    assert rows
    assert {row.pipeline for row in rows} == {"iknowledge:index:a"}
    assert {row.stage for row in rows} == {"embed"}
    assert {row.group_key for row in rows} == {"embed:a"}


async def test_list_jobs_page_cap_enforced(engine: EngineState) -> None:
    # A limit above the hard cap (500) is clamped, never honored verbatim.
    async with _client(engine) as client:
        resp = await client.get("/v1/jobs", params={"limit": 100000})
        assert resp.status_code == 200  # clamped server-side, not rejected


# ── board + queues stats ───────────────────────────────────────────────────────


async def test_board_counts_by_state(engine: EngineState) -> None:
    await engine.submit.submit([SubmitSpec(task_name="t.board", payload={})])
    async with _client(engine) as client:
        counts = (await client.get("/v1/stats/board")).json()["counts"]
        assert counts.get("queued", 0) >= 1


async def test_queues_depth_and_age(engine: EngineState) -> None:
    await engine.submit.submit([SubmitSpec(task_name="t.q1", payload={})])
    await engine.submit.submit([SubmitSpec(task_name="t.q1", payload={})])
    async with _client(engine) as client:
        queues = (await client.get("/v1/stats/queues")).json()["queues"]
        q1 = next(q for q in queues if q["task_name"] == "t.q1")
        assert q1["depth"] >= 2
        assert q1["oldest_age_s"] >= 0


# ── fleet ───────────────────────────────────────────────────────────────────────


async def test_workers_lists_registered_fleet(db: asyncpg.Connection, engine: EngineState) -> None:
    # The registry is in-memory; the fleet view reads the `workers` table the sweeper
    # maintains. Seed a row directly to prove the read path.
    await db.execute(
        "INSERT INTO workers (worker_id, tags, registered_tasks, slots, slots_busy) VALUES ($1, $2, $3, $4, $5)",
        "w-fleet",
        ["gpu"],
        ["embed.batch", "parse.document"],
        8,
        3,
    )
    async with _client(engine) as client:
        workers = (await client.get("/v1/workers")).json()["workers"]
        w = next(x for x in workers if x["worker_id"] == "w-fleet")
        assert w["tags"] == ["gpu"] and w["slots"] == 8 and w["slots_busy"] == 3
        assert w["registered_tasks"] == ["embed.batch", "parse.document"]


async def test_registry_persists_worker_to_table(db: asyncpg.Connection, engine: EngineState) -> None:
    # Registering a worker (via the gRPC Claim path in production) must mirror it into
    # the `workers` read model so the fleet view/count reflect it without any seeding.
    worker = WorkerConn(
        worker_id="w-persist",
        tags=frozenset({"gpu"}),
        free_slots=8,
        labels={"az": "eu"},
        registered_tasks=frozenset({"parse.document", "embed.batch"}),
        slots_total=8,
    )
    await engine.registry.register(worker)
    row = await db.fetchrow(
        "SELECT tags, registered_tasks, labels, slots, slots_busy, stale FROM workers WHERE worker_id=$1",
        "w-persist",
    )
    assert row is not None
    assert list(row["tags"]) == ["gpu"] and row["slots"] == 8 and row["slots_busy"] == 0 and row["stale"] is False
    assert list(row["registered_tasks"]) == ["embed.batch", "parse.document"]

    # A slot frame (doubling as a heartbeat) refreshes the busy gauge: 8 total, 5 free.
    await engine.registry.update_slots(
        "w-persist",
        free_slots=5,
        registered_tasks=frozenset({"parse.document", "tag.apply"}),
    )
    refreshed = await db.fetchrow(
        "SELECT slots_busy, registered_tasks FROM workers WHERE worker_id=$1",
        "w-persist",
    )
    assert refreshed["slots_busy"] == 3
    assert list(refreshed["registered_tasks"]) == ["parse.document", "tag.apply"]

    # A clean disconnect drops the row immediately (operator sees it leave at once).
    await engine.registry.unregister("w-persist")
    assert await db.fetchval("SELECT count(*) FROM workers WHERE worker_id=$1", "w-persist") == 0


async def test_claimed_by_carries_real_worker_id(db: asyncpg.Connection, engine: EngineState) -> None:
    # After the matcher assigns, jobs.claimed_by must be the REAL worker_id (not the
    # batch "engine:{tags}" placeholder) so running jobs are attributable per worker.
    outcome = await engine.submit.submit([SubmitSpec(task_name="t.attr", payload={})])
    job_id = outcome.job_ids[0]
    worker = WorkerConn(worker_id="w-attr", tags=frozenset(), free_slots=4, labels={}, slots_total=4)
    await engine.registry.register(worker)
    assert await engine.matcher.pass_() == 1
    assert await db.fetchval("SELECT claimed_by FROM jobs WHERE id=$1", job_id) == "w-attr"

    # The read layer surfaces it as the normalized `worker` field on the job.
    async with _client(engine) as client:
        got = (await client.get(f"/v1/jobs/{job_id}")).json()
        assert got["worker"] == "w-attr"


async def test_list_jobs_filters_by_worker(engine: EngineState) -> None:
    # Two workers each claim their own job; ?worker= must return only that worker's job.
    for wid in ("w-A", "w-B"):
        await engine.submit.submit([SubmitSpec(task_name=f"t.{wid}", payload={})])
        conn = WorkerConn(worker_id=wid, tags=frozenset(), free_slots=1, labels={}, slots_total=1)
        await engine.registry.register(conn)
        assert await engine.matcher.pass_() == 1
        await engine.registry.unregister(wid)  # free the slot bookkeeping for the next

    async with _client(engine) as client:
        rows = (await client.get("/v1/jobs", params={"worker": "w-A"})).json()["jobs"]
        assert rows and all(j["worker"] == "w-A" for j in rows)
        assert "w-B" not in {j["worker"] for j in rows}


async def test_sweeper_marks_stale_worker(db: asyncpg.Connection, engine: EngineState) -> None:
    # A worker whose last_seen predates the staleness window (missed heartbeats *
    # heartbeat interval) is flagged stale by the sweeper; a fresh one is untouched.
    cfg = engine.config.sweeper
    stale_after_s = cfg.worker_stale_after_heartbeats * cfg.worker_heartbeat_interval_s
    await db.execute(
        "INSERT INTO workers (worker_id, slots, last_seen) VALUES ($1, 4, now() - make_interval(secs => $2))",
        "w-dead",
        stale_after_s + 60,
    )
    await db.execute("INSERT INTO workers (worker_id, slots, last_seen) VALUES ($1, 4, now())", "w-alive")

    await Sweeper(engine).pass_()

    assert await db.fetchval("SELECT stale FROM workers WHERE worker_id=$1", "w-dead") is True
    assert await db.fetchval("SELECT stale FROM workers WHERE worker_id=$1", "w-alive") is False


# ── cron list + toggle ──────────────────────────────────────────────────────────


async def test_cron_list_and_toggle(db: asyncpg.Connection, engine: EngineState) -> None:
    await db.execute(
        "INSERT INTO cron_schedules (schedule_id, cron_expr, task_name) VALUES ($1, $2, $3)",
        "sched-1",
        "*/5 * * * *",
        "t.cron",
    )
    async with _client(engine) as client:
        listed = (await client.get("/v1/cron")).json()["schedules"]
        assert any(s["schedule_id"] == "sched-1" and s["enabled"] for s in listed)

        resp = await client.put("/v1/cron/sched-1", json={"enabled": False})
        assert resp.status_code == 200 and resp.json() == {"enabled": False}
        assert await db.fetchval("SELECT enabled FROM cron_schedules WHERE schedule_id='sched-1'") is False


async def test_cron_toggle_unknown_is_404(engine: EngineState) -> None:
    async with _client(engine) as client:
        resp = await client.put("/v1/cron/nope", json={"enabled": False})
        assert resp.status_code == 404


# ── cron upsert (create + update) ─────────────────────────────────────────────


async def test_cron_upsert_creates_then_updates(db: asyncpg.Connection, engine: EngineState) -> None:
    async with _client(engine) as client:
        # Create.
        created = await client.post(
            "/v1/cron",
            json={
                "schedule_id": "recon",
                "cron_expr": "*/30 * * * *",
                "task_name": "graph.reconcile_dispatch",
                "payload": {"k": "v"},
            },
        )
        assert created.status_code == 200
        body = created.json()
        assert body["schedule_id"] == "recon" and body["task_name"] == "graph.reconcile_dispatch"
        assert body["payload"] == {"k": "v"} and body["enabled"] is True

        # Idempotent update of task_name/payload; SAME cron_expr must keep next_fire.
        await db.execute("UPDATE cron_schedules SET next_fire = now() WHERE schedule_id='recon'")
        before = await db.fetchval("SELECT next_fire FROM cron_schedules WHERE schedule_id='recon'")
        updated = await client.post(
            "/v1/cron",
            json={
                "schedule_id": "recon",
                "cron_expr": "*/30 * * * *",
                "task_name": "graph.reconcile_v2",
                "payload": {},
            },
        )
        assert updated.status_code == 200
        assert updated.json()["task_name"] == "graph.reconcile_v2"
        after = await db.fetchval("SELECT next_fire FROM cron_schedules WHERE schedule_id='recon'")
        assert after == before  # unchanged expr -> window preserved


async def test_cron_upsert_expr_change_resets_next_fire(db: asyncpg.Connection, engine: EngineState) -> None:
    await db.execute(
        "INSERT INTO cron_schedules (schedule_id, cron_expr, task_name, next_fire) VALUES ($1, $2, $3, now())",
        "recon",
        "*/30 * * * *",
        "t.cron",
    )
    async with _client(engine) as client:
        resp = await client.post(
            "/v1/cron",
            json={"schedule_id": "recon", "cron_expr": "*/5 * * * *", "task_name": "t.cron"},
        )
        assert resp.status_code == 200
    # A changed cron_expr resets the window to NULL so the loop recomputes it (no backfill).
    next_fire = await db.fetchval("SELECT next_fire FROM cron_schedules WHERE schedule_id='recon'")
    assert next_fire is None


async def test_cron_upsert_bad_expr_is_422(engine: EngineState) -> None:
    async with _client(engine) as client:
        resp = await client.post(
            "/v1/cron",
            json={"schedule_id": "bad", "cron_expr": "not a cron", "task_name": "t.cron"},
        )
        assert resp.status_code == 422
        assert resp.json()["error_code"] == "validation_error"


# ── cron delete ───────────────────────────────────────────────────────────────


async def test_cron_delete_removes_row(db: asyncpg.Connection, engine: EngineState) -> None:
    await db.execute(
        "INSERT INTO cron_schedules (schedule_id, cron_expr, task_name) VALUES ($1, $2, $3)",
        "gone",
        "*/5 * * * *",
        "t.cron",
    )
    async with _client(engine) as client:
        resp = await client.delete("/v1/cron/gone")
        assert resp.status_code == 200 and resp.json() == {"deleted": True}
    assert await db.fetchval("SELECT count(*) FROM cron_schedules WHERE schedule_id='gone'") == 0


async def test_cron_delete_unknown_is_404(engine: EngineState) -> None:
    async with _client(engine) as client:
        resp = await client.delete("/v1/cron/nope")
        assert resp.status_code == 404


# ── job detail: events timeline + dependency tree ───────────────────────────────


async def test_job_events_timeline(engine: EngineState) -> None:
    dead_id = await _drive_dead(engine)  # produces claimed/failed/died ledger rows
    async with _client(engine) as client:
        events = (await client.get(f"/v1/jobs/{dead_id}/events")).json()["events"]
        assert events, "a job that ran and died has an audit timeline"
        assert all("event" in e and "at" in e for e in events)


async def test_job_tree_edges_scoped_to_ctx(engine: EngineState) -> None:
    # upstream -> downstream dep within one ctx; the tree returns exactly that edge.
    up = (await engine.submit.submit([SubmitSpec(task_name="t.up", payload={}, ctx_id="dag-1")])).job_ids[0]
    down_spec = SubmitSpec(task_name="t.down", payload={}, ctx_id="dag-1", depends_on=[(up, None)])
    down = (await engine.submit.submit([down_spec])).job_ids[0]

    async with _client(engine) as client:
        edges = (await client.get(f"/v1/jobs/{down}/tree")).json()["edges"]
        assert any(e["upstream"] == up and e["downstream"] == down for e in edges)


# ── SSE fan-out (EventStream directly, no long-lived HTTP hold) ─────────────────


async def test_event_stream_fans_out_new_events(db: asyncpg.Connection, engine: EngineState) -> None:
    stream = EventStream(engine.pools)
    poller = asyncio.create_task(stream.start())
    try:
        # Let the poller establish its starting cursor at the current ledger head.
        await asyncio.sleep(0.1)
        async with stream.subscribe(tenant="default") as queue:
            # Append a fresh ledger row AFTER subscribing; the poller must deliver it.
            await db.execute(
                "INSERT INTO job_events (job_id, tenant, event) VALUES ($1, $2, $3)",
                "00000000-0000-0000-0000-0000000000aa",
                "default",
                "unit_test_ping",
            )
            row = await asyncio.wait_for(queue.get(), timeout=3.0)
            assert row.event == "unit_test_ping"
    finally:
        stream.stop()
        await asyncio.wait_for(poller, timeout=3.0)


async def test_event_stream_tenant_scoped(db: asyncpg.Connection, engine: EngineState) -> None:
    stream = EventStream(engine.pools)
    poller = asyncio.create_task(stream.start())
    try:
        await asyncio.sleep(0.1)
        async with stream.subscribe(tenant="default") as queue:
            # A foreign-tenant event must NOT reach a default-tenant subscriber.
            await db.execute(
                "INSERT INTO job_events (job_id, tenant, event) VALUES ($1, $2, $3)",
                "00000000-0000-0000-0000-0000000000bb",
                "acme",
                "foreign_ping",
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(queue.get(), timeout=1.5)
    finally:
        stream.stop()
        await asyncio.wait_for(poller, timeout=3.0)
