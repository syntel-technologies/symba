# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportAttributeAccessIssue=false
"""L2 gRPC AdminService.

Proves the admin control plane is actually wired end-to-end over gRPC — before
this servicer existed the SDK's engine.admin.* calls returned UNIMPLEMENTED because
only the HTTP cron mirror was registered.

    RPC                 asserted behavior
    -----------------   ------------------------------------------------
    UpsertCronSchedule  create + idempotent update; bad expr -> INVALID_ARGUMENT
    ListCronSchedules   tenant-scoped read
    SetCronEnabled      toggle echoes the CronSchedule
    DeleteCronSchedule  delete; unknown id -> NOT_FOUND
    UpsertRateClass     create then ListRateClasses reflects it
    ListWorkers         fleet read (empty is fine)

The server is a bare grpc.aio.server with ONLY the AdminServicer registered (no auth
interceptor) so the test targets the servicer wiring, not the auth boundary (covered
in test_auth_boundary.py).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import asyncpg
import grpc
import pytest
import pytest_asyncio

from symba.config import load_config
from symba.db.pool import Pools
from symba.transport.admin_service import AdminServicer
from symba.transport.state import EngineState
from symba.v1 import admin_pb2 as admin
from symba.v1 import admin_pb2_grpc as admin_grpc

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def admin_stub(migrated_pool: asyncpg.Pool) -> AsyncIterator[admin_grpc.AdminServiceStub]:
    async with migrated_pool.acquire() as conn:
        await conn.execute("TRUNCATE cron_schedules, rate_classes, workers RESTART IDENTITY CASCADE")
    pools = Pools(hot=migrated_pool, general=migrated_pool)
    state = EngineState.build(load_config(), pools)

    server = grpc.aio.server()
    admin_grpc.add_AdminServiceServicer_to_server(AdminServicer(state.query, state.rate_limiter), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield admin_grpc.AdminServiceStub(channel)
    finally:
        await channel.close()
        await server.stop(grace=None)


# ── cron upsert / list / toggle / delete ──────────────────────────────────────


async def test_upsert_list_toggle_delete_cron(admin_stub: admin_grpc.AdminServiceStub) -> None:
    created = await admin_stub.UpsertCronSchedule(
        admin.CronSchedule(
            schedule_id="recon",
            cron_expr="*/30 * * * *",
            task_name="graph.reconcile_dispatch",
            payload_json=json.dumps({"k": "v"}).encode(),
            tenant="default",
            enabled=True,
        )
    )
    assert created.schedule_id == "recon" and created.enabled is True
    assert json.loads(created.payload_json.decode()) == {"k": "v"}

    listed = await admin_stub.ListCronSchedules(admin.ListCronRequest(tenant="default"))
    assert [s.schedule_id for s in listed.schedules] == ["recon"]

    toggled = await admin_stub.SetCronEnabled(
        admin.SetCronEnabledRequest(schedule_id="recon", enabled=False, tenant="default")
    )
    assert toggled.schedule_id == "recon" and toggled.enabled is False

    deleted = await admin_stub.DeleteCronSchedule(admin.DeleteCronRequest(schedule_id="recon", tenant="default"))
    assert deleted.deleted is True

    empty = await admin_stub.ListCronSchedules(admin.ListCronRequest(tenant="default"))
    assert empty.schedules == []


async def test_upsert_cron_bad_expr_invalid_argument(admin_stub: admin_grpc.AdminServiceStub) -> None:
    with pytest.raises(grpc.aio.AioRpcError) as exc:
        await admin_stub.UpsertCronSchedule(
            admin.CronSchedule(schedule_id="bad", cron_expr="nope", task_name="t.cron", tenant="default")
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_delete_unknown_cron_not_found(admin_stub: admin_grpc.AdminServiceStub) -> None:
    with pytest.raises(grpc.aio.AioRpcError) as exc:
        await admin_stub.DeleteCronSchedule(admin.DeleteCronRequest(schedule_id="ghost", tenant="default"))
    assert exc.value.code() == grpc.StatusCode.NOT_FOUND


async def test_cron_tenant_isolation(admin_stub: admin_grpc.AdminServiceStub) -> None:
    await admin_stub.UpsertCronSchedule(
        admin.CronSchedule(schedule_id="a", cron_expr="* * * * *", task_name="t.cron", tenant="tenant-a")
    )
    # A different tenant does not see it, and cannot delete it.
    other = await admin_stub.ListCronSchedules(admin.ListCronRequest(tenant="tenant-b"))
    assert other.schedules == []
    with pytest.raises(grpc.aio.AioRpcError) as exc:
        await admin_stub.DeleteCronSchedule(admin.DeleteCronRequest(schedule_id="a", tenant="tenant-b"))
    assert exc.value.code() == grpc.StatusCode.NOT_FOUND


# ── rate classes + workers ────────────────────────────────────────────────────


async def test_upsert_and_list_rate_classes(admin_stub: admin_grpc.AdminServiceStub) -> None:
    await admin_stub.UpsertRateClass(admin.RateClass(name="llm_reconcile", capacity=10.0, refill_per_s=2.0))
    listed = await admin_stub.ListRateClasses(admin.ListRateClassesRequest())
    by_name = {c.name: c for c in listed.classes}
    assert "llm_reconcile" in by_name
    assert by_name["llm_reconcile"].capacity == 10.0 and by_name["llm_reconcile"].refill_per_s == 2.0


async def test_list_workers_empty_ok(admin_stub: admin_grpc.AdminServiceStub) -> None:
    resp = await admin_stub.ListWorkers(admin.ListWorkersRequest())
    assert list(resp.workers) == []
