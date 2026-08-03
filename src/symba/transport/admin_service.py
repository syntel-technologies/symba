# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false, reportUnusedFunction=false, reportUnknownParameterType=false
"""gRPC AdminService adapter (ops control plane).

Thin transport over the SAME service layer that backs the HTTP mirror
(http_server.py); the two transports encode/decode only, logic stays in services
(workspace rule: no logic in transports). The SDK's ``engine.admin`` dials
AdminServiceStub for rate-class + cron + fleet ops, so the engine must register a
gRPC servicer for it — before this, every ``engine.admin.*`` call returned
UNIMPLEMENTED because only the HTTP cron surface existed.

    RPC                 service call
    -----------------   ------------------------------------------------
    ListRateClasses     rate_limiter.list_classes()
    UpsertRateClass     rate_limiter.upsert(...)
    ListCronSchedules   query.cron(tenant)
    UpsertCronSchedule  query.upsert_cron(...)
    SetCronEnabled      query.set_cron_enabled(...)  -> echoes the CronSchedule
    DeleteCronSchedule  query.delete_cron(...)
    ListWorkers         query.workers()

Tenant: cron RPCs carry an explicit ``tenant`` field on the request (the SDK sets
AdminClient.tenant on each call), mirroring the control-plane servicer. Rate
classes and workers are engine-global (not tenant-partitioned in the schema), so
those RPCs take no tenant. The AuthInterceptor remains the authentication
boundary; this servicer reads the request tenant as the query scope.
"""

from __future__ import annotations

import json

import grpc

from symba.core.errors import SymbaError
from symba.db.records import CronRow, WorkerRow
from symba.observability.logging import logger
from symba.services.query_service import QueryService
from symba.services.rate_limiter import RateLimiter
from symba.v1 import admin_pb2 as admin
from symba.v1 import admin_pb2_grpc as admin_grpc

logger = logger.bind(service="admin_service", context="engine/transport")

_GRPC_CODES = {name: getattr(grpc.StatusCode, name) for name in dir(grpc.StatusCode) if name.isupper()}

_DEFAULT_TENANT = "default"


async def _abort(context: grpc.aio.ServicerContext, err: SymbaError) -> None:
    code = _GRPC_CODES.get(err.grpc_code, grpc.StatusCode.INTERNAL)
    await context.abort(code, err.message)


def _set_ts(field: object, value: object) -> None:
    # Mirrors client_service._set_ts: the object-typed value dodges pyright's
    # "datetime vs None have no overlap" false positive on the slotted-dataclass
    # optional timestamps while still only writing a real datetime.
    if value is not None:
        field.FromDatetime(value)  # type: ignore[attr-defined]


def _cron_to_proto(row: CronRow) -> admin.CronSchedule:
    schedule = admin.CronSchedule(
        schedule_id=row.schedule_id,
        cron_expr=row.cron_expr,
        task_name=row.task_name,
        payload_json=json.dumps(row.payload or {}).encode(),
        tenant=row.tenant,
        enabled=row.enabled,
    )
    _set_ts(schedule.last_fire, row.last_fire)
    _set_ts(schedule.next_fire, row.next_fire)
    return schedule


def _worker_to_proto(row: WorkerRow) -> admin.Worker:
    worker = admin.Worker(
        worker_id=row.worker_id,
        tags=list(row.tags),
        labels={k: str(v) for k, v in (row.labels or {}).items()},
        slots=row.slots,
        slots_busy=row.slots_busy,
        stale=row.stale,
    )
    _set_ts(worker.last_seen, row.last_seen)
    return worker


class AdminServicer(admin_grpc.AdminServiceServicer):
    """gRPC ops servicer. Encodes/decodes only; logic lives in services."""

    def __init__(self, query: QueryService, rate_limiter: RateLimiter) -> None:
        self._query = query
        self._rate_limiter = rate_limiter

    async def ListRateClasses(
        self, request: admin.ListRateClassesRequest, context: grpc.aio.ServicerContext
    ) -> admin.ListRateClassesResponse:
        classes = await self._rate_limiter.list_classes()
        return admin.ListRateClassesResponse(
            classes=[admin.RateClass(name=n, capacity=c, refill_per_s=r) for n, c, r in classes]
        )

    async def UpsertRateClass(
        self, request: admin.RateClass, context: grpc.aio.ServicerContext
    ) -> admin.RateClass:
        try:
            await self._rate_limiter.upsert(
                name=request.name, capacity=request.capacity, refill_per_s=request.refill_per_s
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return admin.RateClass(
            name=request.name, capacity=request.capacity, refill_per_s=request.refill_per_s
        )

    async def ListCronSchedules(
        self, request: admin.ListCronRequest, context: grpc.aio.ServicerContext
    ) -> admin.ListCronResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        rows = await self._query.cron(tenant=tenant)
        return admin.ListCronResponse(schedules=[_cron_to_proto(r) for r in rows])

    async def UpsertCronSchedule(
        self, request: admin.CronSchedule, context: grpc.aio.ServicerContext
    ) -> admin.CronSchedule:
        tenant = request.tenant or _DEFAULT_TENANT
        payload = json.loads(request.payload_json.decode()) if request.payload_json else {}
        try:
            row = await self._query.upsert_cron(
                schedule_id=request.schedule_id,
                cron_expr=request.cron_expr,
                task_name=request.task_name,
                payload=payload,
                tenant=tenant,
                enabled=request.enabled,
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return _cron_to_proto(row)

    async def SetCronEnabled(
        self, request: admin.SetCronEnabledRequest, context: grpc.aio.ServicerContext
    ) -> admin.CronSchedule:
        tenant = request.tenant or _DEFAULT_TENANT
        try:
            row = await self._query.set_cron_enabled(
                schedule_id=request.schedule_id, tenant=tenant, enabled=request.enabled
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return _cron_to_proto(row)

    async def DeleteCronSchedule(
        self, request: admin.DeleteCronRequest, context: grpc.aio.ServicerContext
    ) -> admin.DeleteCronResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        try:
            await self._query.delete_cron(schedule_id=request.schedule_id, tenant=tenant)
        except SymbaError as err:
            await _abort(context, err)
            raise
        return admin.DeleteCronResponse(deleted=True)

    async def ListWorkers(
        self, request: admin.ListWorkersRequest, context: grpc.aio.ServicerContext
    ) -> admin.ListWorkersResponse:
        rows = await self._query.workers()
        return admin.ListWorkersResponse(workers=[_worker_to_proto(r) for r in rows])


__all__ = ["AdminServicer"]
