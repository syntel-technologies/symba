# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnusedFunction=false, reportUnknownParameterType=false, reportMissingTypeArgument=false, reportUnknownLambdaType=false
"""gRPC WorkerService adapter.

Thin transport over the service layer (workspace rule: no logic in the transport).
Each RPC decodes the request, calls a service, encodes the reply, and maps engine
errors to gRPC status codes.

    Claim (bidi stream) — the one long-lived RPC
    ---------------------------------------------
    First frame registers the worker (id, tags, free_slots). A background reader
    task keeps free_slots current from subsequent frames (worker-driven flow
    control). The main coroutine drains this worker's assignment queue (filled by
    the matcher) and yields JobAssignments. On disconnect the worker is
    unregistered; in-flight leases are recovered by the sweeper via lease TTL, so
    no assignment is lost (crash-only).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator

import grpc

from symba.core.errors import SymbaError
from symba.core.versioning import check_supported
from symba.observability.logging import logger
from symba.services.checkpoint_service import CheckpointService
from symba.services.job_service import JobService
from symba.services.registry import WorkerConn, WorkerRegistry
from symba.services.signal_service import SignalService
from symba.v1 import data_plane_pb2 as dp
from symba.v1 import data_plane_pb2_grpc as dp_grpc

logger = logger.bind(service="worker_service", context="engine/transport")

_GRPC_CODES = {name: getattr(grpc.StatusCode, name) for name in dir(grpc.StatusCode) if name.isupper()}
_SLOTS_TOTAL_LABEL = "symba.slots_total"


def _slots_total(first: dp.ClaimRequest) -> int:
    """Read stable capacity from the reserved label with legacy fallback."""
    try:
        advertised = int(first.labels.get(_SLOTS_TOTAL_LABEL, "0"))
    except (TypeError, ValueError):
        advertised = 0
    return max(int(first.free_slots), advertised)


async def _abort(context: grpc.aio.ServicerContext, err: SymbaError) -> None:
    code = _GRPC_CODES.get(err.grpc_code, grpc.StatusCode.INTERNAL)
    await context.abort(code, err.message)


class WorkerServicer(dp_grpc.WorkerServiceServicer):
    def __init__(
        self,
        registry: WorkerRegistry,
        jobs: JobService,
        signals: SignalService,
        checkpoints: CheckpointService,
    ) -> None:
        self._registry = registry
        self._jobs = jobs
        self._signals = signals
        self._checkpoints = checkpoints

    async def Claim(
        self,
        request_iterator: AsyncIterator[dp.ClaimRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[dp.JobAssignment]:
        first = await anext(aiter(request_iterator), None)
        if first is None:
            return
        # Handshake: reject a client outside the supported protocol window BEFORE
        # registering it, naming both versions. The decision is
        # pure (core/versioning.py); the transport only maps the raise to a gRPC abort.
        try:
            check_supported(first.sdk_version)
        except SymbaError as err:
            logger.warning(
                "[Claim] Rejected incompatible worker",
                worker_id=first.worker_id,
                sdk_version=first.sdk_version,
            )
            await _abort(context, err)
            raise  # unreachable; abort raises
        conn = WorkerConn(
            worker_id=first.worker_id,
            tags=frozenset(first.tags),
            free_slots=first.free_slots,
            labels=dict(first.labels),
            registered_tasks=frozenset(first.registered_tasks),
            # Reconnects can occur while jobs are still executing, so free_slots
            # is not a stable capacity value. New SDKs send the configured total
            # in a reserved label; old SDKs fall back to the registry high-water.
            slots_total=_slots_total(first),
        )
        await self._registry.register(conn)
        logger.info("[Claim] Worker connected", worker_id=conn.worker_id, tags=sorted(conn.tags), slots=conn.free_slots)

        reader = asyncio.create_task(self._read_slots(conn, request_iterator))
        assignment_task: asyncio.Task[dp.JobAssignment] | None = None
        try:
            while True:
                assignment_task = asyncio.create_task(conn.queue.get())
                done, _ = await asyncio.wait(
                    {assignment_task, reader},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if reader in done:
                    break
                assignment = assignment_task.result()
                assignment_task = None
                yield assignment
        except asyncio.CancelledError:
            raise
        finally:
            if assignment_task is not None:
                assignment_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await assignment_task
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
            removed = await self._registry.unregister(conn.worker_id, expected=conn)
            logger.info(
                "[Claim] Worker disconnected",
                worker_id=conn.worker_id,
                superseded=not removed,
            )

    async def _read_slots(self, conn: WorkerConn, request_iterator: AsyncIterator[dp.ClaimRequest]) -> None:
        try:
            async for frame in request_iterator:
                await self._registry.update_slots(
                    conn.worker_id,
                    frame.free_slots,
                    frozenset(frame.tags),
                    frozenset(frame.registered_tasks),
                    expected=conn,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("[Claim] Slot reader ended", worker_id=conn.worker_id, exc_info=True)

    async def Heartbeat(self, request: dp.HeartbeatRequest, context: grpc.aio.ServicerContext) -> dp.HeartbeatResponse:
        try:
            expires_at, cancelled = await self._jobs.heartbeat(job_id=request.job_id, lease_token=request.lease_token)
        except SymbaError as err:
            await _abort(context, err)
            raise  # unreachable; abort raises
        resp = dp.HeartbeatResponse(cancelled=cancelled)
        resp.lease_expires_at.FromDatetime(expires_at)
        return resp

    async def Complete(self, request: dp.CompleteRequest, context: grpc.aio.ServicerContext) -> dp.CompleteResponse:
        result = _decode_json(request.result_json)
        try:
            accepted = await self._jobs.complete(
                job_id=request.job_id,
                lease_token=request.lease_token,
                result=result,
                drop_chain_tail=request.drop_chain_tail,
                skipped=request.skipped,
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return dp.CompleteResponse(accepted=accepted)

    async def Fail(self, request: dp.FailRequest, context: grpc.aio.ServicerContext) -> dp.FailResponse:
        try:
            outcome = await self._jobs.fail(
                job_id=request.job_id,
                lease_token=request.lease_token,
                error_type=request.error_type,
                error_message=request.error_message,
                stack_hash=request.stack_hash,
                retryable=request.retryable,
                worker_max_attempts=request.max_attempts or None,
                error_message_safe=request.error_message_safe,
                error_metadata=_decode_error_metadata(request.error_metadata_json),
                rate_limited=request.rate_limited,
                retry_after_s=request.retry_after_s or None,
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return dp.FailResponse(accepted=outcome.accepted, will_retry=outcome.will_retry)

    async def GetResult(self, request: dp.GetResultRequest, context: grpc.aio.ServicerContext) -> dp.GetResultResponse:
        # tenant scope comes from the requesting job's lease context; M1 uses the
        # default tenant until per-request tenancy lands (M6). Per the proto, job_id
        # is the ASKING (running) job for authz scope and task_name names the ANCESTOR
        # to resolve within its ctx_id — NOT the asking job's own result.
        result_json, found = await self._jobs.get_result(
            job_id=request.job_id, tenant="default", task_name=request.task_name or None
        )
        return dp.GetResultResponse(result_json=result_json, found=found)

    async def Wait(self, request: dp.WaitRequest, context: grpc.aio.ServicerContext) -> dp.WaitResponse:
        # Human-in-the-loop: park RUNNING -> WAITING, or resume inline if a
        # signal is already pending. Tenant defaults until per-request tenancy (M6).
        try:
            outcome = await self._signals.wait(
                job_id=request.job_id,
                lease_token=request.lease_token,
                tenant="default",
                wait_key=request.wait_key,
                timeout_s=request.timeout_s,
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        resp = dp.WaitResponse(parked=not outcome.resumed_immediately)
        if outcome.resumed_immediately and outcome.payload is not None:
            resp.event_payload_json = json.dumps(outcome.payload).encode()
        return resp

    async def PutCheckpoint(
        self, request: dp.PutCheckpointRequest, context: grpc.aio.ServicerContext
    ) -> dp.PutCheckpointResponse:
        # Advisory, not lease-guarded (see checkpoint_put.sql). A malformed
        # body is the only failure mode we surface.
        data = _decode_json(request.checkpoint_json) or {}
        await self._checkpoints.put(job_id=request.job_id, tenant="default", data=data)
        return dp.PutCheckpointResponse(accepted=True)

    async def GetCheckpoint(
        self, request: dp.GetCheckpointRequest, context: grpc.aio.ServicerContext
    ) -> dp.GetCheckpointResponse:
        data = await self._checkpoints.get(job_id=request.job_id, tenant="default")
        if data is None:
            return dp.GetCheckpointResponse(found=False)
        return dp.GetCheckpointResponse(checkpoint_json=json.dumps(data).encode(), found=True)


def _decode_json(raw: bytes) -> dict[str, object] | None:
    if not raw:
        return None
    return json.loads(raw.decode())


def _decode_error_metadata(raw: bytes) -> dict[str, object]:
    """Decode the SDK's safe envelope; malformed metadata fails closed."""

    if not raw:
        return {}
    try:
        value = json.loads(raw.decode())
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}
