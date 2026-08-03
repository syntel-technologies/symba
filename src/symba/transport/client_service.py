# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false, reportUnusedFunction=false, reportUnknownParameterType=false
"""gRPC ClientService adapter (control plane).

Thin transport over the SAME service layer that backs the HTTP mirror
(http_server.py); the two transports encode/decode only, logic stays in services
(workspace rule: no logic in transports). This closes ENG-1: the SDK dials
ClientServiceStub for every control-plane call, so the engine must register a gRPC
servicer for it, not only the HTTP mirror.

    RPC                service call
    -----------------  ------------------------------------------------
    Submit             submit.submit(specs)
    FanOut             fanout.fan_out(...)
    Query              query.jobs(...) (offset paging behind page_token)
    GetJob             submit.get_job(...)
    AwaitJob           submit.get_job(...) polled to a terminal state
    Cancel             submit.get_job(tenant guard) + cancel.cancel(...)
    Signal             signals.signal(...)
    Resubmit           resubmit.resubmit_many(...)
    StreamEvents       events.subscribe(tenant) live tail by ctx_id

Tenant: the control-plane proto carries an explicit `tenant` field on every
request (the SDK sets Engine.tenant on each call). The AuthInterceptor is the
authentication boundary; this servicer reads the request tenant field as the
scope for every downstream query, mirroring the HTTP transport's tenant handling.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from symba.core.errors import SymbaError
from symba.core.states import JobState, is_terminal
from symba.db.records import EventRow, JobRow, SubmitSpec
from symba.observability.logging import logger
from symba.services.cancel_service import CancelService
from symba.services.event_stream import EventStream
from symba.services.fanout_service import FanOutService
from symba.services.query_service import QueryService
from symba.services.resubmit_service import ResubmitService
from symba.services.signal_service import SignalService
from symba.services.submit_service import SubmitService
from symba.v1 import common_pb2 as common
from symba.v1 import control_plane_pb2 as cp
from symba.v1 import control_plane_pb2_grpc as cp_grpc

logger = logger.bind(service="client_service", context="engine/transport")

_GRPC_CODES = {name: getattr(grpc.StatusCode, name) for name in dir(grpc.StatusCode) if name.isupper()}

_DEFAULT_TENANT = "default"

# Engine state string -> proto JobState enum. An unknown/empty string maps to
# JOB_STATE_UNSPECIFIED so a partial read never crashes the encode.
_STATE_TO_PROTO = {
    JobState.SUBMITTED: common.SUBMITTED,
    JobState.QUEUED: common.QUEUED,
    JobState.RUNNING: common.RUNNING,
    JobState.WAITING: common.WAITING,
    JobState.SUCCEEDED: common.SUCCEEDED,
    JobState.DEAD: common.DEAD,
    JobState.CANCELLED: common.CANCELLED,
}
_PROTO_TO_STATE = {v: k.value for k, v in _STATE_TO_PROTO.items()}


async def _abort(context: grpc.aio.ServicerContext, err: SymbaError) -> None:
    code = _GRPC_CODES.get(err.grpc_code, grpc.StatusCode.INTERNAL)
    await context.abort(code, err.message)


def _decode_payload(raw: bytes) -> dict[str, object]:
    # A chained tail / empty payload is sent as empty bytes; decode to {}.
    if not raw:
        return {}
    return json.loads(raw.decode())


def _spec_from_proto(tenant: str, spec: common.JobSpec) -> SubmitSpec:
    """Map a control-plane JobSpec into the engine's SubmitSpec.

    Chain contract: the SDK sends ``chain = [task, next1, next2, ...]`` where the
    head equals ``task_name`` (specs._validate_chain_head guarantees it). The engine
    models the chain as ``on_success`` (the next hop) + ``chain_tail`` (the rest), so
    we drop the head and split the remainder.
    """
    chain = list(spec.chain)
    # Drop the leading task itself if the SDK led the chain with it; what remains is
    # the forward path (on_success, then chain_tail).
    if chain and chain[0] == spec.task_name:
        chain = chain[1:]
    on_success = chain[0] if chain else None
    chain_tail = chain[1:] if len(chain) > 1 else []

    depends_on = [(dep.job_id, dep.alias or None) for dep in spec.depends_on]
    on_failure = None
    if spec.HasField("on_failure"):
        of = _spec_from_proto(tenant, spec.on_failure)
        # on_failure is stored as a SubmitSpec-shaped dict on the parent spec.
        on_failure = {
            "task_name": of.task_name,
            "payload": of.payload,
            "on_success": of.on_success,
            "chain_tail": of.chain_tail,
        }
    # ToDatetime() defaults to a tz-NAIVE datetime; the engine compares run_at
    # against datetime.now(UTC) (tz-aware) when deciding queued-vs-deferred, so we
    # must attach UTC here or that comparison raises TypeError (naive vs aware).
    run_at = spec.run_at.ToDatetime(tzinfo=UTC) if spec.HasField("run_at") else None
    max_attempts = spec.retry.max_attempts if spec.HasField("retry") and spec.retry.max_attempts else 5

    return SubmitSpec(
        task_name=spec.task_name,
        payload=_decode_payload(spec.payload_json),
        tenant=tenant,
        pipeline=spec.pipeline or None,
        stage=spec.stage or None,
        ctx_id=spec.ctx_id or None,
        priority=spec.priority,
        group_key=spec.group_key or None,
        max_concurrent_per_group=spec.max_concurrent_per_group or None,
        dedup_key=spec.dedup_key or None,
        runs_on=list(spec.runs_on),
        rate_class=spec.rate_class or None,
        on_success=on_success,
        chain_tail=chain_tail,
        on_failure=on_failure,
        depends_on=depends_on,
        max_attempts=max_attempts,
        timeout_s=spec.timeout_s or 600,
        run_at=run_at,
        lease_ttl_s=spec.lease_ttl_s or 60,
    )


def _set_ts(field: Timestamp, value: object) -> None:
    if value is not None:
        field.FromDatetime(value)  # type: ignore[arg-type]


# Page size for the snapshot ledger replay; a page shorter than this means the
# backlog is exhausted and the snapshot stream can close.
_EVENT_PAGE = 500


def _event_to_proto(row: EventRow) -> common.JobEvent:
    event = common.JobEvent(job_id=row.job_id, event=row.event)
    if row.detail is not None:
        event.detail_json = json.dumps(row.detail).encode()
    _set_ts(event.at, row.at)
    return event


def _job_from_row(row: JobRow) -> common.Job:
    job = common.Job(
        id=row.id,
        tenant=row.tenant,
        state=_STATE_TO_PROTO.get(JobState(row.state), common.JOB_STATE_UNSPECIFIED),
        attempt=row.attempt,
    )
    job.spec.task_name = row.task_name
    if row.ctx_id:
        job.spec.ctx_id = row.ctx_id
    if row.group_key:
        job.spec.group_key = row.group_key
    if row.payload:
        job.spec.payload_json = json.dumps(row.payload).encode()
    if row.result is not None:
        job.result_json = json.dumps(row.result).encode()
    if row.claimed_by:
        job.claimed_by = row.claimed_by
    if row.error_history:
        # Surface the most recent error message for JobStatus.last_error AND the full
        # per-attempt trail for JobFailed.error_history (the SDK reads both).
        last = row.error_history[-1]
        job.last_error = str(last.get("message") or last.get("error_type") or "")
        job.error_history_json = json.dumps(row.error_history).encode()
    _set_ts(job.created_at, row.created_at)
    _set_ts(job.started_at, row.started_at)
    _set_ts(job.finished_at, row.finished_at)
    return job


def _job_from_list_item(item: object) -> common.Job:
    # JobListItem is the lighter list projection (no payload/result bodies).
    job = common.Job(
        id=item.id,  # type: ignore[attr-defined]
        tenant=item.tenant,  # type: ignore[attr-defined]
        state=_STATE_TO_PROTO.get(JobState(item.state), common.JOB_STATE_UNSPECIFIED),  # type: ignore[attr-defined]
        attempt=item.attempt,  # type: ignore[attr-defined]
    )
    job.spec.task_name = item.task_name  # type: ignore[attr-defined]
    if item.ctx_id:  # type: ignore[attr-defined]
        job.spec.ctx_id = item.ctx_id  # type: ignore[attr-defined]
    if item.group_key:  # type: ignore[attr-defined]
        job.spec.group_key = item.group_key  # type: ignore[attr-defined]
    if item.claimed_by:  # type: ignore[attr-defined]
        job.claimed_by = item.claimed_by  # type: ignore[attr-defined]
    _set_ts(job.created_at, item.created_at)  # type: ignore[attr-defined]
    _set_ts(job.started_at, item.started_at)  # type: ignore[attr-defined]
    _set_ts(job.finished_at, item.finished_at)  # type: ignore[attr-defined]
    return job


class ClientServicer(cp_grpc.ClientServiceServicer):
    """gRPC control-plane servicer. Encodes/decodes only; logic lives in services."""

    def __init__(
        self,
        submit: SubmitService,
        fanout: FanOutService,
        cancel: CancelService,
        signals: SignalService,
        resubmit: ResubmitService,
        query: QueryService,
        events: EventStream,
    ) -> None:
        self._submit = submit
        self._fanout = fanout
        self._cancel = cancel
        self._signals = signals
        self._resubmit = resubmit
        self._query = query
        self._events = events

    async def Submit(self, request: cp.SubmitRequest, context: grpc.aio.ServicerContext) -> cp.SubmitResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        specs = [_spec_from_proto(tenant, s) for s in request.specs]
        try:
            outcome = await self._submit.submit(specs)
        except SymbaError as err:
            await _abort(context, err)
            raise
        return cp.SubmitResponse(job_ids=outcome.job_ids, deduplicated=outcome.deduplicated)

    async def FanOut(self, request: cp.FanOutRequest, context: grpc.aio.ServicerContext) -> cp.FanOutResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        try:
            outcome = await self._fanout.fan_out(
                tenant=tenant,
                ctx_id=request.ctx_id or None,
                children=[_spec_from_proto(tenant, c) for c in request.children],
                on_complete=_spec_from_proto(tenant, request.on_complete),
                policy=request.gate_policy or "all_success",
            )
        except (SymbaError, ValueError) as err:
            if isinstance(err, ValueError):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(err))
            await _abort(context, err)  # type: ignore[arg-type]
            raise
        return cp.FanOutResponse(child_job_ids=outcome.child_job_ids, gate_id=outcome.gate_id)

    async def GetGate(self, request: cp.GetGateRequest, context: grpc.aio.ServicerContext) -> cp.GateStatus:
        # Authoritative gate aggregate straight from the gates row. The SDK's
        # Gate.status() calls this instead of recounting child jobs, because only the
        # gate row records succeeded EXCLUDING ctx.skip() children (skips settle the
        # gate as terminal but are not successes).
        tenant = request.tenant or _DEFAULT_TENANT
        row = await self._fanout.gate_status(tenant=tenant, gate_id=request.gate_id)
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"gate {request.gate_id} not found")
        status = cp.GateStatus(
            gate_id=row.gate_id,
            expected=row.expected,
            terminal=row.terminal,
            succeeded=row.succeeded,
        )
        _set_ts(status.fired_at, row.fired_at)
        return status

    async def GetJob(self, request: cp.GetJobRequest, context: grpc.aio.ServicerContext) -> common.Job:
        tenant = request.tenant or _DEFAULT_TENANT
        try:
            row = await self._submit.get_job(job_id=request.job_id, tenant=tenant)
        except SymbaError as err:
            await _abort(context, err)
            raise
        return _job_from_row(row)

    async def AwaitJob(self, request: cp.AwaitJobRequest, context: grpc.aio.ServicerContext) -> common.Job:
        # Server-side long-poll: read get_job on a short cadence until the job is
        # terminal or the caller's timeout elapses. Bounded by timeout_s (default 30s).
        tenant = request.tenant or _DEFAULT_TENANT
        deadline = asyncio.get_event_loop().time() + (request.timeout_s or 30)
        poll_interval_s = 0.5
        while True:
            try:
                row = await self._submit.get_job(job_id=request.job_id, tenant=tenant)
            except SymbaError as err:
                await _abort(context, err)
                raise
            if is_terminal(JobState(row.state)) or asyncio.get_event_loop().time() >= deadline:
                return _job_from_row(row)
            await asyncio.sleep(poll_interval_s)

    async def Cancel(self, request: cp.CancelRequest, context: grpc.aio.ServicerContext) -> cp.CancelResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        try:
            # Tenant guard: get_job raises NotFound (404 / NOT_FOUND) for a foreign or
            # unknown id, so cancel cannot act across tenants.
            prev = await self._submit.get_job(job_id=request.job_id, tenant=tenant)
            outcome = await self._cancel.cancel(job_id=request.job_id, cascade=request.cascade)
        except SymbaError as err:
            await _abort(context, err)
            raise
        return cp.CancelResponse(
            previous_state=_STATE_TO_PROTO.get(JobState(prev.state), common.JOB_STATE_UNSPECIFIED),
            cancelled=outcome.cancelled,
            note=outcome.note,
        )

    async def Signal(self, request: cp.SignalRequest, context: grpc.aio.ServicerContext) -> cp.SignalResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        payload = _decode_payload(request.payload_json)
        try:
            outcome = await self._signals.signal(
                tenant=tenant,
                wait_key=request.wait_key,
                payload=payload,
                signaled_by=request.signaled_by or None,
            )
        except SymbaError as err:
            await _abort(context, err)
            raise
        return cp.SignalResponse(delivered=1 if outcome.delivered else 0)

    async def Resubmit(self, request: cp.ResubmitRequest, context: grpc.aio.ServicerContext) -> cp.SubmitResponse:
        tenant = request.tenant or _DEFAULT_TENANT
        outcomes = await self._resubmit.resubmit_many(job_ids=list(request.job_ids), tenant=tenant)
        # SubmitResponse carries the fresh job ids; a skipped (non-terminal/unknown) id
        # is simply absent from the batch (resubmit_many logs it).
        return cp.SubmitResponse(
            job_ids=[o.new_job_id for o in outcomes],
            deduplicated=[False] * len(outcomes),
        )

    async def Query(self, request: cp.QueryRequest, context: grpc.aio.ServicerContext) -> cp.QueryResponse:
        # The service exposes offset paging; encode the next offset in page_token so
        # the SDK's keyset-style cursor loop terminates when next_page_token is empty.
        tenant = request.tenant or _DEFAULT_TENANT
        page_size = request.page_size or 100
        offset = int(request.page_token) if request.page_token else 0
        state = _PROTO_TO_STATE.get(request.state) if request.state else None
        items = await self._query.jobs(
            tenant=tenant,
            state=state,
            task_name=request.task_name or None,
            ctx_id=request.ctx_id or None,
            parent_gate_id=request.parent_gate_id or None,
            limit=page_size,
            offset=offset,
        )
        jobs = [_job_from_list_item(item) for item in items]
        # A full page implies there may be more; hand back the next offset. A short
        # page is the last one -> empty token stops the SDK's iteration.
        next_token = str(offset + page_size) if len(items) == page_size else ""
        return cp.QueryResponse(jobs=jobs, next_page_token=next_token)

    async def StreamEvents(self, request: cp.StreamEventsRequest, context: grpc.aio.ServicerContext):
        # Two modes on one RPC (see StreamEventsRequest.snapshot):
        #   snapshot=true  -> replay the persisted ledger for ctx_id from the start,
        #                     paging until exhausted, then RETURN (stream closes). This
        #                     is the bounded read JobHandle.events() needs.
        #   snapshot=false -> live tail: forward rows published after we subscribe,
        #                     forever, for the UI stream. The SDK reconnects/de-dupes.
        tenant = request.tenant or _DEFAULT_TENANT
        ctx_id = request.ctx_id or None

        if request.snapshot:
            if ctx_id is None:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "snapshot StreamEvents requires ctx_id"
                )
            after_id = 0
            while True:
                rows = await self._events.snapshot_for_ctx(
                    tenant=tenant, ctx_id=ctx_id, after_id=after_id
                )
                for row in rows:
                    yield _event_to_proto(row)
                if len(rows) < _EVENT_PAGE:
                    return  # short page: backlog exhausted, close the stream
                after_id = rows[-1].id
            # unreachable

        async with self._events.subscribe(tenant=tenant) as queue:
            while True:
                row = await queue.get()
                if ctx_id is not None and row.ctx_id != ctx_id:
                    continue
                yield _event_to_proto(row)
