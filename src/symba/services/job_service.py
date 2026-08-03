# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Worker-facing job lifecycle service.

Owns the transaction boundaries and business rules for the terminal transitions
a worker drives: complete, fail (retry-or-die), and the lazy result read. The
gRPC WorkerServiceServicer is a thin adapter over this (workspace rule: logic in
services, transports decode/encode only).

    complete — one tx
    ----------------------------
    1. complete_move       hot -> archive (succeeded), lease-guarded
    2. decrement_group     release the group slot
    3. record_event        append 'succeeded' to the audit ledger
    (chain fan-out / gate settle land in M2; the TerminalRow carries the fields.)

    fail — one tx, retry-or-die
    --------------------------------------
    1. fail_peek           FOR UPDATE read of attempt/max_attempts/backoff
    2. should_retry?  yes  -> fail_retry (RUNNING -> QUEUED with backoff run_at)
                       no   -> fail_die + decrement_group  (RUNNING -> DEAD)
    3. record_event
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from symba.config import SymbaConfig
from symba.core import chain
from symba.core.errors import ResultTooLarge, StaleLease
from symba.core.retry import BackoffPolicy, should_retry
from symba.core.states import JobState
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.observability import metrics
from symba.observability.logging import logger
from symba.observability.tracing import engine_span
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="job_service", context="engine/services")


@dataclass(slots=True)
class FailOutcome:
    accepted: bool
    will_retry: bool


class JobService:
    def __init__(self, pools: Pools, config: SymbaConfig, registry: WorkerRegistry | None = None) -> None:
        self._pools = pools
        self._max_result_bytes = config.limits.max_result_kb * 1024
        # Optional: when present, a completed job that flips dependents to 'queued'
        # wakes the dispatcher this tick instead of waiting out the idle-decay
        # timer. None in unit/L2 setups that don't dispatch.
        self._registry = registry

    async def complete(
        self,
        *,
        job_id: str,
        lease_token: str,
        result: dict[str, Any] | None,
        drop_chain_tail: bool = False,
        skipped: bool = False,
    ) -> bool:
        # Backpressure: a result over the cap must be stored by reference, not
        # inlined. Reject BEFORE the archive move so a stale worker cannot bloat
        # the archive; the lease still expires and the job retries.
        if result is not None:
            size = len(json.dumps(result).encode())
            if size > self._max_result_bytes:
                raise ResultTooLarge(job_id=job_id, size_bytes=size, cap_bytes=self._max_result_bytes)

        # Everything below is ONE transaction: terminal move + group release +
        # chain continuation + ledger. A crash rolls the whole thing back, so a
        # chain never loses a link (the predecessor archives and the
        # continuation exists together, or neither does).
        continuation_id: str | None = None
        flipped_dependents: list[str] = []
        gate_fired = False
        # `complete` span wraps the whole terminal tx; the engine-side terminal
        # of the per-job trace, joined app-side via ctx_id. No-op unless OTLP is set.
        # ctx_id is unknown until the move returns, so it is stamped inside the block;
        # the span still opens here so the DB time is captured.
        with engine_span("symba.complete", ctx_id=None, job_id=job_id):
            async with self._pools.acquire_hot() as conn, conn.transaction():
                term = await repo.complete_move(conn, job_id=job_id, lease_token=lease_token, result=result)
                if term is None:
                    raise StaleLease(job_id=job_id)
                await repo.decrement_group(
                    conn, tenant=term.tenant, group_key=term.group_key, task_name=term.task_name
                )

                # Chain continuation: advance unless the worker called stop_chain.
                if not drop_chain_tail:
                    step = chain.advance(term.on_success, tuple(term.chain_tail))
                    if step is not None:
                        continuation_id = await repo.insert_continuation(
                            conn,
                            next_task=step.next_task,
                            next_on_success=step.on_success,
                            next_chain_tail=list(step.chain_tail),
                            predecessor=term,
                        )

                # depends_on flip: every dependent of this job loses one
                # remaining_dep; any that hit zero AND are due flip to 'queued' here, in
                # the same tx, so they never become claimable before this job is durable.
                flipped_dependents = await repo.decrement_deps(conn, upstream_job_id=job_id)

                # Ledger BEFORE gate settle: a ctx.skip() archives as succeeded but
                # writes event='skipped' so __gate__.results (read inside settle) can
                # exclude it — including when THIS child is the one that fires the gate.
                await repo.record_event(conn, job_id=job_id, event="skipped" if skipped else "succeeded")

                # Gate settle: a child of a fan-out gate bumps its barrier;
                # the tx that crosses the policy threshold fires the continuation once.
                # A ctx.skip() child is a NON-failure that does NOT count toward
                # succeeded (spec 7.2): it settles the gate without advancing the
                # success count, so an all-skipped gate still fires with succeeded=0.
                gate_fired = await self._settle_gate(
                    conn,
                    parent_gate_id=term.parent_gate_id,
                    child_succeeded=not skipped,
                    child_failed=False,
                )
        if (flipped_dependents or gate_fired or continuation_id) and self._registry is not None:
            self._registry.wake.set()  # newly-claimable work exists
        metrics.jobs_total.labels(tenant=term.tenant, task=term.task_name, state="succeeded").inc()
        metrics.archive_moved_total.labels(final_state="succeeded").inc()
        # Latency histograms, from DB clocks captured in complete.sql (one clock).
        if term.started_at is not None and term.finished_at is not None:
            metrics.job_duration_seconds.labels(task=term.task_name).observe(
                (term.finished_at - term.started_at).total_seconds()
            )
        if term.started_at is not None and term.created_at is not None:
            metrics.job_queue_wait_seconds.labels(task=term.task_name).observe(
                max(0.0, (term.started_at - term.created_at).total_seconds())
            )
        logger.info(
            "[complete] Job succeeded",
            job_id=job_id,
            task=term.task_name,
            continuation_id=continuation_id,
            chain_stopped=drop_chain_tail,
            dependents_ready=len(flipped_dependents),
            gate_fired=gate_fired,
        )
        return True

    async def _settle_gate(
        self,
        conn: Any,
        *,
        parent_gate_id: str | None,
        child_succeeded: bool,
        child_failed: bool,
    ) -> bool:
        """Bump the child's gate; materialize the continuation on fire.

        Returns True iff THIS child's completion fired the gate (claim-once), so the
        caller knows to wake the dispatcher. Runs in the child's terminal tx.

        Child outcome (spec 7.2): success -> (True, False); dead -> (False, True);
        skip -> (False, False). Only a DEAD child blocks all_success.

        On fire, the aggregate manifest is merged into the continuation's payload
        under the reserved ``__gate__`` key, preserving the caller's on_complete
        payload. Downstream handlers read ``ctx.payload["__gate__"]`` for counts
        and per-child ``results`` (SDK-fake shape: job_id/task/result).
        """
        if parent_gate_id is None:
            return False
        fire = await repo.bump_gate(
            conn, gate_id=parent_gate_id, child_succeeded=child_succeeded, child_failed=child_failed
        )
        if fire is None or not fire.fired:
            return False
        # Per-child results for assemble/reduce handlers. Must run after the
        # firing child's archive move (caller ordering) so jobs_archive is complete.
        child_results = await repo.gate_child_results(conn, gate_id=parent_gate_id)
        # The gate's stored on_complete is a SubmitSpec-shaped dict; materialize it
        # as a fresh QUEUED job in the SAME tx so the continuation is durable with
        # the fire and immediately claimable. remaining_deps=0: the gate WAS the
        # barrier, so the continuation has no further deps to satisfy.
        oc = _merge_gate_manifest(
            fire.on_complete, gate_id=parent_gate_id, fire=fire, results=child_results
        )
        spec = SubmitSpec(**{**oc, "tenant": fire.tenant, "ctx_id": fire.ctx_id, "state": JobState.QUEUED})
        await repo.submit(conn, spec)
        return True

    async def fail(
        self,
        *,
        job_id: str,
        lease_token: str,
        error_type: str,
        error_message: str,
        stack_hash: str,
        retryable: bool,
        worker_max_attempts: int | None = None,
    ) -> FailOutcome:
        error_entry = _error_entry(error_type, error_message, stack_hash, retryable)
        gate_fired = False
        async with self._pools.acquire_hot() as conn, conn.transaction():
            peek = await repo.fail_peek(conn, job_id=job_id, lease_token=lease_token)
            if peek is None:
                raise StaleLease(job_id=job_id)

            attempt = peek["attempt"]
            # The retry LIMIT is a worker-side task property (@task max_attempts) the
            # submitter may not know, so a worker-reported cap wins over the stored
            # default. None/0 -> fall back to the job's stored max_attempts.
            max_attempts = worker_max_attempts or peek["max_attempts"]
            will_retry = should_retry(attempt, max_attempts, retryable)
            if will_retry:
                next_run_at = _next_run_at(attempt, peek["backoff"])
                await repo.fail_retry(
                    conn, job_id=job_id, lease_token=lease_token, next_run_at=next_run_at, error_entry=error_entry
                )
                await repo.record_event(conn, job_id=job_id, event="retry_scheduled", detail=error_entry)
                logger.info(
                    "[fail] Retry scheduled", job_id=job_id, attempt=attempt, next_run_at=next_run_at.isoformat()
                )
                return FailOutcome(accepted=True, will_retry=True)

            term = await repo.fail_die(conn, job_id=job_id, lease_token=lease_token, error_entry=error_entry)
            if term is None:  # lost the race between peek and die (extremely rare)
                raise StaleLease(job_id=job_id)
            await repo.decrement_group(conn, tenant=term.tenant, group_key=term.group_key, task_name=term.task_name)
            # A dead child still settles its gate (counts toward all_terminal/quorum,
            # never toward all_success); the gate fires once if the policy allows it.
            gate_fired = await self._settle_gate(
                conn, parent_gate_id=term.parent_gate_id, child_succeeded=False, child_failed=True
            )

            # on_failure hook: materialize the error handler as a fresh job
            # in the SAME tx, so a death always leaves its recovery step enqueued.
            hook_id: str | None = None
            if term.on_failure:
                hook_spec = SubmitSpec(
                    **{**term.on_failure, "tenant": term.tenant, "ctx_id": term.ctx_id, "state": JobState.QUEUED}
                )
                hook_res = await repo.submit(conn, hook_spec)
                hook_id = hook_res.job_id

            # Cascade-cancel: a dead upstream can never satisfy its dependents,
            # so archive the whole live dependent cone as 'cancelled' and audit each.
            cancelled = await repo.cascade_cancel(conn, root_job_id=job_id)
            for dep_id in cancelled:
                await repo.record_event(
                    conn, job_id=dep_id, event="dependency_cancelled", detail={"root_cause": job_id}
                )

            await repo.record_event(conn, job_id=job_id, event="dead", detail=error_entry)

        if (gate_fired or hook_id) and self._registry is not None:
            self._registry.wake.set()
        metrics.jobs_total.labels(tenant=term.tenant, task=term.task_name, state="dead").inc()
        metrics.archive_moved_total.labels(final_state="dead").inc()
        logger.warning("[fail] Job dead", job_id=job_id, task=term.task_name, error_type=error_type)
        return FailOutcome(accepted=True, will_retry=False)

    async def heartbeat(self, *, job_id: str, lease_token: str) -> tuple[datetime, bool]:
        """Extend the lease; return (new_expiry, cancelled). Raises StaleLease on 0 rows."""
        async with self._pools.acquire_hot() as conn:
            row = await repo.heartbeat(conn, job_id=job_id, lease_token=lease_token)
        if row is None:
            raise StaleLease(job_id=job_id)
        return row["lease_expires_at"], row["cancel_requested"]

    async def get_result(
        self, *, job_id: str, tenant: str, task_name: str | None = None
    ) -> tuple[bytes, bool]:
        # task_name set (lazy ctx.output.fetch): job_id is the ASKING job (authz
        # scope) and we resolve the ancestor named task_name within its ctx_id.
        # task_name empty (direct result read): job_id IS the target.
        async with self._pools.acquire_hot() as conn:
            if task_name:
                row = await repo.get_ancestor_result(
                    conn, asking_job_id=job_id, task_name=task_name, tenant=tenant
                )
            else:
                row = await repo.get_result(conn, job_id=job_id, tenant=tenant)
        if row is None or row.result is None:
            metrics.get_result_total.labels(source="miss").inc()
            return b"", False
        metrics.get_result_total.labels(source="hit").inc()
        return json.dumps(row.result).encode(), True


def _merge_gate_manifest(
    on_complete: dict[str, Any],
    *,
    gate_id: str,
    fire: repo.GateFire,
    results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Merge the gate manifest into the continuation's payload under ``__gate__``.

    The caller's ``on_complete.payload`` is PRESERVED; the aggregate manifest is
    added under the reserved ``__gate__`` key so a continuation handler can read
    both its own payload and the gate's expected/succeeded counts plus per-child
    results (SDK-fake shape).

    Shape (matches the SDK fake ``testing/fake_engine._maybe_fire_gate``):
        {**caller_payload, "__gate__": {gate_id, results, expected, succeeded}}
    """
    oc = dict(on_complete)
    payload = dict(oc.get("payload") or {})
    payload["__gate__"] = {
        "gate_id": gate_id,
        "results": list(results or []),
        "expected": fire.expected,
        "succeeded": fire.succeeded,
    }
    oc["payload"] = payload
    return oc


def _error_entry(error_type: str, message: str, stack_hash: str, retryable: bool) -> dict[str, Any]:
    return {
        "type": error_type,
        "message": message[:2048],  # 2KB cap (proto contract)
        "stack_hash": stack_hash,
        "retryable": retryable,
        "at": datetime.now(UTC).isoformat(),
    }


def _next_run_at(attempt: int, backoff: dict[str, Any] | None) -> datetime:
    cfg = backoff or {}
    policy = BackoffPolicy(
        base_s=cfg.get("base_s", 1.0),
        factor=cfg.get("factor", 2.0),
        cap_s=cfg.get("cap_s", 300.0),
        jitter=cfg.get("jitter", True),
    )
    return datetime.now(UTC) + timedelta(seconds=policy.delay(attempt))
