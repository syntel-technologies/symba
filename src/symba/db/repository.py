# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Hot-path repository: binds queries.Q to asyncpg and owns SQL execution only.

Layering (workspace rule): routers -> services -> repositories. Services own
business rules, transaction boundaries, and pool selection; this module owns
nothing but "run this SQL, shape the rows". Multi-statement transactions
(complete, fail-die, cancel-tree) are composed here as small functions that
take an already-open connection, so the SERVICE controls the transaction scope
(async with conn.transaction(): ...).

    invariants carried from the .sql headers
    ----------------------------------------
    X1  claim revalidates run_at <= now() inside the locking statement.
    X2  every time comparison uses DB now(); no client timestamps.
    lease-guard: complete/fail/fail_die match on lease_token; 0 rows -> StaleLease.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from symba.db.queries import Q
from symba.db.records import (
    ClaimedJob,
    CronDue,
    CronRow,
    EventRow,
    ExhaustedLease,
    InlineUpstream,
    JobListItem,
    JobRow,
    QueueStat,
    ResultRow,
    SubmitResult,
    SubmitSpec,
    TerminalRow,
    TreeEdge,
    WorkerRow,
)


def _submit_args(spec: SubmitSpec) -> tuple[Any, ...]:
    return (
        spec.task_name,
        spec.payload,
        spec.pipeline,
        spec.stage,
        spec.tenant,
        spec.ctx_id,
        spec.state,
        spec.priority,
        spec.group_key,
        spec.max_concurrent_per_group,
        spec.dedup_key,
        spec.runs_on,
        spec.rate_class,
        spec.on_success,
        spec.chain_tail,
        spec.on_failure,
        spec.remaining_deps,
        spec.max_attempts,
        spec.backoff,
        spec.timeout_s,
        spec.run_at,
        spec.lease_ttl_s,
        spec.parent_gate_id,
    )


async def submit(conn: asyncpg.Connection, spec: SubmitSpec) -> SubmitResult:
    """Insert one job. A dedup hit returns the EXISTING id with deduplicated=True.

    submit.sql uses ON CONFLICT DO UPDATE (no-op touch) + `xmax <> 0` so the
    conflicting row's id comes back on a dedup hit instead of zero rows; that lets
    the caller echo h1.id for a duplicate submit (idempotent, NOT an error).
    """
    row = await conn.fetchrow(Q.SUBMIT, *_submit_args(spec))
    if row is None:
        return SubmitResult(job_id=None, deduplicated=True)
    return SubmitResult(job_id=str(row["id"]), deduplicated=bool(row["deduplicated"]))


async def count_tenant_live(conn: asyncpg.Connection, *, tenant: str) -> int:
    """A tenant's live (non-terminal) job count for the queue-cap gate."""
    return int(await conn.fetchval(Q.COUNT_TENANT_LIVE, tenant) or 0)


async def claim(
    conn: asyncpg.Connection,
    *,
    worker_tags: list[str],
    exhausted_rate_classes: list[str],
    limit: int,
    claimed_by: str,
    per_group_cap: int,
    rate_quotas: dict[str, int] | None = None,
) -> list[ClaimedJob]:
    """THE hot query. One transaction; caller opens it."""
    rows = await conn.fetch(
        Q.CLAIM,
        worker_tags,
        exhausted_rate_classes,
        limit,
        claimed_by,
        per_group_cap,
        rate_quotas or {},
    )
    return [_to_claimed(r) for r in rows]


async def running_counts_by_worker(
    conn: asyncpg.Connection,
    *,
    worker_ids: list[str],
) -> dict[str, int]:
    """Return live attributed-job counts for the requested workers."""
    if not worker_ids:
        return {}
    rows = await conn.fetch(Q.RUNNING_COUNTS_BY_WORKER, worker_ids)
    return {str(row["worker_id"]): int(row["running_count"]) for row in rows}


async def set_claimed_by(
    conn: asyncpg.Connection,
    *,
    job_ids: list[str],
    worker_ids: list[str],
    lease_tokens: list[str],
) -> None:
    """Re-stamp just-assigned jobs with their real worker_id.

    ONE statement per dispatch pass: the three lists are parallel (job_ids[i] was
    assigned to worker_ids[i] under lease_tokens[i]). No-op on empty input.
    """
    if not job_ids:
        return
    await conn.execute(Q.SET_CLAIMED_BY, job_ids, worker_ids, lease_tokens)


def _to_claimed(r: asyncpg.Record) -> ClaimedJob:
    d = dict(r)
    return ClaimedJob(
        id=str(d["id"]),
        task_name=d["task_name"],
        tenant=d["tenant"],
        group_key=d["group_key"],
        max_concurrent_per_group=d["max_concurrent_per_group"],
        priority=d["priority"],
        lease_token=d["lease_token"],
        lease_ttl_s=d["lease_ttl_s"],
        payload=d["payload"],
        attempt=d["attempt"],
        raw=d,
    )


def _terminal(r: asyncpg.Record) -> TerminalRow:
    d = dict(r)
    return TerminalRow(
        id=str(d["id"]),
        tenant=d["tenant"],
        ctx_id=d.get("ctx_id"),
        task_name=d["task_name"],
        group_key=d.get("group_key"),
        max_concurrent_per_group=d.get("max_concurrent_per_group"),
        on_success=d.get("on_success"),
        chain_tail=d.get("chain_tail") or [],
        on_failure=d.get("on_failure"),
        parent_gate_id=(str(d["parent_gate_id"]) if d.get("parent_gate_id") else None),
        prior_state=d.get("prior_state"),
        pipeline=d.get("pipeline"),
        stage=d.get("stage"),
        priority=d.get("priority", 0),
        runs_on=d.get("runs_on") or [],
        rate_class=d.get("rate_class"),
        lease_ttl_s=d.get("lease_ttl_s", 60),
        created_at=d.get("created_at"),
        started_at=d.get("started_at"),
        finished_at=d.get("finished_at"),
    )


async def complete_move(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
    result: dict[str, Any] | None,
) -> TerminalRow | None:
    """Statement 1 of Complete: atomic hot->archive move, lease-guarded.

    Returns None when 0 rows moved (stale lease / already terminal). Statements
    2-6 (counter, chain, deps, gate, ledger) are the service's job in the same tx.
    """
    row = await conn.fetchrow(Q.COMPLETE, job_id, lease_token, result, "succeeded")
    return _terminal(row) if row is not None else None


async def insert_continuation(
    conn: asyncpg.Connection,
    *,
    next_task: str,
    next_on_success: str | None,
    next_chain_tail: list[str],
    predecessor: TerminalRow,
) -> str:
    """Statement 3 of Complete: enqueue the chain continuation.

    Runs in the SAME tx as the terminal move. Inherits lineage + routing +
    on_failure from the just-succeeded predecessor so the chain stays in one
    pipeline/lane/stage and a DEAD tail still fires the submitter's failure hook.
    """
    new_id = await conn.fetchval(
        Q.COMPLETE_CONTINUATION,
        next_task,
        next_on_success,
        next_chain_tail,
        predecessor.tenant,
        predecessor.ctx_id,
        predecessor.pipeline,
        predecessor.stage,
        predecessor.priority,
        predecessor.group_key,
        predecessor.max_concurrent_per_group,
        predecessor.runs_on,
        predecessor.rate_class,
        predecessor.lease_ttl_s,
        predecessor.on_failure,
    )
    return str(new_id)


async def fetch_inline_upstream(conn: asyncpg.Connection, *, job_ids: list[str]) -> dict[str, list[InlineUpstream]]:
    """Batch-load the Job.upstream inline tier for a claim assignment batch.

    Returns asking_job_id -> producers (chain predecessor + depends_on). Empty
    input yields an empty dict (no round-trip).
    """
    if not job_ids:
        return {}
    rows = await conn.fetch(Q.INLINE_UPSTREAM, job_ids)
    out: dict[str, list[InlineUpstream]] = {}
    for r in rows:
        item = InlineUpstream(
            asking_job_id=str(r["asking_job_id"]),
            key=r["key"],
            producer_job_id=str(r["producer_job_id"]),
            result=r["result"],
        )
        out.setdefault(item.asking_job_id, []).append(item)
    return out


async def decrement_deps(conn: asyncpg.Connection, *, upstream_job_id: str) -> list[str]:
    """Statement 4 of Complete: decrement dependents, flip ready.

    Runs in the SAME tx as the terminal move. Returns the ids that flipped to
    'queued' so the service can wake the dispatcher for them.
    """
    rows = await conn.fetch(Q.DECREMENT_DEPS, upstream_job_id)
    return [str(r["id"]) for r in rows if r["flipped"]]


async def insert_dependency(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    depends_on_job_id: str,
    alias: str | None,
) -> None:
    """Record one depends_on edge. Idempotent on the PK."""
    await conn.execute(Q.INSERT_DEPENDENCY, job_id, depends_on_job_id, alias)


async def create_gate(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    ctx_id: str | None,
    policy: str,
    expected_children: int,
    quorum_n: int | None,
    on_complete: dict[str, Any],
) -> str:
    """Open a fan-out gate. Runs in the FanOut submit tx."""
    gate_id = await conn.fetchval(Q.CREATE_GATE, tenant, ctx_id, policy, expected_children, quorum_n, on_complete)
    return str(gate_id)


@dataclass(slots=True)
class GateFire:
    fired: bool
    on_complete: dict[str, Any]
    tenant: str
    ctx_id: str | None
    # Post-bump counts, fed into the __gate__ manifest on the continuation payload.
    # `succeeded` excludes skips and DEAD children; `expected` is the child count.
    expected: int
    succeeded: int


async def bump_gate(
    conn: asyncpg.Connection, *, gate_id: str, child_succeeded: bool, child_failed: bool
) -> GateFire | None:
    """Settle one child into its gate; claim-once fire.

    Returns None when the gate row is gone (never happens in a healthy tx). The
    `fired` flag is True for exactly the tx that crossed the policy threshold.

    Outcome encoding (spec 7.2): success -> (True, False); dead -> (False, True);
    skip -> (False, False) — a non-failure no-op that advances only
    completed_children.
    """
    row = await conn.fetchrow(Q.BUMP_GATE, gate_id, child_succeeded, child_failed)
    if row is None:
        return None
    return GateFire(
        fired=bool(row["fired"]),
        on_complete=row["on_complete"],
        tenant=row["tenant"],
        ctx_id=row["ctx_id"],
        expected=row["expected_children"],
        succeeded=row["succeeded_children"],
    )


async def gate_child_results(conn: asyncpg.Connection, *, gate_id: str) -> list[dict[str, Any]]:
    """Succeeded children's results for ``__gate__.results`` (SDK-fake shape).

    Call only after the firing child's archive move — all successes are then in
    ``jobs_archive``. Returns ``[{job_id, task, result}, ...]`` ordered by finish.
    """
    rows = await conn.fetch(Q.GATE_CHILD_RESULTS, gate_id)
    return [
        {
            "job_id": str(r["job_id"]),
            "task": r["task"],
            "result": r["result"],
        }
        for r in rows
    ]


@dataclass(slots=True)
class GateStatusRow:
    gate_id: str
    expected: int
    terminal: int  # completed_children (succeeded + failed + skipped)
    succeeded: int  # excludes skips + failures
    fired_at: datetime | None


async def get_gate(conn: asyncpg.Connection, *, tenant: str, gate_id: str) -> GateStatusRow | None:
    """Read the authoritative gate aggregate for Gate.status() (spec 7.2)."""
    row = await conn.fetchrow(Q.GET_GATE, tenant, gate_id)
    if row is None:
        return None
    return GateStatusRow(
        gate_id=str(row["id"]),
        expected=int(row["expected_children"]),
        terminal=int(row["completed_children"]),
        succeeded=int(row["succeeded_children"]),
        fired_at=row["fired_at"],
    )


async def cascade_cancel(conn: asyncpg.Connection, *, root_job_id: str) -> list[str]:
    """Archive the whole live dependent cone of a dead/cancelled job.

    Runs in the root's terminal tx. Returns the ids cancelled so the service can
    write one 'dependency_cancelled' ledger row per node naming the root cause.
    """
    rows = await conn.fetch(Q.CASCADE_CANCEL, root_job_id)
    return [str(r["id"]) for r in rows]


async def heartbeat(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
) -> asyncpg.Record | None:
    """Extend the lease, return (lease_expires_at, cancel_requested). None -> stale."""
    return await conn.fetchrow(Q.HEARTBEAT, job_id, lease_token)


async def fail_peek(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
) -> asyncpg.Record | None:
    """Lease-guarded FOR UPDATE read of retry inputs. None -> stale lease.

    Must run first in the Fail transaction so the row is locked before the
    service decides retry-vs-die and issues fail/fail_die against the same row.
    """
    return await conn.fetchrow(Q.FAIL_PEEK, job_id, lease_token)


async def fail_retry(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
    next_run_at: datetime,
    error_entry: dict[str, Any],
) -> asyncpg.Record | None:
    """RUNNING -> QUEUED with backoff (retry shape), lease-guarded."""
    return await conn.fetchrow(Q.FAIL, job_id, lease_token, next_run_at, error_entry)


async def fail_die(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
    error_entry: dict[str, Any],
) -> TerminalRow | None:
    """RUNNING -> DEAD archive move (die shape). None on stale lease."""
    row = await conn.fetchrow(Q.FAIL_DIE, job_id, lease_token, error_entry)
    return _terminal(row) if row is not None else None


async def cancel_immediate(conn: asyncpg.Connection, *, job_id: str) -> TerminalRow | None:
    """Non-running cancel: submitted/queued/waiting -> CANCELLED archive."""
    row = await conn.fetchrow(Q.CANCEL, job_id)
    return _terminal(row) if row is not None else None


async def cancel_running(conn: asyncpg.Connection, *, job_id: str) -> str | None:
    """Cooperative cancel of a RUNNING job. Returns claimed_by or None."""
    row = await conn.fetchrow(Q.CANCEL_RUNNING, job_id)
    return row["claimed_by"] if row is not None else None


async def get_job(conn: asyncpg.Connection, *, job_id: str, tenant: str) -> JobRow | None:
    """Single-job read across hot + archive. None -> unknown/foreign tenant."""
    row = await conn.fetchrow(Q.GET_JOB, job_id, tenant)
    if row is None:
        return None
    return JobRow(
        id=str(row["id"]),
        tenant=row["tenant"],
        task_name=row["task_name"],
        state=row["state"],
        attempt=row["attempt"],
        priority=row["priority"],
        group_key=row["group_key"],
        ctx_id=row["ctx_id"],
        claimed_by=row["claimed_by"],
        payload=row["payload"],
        result=row["result"],
        error_history=row["error_history"] or [],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


async def get_result(conn: asyncpg.Connection, *, job_id: str, tenant: str) -> ResultRow | None:
    """Lazy result fetch across hot+archive via jobs_all."""
    row = await conn.fetchrow(Q.GET_RESULT, job_id, tenant)
    if row is None:
        return None
    return ResultRow(
        id=str(row["id"]),
        state=row["state"],
        result=row["result"],
        error_history=row["error_history"] or [],
    )


async def get_ancestor_result(
    conn: asyncpg.Connection, *, asking_job_id: str, task_name: str, tenant: str
) -> ResultRow | None:
    """Resolve an ancestor's result by task_name within the asking job's ctx_id.

    Backs the lazy GetResult tier for chain/depends_on outputs (spec 10.3): the
    running job asks for a named ancestor, NOT its own result.
    """
    row = await conn.fetchrow(Q.GET_ANCESTOR_RESULT, asking_job_id, task_name, tenant)
    if row is None:
        return None
    return ResultRow(
        id=str(row["id"]),
        state=row["state"],
        result=row["result"],
        error_history=row["error_history"] or [],
    )


async def decrement_group(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    group_key: str | None,
    task_name: str,
    by: int = 1,
) -> None:
    """Release group slot(s) on terminal/retry.

    No-op when the job carried no group cap (group_running has no row).
    """
    if group_key is None:
        return
    await conn.execute(Q.DECREMENT_GROUP, tenant, group_key, task_name, by)


async def record_event(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    event: str,
    detail: dict[str, Any] | None = None,
) -> None:
    """Append one audit row. job_events is append-only."""
    await conn.execute(Q.RECORD_EVENT, job_id, event, detail)


async def sweep_leases(
    conn: asyncpg.Connection,
    *,
    limit: int,
    stale_worker_grace_s: int = 45,
) -> int:
    """Reclaim expired leases and jobs abandoned by a missing worker."""
    return await conn.fetchval(Q.SWEEP_LEASES, limit, stale_worker_grace_s) or 0


async def list_exhausted_leases(
    conn: asyncpg.Connection,
    *,
    limit: int,
    stale_worker_grace_s: int = 45,
) -> list[ExhaustedLease]:
    """Return abandoned running jobs that have consumed every attempt."""
    rows = await conn.fetch(Q.LIST_EXHAUSTED_LEASES, limit, stale_worker_grace_s)
    return [ExhaustedLease(job_id=str(row["id"]), lease_token=str(row["lease_token"])) for row in rows]


async def sweep_waits(conn: asyncpg.Connection, *, limit: int) -> list[str]:
    """Expire WAITING jobs past deadline. Returns released job ids."""
    rows = await conn.fetch(Q.SWEEP_WAITS, limit)
    return [str(r["id"]) for r in rows]


async def signal_wake(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    wait_key: str,
    payload: dict[str, Any],
) -> asyncpg.Record | None:
    """Wake the oldest WAITING job for (tenant, wait_key) (wait-first)."""
    return await conn.fetchrow(Q.SIGNAL, tenant, wait_key, payload)


async def signal_park(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    wait_key: str,
    payload: dict[str, Any] | None,
    signaled_by: str | None,
) -> str:
    """Store a signal for a future waiter (signal-first). Returns the row id."""
    return str(await conn.fetchval(Q.SIGNAL_INSERT, tenant, wait_key, payload, signaled_by))


async def wait_consume(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    wait_key: str,
) -> dict[str, Any] | None:
    """Consume a pending signal if one exists (wait-first).

    Returns the parked payload (never parks the job), or None if no signal is pending
    (the caller then parks via wait_park). A JSON `null` payload comes back as None too;
    callers treat "consumed with no payload" and "nothing to consume" distinctly only
    via the row count, so wait_park is called ONLY when this returned no row — which the
    service tracks with a sentinel, not by inspecting the payload.
    """
    row = await conn.fetchrow(Q.WAIT_CONSUME, tenant, wait_key)
    if row is None:
        return None
    return row["payload"] if row["payload"] is not None else {}


async def wait_park(
    conn: asyncpg.Connection,
    *,
    job_id: str,
    lease_token: str,
    wait_key: str,
    timeout_s: int,
) -> str | None:
    """Park a RUNNING job into WAITING, lease-guarded. id or None if stale."""
    return await conn.fetchval(Q.WAIT_PARK, job_id, lease_token, wait_key, timeout_s)


_SWEEPER_LOCK_KEY = "symba:sweeper"


async def try_sweeper_lock(conn: asyncpg.Connection) -> bool:
    """Non-blocking session-scoped election. True -> this engine sweeps.

    The lock is SESSION-scoped (not xact): held for the whole pass on this
    connection and auto-released if the holder dies (election is the only
    justified advisory-lock use in a queue). The loser simply skips the pass.
    """
    return bool(await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", _SWEEPER_LOCK_KEY))


async def unlock_sweeper(conn: asyncpg.Connection) -> None:
    await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", _SWEEPER_LOCK_KEY)


async def reconcile_group_running(conn: asyncpg.Connection) -> int:
    """Self-heal group_running drift against actual running counts.

    Returns the number of counter rows corrected.
    """
    result = await conn.execute(Q.RECONCILE_GROUPS)
    return int(result.split()[-1]) if result.startswith("UPDATE") else 0


# ── /metrics gauge refresh ────────────────────────────────────────────────────


async def metrics_queue_gauges(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Depth per (task,state), oldest queued age per task, WAITING per tenant.

    A tagged union (metric column) so ONE query feeds three Prometheus gauges.
    Returned as raw Records — the sweeper fans them out to metric families.
    """
    return await conn.fetch(Q.METRICS_QUEUE_GAUGES)


async def metrics_gate_age(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Age (seconds) of the oldest UNFIRED gate per policy."""
    return await conn.fetch(Q.METRICS_GATE_AGE)


async def metrics_pg_xact_age(conn: asyncpg.Connection) -> float:
    """Age (seconds) of the oldest in-progress transaction (MVCC horizon)."""
    return float(await conn.fetchval(Q.METRICS_PG_XACT_AGE) or 0.0)


# ── rate classes ─────────────────────────────────────────────────────────────


async def distinct_rate_classes(conn: asyncpg.Connection, *, worker_tags: list[str]) -> list[str]:
    """Peek the rate classes present in the ready set for these tags."""
    rows = await conn.fetch(Q.DISTINCT_RATE_CLASSES, worker_tags)
    return [r["rate_class"] for r in rows]


async def rate_take_pg(conn: asyncpg.Connection, *, rate_class: str, want: int) -> int:
    """PG-fallback refill-and-take for one class. Returns granted (0..want)."""
    return await conn.fetchval(Q.RATE_TAKE_PG, rate_class, want) or 0


async def rate_return_pg(conn: asyncpg.Connection, *, rate_class: str, tokens: int) -> None:
    """Return unused reserved tokens to the PG-fallback bucket."""
    if tokens <= 0:
        return
    await conn.execute(Q.RATE_RETURN_PG, rate_class, tokens)


async def rate_classes_all(conn: asyncpg.Connection) -> list[tuple[str, float, float]]:
    """Load the full rate-class config for the in-memory cache."""
    rows = await conn.fetch(Q.RATE_CLASSES_ALL)
    return [(r["name"], float(r["capacity"]), float(r["refill_per_s"])) for r in rows]


async def rate_class_upsert(conn: asyncpg.Connection, *, name: str, capacity: float, refill_per_s: float) -> None:
    """Create/update a rate-class config via the admin API."""
    await conn.execute(Q.RATE_CLASS_UPSERT, name, capacity, refill_per_s)


# ── cron ─────────────────────────────────────────────────────────────────────


async def cron_due(conn: asyncpg.Connection) -> list[CronDue]:
    """Enabled schedules whose next_fire has arrived. DB now(), no lock."""
    rows = await conn.fetch(Q.CRON_DUE)
    return [
        CronDue(
            schedule_id=r["schedule_id"],
            cron_expr=r["cron_expr"],
            task_name=r["task_name"],
            payload=r["payload"],
            tenant=r["tenant"],
            last_fire=r["last_fire"],
            next_fire=r["next_fire"],
        )
        for r in rows
    ]


async def cron_advance(
    conn: asyncpg.Connection,
    *,
    schedule_id: str,
    new_next_fire: datetime,
    expected_next_fire: datetime | None,
    fired_at: datetime | None,
) -> bool:
    """CAS-advance a schedule's window. True iff this engine won the advance."""
    won = await conn.fetchval(Q.CRON_ADVANCE, schedule_id, new_next_fire, expected_next_fire, fired_at)
    return won is not None


# ── checkpoints ───────────────────────────────────────────────────────────────


async def checkpoint_put(conn: asyncpg.Connection, *, job_id: str, tenant: str, data: dict[str, Any]) -> None:
    """Durable write-behind checkpoint upsert. Not lease-guarded (advisory)."""
    await conn.execute(Q.CHECKPOINT_PUT, job_id, tenant, data)


async def checkpoint_get(conn: asyncpg.Connection, *, job_id: str, tenant: str) -> dict[str, Any] | None:
    """Read the durable checkpoint, tenant-scoped. None if absent."""
    return await conn.fetchval(Q.CHECKPOINT_GET, job_id, tenant)


# ── resubmit / DLQ replay ─────────────────────────────────────────────────────


async def resubmit(conn: asyncpg.Connection, *, job_id: str, tenant: str) -> str | None:
    """Insert a fresh queued attempt of an archived terminal job.

    Returns the new job id, or None if the original is not a terminal job in this
    tenant (the service raises NotFound).
    """
    return await conn.fetchval(Q.RESUBMIT, job_id, tenant)


# ── UI read/stats surface ────────────────────────────────────────────────────


async def list_jobs(
    conn: asyncpg.Connection,
    *,
    tenant: str,
    state: str | None,
    task_name: str | None,
    ctx_id: str | None,
    limit: int,
    offset: int,
    claimed_by: str | None = None,
    parent_gate_id: str | None = None,
    pipeline: str | None = None,
    stage: str | None = None,
    group_key: str | None = None,
    created_after: datetime | None = None,
) -> list[JobListItem]:
    """Paged, filtered job list across hot + archive (UI views)."""
    rows = await conn.fetch(
        Q.LIST_JOBS,
        tenant,
        state,
        task_name,
        ctx_id,
        limit,
        offset,
        claimed_by,
        parent_gate_id,
        pipeline,
        stage,
        group_key,
        created_after,
    )
    return [
        JobListItem(
            id=str(r["id"]),
            tenant=r["tenant"],
            task_name=r["task_name"],
            pipeline=r["pipeline"],
            stage=r["stage"],
            state=r["state"],
            attempt=r["attempt"],
            priority=r["priority"],
            group_key=r["group_key"],
            ctx_id=r["ctx_id"],
            wait_key=r["wait_key"],
            claimed_by=r["claimed_by"],
            error_history=r["error_history"] or [],
            created_at=r["created_at"],
            started_at=r["started_at"],
            finished_at=r["finished_at"],
        )
        for r in rows
    ]


async def stats_board(conn: asyncpg.Connection, *, tenant: str) -> dict[str, int]:
    """State -> count aggregate for the live board."""
    rows = await conn.fetch(Q.STATS_BOARD, tenant)
    return {r["state"]: int(r["n"]) for r in rows}


async def stats_queues(conn: asyncpg.Connection, *, tenant: str) -> list[QueueStat]:
    """Per-(task, rate_class) queue depth + oldest age."""
    rows = await conn.fetch(Q.STATS_QUEUES, tenant)
    return [
        QueueStat(
            task_name=r["task_name"],
            rate_class=r["rate_class"],
            depth=int(r["depth"]),
            oldest_age_s=int(r["oldest_age_s"] or 0),
        )
        for r in rows
    ]


async def list_workers(conn: asyncpg.Connection) -> list[WorkerRow]:
    """The whole fleet, freshest first."""
    rows = await conn.fetch(Q.LIST_WORKERS)
    return [
        WorkerRow(
            worker_id=r["worker_id"],
            tags=list(r["tags"]),
            labels=r["labels"] or {},
            slots=r["slots"],
            slots_busy=r["slots_busy"],
            last_seen=r["last_seen"],
            stale=r["stale"],
        )
        for r in rows
    ]


async def worker_upsert(
    conn: asyncpg.Connection,
    *,
    worker_id: str,
    tags: list[str],
    labels: dict[str, Any],
    slots: int,
    slots_busy: int,
) -> None:
    """Persist/refresh one live worker into the fleet read model."""
    await conn.execute(Q.WORKER_UPSERT, worker_id, tags, labels, slots, slots_busy)


async def worker_delete(conn: asyncpg.Connection, *, worker_id: str) -> None:
    """Drop a worker from the fleet read model on clean disconnect."""
    await conn.execute(Q.WORKER_DELETE, worker_id)


async def mark_workers_stale(conn: asyncpg.Connection, *, stale_after_s: int) -> int:
    """Flag workers past their heartbeat window stale. Returns rows flagged."""
    result = await conn.execute(Q.MARK_WORKERS_STALE, stale_after_s)
    return int(result.split()[-1]) if result.startswith("UPDATE") else 0


async def list_cron(conn: asyncpg.Connection, *, tenant: str) -> list[CronRow]:
    """Cron schedules for a tenant."""
    rows = await conn.fetch(Q.LIST_CRON, tenant)
    return [
        CronRow(
            schedule_id=r["schedule_id"],
            cron_expr=r["cron_expr"],
            task_name=r["task_name"],
            tenant=r["tenant"],
            enabled=r["enabled"],
            last_fire=r["last_fire"],
            next_fire=r["next_fire"],
            created_at=r["created_at"],
            payload=r["payload"] or {},
        )
        for r in rows
    ]


async def cron_set_enabled(conn: asyncpg.Connection, *, schedule_id: str, tenant: str, enabled: bool) -> bool:
    """Enable/disable one schedule. False if unknown/foreign tenant."""
    return await conn.fetchval(Q.CRON_SET_ENABLED, schedule_id, tenant, enabled) is not None


async def cron_upsert(
    conn: asyncpg.Connection,
    *,
    schedule_id: str,
    cron_expr: str,
    task_name: str,
    payload: dict[str, Any],
    tenant: str,
    enabled: bool,
) -> CronRow | None:
    """Create/update one schedule for a tenant. None if a foreign tenant owns the id.

    On a cron_expr change the SQL resets next_fire to NULL so the cron loop
    recomputes the window (no backfill); an unrelated edit keeps the stored window.
    """
    row = await conn.fetchrow(Q.CRON_UPSERT, schedule_id, cron_expr, task_name, payload, tenant, enabled)
    if row is None:
        return None
    return CronRow(
        schedule_id=row["schedule_id"],
        cron_expr=row["cron_expr"],
        task_name=row["task_name"],
        tenant=row["tenant"],
        enabled=row["enabled"],
        last_fire=row["last_fire"],
        next_fire=row["next_fire"],
        created_at=row["created_at"],
        payload=row["payload"] or {},
    )


async def cron_delete(conn: asyncpg.Connection, *, schedule_id: str, tenant: str) -> bool:
    """Hard-delete one schedule. False if unknown/foreign tenant."""
    return await conn.fetchval(Q.CRON_DELETE, schedule_id, tenant) is not None


async def job_events(conn: asyncpg.Connection, *, job_id: str, tenant: str) -> list[EventRow]:
    """The audit timeline for one job, oldest-first."""
    rows = await conn.fetch(Q.JOB_EVENTS, job_id, tenant)
    return [
        EventRow(
            id=0,
            job_id=job_id,
            tenant=tenant,
            ctx_id=None,
            event=r["event"],
            at=r["at"],
            detail=r["detail"],
        )
        for r in rows
    ]


async def job_tree_edges(conn: asyncpg.Connection, *, tenant: str, ctx_id: str) -> list[TreeEdge]:
    """Dependency edges within a ctx pipeline DAG."""
    rows = await conn.fetch(Q.JOB_TREE_EDGES, tenant, ctx_id)
    return [TreeEdge(upstream=str(r["upstream"]), downstream=str(r["downstream"]), alias=r["alias"]) for r in rows]


def _event_row(r: Any) -> EventRow:
    return EventRow(
        id=int(r["id"]),
        job_id=str(r["job_id"]),
        tenant=r["tenant"],
        ctx_id=r["ctx_id"],
        event=r["event"],
        at=r["at"],
        detail=r["detail"],
    )


async def events_since(conn: asyncpg.Connection, *, after_id: int, tenant: str, limit: int) -> list[EventRow]:
    """Tail the ledger for the SSE fan-out (live updates)."""
    rows = await conn.fetch(Q.EVENTS_SINCE, after_id, tenant, limit)
    return [_event_row(r) for r in rows]


async def events_for_ctx(
    conn: asyncpg.Connection, *, tenant: str, ctx_id: str, after_id: int, limit: int
) -> list[EventRow]:
    """Replay the persisted ledger for one ctx (snapshot read, ordered by id)."""
    rows = await conn.fetch(Q.EVENTS_FOR_CTX, tenant, ctx_id, after_id, limit)
    return [_event_row(r) for r in rows]


async def max_event_id(conn: asyncpg.Connection, *, tenant: str) -> int:
    """The current head of the ledger so a fresh SSE stream starts from 'now'."""
    return int(await conn.fetchval("SELECT COALESCE(max(id), 0) FROM job_events WHERE tenant = $1", tenant) or 0)
