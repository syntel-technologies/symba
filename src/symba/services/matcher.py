# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""The matcher: one claim pass over connected workers.

    pass_() steps
    ------------------------
    1. snapshot workers with free_slots > 0, group by identical tag-set
       (one claim query per tag group).
    2. [M3] peek rate_class buckets, reserve tokens, pass exhausted classes as $2.
       At M1 the exhausted list is empty (rate limiting lands in M3).
    3. claim with LIMIT = sum(free_slots) for the tag group.
    4. fairness shaping: interleave returned rows round-robin by group_key within
       an equal priority band, then assign to workers round-robin.
    5. push JobAssignments onto worker queues; decrement local free_slots.

Per-group exactness for max_concurrent_per_group=1 is enforced two ways: the SQL
fairness cap ($5 = per_group_cap) bounds rows-per-group in a batch, and the claim
runs in its own transaction so group_running is authoritative across batches.
"""

from __future__ import annotations

import json
from collections import defaultdict

from symba.config import SymbaConfig
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import ClaimedJob, InlineUpstream
from symba.observability import metrics
from symba.observability.logging import logger
from symba.observability.tracing import engine_span
from symba.services.rate_limiter import RateLimiter
from symba.services.registry import WorkerConn, WorkerRegistry
from symba.v1 import common_pb2 as pb
from symba.v1 import data_plane_pb2 as dp

logger = logger.bind(service="matcher", context="engine/services")


class Matcher:
    def __init__(
        self, pools: Pools, registry: WorkerRegistry, cfg: SymbaConfig, rate_limiter: RateLimiter | None = None
    ) -> None:
        self._pools = pools
        self._registry = registry
        self._cfg = cfg
        self._rate = rate_limiter

    async def pass_(self) -> int:
        """Run one matching pass. Returns the number of jobs assigned this tick."""
        workers = await self._registry.snapshot_available()
        if not workers:
            return 0

        assigned_total = 0
        for tag_key, group in _group_by_tags(workers).items():
            assigned_total += await self._match_tag_group(list(tag_key), group)
        return assigned_total

    async def _match_tag_group(self, tags: list[str], workers: list[WorkerConn]) -> int:
        limit = sum(w.free_slots for w in workers)
        if limit <= 0:
            return 0

        per_group_cap = self._per_group_cap(limit)
        # Step 2: reserve rate-class tokens BEFORE the claim so the SQL
        # stays sargable — exhausted classes are skipped via $2, not filtered in SQL.
        exhausted, reserved = await self._reserve_rate_tokens(tags, limit)

        with metrics.claim_query_duration_seconds.time():
            async with self._pools.acquire_hot() as conn, conn.transaction():
                claimed = await repo.claim(
                    conn,
                    worker_tags=tags,
                    exhausted_rate_classes=exhausted,
                    limit=limit,
                    claimed_by=f"engine:{tags or 'untagged'}",
                    per_group_cap=per_group_cap,
                )

        # Step 5: return unused reservations (claimed fewer of a class than
        # reserved), so an over-eager reservation never spuriously drains a bucket.
        await self._return_unused_tokens(reserved, claimed)

        if not claimed:
            return 0

        # Inline tier (spec 10.3): one batch read of chain predecessor + depends_on
        # results, attached to Job.upstream so ctx.output[] works without GetResult.
        async with self._pools.acquire_hot() as conn:
            upstream_by_job = await repo.fetch_inline_upstream(conn, job_ids=[j.id for j in claimed])

        ordered = _fairness_shape(claimed)
        assigned, attribution = await self._assign_round_robin(ordered, workers, upstream_by_job)
        # Re-stamp claimed_by with the REAL worker each job landed on:
        # claim.sql stamped the whole batch "engine:{tags}"; this records the actual
        # assignment target so the fleet view can attribute running jobs per worker.
        # One batched UPDATE per pass on the hot pool, guarded on the pass's lease
        # tokens so it cannot clobber a job a concurrent terminal move already took.
        await self._persist_attribution(attribution)
        return assigned

    async def _persist_attribution(self, attribution: list[tuple[str, str, str]]) -> None:
        if not attribution:
            return
        job_ids = [a[0] for a in attribution]
        worker_ids = [a[1] for a in attribution]
        lease_tokens = [a[2] for a in attribution]
        try:
            async with self._pools.acquire_hot() as conn:
                await repo.set_claimed_by(conn, job_ids=job_ids, worker_ids=worker_ids, lease_tokens=lease_tokens)
        except Exception:
            # Contained: attribution is advisory (fleet drill-in), never load-bearing
            # for correctness. A blip leaves claimed_by as "engine:{tags}" for these
            # rows this pass; they self-correct on the next attempt/re-stamp.
            logger.error("[matcher] claimed_by re-stamp failed", count=len(job_ids), exc_info=True)

    async def _reserve_rate_tokens(self, tags: list[str], limit: int) -> tuple[list[str], dict[str, int]]:
        """Reserve up to `limit` tokens per ready rate_class. Returns (exhausted, reserved)."""
        if self._rate is None:
            return [], {}
        async with self._pools.general.acquire() as conn:
            classes = await repo.distinct_rate_classes(conn, worker_tags=tags)
        exhausted: list[str] = []
        reserved: dict[str, int] = {}
        for rc in classes:
            granted = await self._rate.reserve(rc, limit)
            if granted <= 0:
                exhausted.append(rc)
            else:
                reserved[rc] = granted
        return exhausted, reserved

    async def _return_unused_tokens(self, reserved: dict[str, int], claimed: list[ClaimedJob]) -> None:
        if self._rate is None or not reserved:
            return
        consumed: dict[str, int] = defaultdict(int)
        for job in claimed:
            rc = job.raw.get("rate_class")
            if rc is not None:
                consumed[rc] += 1
        for rc, granted in reserved.items():
            unused = granted - consumed.get(rc, 0)
            if unused > 0:
                await self._rate.release(rc, unused)

    def _per_group_cap(self, limit: int) -> int:
        # Default per-group batch cap = GREATEST(2, limit/8) unless the
        # operator pins it. Keeps one hot group from monopolizing a batch.
        configured = self._cfg.dispatcher.max_per_group_per_batch
        return configured if configured > 0 else max(2, limit // 8)

    async def _assign_round_robin(
        self,
        jobs: list[ClaimedJob],
        workers: list[WorkerConn],
        upstream_by_job: dict[str, list[InlineUpstream]] | None = None,
    ) -> tuple[int, list[tuple[str, str, str]]]:
        """Assign claimed jobs to workers round-robin.

        Returns (assigned_count, attribution) where attribution is the list of
        (job_id, worker_id, lease_token) triples the caller re-stamps into
        jobs.claimed_by so each running job is attributable to its worker.
        """
        upstream_by_job = upstream_by_job or {}
        cap_bytes = self._cfg.matcher.max_upstream_inline_kb * 1024
        assigned = 0
        attribution: list[tuple[str, str, str]] = []
        idx = 0
        for job in jobs:
            # Find the next worker (round-robin) that still has a free slot.
            placed = False
            for _ in range(len(workers)):
                w = workers[idx % len(workers)]
                idx += 1
                if w.free_slots > 0:
                    upstream = _cap_upstream(upstream_by_job.get(job.id, []), cap_bytes, job_id=job.id)
                    await w.queue.put(_to_assignment(job, upstream=upstream))
                    w.free_slots -= 1
                    assigned += 1
                    placed = True
                    attribution.append((job.id, w.worker_id, job.lease_token))
                    metrics.jobs_total.labels(tenant=job.tenant, task=job.task_name, state="assigned").inc()
                    _observe_ready_to_claim(job)
                    break
            if not placed:
                # No worker slots left this pass; the claimed-but-unassigned rows
                # keep their lease and are re-dispatched next tick or reclaimed by
                # the sweeper if this engine dies. (Rare: limit == sum(free_slots).)
                logger.warning("[matcher] No free slot for claimed job", job_id=job.id)
        return assigned, attribution


def _observe_ready_to_claim(job: ClaimedJob) -> None:
    """Health signal: ms from becoming eligible (run_at) to claimed
    (started_at). Both are DB clocks captured by claim.sql (one clock), so the
    gap is pure dispatcher latency, free of engine<->DB skew. Defensive: skip if a
    caller (tests) synthesized a ClaimedJob without these timestamps.

    Also opens+closes the `claim` span for this job (keyed on ctx_id): the
    engine-side dispatch event of the per-job trace. No-op unless OTLP is set."""
    ctx_id = job.raw.get("ctx_id")
    with engine_span("symba.claim", ctx_id=ctx_id, job_id=job.id, task=job.task_name, attempt=job.attempt):
        pass
    raw = job.raw
    run_at = raw.get("run_at")
    started_at = raw.get("started_at")
    if run_at is None or started_at is None:
        return
    ms = (started_at - run_at).total_seconds() * 1000.0
    metrics.ready_to_claim_ms.observe(max(0.0, ms))


def _group_by_tags(workers: list[WorkerConn]) -> dict[tuple[str, ...], list[WorkerConn]]:
    groups: dict[tuple[str, ...], list[WorkerConn]] = defaultdict(list)
    for w in workers:
        groups[tuple(sorted(w.tags))].append(w)
    return groups


def _fairness_shape(jobs: list[ClaimedJob]) -> list[ClaimedJob]:
    """Interleave round-robin by group_key within an equal priority band (step 4).
    Priority always wins first; within one priority, no single group_key
    monopolizes the head of the assignment order."""
    by_priority: dict[int, list[ClaimedJob]] = defaultdict(list)
    for j in jobs:
        by_priority[j.priority].append(j)

    out: list[ClaimedJob] = []
    for priority in sorted(by_priority, reverse=True):
        out.extend(_round_robin_by_group(by_priority[priority]))
    return out


def _round_robin_by_group(jobs: list[ClaimedJob]) -> list[ClaimedJob]:
    buckets: dict[str, list[ClaimedJob]] = defaultdict(list)
    for j in jobs:
        buckets[j.group_key or j.id].append(j)  # ungrouped jobs are their own bucket
    queues = list(buckets.values())
    out: list[ClaimedJob] = []
    while queues:
        for q in list(queues):
            out.append(q.pop(0))
            if not q:
                queues.remove(q)
    return out


def _cap_upstream(
    producers: list[InlineUpstream], cap_bytes: int, *, job_id: str
) -> list[InlineUpstream]:
    """Enforce the per-assignment inline-tier byte cap.

    Oversized sets are dropped entirely (worker falls back to lazy GetResult via
    ``await ctx.output.fetch``). Prefer an empty inline tier over a partial one
    that silently omits a key the handler expects under ``[]``.
    """
    if not producers:
        return producers
    total = sum(len(json.dumps(p.result if p.result is not None else {}).encode()) for p in producers)
    if total <= cap_bytes:
        return producers
    logger.error(
        "[matcher] Inline upstream exceeds cap; omitting (use ctx.output.fetch)",
        job_id=job_id,
        size_bytes=total,
        cap_bytes=cap_bytes,
        producers=[p.key for p in producers],
    )
    return []


def _to_assignment(
    job: ClaimedJob, *, upstream: list[InlineUpstream] | None = None
) -> dp.JobAssignment:
    raw = job.raw
    # Identity/routing fields must be copied from the claimed row. If ctx_id is
    # omitted the SDK falls back to job.id (dispatch.py), so every chain link
    # looks like its own lineage in logs/Ctx even when the DB shares one ctx.
    spec = pb.JobSpec(
        task_name=job.task_name,
        payload_json=json.dumps(job.payload).encode(),
        pipeline=raw.get("pipeline") or "",
        stage=raw.get("stage") or "",
        ctx_id=raw.get("ctx_id") or "",
        group_key=job.group_key or "",
        dedup_key=raw.get("dedup_key") or "",
        runs_on=list(raw.get("runs_on") or []),
        rate_class=raw.get("rate_class") or "",
        priority=job.priority,
        timeout_s=int(raw["timeout_s"]) if raw.get("timeout_s") is not None else 0,
        lease_ttl_s=job.lease_ttl_s,
        max_concurrent_per_group=job.max_concurrent_per_group or 0,
    )
    pb_job = pb.Job(
        id=job.id,
        tenant=job.tenant,
        spec=spec,
        state=pb.JobState.RUNNING,
        attempt=job.attempt,
    )
    for u in upstream or []:
        pb_job.upstream.append(
            pb.UpstreamResult(
                key=u.key,
                job_id=u.producer_job_id,
                result_json=json.dumps(u.result if u.result is not None else {}).encode(),
            )
        )
    assignment = dp.JobAssignment(job=pb_job, lease_token=job.lease_token)
    lease_expires = raw.get("lease_expires_at")
    if lease_expires is not None:
        assignment.lease_expires_at.FromDatetime(lease_expires)
    # Preloaded checkpoint (claim.sql LEFT JOINs it) so a resumed attempt
    # does not re-buy expensive pre-wait work.
    checkpoint = raw.get("checkpoint_data")
    if checkpoint is not None:
        assignment.checkpoint_json = json.dumps(checkpoint).encode()
    # Consumed signal payload handed back on a WAITING->QUEUED resume.
    event_payload = raw.get("event_payload")
    if event_payload is not None:
        assignment.event_payload_json = json.dumps(event_payload).encode()
    return assignment
