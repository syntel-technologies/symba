# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Periodic subsystem loops: dispatcher, sweeper, cron.

    containment contract
    ---------------------------------------------------
    one PASS may fail (PG blip, one bad row) -> log ERROR, bump
    symba_loop_errors_total{loop}, back off, KEEP LOOPING.
    the LOOP never dies on a transient error; a persistent failure is
    surfaced by the metric alert, not by a silent process death. This is the
    deliberate override of the crash-only default for these three loops.

At M0 the pass bodies are inert (they tick and return 0 work) so the process
boots health-green and the loop scaffolding + containment are exercised. M1
fills `pass_()` with the matcher, the six sweeper statements, and the cron
tick.
"""

from __future__ import annotations

import abc
import asyncio
from datetime import UTC, datetime
from typing import Any

from croniter import croniter

from symba.core.errors import StaleLease
from symba.db import repository as repo
from symba.db.records import CronDue, SubmitSpec
from symba.observability import metrics
from symba.observability.logging import logger
from symba.observability.metrics import loop_errors_total
from symba.transport.state import EngineState

logger = logger.bind(service="loops", context="engine/services")


class PeriodicLoop(abc.ABC):
    """A crash-contained periodic task. Subclasses implement one `pass_()`."""

    name: str

    def __init__(self, state: EngineState) -> None:
        self.state = state
        self._stop = asyncio.Event()

    @abc.abstractmethod
    async def pass_(self) -> int:
        """Do one unit of work; return a count used only for adaptive backoff."""

    @abc.abstractmethod
    def _next_sleep_s(self, did_work: bool) -> float:
        """Seconds to sleep before the next pass."""

    async def run(self) -> None:
        logger.info("[loop] Starting", loop=self.name)
        while not self._stop.is_set():
            did_work = False
            try:
                did_work = await self.pass_() > 0
            except Exception:
                # Contain the pass, never kill the loop.
                logger.error("[loop] Pass failed", loop=self.name, exc_info=True)
                loop_errors_total.labels(loop=self.name).inc()
            await asyncio.sleep(self._next_sleep_s(did_work))

    def stop(self) -> None:
        self._stop.set()


class Dispatcher(PeriodicLoop):
    """The only latency tier: adaptive-tick claim matcher.

    Each pass delegates to the matcher (snapshot workers -> claim -> shape ->
    assign). Adaptive tick: floor when busy, decay toward max_tick_ms when idle.
    A registry wake event (a worker's slots going 0 -> positive, or a new worker)
    short-circuits the idle sleep so latency stays low without busy-spinning.
    """

    name = "dispatcher"

    def __init__(self, state: EngineState) -> None:
        super().__init__(state)
        self._tick_ms = state.config.dispatcher.min_tick_ms

    async def pass_(self) -> int:
        with metrics.dispatch_pass_seconds.time():
            assigned = await self.state.matcher.pass_()
        metrics.dispatch_tick_ms.set(self._tick_ms)
        return assigned

    def _next_sleep_s(self, did_work: bool) -> float:
        cfg = self.state.config.dispatcher
        self._tick_ms = cfg.min_tick_ms if did_work else min(self._tick_ms * 2, cfg.max_tick_ms)
        return self._tick_ms / 1000

    async def run(self) -> None:
        # Override the base loop to add the wake-event short-circuit.
        logger.info("[loop] Starting", loop=self.name)
        wake = self.state.registry.wake
        while not self._stop.is_set():
            did_work = False
            try:
                did_work = await self.pass_() > 0
            except Exception:
                logger.error("[loop] Pass failed", loop=self.name, exc_info=True)
                loop_errors_total.labels(loop=self.name).inc()
            wake.clear()
            try:
                await asyncio.wait_for(wake.wait(), timeout=self._next_sleep_s(did_work))
            except TimeoutError:
                pass


class Sweeper(PeriodicLoop):
    """Lease reclaim, wait expiry, counter reconcile, GC, partitions.

        pass = non-blocking election, then the maintenance statements
        -------------------------------------------------------------
        pg_try_advisory_lock(hashtext('symba:sweeper')) elects ONE engine per pass;
        the loser skips (returns 0) so N engines do not fight. The winner runs, in
        one general-pool connection held for the pass:
            1. sweep_leases           reclaim expired leases -> queued (+group dec)
            2. sweep_waits            expire WAITING past deadline -> queued
            3. reconcile_group_running self-heal counter drift
        The session lock auto-releases if this engine dies (crash-only).

    M1 scope: statements 1-3. GC (checkpoints/signals), partition pre-create/drop,
    and worker-staleness marking land in M4 as those tables see load.

        /metrics gauge refresh
        -----------------------------------
        The sweeper is the natural owner of the point-in-time gauges (queue depth,
        oldest age, waiting count, gate age, MVCC horizon, rate-bucket tokens): it
        already holds the single-engine election, so only ONE engine emits them and
        they cannot double-count across a cluster. The refresh runs on the SAME
        election-won connection, inside its own try/except so a gauge query failure
        can never abort lease reclaim — the maintenance work is load-bearing, the
        gauges are advisory.
    """

    name = "sweeper"

    async def pass_(self) -> int:
        cfg = self.state.config.sweeper
        async with self.state.pools.general.acquire() as conn:
            if not await repo.try_sweeper_lock(conn):
                return 0  # another engine is sweeping this pass
            try:
                stale_after_s = cfg.worker_stale_after_heartbeats * cfg.worker_heartbeat_interval_s
                exhausted_candidates = await repo.list_exhausted_leases(
                    conn,
                    limit=cfg.batch_size,
                    stale_worker_grace_s=stale_after_s,
                )
                exhausted = 0
                for candidate in exhausted_candidates:
                    try:
                        await self.state.jobs.fail(
                            job_id=candidate.job_id,
                            lease_token=candidate.lease_token,
                            error_type="LeaseAttemptsExhausted",
                            error_message=(
                                "Worker lease expired after the job consumed its maximum execution attempts"
                            ),
                            stack_hash="",
                            retryable=False,
                        )
                        exhausted += 1
                    except StaleLease:
                        # A heartbeat/completion won the race after the
                        # read-only candidate scan. Its new owner decides.
                        logger.debug(
                            "[sweeper] Exhausted lease changed before terminalization",
                            job_id=candidate.job_id,
                        )
                reclaimed = await repo.sweep_leases(
                    conn,
                    limit=cfg.batch_size,
                    stale_worker_grace_s=stale_after_s,
                )
                expired_ids = await repo.sweep_waits(conn, limit=cfg.batch_size)
                # One `wait_timed_out` ledger row per released job so the timeout is
                # observable in the timeline, not just inferred from the state flip.
                for job_id in expired_ids:
                    await repo.record_event(conn, job_id=job_id, event="wait_timed_out")
                drift = await repo.reconcile_group_running(conn)
                # Flag workers whose heartbeat window lapsed. A crashed
                # worker (no clean Claim-stream close) leaves a stale `workers` row;
                # this marks it so the fleet view shows it dead before its leases are
                # reclaimed. Threshold = missed heartbeats * heartbeat interval.
                stale_workers = await repo.mark_workers_stale(conn, stale_after_s=stale_after_s)
                await self._refresh_gauges(conn)
            finally:
                await repo.unlock_sweeper(conn)
        expired = len(expired_ids)
        if reclaimed or exhausted:
            metrics.lease_reclaims_total.inc(reclaimed + exhausted)
        if expired:
            metrics.wait_timeouts_total.inc(expired)
        if reclaimed or exhausted or expired or drift or stale_workers:
            logger.info(
                "[sweeper] Pass done",
                reclaimed=reclaimed,
                lease_attempts_exhausted=exhausted,
                wait_expired=expired,
                drift_healed=drift,
                workers_marked_stale=stale_workers,
            )
        return reclaimed + exhausted + expired

    async def _refresh_gauges(self, conn: Any) -> None:
        """Repopulate the point-in-time /metrics gauges.

        Contained: a failure here logs + bumps loop_errors{loop=sweeper_gauges} and
        returns; it must never abort the lease-reclaim pass. Each gauge family is
        .clear()'d before repopulating so a drained label series (e.g. a task that
        emptied its queue) disappears instead of freezing at its last value.
        """
        try:
            rows = await repo.metrics_queue_gauges(conn)
            metrics.queue_depth.clear()
            metrics.queue_oldest_age_seconds.clear()
            metrics.waiting_jobs.clear()
            for row in rows:
                metric, k1, k2, v = row["metric"], row["k1"], row["k2"], row["v"]
                if metric == "depth":
                    metrics.queue_depth.labels(task=k1, state=k2).set(v)
                elif metric == "oldest_age":
                    metrics.queue_oldest_age_seconds.labels(task=k1).set(v)
                elif metric == "waiting":
                    metrics.waiting_jobs.labels(tenant=k1).set(v)

            gate_rows = await repo.metrics_gate_age(conn)
            metrics.gate_age_seconds.clear()
            for row in gate_rows:
                metrics.gate_age_seconds.labels(policy=row["k1"]).set(row["v"])

            metrics.pg_oldest_xact_age_seconds.set(await repo.metrics_pg_xact_age(conn))

            tokens = await self.state.rate_limiter.snapshot_tokens()
            metrics.rate_bucket_tokens.clear()
            for rate_class, level in tokens.items():
                metrics.rate_bucket_tokens.labels(rate_class=rate_class).set(level)
        except Exception:
            logger.error("[sweeper] Gauge refresh failed", exc_info=True)
            loop_errors_total.labels(loop="sweeper_gauges").inc()

    def _next_sleep_s(self, did_work: bool) -> float:
        return float(self.state.config.sweeper.interval_s)


class CronService(PeriodicLoop):
    """Deterministic schedule firing.

    why this is the simplest correct mechanism
    ------------------------------------------------------
    Every engine ticks (1s). For each due schedule it submits a job with a
    DETERMINISTIC dedup key `cron:{schedule_id}:{next_fire_iso}`. Cross-engine
    dedup falls out of the ordinary ux_jobs_dedup unique index — correctness
    does NOT depend on an election.

    fire-window computation
    --------------------------------------
    The next window is advanced from the INTENDED previous tick (next_fire /
    last_fire), never from now() — computing from now() under skewed loop
    timing produced both duplicates and skipped ticks. A computed fire time in
    the past is clamped to now(). No missed-window backfill: if all
    instances were down, the schedule fires ONCE on recovery, not N times.

        schedule row          this pass
        ------------          ---------
        next_fire <= now  →   submit(dedup cron:{id}:{next_fire})  →  CAS advance
        next_fire IS NULL →   base = last_fire or now; fire that window, then advance
    """

    name = "cron"

    async def pass_(self) -> int:
        async with self.state.pools.general.acquire() as conn:
            due = await repo.cron_due(conn)
        if not due:
            return 0
        fired = 0
        for schedule in due:
            # Contain a single bad schedule (bad cron expr, submit error): log and
            # skip it, never let one poisoned row kill the whole cron loop.
            try:
                if await self._fire(schedule):
                    fired += 1
            except Exception:
                logger.error("[cron] Schedule fire failed", schedule_id=schedule.schedule_id, exc_info=True)
                loop_errors_total.labels(loop=self.name).inc()
        return fired

    async def _fire(self, schedule: CronDue) -> bool:
        now = datetime.now(UTC)
        # First encounter (next_fire NULL): do NOT fire immediately — that would
        # fire on the tick the schedule was created rather than at its cron window.
        # Initialize the window to the first occurrence after now (no backfill) and
        # return; the tick that reaches that window will fire it.
        if schedule.next_fire is None:
            first = self._next_after(schedule.cron_expr, now)
            async with self.state.pools.general.acquire() as conn:
                await repo.cron_advance(
                    conn,
                    schedule_id=schedule.schedule_id,
                    new_next_fire=first,
                    expected_next_fire=None,
                    fired_at=schedule.last_fire,
                )
            return False
        # The window we are firing FOR is the stored next_fire — never a fresh now()
        # (advance from the intended tick, not wall-clock at pass time).
        fire_at = schedule.next_fire
        if fire_at > now:
            return False  # not actually due yet (defensive; cron_due already filtered)
        # Deterministic dedup key: identical across every engine for this window, so
        # ux_jobs_dedup collapses N concurrent submits to exactly one job.
        dedup_key = f"cron:{schedule.schedule_id}:{fire_at.isoformat()}"
        spec = SubmitSpec(
            task_name=schedule.task_name,
            payload=schedule.payload,
            tenant=schedule.tenant,
            dedup_key=dedup_key,
            state="queued",
        )
        outcome = await self.state.submit.submit([spec])
        # Advance to the window AFTER the one we just fired, computed from fire_at
        # (the intended tick), and CAS-guarded so a racing engine cannot double-advance.
        next_fire = self._next_after(schedule.cron_expr, fire_at)
        if next_fire < now:
            next_fire = now  # never leave a stale past window
        async with self.state.pools.general.acquire() as conn:
            won = await repo.cron_advance(
                conn,
                schedule_id=schedule.schedule_id,
                new_next_fire=next_fire,
                expected_next_fire=schedule.next_fire,
                fired_at=fire_at,
            )
        submitted = bool(won and not outcome.deduplicated[0])
        metrics.cron_fires_total.labels(outcome="submitted" if submitted else "deduped").inc()
        if submitted:
            logger.info("[cron] Fired", schedule_id=schedule.schedule_id, fire_at=fire_at.isoformat())
        return submitted

    @staticmethod
    def _next_after(cron_expr: str, after: datetime) -> datetime:
        """The occurrence strictly after `after` (advance from the intended tick)."""
        itr = croniter(cron_expr, after)
        nxt: datetime = itr.get_next(datetime)
        return nxt if nxt.tzinfo else nxt.replace(tzinfo=UTC)

    def _next_sleep_s(self, did_work: bool) -> float:
        return self.state.config.cron.tick_s


class RateRefiller(PeriodicLoop):
    """Reload the rate-class config cache so admin edits take effect.

    Token refill itself is lazy (every take refills from elapsed time), so this
    loop only re-reads the (name, capacity, refill_per_s) config from Postgres on a
    slow tick — runtime edits via the admin API apply within one interval WITHOUT a
    restart. Runs on EVERY engine (each keeps its own in-memory cache).
    """

    name = "rate_refiller"

    async def pass_(self) -> int:
        await self.state.rate_limiter.load()
        return 0

    def _next_sleep_s(self, did_work: bool) -> float:
        # Same cadence as the sweeper: config rarely changes; one tick of lag is fine.
        return float(self.state.config.sweeper.interval_s)
