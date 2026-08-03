"""Prometheus metrics registry.

All engine metrics declared once here. Exposed on the HTTP port at /metrics.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry(auto_describe=True)


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    return Counter(name, doc, labels, registry=REGISTRY)


def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    return Gauge(name, doc, labels, registry=REGISTRY)


def _hist(name: str, doc: str, labels: list[str], buckets: tuple[float, ...] | None = None) -> Histogram:
    if buckets is not None:
        return Histogram(name, doc, labels, registry=REGISTRY, buckets=buckets)
    return Histogram(name, doc, labels, registry=REGISTRY)


# Millisecond-scaled buckets for the ready-to-claim histogram. The default
# prometheus buckets top out at 10 (seconds-shaped): a metric measured in ms would
# dump everything >10ms into +Inf, making the p95<150ms latency assertion and the
# "ready-to-claim p95 > 500ms" alert blind. These straddle the target latency floor.
_READY_TO_CLAIM_MS_BUCKETS = (1, 5, 10, 25, 50, 100, 150, 250, 500, 1000, 2500, 5000)


# Counter names are given WITHOUT the _total suffix (prometheus_client appends it).
jobs_total = _counter("symba_jobs", "Jobs by terminal/transition state", ["tenant", "task", "state"])
job_duration_seconds = _hist("symba_job_duration_seconds", "Execution time", ["task"])
job_queue_wait_seconds = _hist("symba_job_queue_wait_seconds", "Queued -> claimed", ["task"])
ready_to_claim_ms = _hist("symba_ready_to_claim_ms", "N2 health signal", [], buckets=_READY_TO_CLAIM_MS_BUCKETS)
dispatch_pass_seconds = _hist("symba_dispatch_pass_seconds", "Dispatcher tick cost", [])
dispatch_tick_ms = _gauge("symba_dispatch_tick_ms", "Current adaptive tick", [])
queue_depth = _gauge("symba_queue_depth", "Queue depth", ["task", "state"])
queue_oldest_age_seconds = _gauge("symba_queue_oldest_age_seconds", "Oldest queued age", ["task"])
rate_bucket_tokens = _gauge("symba_rate_bucket_tokens", "Tokens available", ["rate_class"])
gate_age_seconds = _gauge("symba_gate_age_seconds", "Unfired gate age", ["policy"])
waiting_jobs = _gauge("symba_waiting_jobs", "Jobs in WAITING", ["tenant"])
lease_reclaims_total = _counter("symba_lease_reclaims", "Leases reclaimed", [])
wait_timeouts_total = _counter("symba_wait_timeouts", "Wait timeouts", [])
signals_total = _counter("symba_signals", "Signals by rendezvous outcome", ["outcome"])
cron_fires_total = _counter("symba_cron_fires", "Cron schedule fires", ["outcome"])
checkpoints_total = _counter("symba_checkpoints", "Checkpoint writes/reads", ["op"])
resubmits_total = _counter("symba_resubmits", "DLQ/failed-job resubmits", [])
claim_query_duration_seconds = _hist("symba_claim_query_duration_seconds", "Claim SQL time", [])
pool_acquire_wait_seconds = _hist("symba_pool_acquire_wait_seconds", "Pool acquire wait", ["pool"])
get_result_total = _counter("symba_get_result", "Lazy-tier fetches", ["source"])
pg_oldest_xact_age_seconds = _gauge("symba_pg_oldest_xact_age_seconds", "MVCC horizon age", [])
archive_moved_total = _counter("symba_archive_moved", "Hot-table evacuations", ["final_state"])
loop_errors_total = _counter("symba_loop_errors", "Periodic loop pass failures", ["loop"])
