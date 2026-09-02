"""SQL query registry.

All SQL lives in db/queries/*.sql and is loaded once at import into a Queries
namespace. Services never inline SQL. Each .sql file carries a header comment
stating its transaction context and lock behavior.
"""

from __future__ import annotations

from pathlib import Path

_QUERIES_DIR = Path(__file__).resolve().parent / "queries"


def _load(name: str) -> str:
    path = _QUERIES_DIR / f"{name}.sql"
    return path.read_text()


class Queries:
    """Loaded SQL, accessed as Q.CLAIM etc. Missing files fail loudly at import."""

    CLAIM = _load("claim")
    RUNNING_COUNTS_BY_WORKER = _load("running_counts_by_worker")
    SET_CLAIMED_BY = _load("set_claimed_by")
    SUBMIT = _load("submit")
    HEARTBEAT = _load("heartbeat")
    COMPLETE = _load("complete")
    COMPLETE_CONTINUATION = _load("complete_continuation")
    FAIL = _load("fail")
    FAIL_PEEK = _load("fail_peek")
    FAIL_DIE = _load("fail_die")
    CANCEL = _load("cancel")
    CANCEL_RUNNING = _load("cancel_running")
    GET_JOB = _load("get_job")
    GET_RESULT = _load("get_result")
    SWEEP_LEASES = _load("sweep_leases")
    LIST_EXHAUSTED_LEASES = _load("list_exhausted_leases")
    SWEEP_WAITS = _load("sweep_waits")
    SIGNAL = _load("signal")
    SIGNAL_INSERT = _load("signal_insert")
    WAIT_CONSUME = _load("wait_consume")
    WAIT_PARK = _load("wait_park")
    DECREMENT_GROUP = _load("decrement_group")
    DECREMENT_DEPS = _load("decrement_deps")
    INSERT_DEPENDENCY = _load("insert_dependency")
    CREATE_GATE = _load("create_gate")
    BUMP_GATE = _load("bump_gate")
    GATE_CHILD_RESULTS = _load("gate_child_results")
    GET_GATE = _load("get_gate")
    CASCADE_CANCEL = _load("cascade_cancel")
    RECORD_EVENT = _load("record_event")
    RECONCILE_GROUPS = _load("reconcile_groups")
    RATE_TAKE_PG = _load("rate_take_pg")
    RATE_RETURN_PG = _load("rate_return_pg")
    RATE_CLASSES_ALL = _load("rate_classes_all")
    RATE_CLASS_UPSERT = _load("rate_class_upsert")
    DISTINCT_RATE_CLASSES = _load("distinct_rate_classes")
    CRON_DUE = _load("cron_due")
    CRON_ADVANCE = _load("cron_advance")
    CHECKPOINT_PUT = _load("checkpoint_put")
    CHECKPOINT_GET = _load("checkpoint_get")
    RESUBMIT = _load("resubmit")
    # UI read/stats surface
    LIST_JOBS = _load("list_jobs")
    STATS_BOARD = _load("stats_board")
    STATS_QUEUES = _load("stats_queues")
    LIST_WORKERS = _load("list_workers")
    WORKER_UPSERT = _load("worker_upsert")
    WORKER_DELETE = _load("worker_delete")
    MARK_WORKERS_STALE = _load("mark_workers_stale")
    LIST_CRON = _load("list_cron")
    CRON_SET_ENABLED = _load("cron_set_enabled")
    CRON_UPSERT = _load("cron_upsert")
    CRON_DELETE = _load("cron_delete")
    JOB_EVENTS = _load("job_events")
    JOB_TREE_EDGES = _load("job_tree_edges")
    EVENTS_SINCE = _load("events_since")
    EVENTS_FOR_CTX = _load("events_for_ctx")
    GET_ANCESTOR_RESULT = _load("get_ancestor_result")
    INLINE_UPSTREAM = _load("inline_upstream")
    # Sweeper-refreshed /metrics gauges
    METRICS_QUEUE_GAUGES = _load("metrics_queue_gauges")
    METRICS_GATE_AGE = _load("metrics_gate_age")
    METRICS_PG_XACT_AGE = _load("metrics_pg_xact_age")
    COUNT_TENANT_LIVE = _load("count_tenant_live")


Q = Queries
