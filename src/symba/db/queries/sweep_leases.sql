-- sweep_leases.sql — reclaim expired leases.
--
-- Transaction context: one statement per sweeper pass (general pool).
-- Lock behavior: FOR UPDATE SKIP LOCKED so overlapping sweeper instances never
-- fight. A running job is presumed dead when its lease expires, or when its
-- worker is no longer live and the job heartbeat has been silent for the worker
-- grace window. The latter prevents a long task lease from delaying recovery
-- after a clean disconnect or engine restart. The heartbeat grace prevents a
-- transient Claim-stream reconnect from causing duplicate execution.
--
-- A cooperative cancellation must survive worker loss. If a running job has
-- cancel_requested=true when its lease becomes reclaimable, archive it as
-- cancelled instead of putting it back on the queue. Re-queuing that row would
-- resurrect explicitly cancelled work under a replacement worker.
--
-- One clock rule: comparison uses DB now() exclusively.
-- Parameters: $1 int LIMIT (batch size), $2 int stale-worker grace seconds
WITH expired AS (
    SELECT id, tenant, group_key, task_name, max_concurrent_per_group,
           attempt, max_attempts, cancel_requested
    FROM jobs j
    WHERE j.state = 'running'
      -- An execution already at its retry ceiling must be terminalized by the
      -- service layer (gate settlement, on_failure, cascade cancel, DLQ), not
      -- silently queued for attempt max_attempts+1. Cancel still wins.
      AND (j.cancel_requested = true OR j.attempt < j.max_attempts)
      AND (
          j.lease_expires_at < now()
          OR (
              j.claimed_by IS NOT NULL
              AND j.last_heartbeat_at < now() - make_interval(secs => $2)
              AND NOT EXISTS (
                  SELECT 1
                  FROM workers w
                  WHERE w.worker_id = j.claimed_by
                    AND w.stale = false
              )
          )
      )
    ORDER BY lease_expires_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
cancelled_moved AS (
    DELETE FROM jobs j
    USING expired e
    WHERE j.id = e.id
      AND e.cancel_requested = true
    RETURNING j.*
),
cancelled AS (
    INSERT INTO jobs_archive
    SELECT
        m.id, m.task_name, m.pipeline, m.stage, m.tenant, m.ctx_id, m.state,
        m.priority, m.group_key, m.max_concurrent_per_group, m.wait_key,
        m.wait_expires_at, m.event_payload, m.dedup_key, m.runs_on, m.rate_class,
        m.payload, m.result, m.parent_gate_id, m.on_success, m.chain_tail,
        m.on_failure, m.remaining_deps, m.attempt, m.max_attempts, m.backoff,
        m.timeout_s, m.run_at, m.lease_ttl_s, m.lease_token, m.claimed_by,
        m.lease_expires_at, m.last_heartbeat_at, m.cancel_requested,
        m.resubmitted_from, m.error_history, m.created_at, m.started_at,
        now() AS finished_at, 'cancelled' AS final_state
    FROM cancelled_moved m
    RETURNING id, tenant, group_key, task_name, max_concurrent_per_group
),
requeued AS (
    UPDATE jobs j
    SET state = 'queued',
        lease_token = NULL,
        claimed_by = NULL,
        lease_expires_at = NULL,
        last_heartbeat_at = NULL,
        error_history = j.error_history
            || jsonb_build_object('type', 'lease_expired', 'at', now())
    FROM expired e
    WHERE j.id = e.id
      AND e.cancel_requested = false
    RETURNING j.id, j.tenant, j.group_key, j.task_name, j.max_concurrent_per_group
),
released AS (
    SELECT tenant, group_key, task_name, max_concurrent_per_group
    FROM requeued
    UNION ALL
    SELECT tenant, group_key, task_name, max_concurrent_per_group
    FROM cancelled
),
counter_dec AS (
    UPDATE group_running g
    SET running = GREATEST(0, g.running - sub.n)
    FROM (
        SELECT tenant, group_key, task_name, count(*) AS n
        FROM released
        WHERE max_concurrent_per_group IS NOT NULL
        GROUP BY tenant, group_key, task_name
    ) sub
    WHERE g.tenant = sub.tenant AND g.group_key = sub.group_key
      AND g.task_name = sub.task_name
    RETURNING 1
)
SELECT count(*) AS reclaimed FROM released;
