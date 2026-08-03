-- complete.sql — terminal move on success (statement 1).
--
-- Transaction context: FIRST statement of the Complete transaction. The service
-- runs statements 2-6 (counter decrement, chain continuation, dependent flip,
-- gate bump, ledger insert) in the SAME transaction.
-- Lock behavior: lease-guarded. DELETE FROM jobs + INSERT INTO jobs_archive in
-- one atomic CTE. 0 rows returned -> STALE_LEASE (another attempt owns the job).
--
-- Parameters:
--   $1 uuid   job_id
--   $2 text   lease_token
--   $3 jsonb  result
--   $4 text   final_state ('succeeded')
WITH moved AS (
    DELETE FROM jobs
    WHERE id = $1 AND lease_token = $2 AND state = 'running'
    RETURNING *
),
archived AS (
    INSERT INTO jobs_archive
    SELECT
        m.id, m.task_name, m.pipeline, m.stage, m.tenant, m.ctx_id, m.state,
        m.priority, m.group_key, m.max_concurrent_per_group, m.wait_key,
        m.wait_expires_at, m.event_payload, m.dedup_key, m.runs_on, m.rate_class,
        m.payload, $3::jsonb AS result, m.parent_gate_id, m.on_success, m.chain_tail,
        m.on_failure, m.remaining_deps, m.attempt, m.max_attempts, m.backoff,
        m.timeout_s, m.run_at, m.lease_ttl_s, m.lease_token, m.claimed_by,
        m.lease_expires_at, m.last_heartbeat_at, m.cancel_requested,
        m.resubmitted_from, m.error_history, m.created_at, m.started_at,
        now() AS finished_at, $4 AS final_state
    FROM moved m
    RETURNING *
)
SELECT a.id, a.tenant, a.ctx_id, a.task_name, a.pipeline, a.stage, a.priority,
       a.group_key, a.max_concurrent_per_group, a.on_success, a.chain_tail,
       a.on_failure, a.parent_gate_id, a.runs_on, a.rate_class, a.lease_ttl_s,
       -- Timing for the latency histograms: queue-wait = started - created,
       -- exec-duration = finished - started. Both use DB clocks (one clock rule).
       a.created_at, a.started_at, a.finished_at
FROM archived a;
