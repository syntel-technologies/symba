-- cancel.sql — cooperative + immediate cancel.
--
-- Transaction context: one statement per job (cancel-tree walks are batched by
-- the service). Lock behavior: state-dependent.
--   - queued/submitted/waiting -> immediate atomic move to CANCELLED archive.
--   - running                  -> set cancel_requested=true; the worker observes
--                                 it via heartbeat and stops cooperatively, then
--                                 Fail/Complete performs the terminal move.
--
-- This file is the IMMEDIATE shape (non-running). The running shape is a plain
-- UPDATE jobs SET cancel_requested = true issued as cancel_running.sql.
--
-- Parameters:
--   $1 uuid  job_id
WITH moved AS (
    DELETE FROM jobs
    WHERE id = $1 AND state IN ('submitted', 'queued', 'waiting')
    RETURNING *
),
archived AS (
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
    FROM moved m
    RETURNING *
)
SELECT a.id, a.tenant, a.ctx_id, a.group_key, a.task_name,
       a.max_concurrent_per_group, a.state AS prior_state
FROM archived a;
