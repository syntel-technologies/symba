-- fail_die.sql — terminal move to DEAD.
--
-- Transaction context: FIRST statement of the die branch. Service then
-- decrements group_running, evaluates on_failure (chain error handler / gate
-- notification), and writes the ledger row IN THE SAME transaction.
-- Lock behavior: lease-guarded atomic DELETE+INSERT (mirrors complete.sql).
-- DEAD rows land in jobs_archive and are NEVER auto-pruned (DLQ contract).
--
-- Parameters:
--   $1 uuid   job_id
--   $2 text   lease_token
--   $3 jsonb  final error entry appended to error_history
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
        m.payload, m.result, m.parent_gate_id, m.on_success, m.chain_tail,
        m.on_failure, m.remaining_deps, m.attempt, m.max_attempts, m.backoff,
        m.timeout_s, m.run_at, m.lease_ttl_s, m.lease_token, m.claimed_by,
        m.lease_expires_at, m.last_heartbeat_at, m.cancel_requested,
        m.resubmitted_from, (m.error_history || $3::jsonb) AS error_history,
        m.created_at, m.started_at,
        now() AS finished_at, 'dead' AS final_state
    FROM moved m
    RETURNING *
)
SELECT a.id, a.tenant, a.ctx_id, a.task_name, a.group_key,
       a.max_concurrent_per_group, a.on_failure, a.parent_gate_id
FROM archived a;
