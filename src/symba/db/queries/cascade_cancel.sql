-- cascade_cancel.sql — transitive dependent cancel on a dead upstream.
--
-- Transaction context: runs INSIDE the dying job's terminal tx (fail-die), OR the
-- cancel-tree tx. When a job dies, every job that transitively depends on it can
-- never become ready, so we archive the whole downstream cone as 'cancelled' in
-- one recursive statement (no per-node round-trips).
-- Lock behavior: DELETE+INSERT over the live dependent set; archived rows carry
-- final_state='cancelled'. Already-terminal (archived) jobs are untouched — the
-- recursion only walks LIVE rows in `jobs`, so it naturally stops at leaves.
--
-- Returns the cancelled ids + their root cause so the service writes one
-- job_events row per node naming $1 as the trigger.
--
-- Parameters:
--   $1 uuid   the dead/cancelled root job_id
WITH RECURSIVE cone AS (
    -- direct dependents of the root that are still live
    SELECT j.id
    FROM jobs j
    JOIN job_dependencies d ON d.job_id = j.id
    WHERE d.depends_on_job_id = $1::uuid
    UNION
    -- their transitive dependents
    SELECT j.id
    FROM jobs j
    JOIN job_dependencies d ON d.job_id = j.id
    JOIN cone c ON c.id = d.depends_on_job_id
),
moved AS (
    DELETE FROM jobs
    WHERE id IN (SELECT id FROM cone)
    RETURNING *
)
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
RETURNING id;
