-- sweep_leases.sql — reclaim expired leases.
--
-- Transaction context: one statement per sweeper pass (general pool).
-- Lock behavior: FOR UPDATE SKIP LOCKED so overlapping sweeper instances never
-- fight. A running job whose lease_expires_at < now() is presumed dead: reset to
-- queued for another attempt (attempt counter already incremented at claim, so
-- the retry budget is honored) and decrement its group_running counter.
--
-- One clock rule: comparison uses DB now() exclusively.
-- Parameters: $1 int LIMIT (batch size)
WITH expired AS (
    SELECT id, tenant, group_key, task_name, max_concurrent_per_group,
           attempt, max_attempts
    FROM jobs
    WHERE state = 'running' AND lease_expires_at < now()
    ORDER BY lease_expires_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
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
    RETURNING j.id, j.tenant, j.group_key, j.task_name, j.max_concurrent_per_group
),
counter_dec AS (
    UPDATE group_running g
    SET running = GREATEST(0, g.running - sub.n)
    FROM (
        SELECT tenant, group_key, task_name, count(*) AS n
        FROM requeued
        WHERE max_concurrent_per_group IS NOT NULL
        GROUP BY tenant, group_key, task_name
    ) sub
    WHERE g.tenant = sub.tenant AND g.group_key = sub.group_key
      AND g.task_name = sub.task_name
    RETURNING 1
)
SELECT count(*) AS reclaimed FROM requeued;
