-- fail.sql — retry-or-die branch.
--
-- Transaction context: one statement inside the Fail transaction (service
-- decrements group_running and inserts the ledger row in the same tx).
-- Lock behavior: lease-guarded UPDATE (retry) with a decision computed by the
-- service via core.retry.should_retry; two shapes selected by $-flag.
--
-- This file is the RETRY shape: RUNNING -> QUEUED with backoff, appending to
-- error_history. The DIE shape (RUNNING -> DEAD, archive move) reuses complete.sql's
-- move mechanics with final_state='dead' and is issued as fail_die.sql.
--
-- Parameters:
--   $1 uuid       job_id
--   $2 text       lease_token
--   $3 timestamptz next run_at (now() + backoff, computed by core.retry)
--   $4 jsonb      error entry appended to error_history
UPDATE jobs
SET state = 'queued',
    run_at = $3,
    lease_token = NULL,
    claimed_by = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = NULL,
    error_history = error_history || $4::jsonb
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING id, tenant, group_key, task_name, max_concurrent_per_group, attempt;
