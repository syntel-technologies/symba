-- fail_peek.sql — read retry-decision inputs for a RUNNING job.
--
-- Transaction context: first statement inside the Fail transaction opened by the
-- service, BEFORE it decides retry-vs-die (core.retry.should_retry needs attempt,
-- max_attempts, backoff). Lease-guarded so a stale worker cannot drive the branch.
-- Lock behavior: FOR UPDATE on the single job row; the subsequent fail/fail_die
-- UPDATE in the same tx runs against this locked row (no lost update).
--
-- Parameters: $1 uuid job_id, $2 text lease_token
-- Returns 0 rows on stale lease / not running (service raises StaleLease).
SELECT attempt, max_attempts, backoff, rate_class
FROM jobs
WHERE id = $1 AND lease_token = $2 AND state = 'running'
FOR UPDATE;
