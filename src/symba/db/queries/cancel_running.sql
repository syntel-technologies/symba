-- cancel_running.sql — request cooperative cancel of a RUNNING job.
-- The worker sees cancel_requested via its next Heartbeat response and stops.
-- Parameters: $1 uuid job_id
UPDATE jobs
SET cancel_requested = true
WHERE id = $1 AND state = 'running'
RETURNING id, claimed_by;
