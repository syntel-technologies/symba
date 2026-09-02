-- list_exhausted_leases.sql -- expired worker leases with no attempts left.
--
-- Read-only candidate selection for the sweeper service. The service sends
-- each row through JobService.fail(retryable=false), which owns terminal
-- archive, gate settlement, on_failure hooks, dependent cancellation, metrics,
-- and audit events. sweep_leases.sql deliberately excludes these rows.
--
-- Parameters: $1 int LIMIT, $2 int stale-worker grace seconds
SELECT j.id, j.lease_token
FROM jobs j
WHERE j.state = 'running'
  AND j.cancel_requested = false
  AND j.attempt >= j.max_attempts
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
ORDER BY j.lease_expires_at ASC
LIMIT $1;
