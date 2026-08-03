-- heartbeat.sql — extend a lease and read the cooperative-cancel flag.
--
-- Transaction context: single autocommit statement on the hot pool; called
-- frequently (worker keepalive), must stay cheap. Lease-guarded: only the current
-- lease holder can extend. Lock behavior: single-row UPDATE by (id, lease_token).
--
-- One clock rule: the new expiry is computed from DB now(), never a client clock.
--
-- Parameters: $1 uuid job_id, $2 text lease_token
-- Returns 0 rows on stale lease (service maps to StaleLease -> gRPC ABORTED).
UPDATE jobs
SET lease_expires_at = now() + make_interval(secs => lease_ttl_s),
    last_heartbeat_at = now()
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING lease_expires_at, cancel_requested;
