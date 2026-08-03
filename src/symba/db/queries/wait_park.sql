-- wait_park.sql — park a RUNNING job into WAITING.
--
-- Runs only after wait_consume.sql found no pending signal: the wait genuinely has to
-- block. Moves RUNNING -> WAITING, records the key it is waiting on and a deadline, and
-- CLEARS the lease so the slot is freed while the job is parked (a waiting job is not
-- running work, so it must not hold a worker slot or a lease that the sweeper would try
-- to reclaim as a stuck job).
--
-- Lease-guarded: the WHERE pins state='running' AND the caller's lease_token, so a
-- stale worker (whose lease already expired and was reclaimed) cannot park a job it no
-- longer owns. 0 rows back => StaleLease (the service raises it).
--
-- Parameters:
--   $1 uuid    job_id
--   $2 text    lease_token (the caller's lease, guards ownership)
--   $3 text    wait_key
--   $4 int     timeout seconds (wait_expires_at = now() + $4)
UPDATE jobs
SET state = 'waiting',
    wait_key = $3,
    wait_expires_at = now() + make_interval(secs => $4),
    lease_token = NULL,
    lease_expires_at = NULL,
    claimed_by = NULL
WHERE id = $1 AND state = 'running' AND lease_token = $2
RETURNING id;
