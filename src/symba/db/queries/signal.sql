-- signal.sql — deliver a signal to a WAITING job, race-free.
--
-- Transaction context: one transaction. Lock behavior: FOR UPDATE SKIP LOCKED on
-- the waiting job. Two orderings, both correct:
--   wait-first:   a waiting job exists -> this statement wakes it (below).
--   signal-first: no waiter yet -> service inserts into `signals` for later
--                 consumption by sweep/claim (signal_park.sql).
--
-- This file wakes the OLDEST waiting job for (tenant, wait_key), attaching the
-- payload, moving WAITING -> QUEUED. Returns the woken job id (0 rows if none).
--
-- Parameters: $1 text tenant, $2 text wait_key, $3 jsonb payload
WITH target AS (
    SELECT id
    FROM jobs
    WHERE state = 'waiting' AND tenant = $1 AND wait_key = $2
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
woken AS (
    UPDATE jobs j
    SET state = 'queued',
        wait_key = NULL,
        wait_expires_at = NULL,
        event_payload = $3::jsonb,
        run_at = now()
    FROM target t
    WHERE j.id = t.id
    RETURNING j.id, j.tenant, j.ctx_id, j.task_name
)
SELECT id, tenant, ctx_id, task_name FROM woken;
