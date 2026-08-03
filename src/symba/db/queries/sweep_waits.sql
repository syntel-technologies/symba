-- sweep_waits.sql — expire WAITING jobs past their deadline.
--
-- Transaction context: one statement per sweeper pass (general pool).
-- Lock behavior: FOR UPDATE SKIP LOCKED. A job that waited past wait_expires_at
-- is released back to queued with a synthetic timeout event_payload so the
-- handler can branch on "no signal arrived". This guarantees no job waits forever.
--
-- Parameters: $1 int LIMIT (batch size)
WITH expired AS (
    SELECT id
    FROM jobs
    WHERE state = 'waiting' AND wait_expires_at IS NOT NULL
      AND wait_expires_at < now()
    ORDER BY wait_expires_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
released AS (
    UPDATE jobs j
    SET state = 'queued',
        wait_key = NULL,
        wait_expires_at = NULL,
        event_payload = jsonb_build_object('__timeout__', true),
        run_at = now()
    FROM expired e
    WHERE j.id = e.id
    RETURNING j.id
)
-- Return the released ids so the sweeper can append a `wait_timed_out` audit event
-- for each: the synthetic payload lets the handler branch, the ledger row makes
-- the timeout observable in the UI/timeline.
SELECT id FROM released;
