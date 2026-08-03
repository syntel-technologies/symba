-- wait_consume.sql — wait-first half of the rendezvous.
--
-- A worker's Wait RPC runs this FIRST: is there already a pending signal for this
-- (tenant, wait_key)? If yes, consume it and return its payload — the job NEVER parks
-- (the signal arrived before the wait). If no row comes back, the caller falls through
-- to wait_park.sql to park the running job into WAITING.
--
--   signal-first race:   park.sql wrote a signals row  ──► this consumes it here
--   wait-first race:     no signal yet                 ──► 0 rows ──► caller parks
--
-- Transaction context: the Wait RPC transaction (hot pool). Lock behavior:
-- FOR UPDATE SKIP LOCKED on the OLDEST unconsumed signal so two concurrent waits on
-- the same key never consume the same signal (each gets a distinct row or parks).
--
-- Parameters: $1 text tenant, $2 text wait_key
WITH pending AS (
    SELECT id
    FROM signals
    WHERE tenant = $1 AND wait_key = $2 AND consumed_at IS NULL
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
consumed AS (
    UPDATE signals s
    SET consumed_at = now()
    FROM pending p
    WHERE s.id = p.id
    RETURNING s.payload
)
SELECT payload FROM consumed;
