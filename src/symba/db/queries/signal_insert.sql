-- signal_insert.sql — park a signal for a future waiter.
--
-- The signal-first half: signal.sql found no WAITING job to wake, so the payload is
-- stored here for whichever wait arrives next (wait_consume.sql consumes it). Kept as
-- a separate statement (not folded into signal.sql) so the SignalService can run the
-- wake and the park in ONE transaction and pick exactly one — never both, never
-- neither (correctness must hold under both signal-first and wait-first orderings).
--
-- Parameters: $1 text tenant, $2 text wait_key, $3 jsonb payload, $4 text signaled_by
INSERT INTO signals (tenant, wait_key, payload, signaled_by)
VALUES ($1, $2, $3::jsonb, $4)
RETURNING id;
