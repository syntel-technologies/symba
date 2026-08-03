-- metrics_gate_age.sql — age of the oldest UNFIRED gate per policy.
--
-- A gate that never fires (a child stuck forever, a lost decrement) is a silent
-- stall; the "unfired gate > 1h" alert needs this gauge. Only rows with
-- fired_at IS NULL are still open; created_at is when the barrier opened.
--
-- Transaction context: general pool, read-only, no lock.
-- Parameters: none.
SELECT policy AS k1,
       extract(epoch FROM now() - min(created_at))::double precision AS v
FROM gates
WHERE fired_at IS NULL
GROUP BY policy;
