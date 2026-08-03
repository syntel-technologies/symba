-- decrement_group.sql — release group slot(s) on terminal/retry.
--
-- Transaction context: runs inside the Complete/Fail transaction opened by the
-- service. Lock behavior: single-row UPDATE on the group_running counter.
-- GREATEST(0, ...) makes the decrement idempotent under concurrent sweeps.
--
-- Parameters: $1 tenant, $2 group_key, $3 task_name, $4 int decrement amount
UPDATE group_running
SET running = GREATEST(0, running - $4)
WHERE tenant = $1 AND group_key = $2 AND task_name = $3;
