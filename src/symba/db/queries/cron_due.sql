-- cron_due.sql — enabled schedules whose next_fire has arrived.
--
-- The cron loop ticks every 1s and asks: which enabled schedules are due to fire
-- (next_fire <= now(), or next_fire NULL meaning never computed)? Every engine
-- runs this — correctness comes from the deterministic dedup key on submit, NOT
-- from any election (no election is needed for this to be correct). The
-- advisory-lock election only avoids N engines doing redundant submits; the
-- unique index on ux_jobs_dedup is the real guard.
--
-- One clock source ("one clock rule"): next_fire is compared to DB now(), never
-- a client timestamp.
--
-- Transaction context: general pool, read-only snapshot. No lock — the fire/advance
-- is a separate guarded UPDATE (cron_advance.sql) that only commits if next_fire is
-- still the value we read, so two engines racing the same schedule cannot both win
-- the advance.
--
-- No parameters.
SELECT schedule_id, cron_expr, task_name, payload, tenant, last_fire, next_fire
FROM cron_schedules
WHERE enabled = true
  AND (next_fire IS NULL OR next_fire <= now())
ORDER BY next_fire ASC NULLS FIRST;
