-- cron_advance.sql — advance a schedule's fire window, compare-and-swap.
--
-- After the cron loop submits the job for `fired_at`, it advances the schedule so
-- the next tick computes the following window. This is a compare-and-swap on the
-- OLD next_fire ($3): if another engine already advanced this schedule this tick,
-- our WHERE misses (0 rows) and we simply skip — the submit was idempotent via the
-- dedup key anyway, so a lost advance race costs nothing.
--
-- $3 IS NOT DISTINCT FROM handles the first-ever fire where next_fire was NULL.
--
-- Parameters:
--   $1 text        schedule_id
--   $2 timestamptz new next_fire (the fire time AFTER the one we just submitted)
--   $3 timestamptz expected current next_fire (CAS guard; NULL on first fire)
--   $4 timestamptz last_fire we just fired (the window we submitted for)
UPDATE cron_schedules
SET next_fire = $2,
    last_fire = $4
WHERE schedule_id = $1
  AND enabled = true
  AND next_fire IS NOT DISTINCT FROM $3
RETURNING schedule_id;
