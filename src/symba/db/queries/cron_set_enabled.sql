-- cron_set_enabled.sql — enable/disable a schedule.
--
-- The only mutating cron endpoint the UI needs. Disabling does NOT clear next_fire:
-- re-enabling resumes from the stored window, and cron_due's "no backfill" clamp
-- prevents a burst of catch-up fires if it was disabled across many windows.
--
-- Transaction context: general pool, single statement. Tenant-scoped so one tenant
-- cannot toggle another's schedule.
--
-- Parameters: $1 text schedule_id, $2 text tenant, $3 bool enabled
UPDATE cron_schedules
SET enabled = $3
WHERE schedule_id = $1 AND tenant = $2
RETURNING schedule_id;
