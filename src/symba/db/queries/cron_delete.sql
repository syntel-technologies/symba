-- cron_delete.sql — remove one cron schedule (admin API).
--
-- Tenant-scoped hard delete. Returns the schedule_id when a row was actually
-- removed so the service can distinguish "deleted" from "unknown/foreign tenant"
-- (the latter returns zero rows and the service raises NotFound). Disabling
-- (cron_set_enabled=false) is the soft-pause; this is the permanent removal.
--
-- Transaction context: general pool, single statement, no lock.
--
-- Parameters:
--   $1 text schedule_id
--   $2 text tenant
DELETE FROM cron_schedules
WHERE schedule_id = $1 AND tenant = $2
RETURNING schedule_id;
