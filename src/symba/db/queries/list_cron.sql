-- list_cron.sql — cron schedule list (backs the "Cron" view).
--
-- Every schedule with its enable flag and last/next fire so the UI can render the
-- schedule table and offer enable/disable toggles. Tenant-scoped.
--
-- Transaction context: general pool, read-only, no lock.
--
-- Parameters: $1 text tenant
SELECT schedule_id, cron_expr, task_name, payload, tenant, enabled, last_fire, next_fire, created_at
FROM cron_schedules
WHERE tenant = $1
ORDER BY schedule_id;
