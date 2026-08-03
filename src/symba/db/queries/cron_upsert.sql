-- cron_upsert.sql — create or update one cron schedule (admin API).
--
-- The write half of the cron admin surface (read is list_cron, toggle is
-- cron_set_enabled). Tenant-scoped: the schedule is owned by the caller's tenant,
-- so a create pins the tenant and an update can only touch a row already owned by
-- that tenant. A cross-tenant collision on the primary key is rejected below.
--
-- next_fire semantics on update:
--   The cron loop (CronService._fire) treats next_fire IS NULL as "first
--   encounter" and recomputes the window from the (possibly new) cron_expr, WITHOUT
--   backfill. So on a cron_expr CHANGE we reset next_fire = NULL to force that
--   recompute; when cron_expr is UNCHANGED we keep the stored window so an
--   unrelated edit (task_name, payload, enabled) does not perturb firing timing.
--
-- Transaction context: general pool, single statement, CAS-free (the primary key
-- serializes concurrent upserts). Idempotent: the RFC's iKnowledge worker calls
-- this at every deploy/startup with identical args.
--
-- Parameters:
--   $1 text   schedule_id
--   $2 text   cron_expr
--   $3 text   task_name
--   $4 jsonb  payload
--   $5 text   tenant
--   $6 bool   enabled
INSERT INTO cron_schedules (schedule_id, cron_expr, task_name, payload, tenant, enabled, next_fire)
VALUES ($1, $2, $3, $4, $5, $6, NULL)
ON CONFLICT (schedule_id) DO UPDATE
SET cron_expr = EXCLUDED.cron_expr,
    task_name = EXCLUDED.task_name,
    payload   = EXCLUDED.payload,
    enabled   = EXCLUDED.enabled,
    -- Reset the window ONLY when the expression actually changed; otherwise the
    -- existing next_fire is preserved so timing is untouched.
    next_fire = CASE
        WHEN cron_schedules.cron_expr IS DISTINCT FROM EXCLUDED.cron_expr THEN NULL
        ELSE cron_schedules.next_fire
    END
WHERE cron_schedules.tenant = EXCLUDED.tenant
RETURNING schedule_id, cron_expr, task_name, payload, tenant, enabled, last_fire, next_fire, created_at;
