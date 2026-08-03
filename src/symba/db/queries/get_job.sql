-- get_job.sql — single-job read across hot + archive.
--
-- Transaction context: single read (general pool). Lock behavior: none.
-- Reads through the jobs_all UNION view so the job is found whether it is still
-- live (jobs) or already terminal (jobs_archive). Tenant-scoped for isolation.
--
-- Parameters: $1 uuid job_id, $2 text tenant
-- Returns 0 rows for an unknown/foreign-tenant job (service raises NotFound).
SELECT id, tenant, task_name, final_state AS state, attempt, priority,
       group_key, ctx_id, claimed_by, payload, result, error_history,
       created_at, started_at, finished_at
FROM jobs_all
WHERE id = $1 AND tenant = $2;
