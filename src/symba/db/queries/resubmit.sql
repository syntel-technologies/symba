-- resubmit.sql — DLQ replay: fresh attempt of an archived terminal job.
--
-- Ops triages the DLQ (state='dead' in jobs_all, grouped by stack_hash) and replays
-- selected jobs. A resubmit is NOT an in-place retry (the original stays archived as
-- the audit record): it INSERTs a FRESH row into the hot `jobs` table, copying the
-- original's task/payload/routing/policy, with:
--   * a new uuidv7() id (DEFAULT),
--   * resubmitted_from = the original id (lineage),
--   * attempt reset to 0 and error_history cleared (a clean slate),
--   * state = 'queued' so the dispatcher claims it on the next tick,
--   * dedup_key NULLed — the original dedup window is spent; forcing a replay must
--     not collide with the ux_jobs_dedup unique index and silently no-op.
--
-- Guard: only jobs that are actually TERMINAL and belong to the caller's tenant can
-- be resubmitted (a live job is retried via the normal fail path, never resubmitted).
-- 0 rows back => NotFound / not terminal (the service raises).
--
-- Transaction context: general pool, single statement. The read side is jobs_all
-- (archive-backed); the write is the hot table.
--
-- Parameters: $1 uuid original_job_id, $2 text tenant
INSERT INTO jobs (
    task_name, pipeline, stage, tenant, ctx_id, state, priority,
    group_key, max_concurrent_per_group, runs_on, rate_class, payload,
    on_success, chain_tail, on_failure, max_attempts, backoff, timeout_s,
    lease_ttl_s, resubmitted_from
)
SELECT
    task_name, pipeline, stage, tenant, ctx_id, 'queued', priority,
    group_key, max_concurrent_per_group, runs_on, rate_class, payload,
    on_success, chain_tail, on_failure, max_attempts, backoff, timeout_s,
    lease_ttl_s, id
FROM jobs_all
WHERE id = $1
  AND tenant = $2
  AND final_state IN ('dead', 'cancelled', 'succeeded')
RETURNING id;
