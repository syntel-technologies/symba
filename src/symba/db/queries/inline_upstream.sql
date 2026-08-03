-- inline_upstream.sql — assemble the Job.upstream inline tier for a claim batch.
--
-- Transaction context: read-only, run AFTER the claim tx commits (predecessors are
-- already archived by the Complete that made these jobs claimable). Hot or general
-- pool; no locks.
--
-- Spec 10.3 / matcher step: for each claimed job, return
--   1. immediate chain predecessor — newest succeeded archive row in the same
--      (tenant, ctx_id) whose on_success pointed at this job's task_name
--   2. declared depends_on producers — job_dependencies edges, keyed by alias
--      (or producer task_name when alias is null)
--
-- Deeper ancestors are NOT included; the worker hits GetResult (lazy tier) for those.
-- NULL ctx_id yields no chain predecessor (same non-leak rule as get_ancestor_result).
--
-- Parameters: $1 uuid[]  claimed job ids
-- Returns: asking_job_id, key, producer_job_id, result (jsonb, may be null)

WITH claimed AS (
    SELECT j.id, j.task_name, j.tenant, j.ctx_id
    FROM jobs j
    WHERE j.id = ANY($1::uuid[])
),
chain_pred AS (
    SELECT DISTINCT ON (c.id)
           c.id              AS asking_job_id,
           pred.task_name    AS key,
           pred.id           AS producer_job_id,
           pred.result       AS result
    FROM claimed c
    JOIN jobs_archive pred
      ON pred.tenant = c.tenant
     AND c.ctx_id IS NOT NULL
     AND pred.ctx_id = c.ctx_id
     AND pred.on_success = c.task_name
     AND pred.final_state = 'succeeded'
    ORDER BY c.id, pred.finished_at DESC NULLS LAST
),
dep_pred AS (
    SELECT c.id                                    AS asking_job_id,
           COALESCE(d.alias, prod.task_name)       AS key,
           prod.id                                 AS producer_job_id,
           prod.result                             AS result
    FROM claimed c
    JOIN job_dependencies d ON d.job_id = c.id
    JOIN jobs_all prod ON prod.id = d.depends_on_job_id
)
SELECT asking_job_id, key, producer_job_id, result FROM chain_pred
UNION ALL
SELECT asking_job_id, key, producer_job_id, result FROM dep_pred;
