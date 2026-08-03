-- get_ancestor_result.sql — lazy GetResult tier: resolve an ancestor by task_name.
--
-- The worker's `await ctx.output.fetch("head")` sends GetResultRequest{ job_id =
-- the RUNNING job (authz scope), task_name = "head" }. The engine must resolve the
-- ANCESTOR named "head" within the running job's ctx_id — NOT the running job's own
-- result (job_id is scope, not the target). This is the load-bearing half of the
-- chain/depends_on output contract (spec 10.3): without it a chain tail can never
-- read its predecessor's output.
--
-- Resolution: within the asking job's ctx_id (+ tenant), find a job whose task_name
-- (or dependency alias, once aliases are wired) matches and that produced a result.
-- Newest first so a re-run predecessor wins. jobs_all spans hot + archive.
--
-- Transaction context: single read (general pool). Lock behavior: none.
--
-- Parameters: $1 uuid asking_job_id, $2 text task_name, $3 text tenant
-- ctx_id must be a real (non-null) shared lineage: two unrelated jobs that both
-- happen to have NULL ctx_id are NOT ancestors, so a NULL-ctx asker resolves to
-- nothing (the caller then gets a clean "not an ancestor" miss rather than a
-- cross-job leak). iKnowledge-style callers always set ctx_id per document.
WITH scope AS (
    SELECT ctx_id FROM jobs_all WHERE id = $1 AND tenant = $3
)
SELECT j.id, j.final_state AS state, j.result, j.error_history
FROM jobs_all j, scope
WHERE j.tenant = $3
  AND scope.ctx_id IS NOT NULL
  AND j.ctx_id = scope.ctx_id
  AND j.task_name = $2
  AND j.result IS NOT NULL
ORDER BY j.finished_at DESC NULLS LAST
LIMIT 1;
