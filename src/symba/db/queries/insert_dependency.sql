-- insert_dependency.sql — record one depends_on edge.
--
-- Transaction context: runs in the SAME submit transaction as the dependent's
-- INSERT, after both the dependent and (already-persisted) upstream exist, so a
-- crash mid-submit never leaves a half-wired DAG.
-- Lock behavior: single-row insert; PK (job_id, depends_on_job_id) makes repeat
-- edges idempotent.
--
-- The edge records the alias used for ctx.output resolution; a NULL alias means
-- "resolve by the producer's task_name".
--
-- Parameters:
--   $1 uuid   job_id            (the dependent / downstream)
--   $2 uuid   depends_on_job_id (the upstream producer)
--   $3 text   alias             (nullable; defaults to producer task_name at read)
INSERT INTO job_dependencies (job_id, depends_on_job_id, alias)
VALUES ($1::uuid, $2::uuid, $3)
ON CONFLICT (job_id, depends_on_job_id) DO NOTHING;
