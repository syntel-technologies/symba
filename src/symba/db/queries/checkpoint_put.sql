-- checkpoint_put.sql — durable checkpoint write-behind.
--
-- Redis is the fast path; this table is the system of record. A handler calls
-- ctx.checkpoint(data) -> PutCheckpoint RPC -> here. Upsert on job_id (PK): the
-- newest checkpoint wins, updated_at bumped for the sweeper's retention GC.
--
-- Transaction context: hot pool, single statement (its own autocommit tx). NOT
-- lease-guarded on purpose — checkpointing is advisory optimization, not a state
-- transition; a stale worker writing a checkpoint is harmless (it is only ever
-- READ on the next claim, and a reclaimed job re-runs pre-checkpoint code anyway).
--
-- Parameters: $1 uuid job_id, $2 text tenant, $3 jsonb data
INSERT INTO checkpoints (job_id, tenant, data, updated_at)
VALUES ($1, $2, $3::jsonb, now())
ON CONFLICT (job_id) DO UPDATE
SET data = EXCLUDED.data,
    tenant = EXCLUDED.tenant,
    updated_at = now()
RETURNING job_id;
