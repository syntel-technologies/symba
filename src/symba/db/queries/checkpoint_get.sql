-- checkpoint_get.sql — read the durable checkpoint.
--
-- The GetCheckpoint RPC fallback when the Redis fast path misses (Redis down or
-- cold). Also the source claim.sql preloads from. Tenant-scoped so one tenant can
-- never read another's checkpoint even with a guessed job_id.
--
-- Transaction context: hot pool, read-only single statement, no lock.
--
-- Parameters: $1 uuid job_id, $2 text tenant
SELECT data
FROM checkpoints
WHERE job_id = $1 AND tenant = $2;
