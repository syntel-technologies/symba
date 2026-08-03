-- submit.sql — insert one job.
--
-- Transaction context: one statement (callers batch N inserts in one tx).
-- Lock behavior: dedup via the ux_jobs_dedup unique index + ON CONFLICT.
--
-- A dedup hit must return the EXISTING job's id (the SDK contract + SymbaTest
-- promise h2.id == h1.id, deduplicated=true), so we cannot DO NOTHING (that
-- returns zero rows and loses the canonical id). Instead DO UPDATE with a no-op
-- touch (dedup_key = itself) so the conflicting row is returned, and use
-- `xmax <> 0` to tell a real conflict (row already existed) from a fresh insert:
--   * fresh insert -> xmax = 0            -> deduplicated = false
--   * dedup hit    -> xmax <> 0 (updated) -> deduplicated = true
-- The "update" writes the same value, so it's a semantic no-op on the row.
--
-- Parameters follow the column order below; $-numbers assigned by the service.
INSERT INTO jobs (
    task_name, payload, pipeline, stage, tenant, ctx_id, state,
    priority, group_key, max_concurrent_per_group, dedup_key, runs_on, rate_class,
    on_success, chain_tail, on_failure, remaining_deps,
    max_attempts, backoff, timeout_s, run_at, lease_ttl_s, parent_gate_id
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12::text[], $13,
    $14, $15::text[], $16, $17,
    $18, $19, $20, COALESCE($21, now()), $22, $23
)
ON CONFLICT (tenant, dedup_key) WHERE dedup_key IS NOT NULL
DO UPDATE SET dedup_key = EXCLUDED.dedup_key
RETURNING id, (xmax <> 0) AS deduplicated;
