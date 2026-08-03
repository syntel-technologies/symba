-- get_gate.sql — authoritative gate aggregate for Gate.status() (spec 7.2).
--
-- Reads the single gates row the fan-out created; these counts are the source of
-- truth (maintained transactionally by bump_gate as each child settles). A
-- client-side recount over child jobs CANNOT reproduce `succeeded` because a
-- ctx.skip() child lands in a terminal-SUCCEEDED job state yet must be EXCLUDED
-- from succeeded_children — only the gate row records that distinction.
--
--   terminal  = completed_children (succeeded + failed + skipped)
--   succeeded = succeeded_children (EXCLUDES skips and failures)
--   expected  = expected_children
--
-- Transaction context: general pool, read-only, single-row PK lookup.
--
-- Parameters:
--   $1 text  tenant
--   $2 uuid  gate_id
SELECT id,
       expected_children,
       completed_children,
       succeeded_children,
       fired_at
FROM gates
WHERE id = $2::uuid AND tenant = $1;
