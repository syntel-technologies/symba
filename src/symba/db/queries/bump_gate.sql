-- bump_gate.sql — settle a child into its gate, fire exactly once.
--
-- Transaction context: runs INSIDE the completing/dying child's terminal tx, so a
-- gate can only fire once every counted child is durably terminal.
-- Lock behavior: single-row UPDATE on the gate (row lock serializes concurrent
-- children settling the same gate). The fire is claim-once: the CASE flips
-- fired_at from NULL exactly once even under concurrent completions.
--
-- One statement does three things atomically:
--   1. bump completed_children (+1 always), succeeded_children (+1 iff $2),
--      and failed_children (+1 iff $3),
--   2. decide if the policy is now satisfied over the NEW counts,
--   3. if satisfied AND not yet fired, stamp fired_at=now() (the claim).
-- RETURNING reports whether THIS tx won the fire and hands back on_complete so the
-- caller can materialize the continuation, PLUS the post-bump counts
-- (expected_children, succeeded_children) that feed the `__gate__` manifest merged
-- into the continuation payload (JobService._settle_gate). A losing/again call
-- returns fired=false.
--
-- Child outcome encoding (spec 7.2):
--   success  -> $2=true,  $3=false  (advances succeeded_children)
--   dead     -> $2=false, $3=true   (advances failed_children; blocks all_success)
--   skip     -> $2=false, $3=false  (a NON-failure no-op: advances only
--                                     completed_children, never succeeded/failed)
-- So all_success fires when every child is terminal AND none is DEAD: a gate of
-- all-skipped children still fires with succeeded_children=0.
--
-- Parameters:
--   $1 uuid   gate_id
--   $2 bool   child_succeeded
--   $3 bool   child_failed
UPDATE gates
SET
    completed_children = completed_children + 1,
    succeeded_children = succeeded_children + (CASE WHEN $2 THEN 1 ELSE 0 END),
    failed_children    = failed_children + (CASE WHEN $3 THEN 1 ELSE 0 END),
    fired_at = CASE
        WHEN fired_at IS NULL
             AND CASE policy
                 -- all children terminal AND none dead (skips are fine)
                 WHEN 'all_success'  THEN completed_children + 1 >= expected_children
                                          AND failed_children + (CASE WHEN $3 THEN 1 ELSE 0 END) = 0
                 WHEN 'all_terminal' THEN completed_children + 1 >= expected_children
                 ELSE succeeded_children + (CASE WHEN $2 THEN 1 ELSE 0 END) >= LEAST(COALESCE(quorum_n, expected_children), expected_children)
             END
            THEN now()
        ELSE fired_at
    END
WHERE id = $1::uuid
RETURNING
    (fired_at = now()) AS fired,     -- true only for the tx that just claimed it
    on_complete,
    tenant,
    ctx_id,
    expected_children,               -- post-bump; feeds __gate__.expected
    succeeded_children;              -- post-bump; feeds __gate__.succeeded (excludes skips)
