-- bump_gate.sql — settle a child into its gate, resolve exactly once.
--
-- Transaction context: runs INSIDE the completing/dying child's terminal tx, so a
-- gate can only fire once every counted child is durably terminal.
-- Lock behavior: single-row UPDATE on the gate (row lock serializes concurrent
-- children settling the same gate). Resolution is claim-once: fired_at flips
-- from NULL exactly once even under concurrent completions.
--
-- One statement does three things atomically:
--   1. bump completed_children (+1 always), succeeded_children (+1 iff $2),
--      and failed_children (+1 iff $3),
--   2. decide if the policy is now satisfied over the NEW counts,
--   3. claim resolution when it is satisfied OR every child is terminal.
-- RETURNING reports whether THIS tx won resolution, whether the policy was
-- satisfied, and hands back on_complete so the caller can materialize either the
-- success continuation or its nested on_failure hook, PLUS the post-bump counts
-- (expected_children, succeeded_children) that feed the `__gate__` manifest merged
-- into the continuation payload (JobService._settle_gate). A losing/again call
-- returns resolved=false.
--
-- Child outcome encoding (spec 7.2):
--   success  -> $2=true,  $3=false  (advances succeeded_children)
--   dead     -> $2=false, $3=true   (advances failed_children; blocks all_success)
--   skip     -> $2=false, $3=false  (a NON-failure no-op: advances only
--                                     completed_children, never succeeded/failed)
-- So all_success succeeds when every child is terminal AND none is DEAD: a gate
-- of all-skipped children succeeds with succeeded_children=0. If a child is DEAD,
-- the gate resolves unsuccessfully after every child is terminal.
--
-- Parameters:
--   $1 uuid   gate_id
--   $2 bool   child_succeeded
--   $3 bool   child_failed
WITH locked AS (
    SELECT id,
           completed_children,
           succeeded_children,
           failed_children,
           expected_children,
           policy,
           quorum_n,
           fired_at
    FROM gates
    WHERE id = $1::uuid
    FOR UPDATE
),
post_bump AS (
    SELECT id,
           completed_children + 1 AS completed_children,
           succeeded_children + (CASE WHEN $2 THEN 1 ELSE 0 END) AS succeeded_children,
           failed_children + (CASE WHEN $3 THEN 1 ELSE 0 END) AS failed_children,
           expected_children,
           policy,
           quorum_n,
           fired_at
    FROM locked
),
decision AS (
    SELECT *,
           CASE policy
               -- all children terminal AND none dead (skips are fine)
               WHEN 'all_success'  THEN completed_children >= expected_children
                                        AND failed_children = 0
               WHEN 'all_terminal' THEN completed_children >= expected_children
               ELSE succeeded_children >= LEAST(COALESCE(quorum_n, expected_children), expected_children)
           END AS satisfied
    FROM post_bump
),
resolution AS (
    SELECT *,
           fired_at IS NULL
               AND (satisfied OR completed_children >= expected_children) AS resolved
    FROM decision
),
updated AS (
    UPDATE gates AS g
    SET completed_children = r.completed_children,
        succeeded_children = r.succeeded_children,
        failed_children = r.failed_children,
        fired_at = CASE WHEN r.resolved THEN now() ELSE g.fired_at END
    FROM resolution AS r
    WHERE g.id = r.id
    RETURNING r.resolved,
              r.satisfied,
              g.on_complete,
              g.tenant,
              g.ctx_id,
              g.expected_children,
              g.succeeded_children
)
SELECT * FROM updated;
