-- create_gate.sql — open a fan-out completion barrier.
--
-- Transaction context: FIRST statement of FanOut. Runs in the SAME tx as the N
-- child inserts, so children never exist without their gate (or vice versa).
-- Lock behavior: single-row insert.
--
-- The gate stores the continuation JobSpec (on_complete) verbatim as JSONB; it is
-- materialized into a real job only when the gate FIRES (bump_gate claim-once).
--
-- Parameters:
--   $1 text   tenant
--   $2 text   ctx_id
--   $3 text   policy            (all_success | all_terminal | quorum(n))
--   $4 int    expected_children
--   $5 int    quorum_n          (nullable; only for quorum(n))
--   $6 jsonb  on_complete       (the continuation JobSpec)
INSERT INTO gates (tenant, ctx_id, policy, expected_children, quorum_n, on_complete)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id;
