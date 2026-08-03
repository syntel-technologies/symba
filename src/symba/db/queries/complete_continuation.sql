-- complete_continuation.sql — chain continuation insert (statement 3).
--
-- Transaction context: statement 3 of the Complete transaction, run ONLY when the
-- just-succeeded job had on_success != NULL and the worker did NOT drop_chain_tail
-- (ctx.stop_chain). Runs in the SAME tx as the terminal move so the chain
-- can never lose a link across a crash: either the predecessor archived AND the
-- continuation exists, or neither did.
-- Lock behavior: plain INSERT. The continuation is a fresh job in state 'queued'
-- (no deps, run_at defaults to now()); the dispatcher's next tick picks it up
-- (after commit, there is nothing further to signal).
--
-- Inheritance: the continuation stays in the SAME pipeline/ctx so the whole
-- chain shares one lineage in the UI DAG, and inherits routing/priority/tenant so a
-- chain does not silently change lanes mid-flight. It also inherits on_failure so a
-- DEAD chain tail still fires the submitter's mark_stage_failed (or equivalent)
-- hook — without this, a failing complete_stage leaves the app spine in-progress
-- forever. It carries NO dedup_key (a continuation is not a user resubmit) and
-- starts a fresh attempt budget. payload is empty by design (SDK-3): thread data
-- via ctx.output[predecessor], not by copying the caller's payload.
--
-- Parameters:
--   $1 text    next_task        (ChainStep.next_task = predecessor.on_success)
--   $2 text    next_on_success  (ChainStep.on_success, may be NULL at chain end)
--   $3 text[]  next_chain_tail  (ChainStep.chain_tail)
--   $4 text    tenant           (inherited)
--   $5 text    ctx_id           (inherited — same pipeline lineage; TEXT, not uuid)
--   $6 text    pipeline         (inherited)
--   $7 text    stage            (inherited)
--   $8 int     priority         (inherited)
--   $9 text    group_key        (inherited, may be NULL)
--   $10 int    max_concurrent_per_group (inherited, may be NULL)
--   $11 text[] runs_on          (inherited routing)
--   $12 text   rate_class       (inherited, may be NULL)
--   $13 int    lease_ttl_s      (inherited)
--   $14 jsonb  on_failure       (inherited; NULL when the root had none)
INSERT INTO jobs (
    task_name, payload, tenant, ctx_id, pipeline, stage, state,
    priority, group_key, max_concurrent_per_group, runs_on, rate_class,
    on_success, chain_tail, on_failure, remaining_deps, lease_ttl_s
)
VALUES (
    $1, '{}'::jsonb, $4, $5, $6, $7, 'queued',
    $8, $9, $10, $11::text[], $12,
    $2, $3::text[], $14::jsonb, 0, $13
)
RETURNING id;
