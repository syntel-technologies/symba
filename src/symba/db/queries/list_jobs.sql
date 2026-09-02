-- list_jobs.sql — the UI job list with optional filters.
--
-- Backs the DLQ view (state='dead' grouped client-side by stack_hash), the Waiting
-- view (state='waiting'), and the per-ctx pipeline view (ctx_id=...). Reads through
-- jobs_all so both live and archived jobs are visible in one query. Every filter is
-- a "$n IS NULL OR col = $n" pass-through so ONE prepared statement serves all views
-- (planner sees the constant-null branch and prunes it) — no dynamic SQL, no drift.
--
-- Ordering is created_at DESC (newest first) with a hard LIMIT/OFFSET page cap so a
-- huge archive can never stream unbounded rows to the console.
--
-- Transaction context: general pool, read-only, no lock. The (tenant, state,
-- task_name, created_at DESC) index ix_jobs_state_task covers the hot side; the
-- archive side is bounded by the page cap.
--
-- Parameters:
--   $1 text       tenant
--   $2 text|null  state filter    (submitted|queued|running|waiting|succeeded|dead|cancelled)
--   $3 text|null  task_name filter
--   $4 text|null  ctx_id filter
--   $5 int        limit  (page size)
--   $6 int        offset (page start)
--   $7 text|null  claimed_by filter (worker_id) — backs the per-worker fleet drill-in
--   $8 text|null  parent_gate_id filter — backs Gate.status() child aggregation
--   $9 text|null  pipeline filter
--   $10 text|null stage filter
--   $11 text|null group_key filter
--   $12 timestamptz|null created-after filter
SELECT id, tenant, task_name, pipeline, stage, final_state AS state, attempt, priority,
       group_key, ctx_id, wait_key, claimed_by, created_at, started_at, finished_at,
       error_history
FROM jobs_all
WHERE tenant = $1
  AND ($2::text IS NULL OR final_state = $2)
  AND ($3::text IS NULL OR task_name = $3)
  AND ($4::text IS NULL OR ctx_id = $4)
  AND ($7::text IS NULL OR claimed_by = $7)
  AND ($8::text IS NULL OR parent_gate_id = $8::uuid)
  AND ($9::text IS NULL OR pipeline = $9)
  AND ($10::text IS NULL OR stage = $10)
  AND ($11::text IS NULL OR group_key = $11)
  AND ($12::timestamptz IS NULL OR created_at > $12)
ORDER BY created_at DESC
LIMIT $5 OFFSET $6;
