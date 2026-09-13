-- worker_upsert.sql — persist a live worker into the fleet registry.
--
-- Called on Claim connect and on every subsequent slot frame (the Claim stream's
-- slot updates double as heartbeats), so last_seen tracks liveness and slots/
-- slots_busy track live capacity. The in-memory WorkerRegistry stays the source of
-- truth for matching; this table is the cross-engine read model the fleet view and
-- the connected-worker count read from (each engine owns/refreshes its own rows).
--
-- Transaction context: general pool, single statement, no explicit tx (autocommit
-- per call). Kept OFF the hot claim pool so worker bookkeeping never contends with
-- the dispatcher (the hot pool is reserved for claim/worker RPCs).
--
-- Parameters:
--   $1 text    worker_id
--   $2 text[]  tags
--   $3 text[]  registered_tasks (operator metadata; never used for routing)
--   $4 jsonb   labels
--   $5 int     slots       (total capacity advertised by the worker)
--   $6 int     slots_busy  (slots - free_slots, derived by the caller)
INSERT INTO workers (worker_id, tags, registered_tasks, labels, slots, slots_busy, last_seen, stale)
VALUES ($1, $2, $3, $4, $5, $6, now(), false)
ON CONFLICT (worker_id) DO UPDATE
SET tags       = EXCLUDED.tags,
    registered_tasks = EXCLUDED.registered_tasks,
    labels     = EXCLUDED.labels,
    slots      = EXCLUDED.slots,
    slots_busy = EXCLUDED.slots_busy,
    last_seen  = now(),
    stale      = false;
