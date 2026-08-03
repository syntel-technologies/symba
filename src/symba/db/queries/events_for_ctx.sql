-- events_for_ctx.sql — replay the full persisted ledger for one ctx (snapshot read).
--
-- Backs StreamEvents(snapshot=true): the bounded, terminating read that
-- JobHandle.events() needs. Unlike events_since.sql (a live tail from a moving
-- id-cursor), this returns ALL rows for a ctx from the beginning, ordered by the
-- monotonic identity id, so a caller can page from id 0 and stop when a short page
-- (< limit) signals the backlog is exhausted.
--
-- Transaction context: general pool, read-only, no lock. Tenant-scoped so a caller
-- can never read another tenant's ledger.
--
-- Parameters: $1 text tenant, $2 text ctx_id, $3 bigint after_id (0 for from start),
--             $4 int limit
SELECT id, job_id, tenant, ctx_id, event, at, detail
FROM job_events
WHERE tenant = $1 AND ctx_id = $2 AND id > $3
ORDER BY id ASC
LIMIT $4;
