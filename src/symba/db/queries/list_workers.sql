-- list_workers.sql — the fleet view.
--
-- The whole worker registry, freshest first. `stale` is maintained by the sweeper
-- (last_seen < now() - 3*heartbeat_interval); the UI flags stale rows so an
-- operator sees a dead worker before its jobs get reclaimed. slots vs slots_busy is
-- the live capacity gauge.
--
-- Transaction context: general pool, read-only, no lock.
--
-- No parameters (the fleet is not tenant-scoped — a worker serves all tenants it is
-- tagged for; per-tenant fleet views are a future concern).
SELECT worker_id, tags, labels, slots, slots_busy, last_seen, stale
FROM workers
ORDER BY last_seen DESC;
