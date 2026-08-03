-- metrics_pg_xact_age.sql — age (seconds) of the oldest in-progress transaction.
--
-- A long-running transaction pins the MVCC horizon: autovacuum cannot reclaim dead
-- tuples newer than it, so the hot `jobs` table bloats and claim latency creeps.
-- This is the "pg_oldest_xact_age_seconds" gauge feeding the vacuum-stall alert.
-- We exclude our own probe (pid <> pg_backend_pid) and idle sessions (xact_start
-- IS NOT NULL means a transaction is actually open).
--
-- Transaction context: general pool, read-only, no lock. Requires the connecting
-- role to see other backends in pg_stat_activity (superuser or pg_monitor); on a
-- restricted role the coalesce yields 0 rather than erroring the sweeper.
-- Parameters: none.
SELECT coalesce(
           extract(epoch FROM now() - min(xact_start)),
           0
       )::double precision AS v
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
  AND pid <> pg_backend_pid();
