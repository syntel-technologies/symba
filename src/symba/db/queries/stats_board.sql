-- stats_board.sql — the live board aggregate (backs the "Live board" view).
--
-- One count(*) per state for a tenant, over jobs_all so terminal states show too.
-- The UI renders this as the top-line status tiles and refreshes it on the SSE
-- channel. GROUP BY final_state hits ix_jobs_state_task on the hot side; the archive
-- side is a partition-pruned scan of recent partitions.
--
-- Transaction context: general pool, read-only, no lock.
--
-- Parameters: $1 text tenant
SELECT final_state AS state, count(*) AS n
FROM jobs_all
WHERE tenant = $1
GROUP BY final_state;
