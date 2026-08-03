-- metrics_queue_gauges.sql — engine-global queue gauges for /metrics.
--
-- One pass over the tiny LIVE `jobs` table produces every state-derived gauge the
-- sweeper refreshes: depth per (task, state), oldest QUEUED age per task, and the
-- WAITING count per tenant. Emitting them from the sweeper (sweeper-refreshed,
-- not computed inline) keeps the hot claim path free of aggregate scans.
--
-- Returned as a tagged union (metric column) so ONE query feeds three Prometheus
-- gauges without three round trips. run_at (not created_at) is the head-of-line
-- clock: a job with a future run_at is not yet queue-visible.
--
-- Transaction context: general pool, read-only, no lock.
-- Parameters: none.
SELECT 'depth'::text        AS metric,
       task_name            AS k1,
       state::text          AS k2,
       count(*)::double precision AS v
FROM jobs
GROUP BY task_name, state

UNION ALL

SELECT 'oldest_age'::text   AS metric,
       task_name            AS k1,
       ''::text             AS k2,
       extract(epoch FROM now() - min(run_at))::double precision AS v
FROM jobs
WHERE state = 'queued'
GROUP BY task_name

UNION ALL

SELECT 'waiting'::text      AS metric,
       tenant               AS k1,
       ''::text             AS k2,
       count(*)::double precision AS v
FROM jobs
WHERE state = 'waiting'
GROUP BY tenant;
