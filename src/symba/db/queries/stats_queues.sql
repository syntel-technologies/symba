-- stats_queues.sql — per-queue depth and oldest age (backs the "Queues" view).
--
-- For every (task_name, rate_class) lane that has QUEUED work, report the depth and
-- the age of the oldest queued job (the head-of-line latency signal). Only the LIVE
-- hot table matters here — terminal jobs are not "queued" — so this reads `jobs`
-- directly (not jobs_all), keeping it on the tiny hot table.
--
-- extract(epoch from now() - min(run_at)) is the oldest-age in seconds; it uses DB
-- now() (one clock rule: never a client timestamp). run_at (not created_at) is
-- correct because a job with a future run_at is not actually head-of-line yet.
--
-- Transaction context: general pool, read-only, no lock.
--
-- Parameters: $1 text tenant
SELECT task_name,
       rate_class,
       count(*) AS depth,
       extract(epoch FROM now() - min(run_at))::bigint AS oldest_age_s
FROM jobs
WHERE tenant = $1 AND state = 'queued'
GROUP BY task_name, rate_class
ORDER BY depth DESC;
