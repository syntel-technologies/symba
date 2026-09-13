-- prepare_group_counters.sql — initialize counters before the locking claim.
--
-- Transaction context: statement 1 of repository.claim's transaction. The
-- following claim.sql statement locks these rows before reading headroom.
-- Lock behavior: inserts only missing rows; a concurrent first claim may wait on
-- the same unique key, then ON CONFLICT DO NOTHING after the winner commits.
--
-- Parameters:
--   $1 text[]  worker tags (must cover runs_on)
--   $2 text[]  exhausted rate classes to skip
--   $3 int     maximum number of groups this claim can consume
--   $4 jsonb   exact reserved-token quota per rate_class
--
-- The ordered, bounded set mirrors claim.sql. At most $3 groups can contribute
-- to a $3-row result, and ordering by the group's best ready job preserves the
-- existing priority policy. The PK order on INSERT prevents lock-order cycles
-- when concurrent tag/rate-class claimers have overlapping group sets.
WITH missing_groups AS MATERIALIZED (
    SELECT j.tenant, j.group_key, j.task_name,
           max(j.priority) AS max_priority,
           min(j.run_at) AS oldest_run_at
    FROM jobs j
    LEFT JOIN group_running g
           ON g.tenant = j.tenant
          AND g.group_key = j.group_key
          AND g.task_name = j.task_name
    WHERE j.state = 'queued'
      AND j.group_key IS NOT NULL
      AND j.max_concurrent_per_group IS NOT NULL
      AND g.tenant IS NULL
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
      AND ($4::jsonb = '{}'::jsonb OR j.rate_class IS NULL
           OR ($4::jsonb ? j.rate_class
               AND COALESCE(($4::jsonb ->> j.rate_class)::int, 0) > 0))
    GROUP BY j.tenant, j.group_key, j.task_name
    ORDER BY max(j.priority) DESC, min(j.run_at) ASC,
             j.tenant ASC, j.group_key ASC, j.task_name ASC
    LIMIT $3
)
INSERT INTO group_running (tenant, group_key, task_name, running)
SELECT tenant, group_key, task_name, 0
FROM missing_groups
ORDER BY tenant ASC, group_key ASC, task_name ASC
ON CONFLICT (tenant, group_key, task_name) DO NOTHING;
