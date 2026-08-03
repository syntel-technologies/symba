-- reconcile_groups.sql — self-heal group_running drift.
--
-- Transaction context: one statement in the sweeper pass (general pool).
-- Lock behavior: UPDATE of drifted counter rows only (predicate skips correct
-- rows so the write set stays minimal). The authoritative count is the number
-- of live 'running' jobs per (tenant, group_key, task_name); the counter is a
-- performance cache that can drift under crashes and is repaired here.
UPDATE group_running g
SET running = COALESCE(actual.n, 0)
FROM group_running g2
LEFT JOIN (
    SELECT tenant, group_key, task_name, count(*) AS n
    FROM jobs
    WHERE state = 'running' AND max_concurrent_per_group IS NOT NULL
    GROUP BY tenant, group_key, task_name
) actual
  ON actual.tenant = g2.tenant
 AND actual.group_key = g2.group_key
 AND actual.task_name = g2.task_name
WHERE g.tenant = g2.tenant
  AND g.group_key = g2.group_key
  AND g.task_name = g2.task_name
  AND g.running <> COALESCE(actual.n, 0);
