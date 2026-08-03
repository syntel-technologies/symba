-- claim.sql — THE hot query.
--
-- Transaction context: ONE transaction (claim + counter bumps). Safe for N
-- concurrent engine instances and N workers.
-- Lock behavior: FOR UPDATE SKIP LOCKED serializes row claims without blocking.
--
-- Parameters:
--   $1 text[]  worker tags (must cover runs_on)
--   $2 text[]  exhausted rate classes to skip
--   $3 int     LIMIT (= sum of free slots for the tag group)
--   $4 text    claimed_by (worker_id)
--   $5 int     per-group fairness cap (rows of any one group_key per batch)
--
-- Invariants:
--   X1  run_at <= now() is revalidated INSIDE this locking statement — never trust
--       an earlier scan, or a concurrent worker can claim a job that was just
--       deferred. Never split "find candidates" from "lock candidates" into two
--       statements.
--   X2  Every time comparison uses DB now(); no client timestamps (clock drift
--       between workers can otherwise cause premature or missed claims).
--   X3  FOR UPDATE SKIP LOCKED MUST name the base table (FOR UPDATE OF j) and that
--       base table (jobs j) MUST appear in the locking query's own FROM clause.
--       A locking clause does NOT propagate into a WITH query, so locking the output
--       of a windowed CTE is a silent no-op and lets two concurrent claimers grab
--       the same row (PG docs: "A locking clause ... does not apply to WITH queries
--       referenced by the primary query"). Every candidate CTE below scans jobs
--       directly with FOR UPDATE OF j.
--
--   Two candidate paths, UNIONed, because the group window is only meaningful for
--   grouped jobs and it defeats the claim index:
--     * ungrouped (group_key IS NULL) — THE hot, common case: no window (each job is
--       its own group, always rn=1), so this scans jobs in ix_jobs_claimable order
--       (priority DESC, run_at ASC WHERE state='queued') and stops at LIMIT. O(log n).
--     * grouped (group_key IS NOT NULL): keep the PARTITION BY group_key window for
--       per-group fairness ($5 cap) + the group_running ceiling. Grouped backlogs are
--       small relative to the ungrouped firehose, so the window scan is bounded.
--   MATERIALIZED on the final candidate set forces one evaluation so the planner
--   cannot re-run a locking scan in a nested loop and over-claim past LIMIT.
WITH ungrouped AS (
    SELECT j.id, j.priority, j.run_at
    FROM jobs j
    WHERE j.state = 'queued'
      AND j.group_key IS NULL
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
    ORDER BY j.priority DESC, j.run_at ASC
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
),
grouped_ranked AS (
    SELECT j.id, j.priority, j.run_at,
           row_number() OVER (PARTITION BY j.group_key
                              ORDER BY j.priority DESC, j.run_at ASC) AS rn_in_group
    FROM jobs j
    LEFT JOIN group_running g
           ON g.tenant = j.tenant
          AND g.group_key = j.group_key
          AND g.task_name = j.task_name
    WHERE j.state = 'queued'
      AND j.group_key IS NOT NULL
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
      AND (j.max_concurrent_per_group IS NULL
           OR COALESCE(g.running, 0) < j.max_concurrent_per_group)
),
grouped AS (
    SELECT j.id, j.priority, j.run_at
    FROM jobs j
    JOIN grouped_ranked r USING (id)
    WHERE r.rn_in_group <= $5
    ORDER BY j.priority DESC, r.rn_in_group ASC, j.run_at ASC
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
),
candidate AS MATERIALIZED (
    -- Both paths already locked their rows (FOR UPDATE OF j); this just merges and
    -- caps the combined batch, highest priority / oldest first, so a mixed
    -- grouped+ungrouped backlog still assigns in priority order.
    SELECT id FROM (
        SELECT id, priority, run_at FROM ungrouped
        UNION ALL
        SELECT id, priority, run_at FROM grouped
    ) merged
    ORDER BY priority DESC, run_at ASC
    LIMIT $3
),
claimed AS (
    UPDATE jobs j
    SET state = 'running',
        claimed_by = $4,
        lease_token = gen_random_uuid()::text,
        attempt = attempt + 1,
        started_at = COALESCE(j.started_at, now()),
        lease_expires_at = now() + make_interval(secs => j.lease_ttl_s),
        last_heartbeat_at = now()
    FROM candidate c
    WHERE j.id = c.id
    RETURNING j.*
),
counter_bump AS (
    INSERT INTO group_running (tenant, group_key, task_name, running)
    SELECT tenant, group_key, task_name, count(*)
    FROM claimed
    WHERE max_concurrent_per_group IS NOT NULL
    GROUP BY tenant, group_key, task_name
    ON CONFLICT (tenant, group_key, task_name) DO UPDATE
    SET running = group_running.running + EXCLUDED.running
    RETURNING 1
)
-- Preload the checkpoint (write-behind system of record) so a resumed
-- attempt gets its checkpoint in the SAME claim round-trip — no per-job N+1 read
-- in the assignment builder. LEFT JOIN: the common case (no checkpoint) is a NULL,
-- and checkpoints is PK'd on job_id so this is a single index probe per claimed row.
SELECT c.*, cp.data AS checkpoint_data
FROM claimed c
LEFT JOIN checkpoints cp ON cp.job_id = c.id;
