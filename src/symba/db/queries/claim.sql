-- claim.sql — THE hot query.
--
-- Transaction context: statement 2 of repository.claim's transaction (after
-- prepare_group_counters.sql). Safe for N concurrent engines and N workers.
-- Lock behavior: capped groups are serialized through group_running, then
-- FOR UPDATE SKIP LOCKED serializes job-row claims without blocking.
--
-- Parameters:
--   $1 text[]  worker tags (must cover runs_on)
--   $2 text[]  exhausted rate classes to skip
--   $3 int     LIMIT (= sum of free slots for the tag group)
--   $4 text    claimed_by (worker_id)
--   $5 int     per-group fairness cap (rows of any one group_key per batch;
--              independent of max_concurrent_per_group)
--   $6 jsonb   exact reserved-token quota per rate_class. Empty preserves the
--              direct repository-call behavior used by maintenance/tests.
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
--   X4  prepare_group_counters.sql guarantees that every selected capped group
--       has a counter row. Locking that row serializes concurrent claimers, and
--       grouped admits only row_number <= (max_concurrent_per_group - running),
--       so a multi-row claim consumes remaining capacity rather than merely
--       checking running < cap.
--
--   Two candidate paths, UNIONed, because the group window is only meaningful for
--   grouped jobs and it defeats the claim index:
--     * ungrouped (group_key IS NULL) — THE hot, common case: no window (each job is
--       its own group, always rn=1), so this scans jobs in ix_jobs_claimable order
--       (priority DESC, run_at ASC WHERE state='queued') and stops at LIMIT. O(log n).
--     * grouped (group_key IS NOT NULL): keep the PARTITION BY group_key window for
--       per-group fairness ($5 cap) + the exact group_running ceiling. Grouped
--       backlogs are small relative to the ungrouped firehose, so the window scan
--       is bounded.
--   MATERIALIZED on the final candidate set forces one evaluation so the planner
--   cannot re-run a locking scan in a nested loop and over-claim past LIMIT.
WITH ready_capped_groups AS MATERIALIZED (
    -- At most one capped group can contribute fewer than one row, so selecting
    -- up to the overall claim limit is sufficient for a full batch. Filter out
    -- groups already known to be full before taking the bounded lock set.
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
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
      AND ($6::jsonb = '{}'::jsonb OR j.rate_class IS NULL
           OR ($6::jsonb ? j.rate_class
               AND COALESCE(($6::jsonb ->> j.rate_class)::int, 0) > 0))
      AND COALESCE(g.running, 0) < j.max_concurrent_per_group
    GROUP BY j.tenant, j.group_key, j.task_name
    ORDER BY max(j.priority) DESC, min(j.run_at) ASC,
             j.tenant ASC, j.group_key ASC, j.task_name ASC
    LIMIT $3
),
group_slots AS MATERIALIZED (
    -- One row lock serializes every claimer for this (tenant, group, task).
    -- SKIP LOCKED lets another engine continue with independent groups.
    SELECT g.tenant, g.group_key, g.task_name, g.running
    FROM group_running g
    JOIN ready_capped_groups ready
      ON ready.tenant = g.tenant
     AND ready.group_key = g.group_key
     AND ready.task_name = g.task_name
    ORDER BY ready.max_priority DESC, ready.oldest_run_at ASC,
             g.tenant ASC, g.group_key ASC, g.task_name ASC
    LIMIT $3
    FOR UPDATE OF g SKIP LOCKED
),
ungrouped_unlimited AS (
    SELECT j.id, j.priority, j.run_at
    FROM jobs j
    WHERE j.state = 'queued'
      AND j.group_key IS NULL
      AND (j.rate_class IS NULL OR $6::jsonb = '{}'::jsonb)
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
    ORDER BY j.priority DESC, j.run_at ASC
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
),
rate_ranked AS MATERIALIZED (
    SELECT j.id, j.priority, j.run_at, j.rate_class,
           row_number() OVER (
               PARTITION BY j.rate_class
               ORDER BY j.priority DESC, j.run_at ASC
           ) AS rn_in_rate
    FROM jobs j
    LEFT JOIN group_slots g
           ON g.tenant = j.tenant
          AND g.group_key = j.group_key
          AND g.task_name = j.task_name
    WHERE $6::jsonb <> '{}'::jsonb
      AND j.state = 'queued'
      AND j.rate_class IS NOT NULL
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR NOT (j.rate_class = ANY($2)))
      AND $6::jsonb ? j.rate_class
      AND COALESCE(($6::jsonb ->> j.rate_class)::int, 0) > 0
      AND (j.group_key IS NULL OR j.max_concurrent_per_group IS NULL
           OR (g.group_key IS NOT NULL
               AND g.running < j.max_concurrent_per_group))
),
ungrouped_limited AS (
    SELECT j.id, j.priority, j.run_at
    FROM jobs j
    JOIN rate_ranked r USING (id)
    WHERE j.group_key IS NULL
      AND r.rn_in_rate <= COALESCE(($6::jsonb ->> r.rate_class)::int, 0)
    ORDER BY j.priority DESC, j.run_at ASC
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
),
grouped_ranked AS (
    SELECT j.id, j.priority, j.run_at, j.rate_class,
           j.max_concurrent_per_group, g.running AS group_running,
           rr.rn_in_rate,
           row_number() OVER (PARTITION BY j.group_key
                              ORDER BY j.priority DESC, j.run_at ASC) AS rn_in_group,
           row_number() OVER (
               PARTITION BY j.tenant, j.group_key, j.task_name,
                            (j.max_concurrent_per_group IS NULL)
               ORDER BY j.priority DESC, j.run_at ASC
           ) AS rn_in_ceiling_group
    FROM jobs j
    LEFT JOIN group_slots g
           ON g.tenant = j.tenant
          AND g.group_key = j.group_key
          AND g.task_name = j.task_name
    LEFT JOIN rate_ranked rr ON rr.id = j.id
    WHERE j.state = 'queued'
      AND j.group_key IS NOT NULL
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))
      AND ($6::jsonb = '{}'::jsonb OR j.rate_class IS NULL
           OR rr.id IS NOT NULL)
      AND (j.max_concurrent_per_group IS NULL
           OR (g.group_key IS NOT NULL
               AND g.running < j.max_concurrent_per_group))
),
grouped AS (
    SELECT j.id, j.priority, j.run_at
    FROM jobs j
    JOIN grouped_ranked r USING (id)
    WHERE r.rn_in_group <= $5
      AND (r.max_concurrent_per_group IS NULL
           OR r.rn_in_ceiling_group
              <= GREATEST(0, r.max_concurrent_per_group - r.group_running))
      AND ($6::jsonb = '{}'::jsonb OR r.rate_class IS NULL
           OR r.rn_in_rate <= COALESCE(($6::jsonb ->> r.rate_class)::int, 0))
    ORDER BY j.priority DESC, r.rn_in_group ASC, j.run_at ASC
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
),
candidate AS MATERIALIZED (
    -- Both paths already locked their rows (FOR UPDATE OF j); this just merges and
    -- caps the combined batch, highest priority / oldest first, so a mixed
    -- grouped+ungrouped backlog still assigns in priority order.
    SELECT id FROM (
        SELECT id, priority, run_at FROM ungrouped_unlimited
        UNION ALL
        SELECT id, priority, run_at FROM ungrouped_limited
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
