-- decrement_deps.sql — Complete/fail-die statement 4.
--
-- Transaction context: runs INSIDE the terminal transaction (after the archive
-- move), so a dependent only becomes claimable once its upstream is durably done.
-- Lock behavior: row locks on the dependent jobs only (small set = this job's
-- direct dependents). No advisory locks.
--
-- ONE UPDATE per dependent (Postgres forbids updating the same row twice across
-- CTEs in a single statement — the two-CTE form silently drops the flip). We
-- decrement remaining_deps and, in the same SET, flip 'submitted'->'queued' iff
-- this decrement lands it at zero AND its run_at is due. Everything else keeps
-- its current state (a job whose deps aren't all done stays 'submitted').
--
-- Returns the ids that flipped to 'queued' so the service can wake the dispatcher
-- directly instead of waiting out the idle-decay tick.
--
-- Parameters:
--   $1 uuid   the just-finished upstream job_id
UPDATE jobs
SET
    remaining_deps = remaining_deps - 1,
    state = CASE
        WHEN remaining_deps - 1 = 0 AND state = 'submitted' AND run_at <= now()
            THEN 'queued'
        ELSE state
    END
WHERE id IN (SELECT job_id FROM job_dependencies WHERE depends_on_job_id = $1::uuid)
RETURNING id, (state = 'queued' AND remaining_deps = 0) AS flipped;
