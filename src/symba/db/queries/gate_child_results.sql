-- gate_child_results.sql — per-child results for a just-fired gate's __gate__ manifest.
--
-- Transaction context: read inside the firing child's terminal tx, AFTER that
-- child has been archived (complete_move / fail_die) and AFTER bump_gate claimed
-- the fire. All succeeded children are in jobs_archive by then (same for the
-- firing child). Lock behavior: none — archive rows are immutable.
--
-- Shape matches the SDK fake (testing/fake_engine._maybe_fire_gate):
--   {"job_id", "task", "result"} per SUCCEEDED child. Skips and DEAD children
--   are excluded (they are not successes). A ctx.skip() still archives as
--   final_state='succeeded' but writes a 'skipped' ledger event — filter those out.
--
-- Parameters: $1 uuid gate_id
SELECT a.id AS job_id, a.task_name AS task, a.result
FROM jobs_archive a
WHERE a.parent_gate_id = $1::uuid
  AND a.final_state = 'succeeded'
  AND NOT EXISTS (
      SELECT 1 FROM job_events e
      WHERE e.job_id = a.id AND e.event = 'skipped'
  )
ORDER BY a.finished_at ASC NULLS LAST, a.id ASC;
