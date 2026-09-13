-- running_counts_by_worker.sql — authoritative capacity guard for dispatch.
--
-- ClaimRequest slot frames can cross assignments already in flight on the
-- bidirectional stream. A stale frame may therefore temporarily advertise more
-- local capacity than is truly available. Cap that advisory value by the live
-- jobs already attributed to each worker before claiming another batch.
--
-- Parameters:
--   $1 text[] worker ids
SELECT claimed_by AS worker_id, count(*)::int AS running_count
FROM jobs
WHERE state = 'running'
  AND claimed_by = ANY($1::text[])
GROUP BY claimed_by;
