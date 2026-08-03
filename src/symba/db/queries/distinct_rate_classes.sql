-- distinct_rate_classes.sql — peek the rate classes present in the ready set.
--
-- Cheap read the matcher runs BEFORE the claim query so it only reserves tokens for
-- classes that actually have work waiting for the given worker tags. Scans the
-- ix_jobs_claimable partial index (state='queued'); the DISTINCT over a tiny hot
-- table is O(rows-in-class), negligible next to the claim itself.
--
-- Transaction context: general/hot pool, standalone read (no lock). Lock behavior:
-- none.
--
-- Parameters:
--   $1 text[]  worker tags (must cover runs_on, same predicate as claim.sql)
SELECT DISTINCT rate_class
FROM jobs
WHERE state = 'queued'
  AND run_at <= now()
  AND rate_class IS NOT NULL
  AND runs_on <@ $1::text[];
