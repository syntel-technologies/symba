-- set_claimed_by.sql — stamp the REAL worker_id onto just-assigned jobs.
--
-- claim.sql stamps a whole batch with one claimed_by ("engine:{tags}") because one
-- claim query serves a tag group; the matcher then splits that batch round-robin
-- across the group's workers. This re-stamp records which worker each job actually
-- landed on, so jobs.claimed_by carries the stable worker_id shown in the fleet view
-- and logs — enabling the per-worker running-jobs drill-in.
--
-- One statement per dispatch pass (not per job): the caller unnests two parallel
-- arrays so N assignments cost ONE round-trip on the hot pool. Guarded on
-- state='running' + matching lease_token so a re-stamp can never clobber a job that
-- a concurrent Fail/Complete already moved out from under this pass.
--
-- Transaction context: hot pool, single statement, no explicit tx. Lock behavior:
-- row locks taken by the UPDATE only (no SKIP LOCKED needed — these rows were just
-- claimed by THIS pass and carry this pass's lease tokens).
--
-- Parameters:
--   $1 uuid[]  job ids (parallel to $2/$3)
--   $2 text[]  worker ids (the assignment target for each job id)
--   $3 text[]  lease tokens (from the claim; guards against a raced terminal move)
UPDATE jobs j
SET claimed_by = a.worker_id
FROM (
    SELECT unnest($1::uuid[]) AS id,
           unnest($2::text[]) AS worker_id,
           unnest($3::text[]) AS lease_token
) a
WHERE j.id = a.id
  AND j.state = 'running'
  AND j.lease_token = a.lease_token;
