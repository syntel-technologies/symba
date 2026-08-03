-- get_result.sql — lazy result fetch across hot + archive.
--
-- Transaction context: single read (general pool). Lock behavior: none.
-- Reads through the jobs_all UNION view so a result is found whether the job is
-- still live or already archived. Large results (>64KB) are stored by reference
-- (a URI in result.ref); callers dereference out-of-band.
--
-- Parameters: $1 uuid job_id, $2 text tenant
SELECT id, final_state AS state, result, error_history
FROM jobs_all
WHERE id = $1 AND tenant = $2;
