-- job_events.sql — the audit timeline for one job (backs the "Job detail" view).
--
-- job_events is the product, not telemetry: the UI job-detail timeline renders
-- these rows verbatim — who submitted, when queued, which worker claimed, every
-- retry with error + stack_hash, who signaled, what payload resumed a wait. Ordered
-- oldest-first so the timeline reads top-to-bottom. NO join to jobs: the ledger
-- outlives the job row (the row moves to the archive; events stay), so this is a
-- pure (job_id, at) index scan on ix_job_events_job.
--
-- Transaction context: general pool, read-only, no lock. Tenant-scoped so a guessed
-- job_id cannot leak another tenant's ledger.
--
-- Parameters: $1 uuid job_id, $2 text tenant
SELECT event, at, detail
FROM job_events
WHERE job_id = $1 AND tenant = $2
ORDER BY at ASC, id ASC;
