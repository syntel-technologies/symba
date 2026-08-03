-- record_event.sql — append one audit row.
--
-- Transaction context: runs inside the caller's transaction so the event is
-- visible iff the state change commits. job_events is append-only + partitioned.
-- Lock behavior: plain INSERT.
--
-- tenant + ctx_id are DERIVED from the job (via the jobs_all hot+archive view)
-- rather than threaded through every callsite: the ledger MUST carry them or the
-- live SSE stream (tenant-filtered) and the snapshot-by-ctx read both return
-- nothing. jobs_all sees the row whether it is still hot or already archived in
-- this same transaction (the terminal "succeeded"/"dead"/"cancelled" events fire
-- right after the archive move). COALESCE keeps tenant NOT NULL for an orphan id.
--
-- Parameters: $1 uuid job_id, $2 text event, $3 jsonb detail (nullable)
INSERT INTO job_events (job_id, tenant, ctx_id, event, at, detail)
SELECT $1,
       COALESCE((SELECT tenant FROM jobs_all WHERE id = $1), 'default'),
       (SELECT ctx_id FROM jobs_all WHERE id = $1),
       $2, now(), $3;
