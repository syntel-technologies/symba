-- Archive rows do not inherit the hot table's primary-key index (LIKE ...
-- INCLUDING DEFAULTS copies defaults only). Completion audit writes and result
-- lookups resolve IDs through jobs_all; without this index each lookup scans
-- the growing archive. Index the partitioned parent so current and future
-- partitions receive the same lookup index. It is deliberately non-unique:
-- partitioned unique constraints must include the partition key (finished_at).
CREATE INDEX ix_jobs_archive_id ON jobs_archive (id);

-- The SSE poller reads the latest ledger ID per tenant. The existing
-- (tenant, ctx_id, at) index cannot provide that ordering as history grows.
CREATE INDEX ix_job_events_tenant_id ON job_events (tenant, id);
