-- Capacity checks group live leases by worker. Keep this index in a new
-- versioned migration so existing installations receive it without changing
-- the checksum of the already-applied V001 baseline.
CREATE INDEX IF NOT EXISTS ix_jobs_running_worker ON jobs (claimed_by)
    WHERE state = 'running';
