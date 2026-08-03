-- V002__job_events.sql — the immutable audit ledger.
-- Monthly declarative partitions on `at`; BRIN index (append-only, time-correlated);
-- B-tree on (job_id, at); NO FK to jobs (the job row moves to the archive, the
-- ledger outlives it); fillfactor 100 (never updated).

CREATE TABLE job_events (
    id      BIGINT GENERATED ALWAYS AS IDENTITY,
    job_id  UUID NOT NULL,
    tenant  TEXT NOT NULL DEFAULT 'default',
    ctx_id  TEXT,
    event   TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    detail  JSONB
) PARTITION BY RANGE (at);

-- Storage params live on leaf partitions (never updated -> fillfactor 100).
CREATE TABLE job_events_default PARTITION OF job_events DEFAULT
    WITH (fillfactor = 100);

CREATE INDEX ix_job_events_at   ON job_events USING BRIN (at);
CREATE INDEX ix_job_events_job  ON job_events (job_id, at);
CREATE INDEX ix_job_events_ctx  ON job_events (tenant, ctx_id, at);
