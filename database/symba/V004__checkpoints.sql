-- V004__checkpoints.sql — durable checkpoints.
-- Redis is the fast path; this is the write-behind system of record. Preloaded
-- into JobAssignment.checkpoint_json on claim. GC'd by the sweeper per retention.

CREATE TABLE checkpoints (
    job_id      UUID PRIMARY KEY,
    tenant      TEXT NOT NULL DEFAULT 'default',
    data        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_checkpoints_updated ON checkpoints (updated_at);
