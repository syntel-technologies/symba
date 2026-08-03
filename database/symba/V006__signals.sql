-- V006__signals.sql — human-in-the-loop rendezvous.
-- Both orders are race-free: signal-first parks the payload here for a later
-- wait to consume; wait-first consumes a pending signal via FOR UPDATE SKIP LOCKED.

CREATE TABLE signals (
    id           UUID PRIMARY KEY DEFAULT uuidv7(),
    tenant       TEXT NOT NULL DEFAULT 'default',
    wait_key     TEXT NOT NULL,
    payload      JSONB,
    signaled_by  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at  TIMESTAMPTZ
);
-- Fan-in scan for unconsumed signals of a key (wait-first path).
CREATE INDEX ix_signals_pending ON signals (tenant, wait_key)
    WHERE consumed_at IS NULL;
