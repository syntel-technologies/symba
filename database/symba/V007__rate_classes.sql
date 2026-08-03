-- V007__rate_classes.sql — token bucket configs.
-- Redis Lua is the fast, engine-wide path; the tokens/refilled_at columns here
-- are the PG fallback bucket (degraded mode) and the runtime-editable config.

CREATE TABLE rate_classes (
    name         TEXT PRIMARY KEY,
    capacity     DOUBLE PRECISION NOT NULL,
    refill_per_s DOUBLE PRECISION NOT NULL,
    tokens       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- PG-fallback bucket state
    refilled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
