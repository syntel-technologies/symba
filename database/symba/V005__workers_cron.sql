-- V005__workers_cron.sql — fleet registry and cron schedules.

-- Worker registry: refreshed on Claim/Heartbeat; sweeper marks stale ones.
CREATE TABLE workers (
    worker_id   TEXT PRIMARY KEY,
    tags        TEXT[] NOT NULL DEFAULT '{}',
    labels      JSONB NOT NULL DEFAULT '{}',
    slots       INT NOT NULL DEFAULT 0,
    slots_busy  INT NOT NULL DEFAULT 0,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    stale       BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX ix_workers_last_seen ON workers (last_seen);

-- Cron schedules. Every instance ticks; correctness comes from the
-- deterministic dedup key on submit, not from the advisory-lock election.
CREATE TABLE cron_schedules (
    schedule_id  TEXT PRIMARY KEY,
    cron_expr    TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}',
    tenant       TEXT NOT NULL DEFAULT 'default',
    enabled      BOOLEAN NOT NULL DEFAULT true,
    last_fire    TIMESTAMPTZ,
    next_fire    TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
