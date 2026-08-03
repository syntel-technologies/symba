-- V001__jobs.sql — the hot table, archive, group counter, and claim indexes.
-- Postgres 18 required: uuidv7() gives time-ordered PKs.
--
-- Hot/archive split: jobs holds LIVE states only
-- (submitted|queued|running|waiting). Terminal transitions MOVE the row into
-- jobs_archive in the same transaction. This keeps the claim scan tiny and the
-- MVCC horizon short.

CREATE TABLE jobs (
    id                UUID PRIMARY KEY DEFAULT uuidv7(),
    task_name         TEXT NOT NULL,
    pipeline          TEXT,
    stage             TEXT,
    tenant            TEXT NOT NULL DEFAULT 'default',
    ctx_id            TEXT,
    state             TEXT NOT NULL DEFAULT 'submitted',
                      -- LIVE states only: submitted|queued|running|waiting
    priority          SMALLINT NOT NULL DEFAULT 0,
    group_key         TEXT,
    max_concurrent_per_group SMALLINT,
    wait_key          TEXT,
    wait_expires_at   TIMESTAMPTZ,
    event_payload     JSONB,                        -- consumed signal payload
    dedup_key         TEXT,
    runs_on           TEXT[] NOT NULL DEFAULT '{}',
    rate_class        TEXT,
    payload           JSONB NOT NULL,
    result            JSONB,                        -- populated transiently pre-archive
    parent_gate_id    UUID,
    on_success        TEXT,
    chain_tail        TEXT[] NOT NULL DEFAULT '{}',
    on_failure        JSONB,
    remaining_deps    INT NOT NULL DEFAULT 0,
    attempt           SMALLINT NOT NULL DEFAULT 0,
    max_attempts      SMALLINT NOT NULL DEFAULT 5,
    backoff           JSONB,
    timeout_s         INT NOT NULL DEFAULT 600,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_ttl_s       INT NOT NULL DEFAULT 60,
    lease_token       TEXT,                         -- guards every mutation
    claimed_by        TEXT,
    lease_expires_at  TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    cancel_requested  BOOLEAN NOT NULL DEFAULT false,  -- cooperative cancel
    resubmitted_from  UUID,                         -- DLQ replay lineage
    error_history     JSONB NOT NULL DEFAULT '[]',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ
)
WITH (
    -- MVCC hygiene: the hot table must be vacuumed aggressively and cheaply.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold    = 200,
    autovacuum_vacuum_cost_limit   = 2000,
    autovacuum_vacuum_cost_delay   = 2,
    fillfactor = 85                                 -- room for HOT updates on transitions
);

-- Terminal rows: same shape + terminal fields, partitioned for drop-based retention.
CREATE TABLE jobs_archive (
    LIKE jobs INCLUDING DEFAULTS,
    finished_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    final_state  TEXT NOT NULL                      -- succeeded|dead|cancelled
) PARTITION BY RANGE (finished_at);

-- Bootstrap partitions (the sweeper pre-creates future months).
CREATE TABLE jobs_archive_default PARTITION OF jobs_archive DEFAULT;

-- UNION view for reads. Columns aligned across both tables.
CREATE VIEW jobs_all AS
    SELECT j.*, NULL::timestamptz AS finished_at, j.state AS final_state FROM jobs j
    UNION ALL
    SELECT a.* FROM jobs_archive a;

-- Per-group running counts, maintained transactionally on claim/terminal.
CREATE TABLE group_running (
    tenant     TEXT NOT NULL,
    group_key  TEXT NOT NULL,
    task_name  TEXT NOT NULL,
    running    INT  NOT NULL DEFAULT 0 CHECK (running >= 0),
    PRIMARY KEY (tenant, group_key, task_name)
);

-- Indexes. Partial indexes stay tiny because the hot table is tiny.
CREATE INDEX ix_jobs_claimable ON jobs (priority DESC, run_at ASC)
    WHERE state = 'queued';                          -- THE claim scan
CREATE INDEX ix_jobs_runs_on   ON jobs USING GIN (runs_on)
    WHERE state = 'queued';                          -- tag cover check
CREATE UNIQUE INDEX ux_jobs_dedup ON jobs (tenant, dedup_key)
    WHERE dedup_key IS NOT NULL;                     -- dedup-among-live by construction
CREATE INDEX ix_jobs_ctx       ON jobs (tenant, ctx_id, created_at DESC);   -- pipeline view
CREATE INDEX ix_jobs_lease     ON jobs (lease_expires_at)
    WHERE state = 'running';                         -- sweeper reclaim scan
CREATE INDEX ix_jobs_waiting   ON jobs (tenant, wait_key)
    WHERE state = 'waiting';                         -- signal fan-in
CREATE INDEX ix_jobs_state_task ON jobs (tenant, state, task_name, created_at DESC);  -- UI
