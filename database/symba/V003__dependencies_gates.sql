-- V003__dependencies_gates.sql — static joins and fan-out gates.

-- depends_on edges. remaining_deps on the job row is the transactional counter;
-- this table records the edges + aliases for ctx.output resolution and cascade.
CREATE TABLE job_dependencies (
    job_id             UUID NOT NULL,   -- the dependent (downstream)
    depends_on_job_id  UUID NOT NULL,   -- the dependency (upstream producer)
    alias              TEXT,            -- key in ctx.output; defaults to producer task_name
    PRIMARY KEY (job_id, depends_on_job_id)
);
CREATE INDEX ix_deps_upstream ON job_dependencies (depends_on_job_id);

-- Gates: a fan-out completion barrier. Fires its continuation exactly once
-- (fired_at claim-once).
CREATE TABLE gates (
    id                 UUID PRIMARY KEY DEFAULT uuidv7(),
    tenant             TEXT NOT NULL DEFAULT 'default',
    ctx_id             TEXT,
    policy             TEXT NOT NULL DEFAULT 'all_success',  -- all_success|all_terminal|quorum(n)
    expected_children  INT NOT NULL,
    completed_children INT NOT NULL DEFAULT 0,
    succeeded_children INT NOT NULL DEFAULT 0,
    quorum_n           INT,                                  -- for quorum(n) policy
    on_complete        JSONB NOT NULL,                       -- continuation JobSpec
    fired_at           TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_gates_ctx ON gates (tenant, ctx_id);
