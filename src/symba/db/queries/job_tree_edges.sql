-- job_tree_edges.sql — dependency edges within a ctx (backs the "Pipeline" DAG).
--
-- The Pipeline view renders a React Flow DAG for one ctx_id: nodes come from
-- list_jobs(ctx_id=...), edges come from here. A dep edge is upstream -> downstream
-- (depends_on_job_id -> job_id). We scope to the ctx by requiring BOTH endpoints to
-- be jobs in that ctx (join jobs_all twice), so a cross-ctx dependency does not drag
-- foreign nodes into the graph.
--
-- Chain edges (on_success lineage) are derived client-side from the node list's
-- on_success field; only the many-to-one dep edges need this relational lookup.
--
-- Transaction context: general pool, read-only, no lock.
--
-- Parameters: $1 text tenant, $2 text ctx_id
SELECT d.depends_on_job_id AS upstream, d.job_id AS downstream, d.alias
FROM job_dependencies d
JOIN jobs_all up   ON up.id = d.depends_on_job_id AND up.tenant = $1 AND up.ctx_id = $2
JOIN jobs_all down ON down.id = d.job_id          AND down.tenant = $1 AND down.ctx_id = $2;
