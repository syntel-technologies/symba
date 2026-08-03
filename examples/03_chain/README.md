# 03 — Chains (on_success + chain_tail)

A chain is a linked list of tasks: the head runs, and on success the engine enqueues
the next task, inheriting the chain context. `on_success` is the immediate successor;
`chain_tail` is the rest of the list.

```bash
./run.sh
```

**What it shows**

- One `POST /v1/jobs` with `on_success: "etl.transform"` and
  `chain_tail: ["etl.load"]` submits a 3-step pipeline as a single spec.
- On each success (§5.4 stmt 3) the engine materializes the next step live — you never
  submit the successors yourself.
- `GET /v1/jobs/{id}/tree` returns the DAG edges (chain edges = `on_success` lineage),
  which the operator UI renders as a pipeline.

A worker registered for `etl.*` must be connected for the chain to advance.
