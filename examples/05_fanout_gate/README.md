# 05 — Fan-out + gate

Spawn N child jobs and a continuation that runs only when the children satisfy a gate
policy — the classic map/reduce barrier.

```bash
./run.sh
```

**What it shows**

- `POST /v1/fanout` inserts the children plus a `gates` row in one transaction and
  returns `{child_job_ids, gate_id}`.
- **Gate policies:**
  - `all_success` — fire only if every child SUCCEEDED (default).
  - `all_terminal` — fire when every child is terminal (success *or* dead).
  - `quorum(n)` — fire when `n` children succeed.
- The gate fires **exactly once** (a `fired_at` claim-once column), then enqueues
  `on_complete`. A gate that never fires shows up in `symba_gate_age_seconds` and the
  "unfired gate > 1h" alert.
