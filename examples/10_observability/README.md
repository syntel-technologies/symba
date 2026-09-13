# 10 — Observability

Everything the operator console is built on, exercised from the command line: the
read/stats surface, the immutable event ledger (N8), the DAG, Prometheus `/metrics`,
and the live SSE event stream.

```bash
./run.sh
```

**What it shows**

- `GET /v1/stats/board` — job counts by state (the live board).
- `GET /v1/stats/queues` — depth + oldest-age per (task, runs_on, rate_class).
- `GET /v1/workers` — the fleet: tags, slots in use, last_seen (stale workers flagged).
- `GET /v1/jobs/{id}/events` — the append-only `job_events` ledger; every transition is
  one row, so any job is fully reconstructable after the fact (N8).
- `GET /v1/jobs/{id}/tree` — chain/dep/gate edges for the pipeline view.
- `GET /metrics` — the Prometheus surface. Ship the example alert rules in
  [`deploy/prometheus/symba_alerts.yml`](../../deploy/prometheus/symba_alerts.yml).
- `GET /v1/events/stream` — a Server-Sent-Events tail of `job_events`, filterable by
  `ctx_id`/state; this is what makes the UI update live. Run it with `curl -N` in a
  second terminal (the browser client uses the same `Authorization` header as REST).
