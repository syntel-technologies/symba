# 01 — Submit and poll

The simplest end-to-end path: submit one job over the control plane, then poll its
status until it reaches a terminal state (`succeeded` / `dead` / `cancelled`).

```bash
./run.sh
```

**What it shows**

- `POST /v1/jobs` takes `{tenant, specs:[...]}` and returns `{job_ids, deduplicated}`.
  The tenant is authoritative from your credential — the body field is ignored when
  auth is on.
- `GET /v1/jobs/{id}` returns the current state; a fresh job is `queued` until a worker
  claims it. Start `../raw_grpc_worker/` (or any worker registered for `demo.echo`) to
  see it progress to `running` → `succeeded`.

Without a connected worker the job stays `queued` — the poll loop times out and says so,
which is the correct signal (an unattended queue is not "done").
