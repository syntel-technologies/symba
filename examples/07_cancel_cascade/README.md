# 07 — Cancel + cascade

Cancel handles every job state explicitly, and can cascade to the transitive dependent
cone.

```bash
./run.sh
```

**What it shows**

- `POST /v1/jobs/{id}/cancel?cascade=true`.
- **Per-state behavior:**
  - `submitted` / `queued` / `waiting` → archived `cancelled` immediately.
  - `running` → `cancel_requested` flag set; the worker sees it on the next
    `Heartbeat` response and aborts cooperatively (lease expiry is the backstop).
  - terminal (`succeeded` / `dead` / `cancelled`) → idempotent no-op.
- With `cascade=true`, every live job that transitively depends on this one is archived
  `cancelled`, each with a `job_events` row naming the root cause.

Cancel is idempotent — double-cancel is always safe, for every job state.
