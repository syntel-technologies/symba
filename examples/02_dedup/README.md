# 02 — Idempotent submit (dedup_key)

Submitting twice with the same `dedup_key` inserts the job **once**. The second call
returns `deduplicated: true` and an empty `job_id` — the client can retry a submit
safely (network blip, at-least-once producer) without double-enqueuing.

```bash
./run.sh
```

**What it shows**

- `dedup_key` on a spec keys the `ux_jobs_dedup` uniqueness constraint. The engine's
  `idempotency_key` derives from `(tenant, dedup_key, job_id)` and is retry-stable, so
  the downstream side effect commits once even across worker crashes/retries (this is
  the invariant the L5 chaos "effect-once" scenario pins).
