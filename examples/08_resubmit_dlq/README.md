# 08 — DLQ replay (resubmit)

A job that exhausts its retries (or fails fatally) lands in `jobs_archive` with
`final_state = dead` — the dead-letter queue. `DEAD` jobs are **never** auto-pruned.
Once you've fixed the cause, replay a fresh attempt with lineage back to the original.

```bash
./run.sh
```

**What it shows**

- `GET /v1/jobs?state_filter=dead` lists the DLQ (the UI groups these by `stack_hash`,
  one row per distinct failure, with a bulk-resubmit action).
- `POST /v1/jobs/{id}/resubmit` queues a new attempt and returns
  `{new_job_id, resubmitted_from}` — the `resubmitted_from` link preserves provenance so
  you can trace a replayed job to what originally died.

The script no-ops gracefully if there are no DEAD jobs yet, and tells you how to make
one.
