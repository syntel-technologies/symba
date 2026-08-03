# 04 — Priority + group concurrency ceilings

Two knobs that shape claim order and parallelism:

- **`priority`** — higher is claimed first (the claim scan orders by priority then age).
- **`group_key` + `max_concurrent_per_group`** — a ceiling on how many jobs in a group
  run at once, enforced by the `group_running` counter table. Use it to cap a noisy
  tenant or a rate-sensitive downstream without a separate rate class.

```bash
./run.sh
```

**What it shows**

- A batch where the `priority: 5` job jumps ahead of the `priority: 0` jobs.
- All three share `group_key: "tenant-acme"` with a ceiling of 2, so no more than two
  are `running` simultaneously; the third waits for a slot to free.
- `GET /v1/stats/queues` shows depth + oldest-age per (task, runs_on, rate_class).
