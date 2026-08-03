# Symba examples

Ten runnable samples that exercise the **engine's real API** — no SDK required. Nine
drive the HTTP/JSON control plane (`curl`), one is a raw-gRPC worker against the data
plane using the committed protobuf stubs. Each sample lives in its own folder with a
focused README.

> The ergonomic worker/client SDK lives in a **separate repository**. These samples
> deliberately use the raw API so they document the wire contract itself and keep
> working even before the SDK exists.

## Prerequisites

Bring the stack up — engine (`:7300` HTTP, `:7233` gRPC), Postgres, Flyway, optional
Redis, and the operator UI (`:8080`):

```bash
docker compose up -d --wait
```

The control-plane samples talk to `http://localhost:7300` by default (override with
`SYMBA_URL`). With the default `[auth] mode = "none"`, loopback calls need no
credentials; set `SYMBA_TOKEN` if your engine runs in `token` mode (sent as
`Authorization: Bearer <token>`).

Shared helpers (`SYMBA_URL`, auth header, a `poll_until_terminal` function, and a
`jq`-or-python JSON reader) live in [`_common.sh`](_common.sh); each script sources it.

## The samples

| # | Folder | Plane | Demonstrates |
|---|---|---|---|
| 01 | [`01_submit_and_poll/`](01_submit_and_poll/) | REST | submit one job, poll `GET /v1/jobs/{id}` to a terminal state |
| 02 | [`02_dedup/`](02_dedup/) | REST | idempotent submit via `dedup_key` — the second submit is deduplicated |
| 03 | [`03_chain/`](03_chain/) | REST | `on_success` + `chain_tail` linked-list chaining |
| 04 | [`04_priority_and_groups/`](04_priority_and_groups/) | REST | priority ordering + per-group concurrency ceiling |
| 05 | [`05_fanout_gate/`](05_fanout_gate/) | REST | `POST /v1/fanout`: N children + a gate that fires the continuation |
| 06 | [`06_signals_waiting/`](06_signals_waiting/) | REST | human-in-the-loop: a WAITING job resumed by `POST /v1/signals` |
| 07 | [`07_cancel_cascade/`](07_cancel_cascade/) | REST | cancel a job and cascade to its dependent cone |
| 08 | [`08_resubmit_dlq/`](08_resubmit_dlq/) | REST | DLQ replay: resubmit a DEAD job, preserving lineage |
| 09 | [`09_cron/`](09_cron/) | REST | list cron schedules and enable/disable one |
| 10 | [`10_observability/`](10_observability/) | REST | board/queues/workers/events/tree, `/metrics`, and the SSE event stream |
| — | [`raw_grpc_worker/`](raw_grpc_worker/) | gRPC | a minimal worker: `Claim` handshake → run → `Complete`, using raw `_pb2` stubs |

## Running one

```bash
cd examples/01_submit_and_poll
./run.sh
```

Every `run.sh` is idempotent and prints what it sends and what it gets back. Samples
06–08 that need a job to actually run (or die) explain their prerequisite in-folder —
most need at least one connected worker, which is what `raw_grpc_worker/` provides.
