# Engine performance gate — investigation and validation

Measured on 13 September 2026 in [nightly run 34759476506](https://github.com/syntel-technologies/symba/actions/runs/34759476506), engine revision `7d6e25b4308eb87741c9a175dbe22a9652738844`.

The harness now builds the actual engine, runs a real echo worker and applies test migrations only once. The protocol field and Python environment failures are repaired. Chaos convergence and the database-clock dispatcher checks pass. The sustained end-to-end HTTP submission test still fails its existing requirements:

| Metric | Observed | Required |
| --- | --- | --- |
| HTTP submissions | 339.03/sec | >450/sec at 500/sec offered load |
| Submit p95 latency | approximately 1.33 sec | <50 ms |
| Completed HTTP iterations | 10,313 in 30.4 sec | sustained offered load |

This is a failed performance measurement, not a passing baseline. The committed baseline remains unseeded; do not copy this failed result into it to normalize a regression. The ready-to-claim k6 scenario and baseline comparison did not run after the submission failure, so their outcome is unknown. The repaired worker no longer crashed in this run.

The run co-locates the HTTP load generator, worker, engine, proxy, PostgreSQL and Redis on a standard GitHub-hosted Ubuntu runner. The measurement does not by itself identify whether runner contention, worker throughput, SQL execution, pool waits or the API path is the bottleneck. No performance threshold was lowered and no production performance claim is justified from this run.

Before the first engine release: reproduce the same workload, record runner CPU/memory, pool wait and query timings, check dispatcher/worker capacity reporting, profile submission and completion together, and review any implementation or explicitly justified benchmark-environment change. Rerun submission, ready-to-claim and baseline comparison successfully; only then seed a measured baseline through a PR. The engine publisher runs this suite and cannot publish artifacts while it fails. Functional CI and security review remain useful independently.

## HTTP-path fix (13 September 2026)

An isolated local PostgreSQL 18.1 + engine + real gRPC echo worker run reproduced the latency failure with the same k6 2.0.0 script. Removing a synchronous dependency's unnecessary thread-pool handoff improved the request path but was insufficient alone. Replacing the two `BaseHTTPMiddleware` layers with streaming-safe ASGI middleware removed per-request task groups and proxy channels. Tracing now also covers rejected authentication and restores the caller's context after each response. The test worker coalesces capacity snapshots like the SDK instead of replaying stale free-slot counts from an unbounded queue.

The resulting local submission run sustained 499.27 requests/sec with p95 16.25 ms; both unchanged p95 <50 ms and p99 <150 ms thresholds passed. This is a local diagnostic result, not the GitHub-hosted baseline. Unit and HTTP auth-boundary tests passed (171 tests, 100% core coverage), including concurrent streaming trace isolation. The cloud nightly run must still pass both phases before the release blocker is considered resolved or a baseline is committed.

The first cloud rerun (`34761333182`) passed throughput at 491.73/sec, but still failed latency at p95 234.97 ms. Native profiling then identified substantial socket/event-loop dispatch and Python HTTP parsing overhead. The runtime now selects uvloop for the shared HTTP/gRPC/PostgreSQL loop on CPython Unix and installs httptools for Uvicorn's automatic parser selection. The stdlib loop remains available on Windows, other Python implementations, and when asyncio debug mode is enabled (uvloop 0.22.1 has a known debug stack-finalization issue). See [uvloop usage](https://github.com/MagicStack/uvloop#using-uvloop), [Uvicorn settings](https://www.uvicorn.org/settings/), and [the debug-mode issue](https://github.com/MagicStack/uvloop/issues/715).

The console proxy now reuses upstream HTTP/1.1 connections. Nightly artifacts capture per-request latency metrics, engine histograms, container resource samples and host CPU/memory counters so a cloud failure can be diagnosed from evidence rather than rerun blindly. No latency, throughput, durability or authorization requirement was relaxed.

## Archive growth finding

`jobs_archive` was created with `LIKE jobs INCLUDING DEFAULTS`, which does not copy the hot table's primary-key index. `record_event.sql` resolves both tenant and context through `jobs_all` after every completion, producing two sequential archive scans per job. This grows with historical rows and saturates the completion pool even when submission itself is cheap. The SSE poller's tenant-scoped latest-ID lookup also lacked a matching index.

Append-only migration `V011__archive_lookup_indexes.sql` adds the partition-parent `(id)` index and the ledger `(tenant, id)` index. The regression test executes the real audit INSERT with `EXPLAIN ANALYZE` over 50,000 archived jobs and rejects sequential archive scans while verifying tenant/context attribution. Existing migrations, transaction durability and audit writes are preserved.

Deployment note: this migration builds indexes using PostgreSQL's standard blocking index creation on the partitioned parents. For a large existing archive, schedule a maintenance window and budget disk space; do not apply it during peak writes or edit already-applied migration checksums. The engine does not run migrations itself.
