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
