# Engine performance validation

The existing nightly gate passed on 13 September 2026 in [run 34763666367](https://github.com/syntel-technologies/symba/actions/runs/34763666367), engine revision `e7ccb890a879a432b01d15a4a01ec08471afa10b`. Both load phases, drain, chaos convergence and the database-clock dispatcher checks passed. The submission summary is now the measured baseline; [provenance](../tests/load/baseline/provenance.json) records the environment, artifact hashes and phase metrics.

| Measurement | Result | Gate |
| --- | --- | --- |
| HTTP submissions, 500/sec offered for 30 seconds | 499.76/sec; 15,001 accepted | >450/sec |
| Submit p95 / p99 | 40.48 ms / 56.70 ms | <50 ms / <150 ms |
| Submission errors / response checks | 0% / 100% | <1% errors |
| Ready-to-claim p95 at 300 submissions/sec for 40 seconds | ≤25 ms histogram upper bound | <150 ms |
| Claim-phase iterations / claimed jobs | 12,001 / 12,001 | ≥11,880 each |
| Drain and chaos checks | Passed | Required |

## Scope and limits

This is a short, warmed benchmark on a standard GitHub-hosted Ubuntu runner with the engine, PostgreSQL 18.1, Redis, proxy, k6 2.0.0 and four real gRPC echo workers co-located. Each worker has 64 slots, preserving the original total of 256. The harness verifies registration, warms 500 jobs, requires successful drain and excludes preceding phases from the claim histogram. A reset or missing claim population fails the measurement. Submission failure still fails the job while the second phase can collect diagnostics after a successful drain.

The result resolves the observed nightly CI failure; it does not prove every production throughput target. The 500/sec phase measures **HTTP ingestion**, and its queue accumulated work that drained after submission stopped. The low claim-latency gate measures **300/sec**, not 500 sustained claims or completions/sec. Larger payloads, authenticated/TLS deployments, multi-engine contention, cold starts and long-running workload capacity need their own measurements. A previous cold-start run exceeded the p99 target. Do not advertise 500 completed jobs/sec with bounded queue delay from these results.

No throughput/latency requirement, transaction durability, lease accounting, audit write or tenant-isolation check was relaxed. Nightly still enforces the absolute limits and a maximum 20% throughput regression against the committed baseline; engine artifact publishing reruns the suite on the exact tag. One successful run is evidence for this revision and environment, not immunity from future regression or shared-runner variance.

## Fixes and regression coverage

- Removed unnecessary thread-pool dispatch for the state-only HTTP principal dependency and replaced two `BaseHTTPMiddleware` layers with streaming-safe ASGI middleware. Tests cover concurrent streaming trace isolation and authentication failures; trace context resets after responses.
- Reused proxy upstream HTTP/1.1 connections. CPython Unix uses uvloop for the shared HTTP/gRPC/database event loop and httptools for Uvicorn parsing. Windows, other Python implementations and asyncio debug mode retain the standard loop. See [uvloop usage](https://github.com/MagicStack/uvloop#using-uvloop), [Uvicorn settings](https://www.uvicorn.org/settings/) and [uvloop's debug-mode issue](https://github.com/MagicStack/uvloop/issues/715).
- Added append-only migration `V011__archive_lookup_indexes.sql`: the archive created with `LIKE ... INCLUDING DEFAULTS` lacked a primary-key lookup index, so every completed job's audit INSERT scanned archive partitions twice. The new `(id)` archive index and `(tenant, id)` ledger index avoid growing history scans. An `EXPLAIN ANALYZE` regression over 50,000 archived jobs rejects sequential archive scans and verifies tenant/context attribution.
- Coalesced bursty worker read-model writes to once per second per connection. Scheduling capacity stays immediate in memory. Metadata/capacity changes, reconnect and transition to fully idle persist immediately; idle heartbeats refresh liveness. Burst/idle/heartbeat and real registry integration tests cover this behavior.
- Repaired the real load worker's protocol and coalesced capacity snapshots instead of queuing stale frames. Nightly records request latency, engine histograms, container resource samples and host counters for diagnosis.

**Migration deployment:** `V011` uses standard blocking index creation on partitioned parents. Large existing archives need a maintenance window and disk-space budget. Do not change already-applied migration checksums; the engine does not self-migrate.

## Selected investigation evidence

| Run | Change / finding | Submit rate | Submit p95 | Outcome |
| --- | --- | --- | --- | --- |
| [34759476506](https://github.com/syntel-technologies/symba/actions/runs/34759476506) | Initial repaired harness | 339.03/sec | ~1.33 sec | Failed |
| [34761333182](https://github.com/syntel-technologies/symba/actions/runs/34761333182) | ASGI HTTP path | 491.73/sec | 234.97 ms | Failed |
| [34762507647](https://github.com/syntel-technologies/symba/actions/runs/34762507647) | Archive indexes | 495.20/sec | 45.38 ms | Failed p99; insufficient worker drain |
| [34763066525](https://github.com/syntel-technologies/symba/actions/runs/34763066525) | Warmed four-worker fleet | 494.88/sec | 169.07 ms | Failed; completion/status contention |
| [34763666367](https://github.com/syntel-technologies/symba/actions/runs/34763666367) | Coalesced worker status writes | 499.76/sec | 40.48 ms | All nightly gates passed |

Earlier failed runs were not committed as baselines. Local profiling helped isolate costs but was not substituted for the hosted CI measurement.
