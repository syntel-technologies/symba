# Load / benchmark suite (L6)

The nightly workflow runs a real engine, PostgreSQL 18.1 and gRPC echo workers, measures two distinct phases, and rejects a throughput regression greater than 20% against the committed submission baseline. Engine artifact publication reruns this suite on the tagged commit.

| Script | Workload | Gate |
| --- | --- | --- |
| `submit_throughput.js` | 500 HTTP submissions/sec for 30 seconds | >450/sec; <1% errors; submit p95 <50 ms and p99 <150 ms |
| `ready_to_claim.js` | 300 submissions/sec for 40 seconds; phase-only engine histogram | Ready-to-claim p95 <150 ms; ≥11,880 iterations and claims; all response checks pass |

HTTP ingestion throughput is not a measurement of sustained completion throughput. The 500/sec phase must drain before the claim phase begins. Claim p95 is the conservative upper bound of a histogram bucket; it is not an interpolated percentile. See [results and limits](../../docs/performance-release-blocker.md).

## Reproducing the workflow

Use a **disposable checkout and stack**, never a deployed database or another running Compose project. The supplied Compose file contains fixed resource names; ensure they do not collide before starting it. The nightly workflow provides the complete authoritative commands.

1. Sync the locked Python 3.13 dev environment, generate protocol stubs and start the isolated Compose stack.
2. Install k6 2.0.0. Start four processes with `uv run --no-sync python tools/ci_load_worker.py 127.0.0.1:7233 ci-load-echo-N 64`, replacing `N` with 0–3.
3. Run `uv run --no-sync python tools/ci_load_prepare.py http://localhost:8080`. This verifies worker registration, warms 500 jobs and requires successful drain.
4. Run `k6 run -e SYMBA_URL=http://localhost:8080 tests/load/submit_throughput.js`.
5. Require drain with `uv run --no-sync python tools/ci_load_prepare.py http://localhost:8080 --drain-only`.
6. Run `k6 run -e SYMBA_URL=http://localhost:8080 tests/load/ready_to_claim.js`, then `uv run --no-sync python tools/load_regression_gate.py`.
7. Stop only the stack and workers created for this run.

The faster worker-free dispatcher test (`tests/integration/test_load_latency.py`, `pytest -m l6`) covers the idle floor and synthetic burst separately. The Compose file does not start benchmark workers for you.

## Baseline provenance

`baseline/submit_throughput.summary.json` is the unmodified successful k6 artifact from [nightly run 34763666367](https://github.com/syntel-technologies/symba/actions/runs/34763666367). `baseline/provenance.json` records its engine revision, runner configuration, hashes and both phase results. It replaces the earlier unseeded placeholder; failed measurements were never used to lower the baseline.

The scripts overwrite summaries with fresh measurements. The comparison reads the committed prior version using `git show HEAD:...`, so it cannot compare a run against itself. Commit a replacement only deliberately through review, with a fully passing run and updated provenance. Standard hosted runners can vary; investigate a failure instead of repeatedly retrying until it disappears or silently changing thresholds. Absolute gates apply independently of the ratio.
