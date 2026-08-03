# Load / benchmark suite (L6)

k6 v2 scripts for the throughput/latency baselines named in the M1 exit criteria
("claim benchmark baseline stored") and the requirement of ≥500
claims/s per engine. The nightly CI job runs these with a stored
baseline and **fails the run on a >20% throughput regression**.

## Scripts

| Script | Measures | CI gate |
|---|---|---|
| `submit_throughput.js` | `POST /v1/jobs` ingestion rate + latency (the front half of the claim pipeline) | `http_reqs rate>450`, `http_req_failed rate<0.01`, submit p95<50ms / p99<150ms |
| `ready_to_claim.js` | queue→claim latency under load, read from the engine's own `symba_ready_to_claim_ms` histogram on `/metrics` (busy-system half of the latency floor) | `symba_ready_to_claim_p95_ms p(95)<150` |

> `ready_to_claim.js` needs **real gRPC workers** draining the queue (compose brings
> the SDK worker containers up); with no workers, ready-to-claim is unbounded and the
> gate fails loudly — which is correct, an unattended queue is not "healthy". The
> CI-fast, worker-free counterpart is `tests/integration/test_load_latency.py` (the
> idle floor + synthetic burst), run under `pytest -m l6`.

> The worker-side gRPC **claim** stream throughput is benchmarked in the nightly
> gRPC suite (added with M3, once multi-worker routing exists). At M1 the meaningful,
> HTTP-drivable baseline is submit rate — submit must comfortably outpace claim so it
> is never the bottleneck when the required 10k-job burst drains.

## Running locally

```bash
# 1. bring the stack up (engine on :8080, PG18 + flyway sidecar)
docker compose up -d

# 2. install k6 v2 (macOS)   brew install k6
#    (linux)                 see https://grafana.com/docs/k6/latest/set-up/install-k6/

# 3. run — writes the baseline JSON under baseline/
k6 run -e SYMBA_URL=http://localhost:8080 tests/load/submit_throughput.js
```

## Baseline

`baseline/submit_throughput.summary.json` is produced by the script's
`handleSummary()` (k6 v2 API). It is **committed** so the nightly regression gate has
something to diff against. Regenerate it deliberately (not on every run) when a
legitimate perf change lands, and note the machine/PG config in the commit message —
absolute numbers are only comparable on like hardware; the gate compares *ratios*
(current `http_reqs.rate` vs baseline `http_reqs.rate`, fail if <0.8×).

Until the first nightly run on CI hardware populates it, the committed baseline is a
placeholder documenting the expected shape; the CI gate treats a placeholder baseline
as "record, don't compare" for the first run.
