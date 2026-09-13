// Symba submit-throughput baseline (M1 exit criteria, throughput requirement).
//
// Written against the k6 v2 API (k6 crossed to v2 in 2026): options.thresholds,
// http_reqs rate gate, and handleSummary() to persist the baseline JSON that the
// nightly regression gate (fail on >20% claim-throughput regression)
// compares against.
//
// WHAT THIS MEASURES
//   The control-plane ingestion firehose: POST /v1/jobs. This is the front half of
//   the claim pipeline (submit -> queued -> claim). The worker-side gRPC claim
//   stream is benchmarked separately in the nightly gRPC suite; at M1 the meaningful,
//   HTTP-drivable baseline is submit rate + submit latency, which is what feeds the
//   claim scan and what the "10k-job burst drains" throughput scenario front-loads.
//
// RUN (against a compose-up engine on :8080)
//   k6 run -e SYMBA_URL=http://localhost:8080 tests/load/submit_throughput.js
//   # baseline is written to tests/load/baseline/submit_throughput.summary.json
//
// The thresholds below are the CI gate. Numbers are conservative M1 floors on a
// single engine + PG18 (target: >=500 claims/s per engine; submit must comfortably
// outpace claim so it is never the bottleneck). Tighten once real baseline data lands.

import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

const SYMBA_URL = __ENV.SYMBA_URL || 'http://localhost:8080';
const TENANT = __ENV.SYMBA_TENANT || 'loadtest';

// Named trend so the baseline file has a stable, greppable submit-latency key
// independent of k6's built-in http_req_duration (which mixes in setup calls).
const submitLatency = new Trend('symba_submit_latency_ms', true);

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    // Measure the sustained 500/s target. Ramping the entire run previously
    // averaged below 300/s, making the unchanged 450/s floor impossible.
    submit_firehose: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    // CI gate. These are the pass/fail lines for the M1 baseline.
    http_req_failed: ['rate<0.01'], // <1% submit errors
    http_reqs: ['rate>450'], // sustain ~500/s submit (>=90% of the 500 target)
    symba_submit_latency_ms: ['p(95)<50', 'p(99)<150'], // ingestion stays snappy
  },
};

export default function () {
  const body = JSON.stringify({
    tenant: TENANT,
    specs: [
      {
        task_name: 'loadtest.echo',
        // A small, representative payload; large-payload behavior is covered by the
        // PAYLOAD_TOO_LARGE backpressure test, not the throughput baseline.
        payload: { n: __ITER, ts: Date.now() },
      },
    ],
  });

  const res = http.post(`${SYMBA_URL}/v1/jobs`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'submit' },
  });

  submitLatency.add(res.timings.duration);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'returned a job id': (r) => {
      try {
        return JSON.parse(r.body).job_ids.length === 1;
      } catch {
        return false;
      }
    },
  });
}

// handleSummary persists the baseline. The nightly job diffs http_reqs.rate against
// the committed baseline and fails the run on a >20% regression.
export function handleSummary(data) {
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    'tests/load/baseline/submit_throughput.summary.json': JSON.stringify(data, null, 2),
  };
}
