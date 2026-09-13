// Symba HTTP submission baseline: 500 requests/s offered for 30 seconds.
// This measures durable ingestion rate and HTTP latency, not sustained worker
// completion throughput. The separate ready_to_claim.js phase tests claim latency
// at 300 submissions/s after this phase drains. See README.md for scope and results.
//
// Run only against an isolated stack with real gRPC workers:
//   k6 run -e SYMBA_URL=http://localhost:8080 tests/load/submit_throughput.js
// The unchanged absolute thresholds and the committed-baseline regression check
// both gate nightly CI and engine artifact publication.

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
