// Symba ready-to-claim latency baseline.
//
// Busy-system half: sustain a submit load against a compose-up engine that
// has REAL gRPC workers claiming, then read the engine's own ready-to-claim
// histogram (symba_ready_to_claim_ms) straight off /metrics and assert its p95 < 150ms.
// This is the "polling tick silently degrading the latency requirement" guard from
// the other side: the CI-fast Python test (tests/integration/test_load_latency.py)
// proves the idle floor + a synthetic burst; this one proves it holds under real
// end-to-end load with workers in the loop.
//
// WHY SCRAPE /metrics INSTEAD OF TIMING IN k6
//   ready-to-claim is run_at -> started_at (queue-visible to claimed), a purely
//   engine-internal interval. k6 sees only submit RTT, not the dispatcher tick. The
//   engine already measures the real thing with DB clocks (skew-free); we assert on
//   that histogram rather than a client-side proxy.
//
// RUN (against a compose-up engine + workers on :8080)
//   docker compose up -d --wait
//   k6 run -e SYMBA_URL=http://localhost:8080 tests/load/ready_to_claim.js
//
// The threshold below is the CI gate: p95 < 150ms. Requires workers to be
// draining the queue; with no workers ready-to-claim is unbounded and this fails
// loudly (which is the correct signal — an unattended queue is not "healthy").

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

const SYMBA_URL = __ENV.SYMBA_URL || 'http://localhost:8080';
const TENANT = __ENV.SYMBA_TENANT || 'loadtest';

// The ready-to-claim histogram's p95, parsed out of /metrics at the end of the run
// and recorded as a single-sample trend so it lands in the baseline JSON with a
// stable key.
const readyToClaimP95 = new Trend('symba_ready_to_claim_p95_ms', true);

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    // Steady submit pressure so the dispatcher is continuously matching against
    // the worker fleet — the "busy system" half of the latency floor (vs the idle floor).
    sustain_submit: {
      executor: 'constant-arrival-rate',
      rate: 300,
      timeUnit: '1s',
      duration: '40s',
      preAllocatedVUs: 40,
      maxVUs: 150,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    // The latency floor, read from the engine's own histogram (see handleSummary).
    symba_ready_to_claim_p95_ms: ['p(95)<150'],
  },
};

export default function () {
  const body = JSON.stringify({
    tenant: TENANT,
    specs: [{ task_name: 'loadtest.echo', payload: { ts: Date.now() } }],
  });
  const res = http.post(`${SYMBA_URL}/v1/jobs`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'submit' },
  });
  check(res, { 'submit accepted': (r) => r.status === 200 });
}

// teardown scrapes /metrics once the fleet has drained the sustained load, parses the
// ready-to-claim histogram, and records its p95 so the threshold above can gate on it.
export function teardown() {
  // Let the last submitted jobs get claimed before reading the histogram.
  sleep(2);
  const res = http.get(`${SYMBA_URL}/metrics`);
  if (res.status !== 200) {
    throw new Error(`could not scrape /metrics: HTTP ${res.status}`);
  }
  const p95 = histogramQuantile(res.body, 'symba_ready_to_claim_ms', 0.95);
  if (p95 === null) {
    throw new Error('no symba_ready_to_claim_ms samples on /metrics — are workers claiming?');
  }
  readyToClaimP95.add(p95);
}

// Coarse Prometheus histogram_quantile over the exposition-format bucket lines:
//   symba_ready_to_claim_ms_bucket{le="150"} 1234
// Returns the upper bound of the first bucket whose cumulative count crosses q*total,
// a conservative upper bound, not PromQL's interpolated histogram_quantile.
// A pass proves the percentile is below the gate; a boundary-bucket failure may
// need finer buckets to distinguish values around the threshold.
function histogramQuantile(body, metric, q) {
  const bucketRe = new RegExp(`^${metric}_bucket\\{[^}]*le="([^"]+)"\\}\\s+([0-9.e+]+)`);
  const buckets = [];
  let total = 0;
  for (const line of body.split('\n')) {
    const m = line.match(bucketRe);
    if (!m) continue;
    const le = m[1] === '+Inf' ? Infinity : parseFloat(m[1]);
    const count = parseFloat(m[2]);
    buckets.push([le, count]);
    if (le === Infinity) total = count;
  }
  if (total === 0) return null;
  buckets.sort((a, b) => a[0] - b[0]);
  const target = q * total;
  for (const [le, count] of buckets) {
    if (count >= target) return le === Infinity ? 1e9 : le;
  }
  return null;
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    'tests/load/baseline/ready_to_claim.summary.json': JSON.stringify(data, null, 2),
  };
}
