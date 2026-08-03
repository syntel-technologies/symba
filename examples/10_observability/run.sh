#!/usr/bin/env bash
# 10 — Observability surface: stats, events, tree, metrics, and the SSE stream.
. "$(dirname "$0")/../_common.sh"

echo "==> live board (counts by state): GET /v1/stats/board"
api GET /v1/stats/board || true
echo

echo "==> queues (depth + oldest age per task): GET /v1/stats/queues"
api GET /v1/stats/queues || true
echo

echo "==> fleet: GET /v1/workers"
api GET /v1/workers || true
echo

echo "==> submit a job so we have something to inspect"
resp="$(api POST /v1/jobs '{ "tenant": "default", "specs": [ { "task_name": "demo.echo", "payload": {} } ] }')"
job_id="$(json "$resp" job_ids.0)"
echo "    job_id = ${job_id}"

echo "==> immutable event ledger (N8): GET /v1/jobs/${job_id}/events"
api GET "/v1/jobs/${job_id}/events" || true
echo

echo "==> DAG edges: GET /v1/jobs/${job_id}/tree"
api GET "/v1/jobs/${job_id}/tree" || true
echo

echo "==> Prometheus metrics: GET /metrics (first 20 symba_ lines)"
api GET /metrics | grep '^symba_' | head -20 || true
echo

echo "==> live SSE stream (Ctrl-C to stop): GET /v1/events/stream"
echo "    curl -N ${SYMBA_URL}/v1/events/stream"
echo "    (not auto-run so the script can exit; try it in another terminal)"
