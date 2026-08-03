#!/usr/bin/env bash
# 01 — Submit one job and poll it to a terminal state.
. "$(dirname "$0")/../_common.sh"

echo "==> POST /v1/jobs"
resp="$(api POST /v1/jobs '{
  "tenant": "default",
  "specs": [
    { "task_name": "demo.echo", "payload": { "hello": "world" } }
  ]
}')"
echo "    $resp"

job_id="$(json "$resp" job_ids.0)"
echo "==> job_id = ${job_id}"

echo "==> polling GET /v1/jobs/${job_id}"
poll_until_terminal "$job_id"
