#!/usr/bin/env bash
# 07 — Cancel a job and cascade to its dependent cone.
. "$(dirname "$0")/../_common.sh"

echo "==> submit a job we will then cancel"
resp="$(api POST /v1/jobs '{
  "tenant": "default",
  "specs": [ { "task_name": "long.job", "payload": { "n": 1 }, "on_success": "downstream.step" } ]
}')"
job_id="$(json "$resp" job_ids.0)"
echo "    job_id = ${job_id}"

echo "==> POST /v1/jobs/${job_id}/cancel?cascade=true"
c="$(api POST "/v1/jobs/${job_id}/cancel?cascade=true")"
echo "    $c"

echo "==> cancelled=$(json "$c" cancelled) was_running=$(json "$c" was_running)"
echo "    A non-running job is archived 'cancelled' immediately; a running one is asked"
echo "    to stop cooperatively (cancel_requested flag surfaced on the next Heartbeat)."
echo "    cascade=true transitively cancels live downstream dependents."
