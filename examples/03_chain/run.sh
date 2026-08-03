#!/usr/bin/env bash
# 03 — Linked-list chaining with on_success + chain_tail.
. "$(dirname "$0")/../_common.sh"

echo "==> submit a 3-step chain: extract -> transform -> load"
resp="$(api POST /v1/jobs '{
  "tenant": "default",
  "specs": [
    {
      "task_name": "etl.extract",
      "payload": { "source": "s3://bucket/in.csv" },
      "on_success": "etl.transform",
      "chain_tail": ["etl.load"]
    }
  ]
}')"
echo "    $resp"

job_id="$(json "$resp" job_ids.0)"
echo "==> head job_id = ${job_id}"
echo "==> as each step succeeds the engine enqueues the next; watch the tree:"
echo "    GET /v1/jobs/${job_id}/tree   (chain edges = on_success lineage)"
api GET "/v1/jobs/${job_id}/tree" || true
echo
poll_until_terminal "$job_id"
