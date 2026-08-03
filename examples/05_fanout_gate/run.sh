#!/usr/bin/env bash
# 05 — Fan-out + gate: N children, a continuation that fires when the gate is met.
. "$(dirname "$0")/../_common.sh"

echo "==> POST /v1/fanout: 3 shard jobs + an 'aggregate' continuation, gate = all_success"
resp="$(api POST /v1/fanout '{
  "tenant": "default",
  "ctx_id": "batch-42",
  "children": [
    { "task_name": "map.shard", "payload": { "shard": 0 } },
    { "task_name": "map.shard", "payload": { "shard": 1 } },
    { "task_name": "map.shard", "payload": { "shard": 2 } }
  ],
  "on_complete": { "task_name": "reduce.aggregate", "payload": { "of": "batch-42" } },
  "gate_policy": "all_success"
}')"
echo "    $resp"

gate_id="$(json "$resp" gate_id)"
echo "==> gate_id = ${gate_id}"
echo "    When all 3 children reach SUCCEEDED the gate fires ONCE and enqueues"
echo "    reduce.aggregate. Try gate_policy 'all_terminal' or 'quorum(2)' too."
