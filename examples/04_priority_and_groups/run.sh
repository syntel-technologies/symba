#!/usr/bin/env bash
# 04 — Priority ordering + per-group concurrency ceiling.
. "$(dirname "$0")/../_common.sh"

echo "==> submit a batch: mixed priority, all in group 'tenant-acme' capped at 2 concurrent"
resp="$(api POST /v1/jobs '{
  "tenant": "default",
  "specs": [
    { "task_name": "report.build", "payload": {"i":1}, "priority": 0, "group_key": "tenant-acme", "max_concurrent_per_group": 2 },
    { "task_name": "report.build", "payload": {"i":2}, "priority": 0, "group_key": "tenant-acme", "max_concurrent_per_group": 2 },
    { "task_name": "report.build", "payload": {"i":3}, "priority": 5, "group_key": "tenant-acme", "max_concurrent_per_group": 2 }
  ]
}')"
echo "    $resp"

echo "==> higher priority (5) is claimed first; group ceiling holds running <= 2 for 'tenant-acme'"
echo "==> queue view: GET /v1/stats/queues"
api GET /v1/stats/queues || true
echo
