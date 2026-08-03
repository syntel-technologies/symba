#!/usr/bin/env bash
# 02 — Idempotent submit via dedup_key.
. "$(dirname "$0")/../_common.sh"

key="order-$(date +%s)"
body="$(printf '{
  "tenant": "default",
  "specs": [ { "task_name": "demo.charge", "payload": { "amount": 100 }, "dedup_key": "%s" } ]
}' "$key")"

echo "==> first submit (dedup_key=${key})"
r1="$(api POST /v1/jobs "$body")"
echo "    $r1"

echo "==> second submit with the SAME dedup_key"
r2="$(api POST /v1/jobs "$body")"
echo "    $r2"

echo "==> first deduplicated=$(json "$r1" deduplicated.0), second deduplicated=$(json "$r2" deduplicated.0)"
echo "    first job_id=$(json "$r1" job_ids.0), second job_id=$(json "$r2" job_ids.0)"
echo "    (the second submit is idempotent: deduplicated=true and it echoes the"
echo "     SAME job_id as the first — the effect happens once, no new job is created)"
