#!/usr/bin/env bash
# 06 — Human-in-the-loop: signal a WAITING job.
. "$(dirname "$0")/../_common.sh"

# A job reaches WAITING when its handler calls Wait(wait_key=...) over gRPC and parks.
# Here we demonstrate the SIGNAL side: POST /v1/signals delivers a payload to whoever
# is waiting on that key (or parks it for a future waiter — rendezvous works both ways).
wait_key="approval-$(date +%s)"

echo "==> POST /v1/signals for wait_key=${wait_key}"
resp="$(api POST /v1/signals "$(printf '{
  "tenant": "default",
  "wait_key": "%s",
  "payload": { "approved": true, "by": "ops@demo" },
  "signaled_by": "ops@demo"
}' "$wait_key")")"
echo "    $resp"

echo "==> delivered=$(json "$resp" delivered)"
echo "    delivered=true  -> a job was WAITING on this key and just resumed"
echo "    delivered=false -> no waiter yet; the signal is parked until one waits (single-delivery)"
echo
echo "    To see delivered=true, first run a worker whose handler calls Wait(\"${wait_key}\")"
echo "    then re-run this script. See ../raw_grpc_worker for the Wait RPC shape."
