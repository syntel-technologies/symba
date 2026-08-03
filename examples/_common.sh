#!/usr/bin/env bash
# Shared helpers for the REST examples. Source it: `. ../_common.sh`.
#
# Exposes:
#   SYMBA_URL        engine control-plane base (default http://localhost:7300)
#   api METHOD PATH [BODY]     -> curl with auth + content-type, prints the response body
#   json BODY KEY              -> read a top-level key from a JSON string (jq or python)
#   poll_until_terminal JOBID  -> GET the job until state is terminal, print each poll
set -euo pipefail

SYMBA_URL="${SYMBA_URL:-http://localhost:7300}"

# Auth header only when a token is configured (engine default mode="none" on loopback
# needs none). Kept as an array so an empty token adds no header at all.
_AUTH=()
if [[ -n "${SYMBA_TOKEN:-}" ]]; then
  _AUTH=(-H "Authorization: Bearer ${SYMBA_TOKEN}")
fi

api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "${SYMBA_URL}${path}" \
      -H 'Content-Type: application/json' "${_AUTH[@]}" -d "$body"
  else
    curl -fsS -X "$method" "${SYMBA_URL}${path}" "${_AUTH[@]}"
  fi
}

# Minimal JSON field reader: prefer jq, fall back to python3 so the samples run with
# neither the SDK nor jq installed. KEY is a dotted path with numeric indices, e.g.
# "job_ids.0" or "jobs.0.state".
json() {
  local body="$1" key="$2"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$body" | jq -r ".${key}"
    return
  fi
  SYMBA_JSON_KEY="$key" python3 - "$body" <<'PY'
import json, os, sys
data = json.loads(sys.argv[1])
for part in os.environ["SYMBA_JSON_KEY"].split("."):
    if isinstance(data, list):
        data = data[int(part)]
    else:
        data = data.get(part) if isinstance(data, dict) else None
    if data is None:
        break
print("" if data is None else data if isinstance(data, str) else json.dumps(data))
PY
}

_TERMINAL="succeeded dead cancelled"

poll_until_terminal() {
  local job_id="$1" tries="${2:-30}" state
  for ((i = 0; i < tries; i++)); do
    local resp
    resp="$(api GET "/v1/jobs/${job_id}")"
    state="$(json "$resp" state)"
    echo "  poll $((i + 1)): ${job_id} -> ${state}"
    for t in $_TERMINAL; do
      [[ "$state" == "$t" ]] && return 0
    done
    sleep 1
  done
  echo "  (did not reach a terminal state in ${tries}s — is a worker connected?)" >&2
  return 0
}
