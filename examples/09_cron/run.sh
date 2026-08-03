#!/usr/bin/env bash
# 09 — Cron schedules: list and enable/disable.
. "$(dirname "$0")/../_common.sh"

echo "==> GET /v1/cron"
list="$(api GET /v1/cron)"
echo "    $list"

sched_id="$(json "$list" schedules.0.id 2>/dev/null || true)"
if [[ -z "${sched_id}" || "${sched_id}" == "None" || "${sched_id}" == "null" ]]; then
  echo "==> no cron schedules defined yet."
  echo "    Cron schedules are seeded via the admin plane / migrations; once present,"
  echo "    this lists them with last/next fire and lets you toggle enabled."
  exit 0
fi

echo "==> disable schedule ${sched_id}: PUT /v1/cron/${sched_id}"
api PUT "/v1/cron/${sched_id}" '{ "enabled": false }'
echo
echo "==> re-enable it"
api PUT "/v1/cron/${sched_id}" '{ "enabled": true }'
echo
echo "    The scheduler is deterministic: it dedups on cron:{id}:{next_fire} and does"
echo "    NOT backfill missed fires while disabled."
