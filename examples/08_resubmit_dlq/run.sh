#!/usr/bin/env bash
# 08 — DLQ replay: resubmit a DEAD job, preserving lineage.
. "$(dirname "$0")/../_common.sh"

echo "==> find DEAD jobs (the dead-letter queue): GET /v1/jobs?state_filter=dead"
dead="$(api GET '/v1/jobs?state_filter=dead&limit=1')"
echo "    $dead"

dead_id="$(json "$dead" jobs.0.id 2>/dev/null || true)"
if [[ -z "${dead_id}" || "${dead_id}" == "None" || "${dead_id}" == "null" ]]; then
  echo "==> no DEAD jobs to replay yet."
  echo "    To create one: submit a task your worker fails fatally until max_attempts,"
  echo "    then re-run this script. The DLQ view groups DEAD jobs by stack_hash."
  exit 0
fi

echo "==> POST /v1/jobs/${dead_id}/resubmit"
r="$(api POST "/v1/jobs/${dead_id}/resubmit")"
echo "    $r"
echo "==> new_job_id=$(json "$r" new_job_id), resubmitted_from=$(json "$r" resubmitted_from)"
echo "    A fresh attempt is queued; resubmitted_from keeps the lineage to the original."
