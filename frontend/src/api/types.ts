// Domain types mirroring the engine DTOs backing endpoints.
//
// These are hand-declared to keep the app compiling without the generated client,
// but they intentionally match the shapes in frontend/openapi.json 1:1. The CI
// drift gate (tools/check_openapi.py) guards the schema; `npm run gen:api` produces
// src/api/schema.ts for compile-time verification against these shapes. If the two
// ever disagree, the generated schema is the source of truth — update here to match.

export type JobState =
  | "submitted"
  | "queued"
  | "running"
  | "waiting"
  | "succeeded"
  | "dead"
  | "cancelled";

export interface ErrorEntry {
  error_type?: string;
  error_message?: string;
  stack_hash?: string;
  at?: string;
  [k: string]: unknown;
}

export interface JobListItem {
  id: string;
  tenant: string;
  task_name: string;
  state: JobState;
  attempt: number;
  priority: number;
  group_key: string | null;
  ctx_id: string | null;
  wait_key: string | null;
  // The worker holding/that last held the job. null when never claimed or still on
  // the pre-attribution "engine:{tags}" placeholder (the engine normalizes that out).
  worker: string | null;
  error_history: ErrorEntry[];
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface JobDetail {
  id: string;
  tenant: string;
  task_name: string;
  state: JobState;
  attempt: number;
  priority: number;
  group_key: string | null;
  ctx_id: string | null;
  worker: string | null;
  payload: Record<string, unknown>;
  result: Record<string, unknown> | null;
  error_history: ErrorEntry[];
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface QueueStat {
  task_name: string;
  rate_class: string | null;
  depth: number;
  oldest_age_s: number;
}

export interface WorkerRow {
  worker_id: string;
  tags: string[];
  labels: Record<string, unknown>;
  slots: number;
  slots_busy: number;
  last_seen: string;
  stale: boolean;
}

export interface CronRow {
  schedule_id: string;
  cron_expr: string;
  task_name: string;
  tenant: string;
  enabled: boolean;
  last_fire: string | null;
  next_fire: string | null;
  created_at: string;
}

export interface EventRow {
  event: string;
  at: string;
  detail: Record<string, unknown> | null;
}

export interface TreeEdge {
  upstream: string;
  downstream: string;
  alias: string | null;
}

// SSE payload pushed on /v1/events/stream (http_server events_stream).
export interface StreamEvent {
  job_id: string;
  ctx_id: string | null;
  event: string;
  at: string;
  detail: Record<string, unknown> | null;
}
