// Thin fetch client over the engine's public REST API (the UI has
// zero private endpoints — every call here is scriptable by an operator).
//
// One tenant is assumed for the console (default); multi-tenant switching is an M6
// concern. Errors surface the engine's structured error envelope so views can show
// the real error_code, not a generic "request failed".

import { authHeaders, onUnauthorized } from "./auth";
import type {
  CronRow,
  EventRow,
  JobDetail,
  JobListItem,
  JobState,
  QueueStat,
  TreeEdge,
  WorkerRow,
} from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    public errorCode: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
    ...init,
    // authHeaders() attaches Bearer <token> when the operator is logged in; under
    // engine auth mode=none it's empty and the request goes through unauthenticated.
    headers: { "content-type": "application/json", ...authHeaders(), ...(init?.headers ?? {}) },
  });
  if (!resp.ok) {
    // 401 means the engine is in token/mtls mode and our credential is missing or
    // stale: trip the login gate (and drop the bad token) so the operator can re-auth.
    if (resp.status === 401) onUnauthorized();
    // The engine's exception middleware returns {error_code, message, ...}.
    const body = (await resp.json().catch(() => ({}))) as {
      error_code?: string;
      message?: string;
      detail?: string;
    };
    throw new ApiError(
      resp.status,
      body.error_code ?? "http_error",
      body.message ?? body.detail ?? resp.statusText,
    );
  }
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export interface JobFilters {
  state?: JobState;
  task_name?: string;
  ctx_id?: string;
  worker?: string;
  limit?: number;
  offset?: number;
}

function qs(params: Record<string, string | number | undefined>): string {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") usp.set(k, String(v));
  }
  const s = usp.toString();
  return s ? `?${s}` : "";
}

export const api = {
  // ── reads ──────────────────────────────────────────────────────────────────
  listJobs: (f: JobFilters = {}) =>
    req<{ jobs: JobListItem[] }>(
      `/v1/jobs${qs({
        state_filter: f.state,
        task_name: f.task_name,
        ctx_id: f.ctx_id,
        worker: f.worker,
        limit: f.limit,
        offset: f.offset,
      })}`,
    ).then((r) => r.jobs),

  // Jobs currently/last held by one worker (backs the fleet drill-in).
  workerJobs: (workerId: string) =>
    req<{ jobs: JobListItem[] }>(`/v1/jobs${qs({ worker: workerId, limit: 200 })}`).then(
      (r) => r.jobs,
    ),

  getJob: (id: string) => req<JobDetail>(`/v1/jobs/${id}`),

  board: () => req<{ counts: Record<string, number> }>("/v1/stats/board").then((r) => r.counts),

  queues: () => req<{ queues: QueueStat[] }>("/v1/stats/queues").then((r) => r.queues),

  workers: () => req<{ workers: WorkerRow[] }>("/v1/workers").then((r) => r.workers),

  cron: () => req<{ schedules: CronRow[] }>("/v1/cron").then((r) => r.schedules),

  events: (id: string) =>
    req<{ events: EventRow[] }>(`/v1/jobs/${id}/events`).then((r) => r.events),

  tree: (id: string) => req<{ edges: TreeEdge[] }>(`/v1/jobs/${id}/tree`).then((r) => r.edges),

  checkpoint: (id: string) =>
    req<{ job_id: string; checkpoint: Record<string, unknown> | null }>(
      `/v1/jobs/${id}/checkpoints`,
    ),

  // ── mutations (every button an operator can click) ──────────────────────────
  setCronEnabled: (scheduleId: string, enabled: boolean) =>
    req<{ enabled: boolean }>(`/v1/cron/${scheduleId}`, {
      method: "PUT",
      body: JSON.stringify({ enabled }),
    }),

  signal: (waitKey: string, payload: Record<string, unknown>, signaledBy: string) =>
    req<{ delivered: boolean; job_id: string | null }>("/v1/signals", {
      method: "POST",
      body: JSON.stringify({ tenant: "default", wait_key: waitKey, payload, signaled_by: signaledBy }),
    }),

  resubmit: (id: string) =>
    req<{ new_job_id: string; resubmitted_from: string }>(`/v1/jobs/${id}/resubmit`, {
      method: "POST",
    }),

  cancel: (id: string) => req<unknown>(`/v1/jobs/${id}/cancel`, { method: "POST" }),
};
