import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "@tanstack/react-router";
import { api } from "../api/client";
import type { JobListItem } from "../api/types";
import { JobTable } from "../components/JobTable";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";

// Per-worker drill-in: the selected worker's live capacity gauge + the jobs
// attributed to it (jobs.claimed_by = worker_id, re-stamped by the matcher at
// assignment). Jobs are grouped by ctx_id first — a worker can carry hundreds of
// jobs across many pipeline runs, and a flat list makes it hard to see which run is
// which. Selecting a ctx group drills into its own job table with full timing.
//
//   Jobs (flat, claimed_by = worker) ──group by ctx_id──> [ ctx A | ctx B | (none) ]
//                                                                │
//                                                     click a group row
//                                                                ▼
//                                          per-ctx JobTable (Started/Finished/Duration)
const RUNNING_FIRST = (a: { state: string }, b: { state: string }) =>
  Number(b.state === "running") - Number(a.state === "running");

const NO_CTX = "(no context)";

interface CtxGroup {
  ctxId: string;
  jobs: JobListItem[];
  runningCount: number;
  latestCreatedAt: string;
}

function groupByCtx(jobs: JobListItem[]): CtxGroup[] {
  const m = new Map<string, JobListItem[]>();
  for (const j of jobs) {
    const key = j.ctx_id ?? NO_CTX;
    (m.get(key) ?? m.set(key, []).get(key)!).push(j);
  }
  const groups: CtxGroup[] = [...m.entries()].map(([ctxId, groupJobs]) => ({
    ctxId,
    jobs: groupJobs,
    runningCount: groupJobs.filter((j) => j.state === "running").length,
    latestCreatedAt: groupJobs.reduce(
      (max, j) => (j.created_at > max ? j.created_at : max),
      groupJobs[0].created_at,
    ),
  }));
  return groups.sort((a, b) => (a.latestCreatedAt < b.latestCreatedAt ? 1 : -1));
}

export function WorkerDetailView() {
  const { workerId } = useParams({ from: "/fleet/$workerId" });
  const [selectedCtx, setSelectedCtx] = useState<string | null>(null);

  // The fleet list is the source of the worker's own row (tags/slots/status); there
  // is no single-worker endpoint, so we filter the list the fleet view already uses.
  const fleetQ = useQuery({
    queryKey: ["workers"],
    queryFn: api.workers,
    refetchInterval: 5_000,
  });
  const jobsQ = useQuery({
    queryKey: ["worker-jobs", workerId],
    queryFn: () => api.workerJobs(workerId),
    refetchInterval: 5_000,
  });

  const groups = useMemo(() => groupByCtx(jobsQ.data ?? []), [jobsQ.data]);

  if (fleetQ.isLoading || jobsQ.isLoading) return <Loading />;
  if (fleetQ.error) return <ErrorBox error={fleetQ.error} />;
  if (jobsQ.error) return <ErrorBox error={jobsQ.error} />;

  const worker = (fleetQ.data ?? []).find((w) => w.worker_id === workerId);
  const tags = worker?.tags ?? [];
  const runningCount = (jobsQ.data ?? []).filter((j) => j.state === "running").length;
  const selectedGroup = groups.find((g) => g.ctxId === selectedCtx) ?? null;

  return (
    <div className="space-y-4">
      <Panel
        title={workerId}
        actions={
          <Link to="/fleet" className="text-xs text-indigo-400 hover:underline">
            ← Fleet
          </Link>
        }
      >
        {worker ? (
          <dl className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm sm:grid-cols-4">
            <div>
              <dt className="text-xs uppercase tracking-wide text-symba-muted">Status</dt>
              <dd>
                {worker.stale ? (
                  <span className="rounded bg-rose-600 px-2 py-0.5 text-xs text-white">stale</span>
                ) : (
                  <span className="rounded bg-emerald-600 px-2 py-0.5 text-xs text-white">live</span>
                )}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase tracking-wide text-symba-muted">Slots (busy / total)</dt>
              <dd>
                {worker.slots_busy} / {worker.slots}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase tracking-wide text-symba-muted">Running now</dt>
              <dd>{runningCount}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase tracking-wide text-symba-muted">Last seen</dt>
              <dd>{new Date(worker.last_seen).toLocaleTimeString()}</dd>
            </div>
            <div className="col-span-2 sm:col-span-4">
              <dt className="text-xs uppercase tracking-wide text-symba-muted">Tags</dt>
              <dd>
                {tags.length ? (
                  <div className="mt-1 flex flex-wrap gap-1">
                    {tags.map((t) => (
                      <span key={t} className="rounded bg-symba-border px-1.5 py-0.5 text-xs">
                        {t}
                      </span>
                    ))}
                  </div>
                ) : (
                  <span className="text-symba-muted">—</span>
                )}
              </dd>
            </div>
          </dl>
        ) : (
          // The worker left the fleet (disconnected/pruned) but still has attributed
          // jobs in the archive — show those rather than a bare error.
          <p className="text-sm text-symba-muted">
            This worker is not currently connected. Showing its last known jobs.
          </p>
        )}
      </Panel>

      <Panel title="Contexts">
        {groups.length === 0 ? (
          <Empty what="jobs for this worker" />
        ) : (
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-symba-muted">
              <tr className="border-b border-symba-border">
                <th className="py-2 pr-4">Ctx</th>
                <th className="py-2 pr-4">Jobs</th>
                <th className="py-2 pr-4">Running</th>
                <th className="py-2 pr-4">Latest activity</th>
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => (
                <tr
                  key={g.ctxId}
                  className={`cursor-pointer border-b border-symba-border/50 hover:bg-symba-border/30 ${
                    selectedCtx === g.ctxId ? "bg-symba-border/40" : ""
                  }`}
                  onClick={() => setSelectedCtx(g.ctxId === selectedCtx ? null : g.ctxId)}
                >
                  <td className="py-2 pr-4 font-mono text-xs">{g.ctxId}</td>
                  <td className="py-2 pr-4">{g.jobs.length}</td>
                  <td className="py-2 pr-4">
                    {g.runningCount > 0 ? (
                      <span className="rounded bg-indigo-600 px-2 py-0.5 text-xs text-white">
                        {g.runningCount}
                      </span>
                    ) : (
                      <span className="text-symba-muted">0</span>
                    )}
                  </td>
                  <td className="py-2 pr-4 text-symba-muted">
                    {new Date(g.latestCreatedAt).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      {selectedGroup && (
        <Panel
          title={`Jobs · ${selectedGroup.ctxId}`}
          actions={
            <button
              className="text-xs text-symba-muted hover:text-symba-text"
              onClick={() => setSelectedCtx(null)}
            >
              ✕ close
            </button>
          }
        >
          <JobTable jobs={[...selectedGroup.jobs].sort(RUNNING_FIRST)} showTiming />
        </Panel>
      )}
    </div>
  );
}
