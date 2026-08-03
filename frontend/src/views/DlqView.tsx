import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { api } from "../api/client";
import type { JobListItem } from "../api/types";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";

// Failed / DLQ: GET /v1/jobs?state=dead grouped by stack_hash — one row
// per distinct failure with a count badge, expand for the error, bulk Resubmit.
function stackHash(j: JobListItem): string {
  const last = (j.error_history ?? []).at(-1);
  return (last?.stack_hash as string) ?? "unknown";
}

export function DlqView() {
  const qc = useQueryClient();
  const { data, isLoading, error } = useQuery({
    queryKey: ["jobs", "dead"],
    queryFn: () => api.listJobs({ state: "dead", limit: 500 }),
  });
  const [expanded, setExpanded] = useState<string | null>(null);

  const resubmit = useMutation({
    mutationFn: async (ids: string[]) => {
      await Promise.all(ids.map((id) => api.resubmit(id)));
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["jobs", "dead"] });
      void qc.invalidateQueries({ queryKey: ["board"] });
    },
  });

  const groups = useMemo(() => {
    const m = new Map<string, JobListItem[]>();
    for (const j of data ?? []) {
      const h = stackHash(j);
      (m.get(h) ?? m.set(h, []).get(h)!).push(j);
    }
    return [...m.entries()].sort((a, b) => b[1].length - a[1].length);
  }, [data]);

  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  if (groups.length === 0) return <Panel title="Failed / DLQ"><Empty what="dead jobs" /></Panel>;

  return (
    <Panel title="Failed / DLQ">
      <div className="space-y-3">
        {groups.map(([hash, jobs]) => {
          const sample = (jobs[0].error_history ?? []).at(-1);
          const open = expanded === hash;
          return (
            <div key={hash} className="rounded border border-symba-border">
              <div className="flex items-center justify-between px-4 py-3">
                <button className="flex items-center gap-3 text-left" onClick={() => setExpanded(open ? null : hash)}>
                  <span className="rounded-full bg-rose-600 px-2 py-0.5 text-xs font-semibold text-white">
                    {jobs.length}
                  </span>
                  <span className="text-sm text-symba-text">{sample?.error_type ?? "error"}</span>
                  <span className="font-mono text-xs text-symba-muted">{hash.slice(0, 12)}</span>
                </button>
                <button
                  className="rounded bg-indigo-600 px-3 py-1 text-xs font-medium text-white hover:bg-indigo-500 disabled:opacity-50"
                  disabled={resubmit.isPending}
                  onClick={() => resubmit.mutate(jobs.map((j) => j.id))}
                >
                  Resubmit all ({jobs.length})
                </button>
              </div>
              {open && (
                <div className="border-t border-symba-border px-4 py-3">
                  <pre className="mb-3 max-h-40 overflow-auto text-xs text-rose-300">
                    {sample?.error_message ?? "(no message)"}
                  </pre>
                  <ul className="space-y-1 text-sm">
                    {jobs.map((j) => (
                      <li key={j.id}>
                        <Link to="/jobs/$jobId" params={{ jobId: j.id }} className="text-indigo-400 hover:underline">
                          {j.task_name} · {j.id.slice(0, 8)}
                        </Link>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
