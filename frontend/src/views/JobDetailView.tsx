import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "@tanstack/react-router";
import { api } from "../api/client";
import { Json } from "../components/Json";
import { ErrorBox, Loading, Panel } from "../components/Panel";
import { StateBadge } from "../components/StateBadge";
import { durationSeconds, formatDuration } from "../lib/duration";

// Job detail: full timeline, upstream/downstream links, payload/result
// viewers (collapsed), checkpoints. Resubmit is enabled only for DEAD jobs; Cancel
// only for live states — mirroring the engine's cancel-per-state matrix so the UI
// never offers an action the API would reject.
const LIVE_STATES = new Set(["submitted", "queued", "running", "waiting"]);

export function JobDetailView() {
  const { jobId } = useParams({ from: "/jobs/$jobId" });
  const qc = useQueryClient();

  const jobQ = useQuery({ queryKey: ["job", jobId], queryFn: () => api.getJob(jobId) });
  const eventsQ = useQuery({ queryKey: ["events", jobId], queryFn: () => api.events(jobId) });
  const treeQ = useQuery({
    queryKey: ["tree", "job", jobId],
    queryFn: () => api.tree(jobId),
  });
  const cpQ = useQuery({ queryKey: ["cp", jobId], queryFn: () => api.checkpoint(jobId) });

  const invalidate = () => {
    void qc.invalidateQueries({ queryKey: ["job", jobId] });
    void qc.invalidateQueries({ queryKey: ["board"] });
  };
  const resubmit = useMutation({ mutationFn: () => api.resubmit(jobId), onSuccess: invalidate });
  const cancel = useMutation({ mutationFn: () => api.cancel(jobId), onSuccess: invalidate });

  if (jobQ.isLoading) return <Loading />;
  if (jobQ.error) return <ErrorBox error={jobQ.error} />;
  const job = jobQ.data!;

  const upstream = (treeQ.data ?? []).filter((e) => e.downstream === jobId);
  const downstream = (treeQ.data ?? []).filter((e) => e.upstream === jobId);

  return (
    <div className="space-y-4">
      <Panel
        title={`${job.task_name}`}
        actions={
          <div className="flex items-center gap-2">
            <StateBadge state={job.state} />
            {job.state === "dead" && (
              <button
                className="rounded bg-indigo-600 px-3 py-1 text-xs text-white disabled:opacity-50"
                disabled={resubmit.isPending}
                onClick={() => resubmit.mutate()}
              >
                Resubmit
              </button>
            )}
            {LIVE_STATES.has(job.state) && (
              <button
                className="rounded bg-rose-600 px-3 py-1 text-xs text-white disabled:opacity-50"
                disabled={cancel.isPending}
                onClick={() => cancel.mutate()}
              >
                Cancel
              </button>
            )}
          </div>
        }
      >
        <dl className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm sm:grid-cols-3">
          <Field k="ID" v={job.id} mono />
          <Field k="Tenant" v={job.tenant} />
          <Field k="Attempt" v={String(job.attempt)} />
          <Field k="Priority" v={String(job.priority)} />
          <Field k="Group" v={job.group_key ?? "—"} />
          <Field k="Ctx" v={job.ctx_id ?? "—"} mono />
          <div>
            <dt className="text-xs uppercase tracking-wide text-symba-muted">Worker</dt>
            <dd className="font-mono text-xs">
              {job.worker ? (
                <Link
                  to="/fleet/$workerId"
                  params={{ workerId: job.worker }}
                  className="text-indigo-400 hover:underline"
                >
                  {job.worker}
                </Link>
              ) : (
                "—"
              )}
            </dd>
          </div>
          <Field k="Created" v={new Date(job.created_at).toLocaleString()} />
          <Field k="Started" v={job.started_at ? new Date(job.started_at).toLocaleString() : "—"} />
          <Field k="Finished" v={job.finished_at ? new Date(job.finished_at).toLocaleString() : "—"} />
          <Field k="Duration" v={formatDuration(durationSeconds(job.started_at, job.finished_at))} />
        </dl>
      </Panel>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Json label="Payload" value={job.payload} defaultOpen />
        <Json label="Result" value={job.result} defaultOpen />
      </div>

      {(job.error_history ?? []).length > 0 && (
        <Json label="Error history" value={job.error_history} />
      )}
      {cpQ.data?.checkpoint && <Json label="Checkpoint" value={cpQ.data.checkpoint} />}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Upstream">
          {upstream.length === 0 ? (
            <p className="text-sm text-symba-muted">None</p>
          ) : (
            <ul className="space-y-1 text-sm">
              {upstream.map((e) => (
                <li key={e.upstream}>
                  <Link to="/jobs/$jobId" params={{ jobId: e.upstream }} className="text-indigo-400 hover:underline">
                    {e.upstream.slice(0, 8)} {e.alias ? `(${e.alias})` : ""}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Panel>
        <Panel title="Downstream">
          {downstream.length === 0 ? (
            <p className="text-sm text-symba-muted">None</p>
          ) : (
            <ul className="space-y-1 text-sm">
              {downstream.map((e) => (
                <li key={e.downstream}>
                  <Link to="/jobs/$jobId" params={{ jobId: e.downstream }} className="text-indigo-400 hover:underline">
                    {e.downstream.slice(0, 8)} {e.alias ? `(${e.alias})` : ""}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>

      <Panel title="Timeline">
        {eventsQ.isLoading ? (
          <Loading />
        ) : (eventsQ.data ?? []).length === 0 ? (
          <p className="text-sm text-symba-muted">No events.</p>
        ) : (
          <ol className="space-y-2">
            {(eventsQ.data ?? []).map((ev, i) => (
              <li key={i} className="flex gap-3 text-sm">
                <span className="w-40 shrink-0 text-symba-muted">{new Date(ev.at).toLocaleString()}</span>
                <span className="font-medium">{ev.event}</span>
                {ev.detail && <span className="text-xs text-symba-muted">{JSON.stringify(ev.detail)}</span>}
              </li>
            ))}
          </ol>
        )}
      </Panel>
    </div>
  );
}

function Field({ k, v, mono }: { k: string; v: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-xs uppercase tracking-wide text-symba-muted">{k}</dt>
      <dd className={mono ? "font-mono text-xs" : ""}>{v}</dd>
    </div>
  );
}
