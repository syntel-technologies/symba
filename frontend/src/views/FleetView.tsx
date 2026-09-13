import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { api } from "../api/client";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";
import { registeredTasks } from "../lib/worker";

// Fleet view: routing tags, registered handlers, slots in use, last_seen; stale
// workers flagged. Tags are intentionally not presented as task capabilities.
// The header shows the connected-worker count (live vs stale); each row links to a
// per-worker drill-in (/fleet/$workerId) listing the jobs that worker is running.
export function FleetView() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["workers"],
    queryFn: api.workers,
    refetchInterval: 5_000, // fleet freshness isn't event-driven; poll faster
  });
  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  const workers = data ?? [];
  const live = workers.filter((w) => !w.stale).length;

  const summary = (
    <span className="text-xs text-symba-muted">
      <span className="font-semibold text-emerald-400">{live}</span> connected
      {workers.length > live && (
        <>
          {" · "}
          <span className="font-semibold text-rose-400">{workers.length - live}</span> stale
        </>
      )}
    </span>
  );

  if (workers.length === 0)
    return (
      <Panel title="Fleet" actions={summary}>
        <Empty what="workers" />
      </Panel>
    );

  return (
    <Panel title="Fleet" actions={summary}>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-symba-muted">
          <tr className="border-b border-symba-border">
            <th className="py-2 pr-4">Worker</th>
            <th className="py-2 pr-4">Routing tags</th>
            <th className="py-2 pr-4">Registered tasks</th>
            <th className="py-2 pr-4">Slots (busy / total)</th>
            <th className="py-2 pr-4">Last seen</th>
            <th className="py-2 pr-4">Status</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((w) => {
            const tags = w.tags ?? [];
            const tasks = registeredTasks(w);
            return (
              <tr key={w.worker_id} className="border-b border-symba-border/50 hover:bg-symba-border/30">
                <td className="py-2 pr-4 font-mono text-xs">
                  <Link
                    to="/fleet/$workerId"
                    params={{ workerId: w.worker_id }}
                    className="text-indigo-400 hover:underline"
                  >
                    {w.worker_id}
                  </Link>
                </td>
                <td className="py-2 pr-4">
                  {tags.length ? (
                    <div className="flex flex-wrap gap-1">
                      {tags.map((t) => (
                        <span key={t} className="rounded bg-symba-border px-1.5 py-0.5 text-xs">
                          {t}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-symba-muted">—</span>
                  )}
                </td>
                <td className="py-2 pr-4">
                  {tasks.length ? (
                    <Link
                      to="/fleet/$workerId"
                      params={{ workerId: w.worker_id }}
                      className="text-xs text-indigo-400 hover:underline"
                    >
                      {tasks.length} handlers
                    </Link>
                  ) : (
                    <span className="text-xs text-symba-muted">not reported</span>
                  )}
                </td>
                <td className="py-2 pr-4">
                  <div className="flex items-center gap-2">
                    <div className="h-1.5 w-24 rounded bg-symba-border">
                      <div
                        className="h-1.5 rounded bg-indigo-500"
                        style={{ width: `${w.slots ? (w.slots_busy / w.slots) * 100 : 0}%` }}
                      />
                    </div>
                    <span className="text-xs text-symba-muted">
                      {w.slots_busy} / {w.slots}
                    </span>
                  </div>
                </td>
                <td className="py-2 pr-4 text-symba-muted">{new Date(w.last_seen).toLocaleTimeString()}</td>
                <td className="py-2 pr-4">
                  {w.stale ? (
                    <span className="rounded bg-rose-600 px-2 py-0.5 text-xs text-white">stale</span>
                  ) : (
                    <span className="rounded bg-emerald-600 px-2 py-0.5 text-xs text-white">live</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}
