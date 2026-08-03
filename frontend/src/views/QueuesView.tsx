import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";

// Queues: depth + oldest-age per (task, rate_class). Oldest-age is the
// head-of-line latency signal — a growing age means work is starved, not just deep.
function fmtAge(s: number): string {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  return `${Math.round(s / 3600)}h`;
}

export function QueuesView() {
  const { data, isLoading, error } = useQuery({ queryKey: ["queues"], queryFn: api.queues });
  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  const queues = data ?? [];
  if (queues.length === 0) return <Panel title="Queues"><Empty what="queued work" /></Panel>;

  const maxDepth = Math.max(...queues.map((q) => q.depth), 1);

  return (
    <Panel title="Queues">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-symba-muted">
          <tr className="border-b border-symba-border">
            <th className="py-2 pr-4">Task</th>
            <th className="py-2 pr-4">Rate class</th>
            <th className="py-2 pr-4">Depth</th>
            <th className="py-2 pr-4">Oldest age</th>
          </tr>
        </thead>
        <tbody>
          {queues.map((q) => (
            <tr key={`${q.task_name}:${q.rate_class ?? ""}`} className="border-b border-symba-border/50">
              <td className="py-2 pr-4">{q.task_name}</td>
              <td className="py-2 pr-4 text-symba-muted">{q.rate_class ?? "—"}</td>
              <td className="py-2 pr-4">
                <div className="flex items-center gap-2">
                  <div className="h-1.5 w-32 rounded bg-symba-border">
                    <div className="h-1.5 rounded bg-sky-500" style={{ width: `${(q.depth / maxDepth) * 100}%` }} />
                  </div>
                  <span>{q.depth}</span>
                </div>
              </td>
              <td className="py-2 pr-4">
                <span className={q.oldest_age_s > 300 ? "text-amber-400" : "text-symba-muted"}>
                  {fmtAge(q.oldest_age_s)}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}
