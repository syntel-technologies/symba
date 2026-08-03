import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { JobState } from "../api/types";
import { ErrorBox, Loading, Panel } from "../components/Panel";
import { stateColor } from "../components/StateBadge";

// Live board: one aggregate over ix_jobs_state_task, SSE-refreshed via
// the ["board"] query key invalidated by useEventStream.
const ORDER: JobState[] = [
  "submitted",
  "queued",
  "running",
  "waiting",
  "succeeded",
  "dead",
  "cancelled",
];

export function BoardView() {
  const { data, isLoading, error } = useQuery({ queryKey: ["board"], queryFn: api.board });
  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  const counts = data ?? {};
  return (
    <Panel title="Live board">
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
        {ORDER.map((s) => (
          <div
            key={s}
            className="rounded-lg border border-symba-border bg-symba-bg p-4"
            style={{ borderLeft: `4px solid ${stateColor(s)}` }}
          >
            <div className="text-3xl font-bold text-symba-text">{counts[s] ?? 0}</div>
            <div className="mt-1 text-sm capitalize text-symba-muted">{s}</div>
          </div>
        ))}
      </div>
    </Panel>
  );
}
