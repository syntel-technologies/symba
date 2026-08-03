import { Link } from "@tanstack/react-router";
import type { JobListItem } from "../api/types";
import { durationSeconds, formatDuration } from "../lib/duration";
import { StateBadge } from "./StateBadge";

// Shared job list table (DRY: DLQ, Waiting, Fleet drill-in, and Pipeline all render
// jobs). Extra per-view columns are injected via `extra` so each view adds only
// what it needs. `showTiming` adds Started/Finished/Duration — opt-in because the
// DLQ/Waiting views already show Created and don't need the full timing block.
export function JobTable({
  jobs,
  extra,
  showTiming = false,
}: {
  jobs: JobListItem[];
  extra?: { header: string; cell: (j: JobListItem) => React.ReactNode }[];
  showTiming?: boolean;
}) {
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs uppercase tracking-wide text-symba-muted">
        <tr className="border-b border-symba-border">
          <th className="py-2 pr-4">Task</th>
          <th className="py-2 pr-4">State</th>
          <th className="py-2 pr-4">Attempt</th>
          <th className="py-2 pr-4">Ctx</th>
          <th className="py-2 pr-4">Created</th>
          {showTiming && (
            <>
              <th className="py-2 pr-4">Started</th>
              <th className="py-2 pr-4">Finished</th>
              <th className="py-2 pr-4">Duration</th>
            </>
          )}
          {extra?.map((c) => (
            <th key={c.header} className="py-2 pr-4">
              {c.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {jobs.map((j) => (
          <tr key={j.id} className="border-b border-symba-border/50 hover:bg-symba-border/30">
            <td className="py-2 pr-4">
              <Link to="/jobs/$jobId" params={{ jobId: j.id }} className="text-indigo-400 hover:underline">
                {j.task_name}
              </Link>
            </td>
            <td className="py-2 pr-4">
              <StateBadge state={j.state} />
            </td>
            <td className="py-2 pr-4">{j.attempt}</td>
            <td className="py-2 pr-4 text-symba-muted">{j.ctx_id ?? "—"}</td>
            <td className="py-2 pr-4 text-symba-muted">{new Date(j.created_at).toLocaleString()}</td>
            {showTiming && (
              <>
                <td className="py-2 pr-4 text-symba-muted">
                  {j.started_at ? new Date(j.started_at).toLocaleString() : "—"}
                </td>
                <td className="py-2 pr-4 text-symba-muted">
                  {j.finished_at ? new Date(j.finished_at).toLocaleString() : "—"}
                </td>
                <td className="py-2 pr-4 text-symba-muted">
                  {formatDuration(durationSeconds(j.started_at, j.finished_at))}
                </td>
              </>
            )}
            {extra?.map((c) => (
              <td key={c.header} className="py-2 pr-4">
                {c.cell(j)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
