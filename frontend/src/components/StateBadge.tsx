import type { JobState } from "../api/types";

// Node/row color = state. Hex (not Tailwind class names) so the same palette
// works for badges, board borders, and React Flow node `style.background`.
const COLORS: Record<JobState, string> = {
  submitted: "#475569", // slate-600
  queued: "#0284c7", // sky-600
  running: "#6366f1", // indigo-500
  waiting: "#f59e0b", // amber-500
  succeeded: "#059669", // emerald-600
  dead: "#e11d48", // rose-600
  cancelled: "#71717a", // zinc-500
};

export function stateColor(state: JobState): string {
  return COLORS[state] ?? COLORS.submitted;
}

export function StateBadge({ state }: { state: JobState }) {
  return (
    <span
      className="inline-block rounded px-2 py-0.5 text-xs font-medium text-white"
      style={{ backgroundColor: stateColor(state) }}
    >
      {state}
    </span>
  );
}
