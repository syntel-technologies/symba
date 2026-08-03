// Shared duration formatting: started_at -> finished_at (or now, while still
// running) rendered as a compact human string. Used by JobTable and JobDetailView
// so "how long did this take" reads the same everywhere in the console.
export function durationSeconds(
  startedAt: string | null,
  finishedAt: string | null,
): number | null {
  if (!startedAt) return null;
  const start = new Date(startedAt).getTime();
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
  return Math.max(0, Math.round((end - start) / 1000));
}

export function formatDuration(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return s ? `${m}m ${s}s` : `${m}m`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return m ? `${h}h ${m}m` : `${h}h`;
}
