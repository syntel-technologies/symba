import { useState } from "react";

// JSON viewer. `defaultOpen` controls the initial state — payload/result default
// open (operators want to see them immediately); error history stays opt-in via
// the caller. Still collapsible so a huge payload doesn't have to stay on-screen.
export function Json({
  value,
  label,
  defaultOpen = false,
}: {
  value: unknown;
  label: string;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const empty = value === null || value === undefined;
  return (
    <div className="rounded border border-symba-border bg-symba-panel">
      <button
        className="flex w-full items-center justify-between px-3 py-2 text-left text-sm text-symba-muted"
        onClick={() => setOpen((o) => !o)}
      >
        <span>{label}</span>
        <span>{empty ? "—" : open ? "▾" : "▸"}</span>
      </button>
      {open && !empty && (
        <pre className="max-h-96 overflow-auto border-t border-symba-border px-3 py-2 text-xs">
          {JSON.stringify(value, null, 2)}
        </pre>
      )}
    </div>
  );
}
