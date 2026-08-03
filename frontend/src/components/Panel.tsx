import type { ReactNode } from "react";

export function Panel({ title, children, actions }: { title: string; children: ReactNode; actions?: ReactNode }) {
  return (
    <section className="rounded-lg border border-symba-border bg-symba-panel">
      <header className="flex items-center justify-between border-b border-symba-border px-4 py-3">
        <h2 className="text-sm font-semibold tracking-wide text-symba-text">{title}</h2>
        {actions}
      </header>
      <div className="p-4">{children}</div>
    </section>
  );
}

export function Loading() {
  return <div className="p-8 text-center text-sm text-symba-muted">Loading…</div>;
}

export function ErrorBox({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="rounded border border-rose-800 bg-rose-950/40 p-4 text-sm text-rose-300">{msg}</div>
  );
}

export function Empty({ what }: { what: string }) {
  return <div className="p-8 text-center text-sm text-symba-muted">No {what}.</div>;
}
