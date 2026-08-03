import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import type { JobListItem } from "../api/types";
import { JobTable } from "../components/JobTable";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";

// Waiting: jobs in WAITING with a Signal button that opens a JSON
// payload form and POSTs /v1/signals. signaled_by is the operator identity (auth is
// M6; until then we send a console default so the ledger still attributes the signal).
const SIGNALED_BY = "console";

function ageSeconds(created: string): number {
  return Math.max(0, Math.round((Date.now() - new Date(created).getTime()) / 1000));
}

export function WaitingView() {
  const qc = useQueryClient();
  const { data, isLoading, error } = useQuery({
    queryKey: ["jobs", "waiting"],
    queryFn: () => api.listJobs({ state: "waiting", limit: 500 }),
  });
  const [signalFor, setSignalFor] = useState<JobListItem | null>(null);
  const [payloadText, setPayloadText] = useState("{}");
  const [formError, setFormError] = useState<string | null>(null);

  const signal = useMutation({
    mutationFn: (args: { waitKey: string; payload: Record<string, unknown> }) =>
      api.signal(args.waitKey, args.payload, SIGNALED_BY),
    onSuccess: () => {
      setSignalFor(null);
      void qc.invalidateQueries({ queryKey: ["jobs", "waiting"] });
      void qc.invalidateQueries({ queryKey: ["board"] });
    },
  });

  function submitSignal() {
    if (!signalFor?.wait_key) return;
    let payload: Record<string, unknown>;
    try {
      payload = JSON.parse(payloadText) as Record<string, unknown>;
    } catch {
      setFormError("Payload must be valid JSON");
      return;
    }
    setFormError(null);
    signal.mutate({ waitKey: signalFor.wait_key, payload });
  }

  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  const jobs = data ?? [];
  if (jobs.length === 0) return <Panel title="Waiting"><Empty what="waiting jobs" /></Panel>;

  return (
    <Panel title="Waiting (human-in-the-loop)">
      <JobTable
        jobs={jobs}
        extra={[
          { header: "Wait key", cell: (j) => <span className="font-mono text-xs">{j.wait_key ?? "—"}</span> },
          { header: "Age", cell: (j) => `${ageSeconds(j.created_at)}s` },
          {
            header: "",
            cell: (j) => (
              <button
                className="rounded bg-amber-500 px-3 py-1 text-xs font-medium text-black hover:bg-amber-400"
                onClick={() => {
                  setSignalFor(j);
                  setPayloadText("{}");
                  setFormError(null);
                }}
              >
                Signal
              </button>
            ),
          },
        ]}
      />

      {signalFor && (
        <div className="fixed inset-0 z-10 flex items-center justify-center bg-black/60">
          <div className="w-[32rem] rounded-lg border border-symba-border bg-symba-panel p-5">
            <h3 className="mb-1 text-sm font-semibold">Signal “{signalFor.wait_key}”</h3>
            <p className="mb-3 text-xs text-symba-muted">
              Delivers the payload to {signalFor.task_name} · {signalFor.id.slice(0, 8)}
            </p>
            <textarea
              className="h-40 w-full rounded border border-symba-border bg-symba-bg p-2 font-mono text-xs text-symba-text"
              value={payloadText}
              onChange={(e) => setPayloadText(e.target.value)}
            />
            {formError && <div className="mt-2 text-xs text-rose-400">{formError}</div>}
            <div className="mt-4 flex justify-end gap-2">
              <button className="rounded px-3 py-1.5 text-sm text-symba-muted" onClick={() => setSignalFor(null)}>
                Cancel
              </button>
              <button
                className="rounded bg-amber-500 px-3 py-1.5 text-sm font-medium text-black hover:bg-amber-400 disabled:opacity-50"
                disabled={signal.isPending}
                onClick={submitSignal}
              >
                Send signal
              </button>
            </div>
          </div>
        </div>
      )}
    </Panel>
  );
}
