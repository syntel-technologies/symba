import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import { Empty, ErrorBox, Loading, Panel } from "../components/Panel";

// Cron: enable/disable, last/next fire. The toggle PUTs /v1/cron/{id};
// disabling does NOT backfill on re-enable (engine cron_due clamp), so an operator
// can safely pause a schedule without a burst when they resume it.
export function CronView() {
  const qc = useQueryClient();
  const { data, isLoading, error } = useQuery({ queryKey: ["cron"], queryFn: api.cron });

  const toggle = useMutation({
    mutationFn: (args: { id: string; enabled: boolean }) => api.setCronEnabled(args.id, args.enabled),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["cron"] }),
  });

  if (isLoading) return <Loading />;
  if (error) return <ErrorBox error={error} />;
  const schedules = data ?? [];
  if (schedules.length === 0) return <Panel title="Cron"><Empty what="schedules" /></Panel>;

  return (
    <Panel title="Cron">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-symba-muted">
          <tr className="border-b border-symba-border">
            <th className="py-2 pr-4">Schedule</th>
            <th className="py-2 pr-4">Expression</th>
            <th className="py-2 pr-4">Task</th>
            <th className="py-2 pr-4">Last fire</th>
            <th className="py-2 pr-4">Next fire</th>
            <th className="py-2 pr-4">Enabled</th>
          </tr>
        </thead>
        <tbody>
          {schedules.map((s) => (
            <tr key={s.schedule_id} className="border-b border-symba-border/50">
              <td className="py-2 pr-4 font-mono text-xs">{s.schedule_id}</td>
              <td className="py-2 pr-4 font-mono text-xs">{s.cron_expr}</td>
              <td className="py-2 pr-4">{s.task_name}</td>
              <td className="py-2 pr-4 text-symba-muted">
                {s.last_fire ? new Date(s.last_fire).toLocaleString() : "—"}
              </td>
              <td className="py-2 pr-4 text-symba-muted">
                {s.next_fire ? new Date(s.next_fire).toLocaleString() : "—"}
              </td>
              <td className="py-2 pr-4">
                <button
                  className={`rounded px-3 py-1 text-xs font-medium ${
                    s.enabled ? "bg-emerald-600 text-white" : "bg-symba-border text-symba-muted"
                  } disabled:opacity-50`}
                  disabled={toggle.isPending}
                  onClick={() => toggle.mutate({ id: s.schedule_id, enabled: !s.enabled })}
                >
                  {s.enabled ? "Enabled" : "Disabled"}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}
