import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { authedEventSourceUrl, onUnauthorized } from "./auth";
import type { StreamEvent } from "./types";

// Subscribe to the engine's single SSE channel (/v1/events/stream) and invalidate
// the relevant TanStack Query caches so views refresh live (live updates
// over one SSE channel fed by a fan-out of job_events). EventSource auto-reconnects
// with backoff on drop, so a transient engine restart heals without a page reload.
//
// We invalidate rather than patch caches: the queries are cheap aggregates/lists and
// invalidation keeps ONE source of truth (the server), avoiding client/server drift.
export function useEventStream(): void {
  const qc = useQueryClient();
  useEffect(() => {
    // EventSource can't set an Authorization header, so the token rides as
    // ?access_token= (engine http_server accepts it for SSE). Empty under mode=none.
    const es = new EventSource(authedEventSourceUrl("/v1/events/stream?tenant=default"));
    // Distinguish a transient drop (EventSource auto-reconnects — leave it alone) from
    // an auth rejection. A 401 closes the stream and it never opens, so onerror fires
    // with readyState=CLOSED and we never saw `open`: only then trip the login gate.
    let everOpened = false;
    es.onopen = () => {
      everOpened = true;
    };
    es.onerror = () => {
      if (!everOpened && es.readyState === EventSource.CLOSED) onUnauthorized();
    };
    es.addEventListener("job_event", (e) => {
      const ev = JSON.parse((e as MessageEvent).data) as StreamEvent;
      // Board + queues + lists change on nearly every event; refresh them.
      void qc.invalidateQueries({ queryKey: ["board"] });
      void qc.invalidateQueries({ queryKey: ["queues"] });
      void qc.invalidateQueries({ queryKey: ["jobs"] });
      // The touched job's detail/timeline, if a view is open on it.
      void qc.invalidateQueries({ queryKey: ["job", ev.job_id] });
      void qc.invalidateQueries({ queryKey: ["events", ev.job_id] });
      if (ev.ctx_id) void qc.invalidateQueries({ queryKey: ["tree", ev.ctx_id] });
    });
    return () => es.close();
  }, [qc]);
}
