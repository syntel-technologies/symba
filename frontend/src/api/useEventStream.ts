import { useEffect, useSyncExternalStore } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { getToken, onUnauthorized, subscribeAuth } from "./auth";
import { runEventStream } from "./sse";
import type { StreamEvent } from "./types";

// Subscribe to the engine's single SSE channel (/v1/events/stream) and invalidate
// the relevant TanStack Query caches so views refresh live (live updates
// over one SSE channel fed by a fan-out of job_events). The fetch stream reconnects
// on drop, so a transient engine restart heals without a page reload.
//
// We invalidate rather than patch caches: the queries are cheap aggregates/lists and
// invalidation keeps ONE source of truth (the server), avoiding client/server drift.
export function useEventStream(): void {
  const qc = useQueryClient();
  // Restart the request when the operator signs in or out. In particular, signing
  // out aborts the still-authorized long-lived response immediately.
  const token = useSyncExternalStore(subscribeAuth, getToken);
  useEffect(() => {
    const controller = new AbortController();
    void runEventStream("/v1/events/stream", {
      signal: controller.signal,
      onUnauthorized,
      onEvent: (message) => {
        if (message.event !== "job_event") return;
        const ev = JSON.parse(message.data) as StreamEvent;
        // Board + queues + lists change on nearly every event; refresh them.
        void qc.invalidateQueries({ queryKey: ["board"] });
        void qc.invalidateQueries({ queryKey: ["queues"] });
        void qc.invalidateQueries({ queryKey: ["jobs"] });
        // The touched job's detail/timeline, if a view is open on it.
        void qc.invalidateQueries({ queryKey: ["job", ev.job_id] });
        void qc.invalidateQueries({ queryKey: ["events", ev.job_id] });
        if (ev.ctx_id) void qc.invalidateQueries({ queryKey: ["tree", ev.ctx_id] });
      },
    });
    return () => controller.abort();
  }, [qc, token]);
}
