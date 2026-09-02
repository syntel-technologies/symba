import { authHeaders } from "./auth";
import { SseParser, type ParsedSseEvent } from "./sseParser";

interface EventStreamOptions {
  signal: AbortSignal;
  onEvent: (event: ParsedSseEvent) => void;
  onUnauthorized: () => void;
  fetcher?: typeof fetch;
}

function waitForReconnect(delayMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, delayMs);
    signal.addEventListener("abort", done, { once: true });
  });
}

// Fetch-based SSE keeps bearer credentials in an Authorization header. It otherwise
// mirrors EventSource's durable behavior: parse incrementally, remember event IDs,
// honor server retry hints, and reconnect after network errors or clean EOF until the
// caller aborts. HTTP 204 and 401 are terminal by design.
export async function runEventStream(url: string, options: EventStreamOptions): Promise<void> {
  const fetcher = options.fetcher ?? fetch;
  let reconnectDelayMs = 3_000;
  let lastEventId = "";

  while (!options.signal.aborted) {
    try {
      const headers = new Headers({ accept: "text/event-stream", ...authHeaders() });
      if (lastEventId) headers.set("last-event-id", lastEventId);
      const response = await fetcher(url, {
        method: "GET",
        headers,
        signal: options.signal,
        cache: "no-store",
        credentials: "same-origin",
        redirect: "error",
        referrerPolicy: "no-referrer",
      });

      if (response.status === 401) {
        options.onUnauthorized();
        return;
      }
      if (response.status === 204) return;
      if (!response.ok) throw new Error(`SSE request failed with HTTP ${response.status}`);
      const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
      if (!contentType.startsWith("text/event-stream")) {
        throw new Error(`SSE response has unsupported content type: ${contentType || "missing"}`);
      }
      if (response.body === null) throw new Error("SSE response body is unavailable");

      const parser = new SseParser(reconnectDelayMs, lastEventId);
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      try {
        while (!options.signal.aborted) {
          const { value, done } = await reader.read();
          if (done) {
            for (const event of parser.feed(decoder.decode())) options.onEvent(event);
            for (const event of parser.finish()) options.onEvent(event);
            break;
          }
          for (const event of parser.feed(decoder.decode(value, { stream: true }))) {
            options.onEvent(event);
          }
        }
      } finally {
        reconnectDelayMs = parser.reconnectDelayMs;
        lastEventId = parser.lastEventId;
        reader.releaseLock();
      }
    } catch (error: unknown) {
      if (options.signal.aborted) return;
      // A failed fetch/stream is transient under EventSource semantics. Reconnect
      // after the current retry delay; the next successful response resumes parsing.
      if (error instanceof DOMException && error.name === "AbortError") return;
    }

    await waitForReconnect(reconnectDelayMs, options.signal);
  }
}
