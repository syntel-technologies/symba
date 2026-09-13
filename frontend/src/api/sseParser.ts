export interface ParsedSseEvent {
  event: string;
  data: string;
  lastEventId: string;
}

// Incremental WHATWG event-stream parser. Fetch chunks can split anywhere, including
// inside CRLF, field names, and UTF-8 code points (the latter is handled by the
// streaming TextDecoder in sse.ts). Keeping this parser transport-free makes those
// boundary cases deterministic and independently testable.
export class SseParser {
  private buffer = "";
  private dataLines: string[] = [];
  private eventName = "";
  public reconnectDelayMs: number;
  public lastEventId: string;

  constructor(
    reconnectDelayMs = 3_000,
    lastEventId = "",
  ) {
    this.reconnectDelayMs = reconnectDelayMs;
    this.lastEventId = lastEventId;
  }

  feed(chunk: string): ParsedSseEvent[] {
    this.buffer += chunk;
    return this.drainCompleteLines();
  }

  finish(): ParsedSseEvent[] {
    // A trailing CR is a complete line ending. Other unterminated data is not
    // dispatched at EOF, matching EventSource behavior.
    if (this.buffer.endsWith("\r")) this.buffer += "\n";
    const events = this.drainCompleteLines();
    this.buffer = "";
    return events;
  }

  private drainCompleteLines(): ParsedSseEvent[] {
    const events: ParsedSseEvent[] = [];
    while (true) {
      const boundary = this.findLineBoundary();
      if (boundary === null) break;
      const line = this.buffer.slice(0, boundary.index);
      this.buffer = this.buffer.slice(boundary.index + boundary.width);
      this.processLine(line, events);
    }
    return events;
  }

  private findLineBoundary(): { index: number; width: number } | null {
    for (let index = 0; index < this.buffer.length; index += 1) {
      const char = this.buffer[index];
      if (char === "\n") return { index, width: 1 };
      if (char !== "\r") continue;
      if (index + 1 === this.buffer.length) return null;
      return { index, width: this.buffer[index + 1] === "\n" ? 2 : 1 };
    }
    return null;
  }

  private processLine(line: string, events: ParsedSseEvent[]): void {
    if (line === "") {
      if (this.dataLines.length > 0) {
        events.push({
          event: this.eventName || "message",
          data: this.dataLines.join("\n"),
          lastEventId: this.lastEventId,
        });
      }
      this.dataLines = [];
      this.eventName = "";
      return;
    }
    if (line.startsWith(":")) return;

    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);

    if (field === "data") {
      this.dataLines.push(value);
    } else if (field === "event") {
      this.eventName = value;
    } else if (field === "id" && !value.includes("\0")) {
      this.lastEventId = value;
    } else if (field === "retry" && /^\d+$/.test(value)) {
      const delay = Number(value);
      if (Number.isSafeInteger(delay)) this.reconnectDelayMs = delay;
    }
  }
}
