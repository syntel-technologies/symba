import assert from "node:assert/strict";
import test from "node:test";

import { SseParser } from "../src/api/sseParser.ts";

test("parses fragmented CRLF, comments, event names, and multiline data", () => {
  const parser = new SseParser();
  const events = [
    ...parser.feed(": keepalive\r"),
    ...parser.feed("\nevent: job_"),
    ...parser.feed("event\r\ndata: first\r\ndata: second\r"),
    ...parser.feed("\n\r\n"),
  ];

  assert.deepEqual(events, [
    { event: "job_event", data: "first\nsecond", lastEventId: "" },
  ]);
});

test("retains event IDs and honors retry hints across messages", () => {
  const parser = new SseParser();
  const events = parser.feed("retry: 1250\nid: event-9\ndata: payload\n\n");

  assert.equal(parser.reconnectDelayMs, 1_250);
  assert.equal(parser.lastEventId, "event-9");
  assert.deepEqual(events, [
    { event: "message", data: "payload", lastEventId: "event-9" },
  ]);
});

test("ignores invalid retry values and null-containing event IDs", () => {
  const parser = new SseParser(3_000, "previous");
  const events = parser.feed("retry: nope\nid: bad\0id\ndata\n\n");

  assert.equal(parser.reconnectDelayMs, 3_000);
  assert.equal(parser.lastEventId, "previous");
  assert.deepEqual(events, [
    { event: "message", data: "", lastEventId: "previous" },
  ]);
});

test("does not dispatch an unterminated event at EOF", () => {
  const parser = new SseParser();

  assert.deepEqual(parser.feed("data: incomplete"), []);
  assert.deepEqual(parser.finish(), []);
});
