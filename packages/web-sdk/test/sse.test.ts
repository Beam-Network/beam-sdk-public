import { describe, expect, it, vi } from "vitest";

import { HttpTransport, SseParser, SseStream } from "../src/core/transport/index.js";
import { flush, sseResponse } from "./helpers.js";

describe("SseParser", () => {
  it("parses a complete frame", () => {
    const events = new SseParser().push('event: track_added\ndata: {"a":1}\nid: 7\n\n');
    expect(events).toEqual([{ type: "track_added", data: '{"a":1}', id: "7" }]);
  });

  it("defaults the type to message when no event field is present", () => {
    expect(new SseParser().push("data: hello\n\n")[0]?.type).toBe("message");
  });

  // The failure this guards against: a frame split across two network reads
  // being silently dropped by a split-on-blank-line parser.
  it("reassembles a frame delivered across several chunks", () => {
    const parser = new SseParser();
    expect(parser.push("event: parti")).toEqual([]);
    expect(parser.push("cipant_joined\nda")).toEqual([]);
    expect(parser.push('ta: {"id":"p1"}\n')).toEqual([]);

    const events = parser.push("\n");
    expect(events).toEqual([{ type: "participant_joined", data: '{"id":"p1"}', id: undefined }]);
  });

  it("parses several frames from one chunk", () => {
    expect(new SseParser().push("data: one\n\ndata: two\n\n")).toHaveLength(2);
  });

  it("joins multi-line data with newlines, per the SSE spec", () => {
    expect(new SseParser().push("data: line1\ndata: line2\n\n")[0]?.data).toBe("line1\nline2");
  });

  it("ignores comments and keep-alives", () => {
    expect(new SseParser().push(": keep-alive\n\n")).toEqual([]);
    expect(new SseParser().push(": ping\ndata: real\n\n")).toHaveLength(1);
  });

  it("strips exactly one leading space after the colon", () => {
    expect(new SseParser().push("data:  two-spaces\n\n")[0]?.data).toBe(" two-spaces");
  });

  it("normalizes CRLF line endings", () => {
    expect(new SseParser().push("event: x\r\ndata: y\r\n\r\n")).toEqual([{ type: "x", data: "y", id: undefined }]);
  });

  it("drops a frame with no data field", () => {
    expect(new SseParser().push("event: no-data\nid: 3\n\n")).toEqual([]);
  });
});

describe("SseStream", () => {
  function streamTransport(responses: Response[]): {
    transport: HttpTransport;
    urls: string[];
    headers: Array<Record<string, string>>;
  } {
    const urls: string[] = [];
    const headers: Array<Record<string, string>> = [];
    let call = 0;

    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      urls.push(String(input));
      const recorded: Record<string, string> = {};
      new Headers(init?.headers).forEach((value, key) => (recorded[key.toLowerCase()] = value));
      headers.push(recorded);
      const response = responses[Math.min(call, responses.length - 1)];
      call += 1;
      return response as Response;
    }) as unknown as typeof fetch;

    return { transport: new HttpTransport({ baseUrl: "https://relay.test", fetch: fetchImpl }), urls, headers };
  }

  it("delivers parsed events to the handler", async () => {
    const { transport } = streamTransport([sseResponse(['data: {"type":"participant_joined"}\n\n'])]);
    const onEvent = vi.fn();
    const controller = new AbortController();

    const stream = new SseStream({
      transport,
      path: "/sfu/events",
      authenticated: false,
      signal: controller.signal,
      onEvent,
    });
    const running = stream.start();
    await flush(20);
    controller.abort();
    await running;

    expect(onEvent).toHaveBeenCalledWith(expect.objectContaining({ data: '{"type":"participant_joined"}' }));
  });

  // Resume is what makes a dropped stream safe: without it a reconnect either
  // replays from the start or silently skips whatever arrived while offline.
  it("sends Last-Event-ID after a reconnect", async () => {
    const { transport, headers } = streamTransport([
      sseResponse(["id: 42\ndata: first\n\n"]),
      sseResponse(["id: 43\ndata: second\n\n"]),
    ]);
    const controller = new AbortController();

    const stream = new SseStream({
      transport,
      path: "/sfu/events",
      authenticated: false,
      signal: controller.signal,
      initialDelayMs: 0,
      onEvent: () => undefined,
    });
    const running = stream.start();
    await flush(30);
    controller.abort();
    await running;

    expect(headers[0]?.["last-event-id"]).toBeUndefined();
    expect(headers[1]?.["last-event-id"]).toBe("42");
    expect(stream.lastEventId).toBe("43");
  });

  it("stops reconnecting once closed", async () => {
    const { transport, urls } = streamTransport([sseResponse(["data: x\n\n"])]);
    const stream = new SseStream({
      transport,
      path: "/sfu/events",
      authenticated: false,
      initialDelayMs: 0,
      onEvent: () => undefined,
    });

    const running = stream.start();
    await flush(10);
    stream.close();
    await running;

    const seen = urls.length;
    await flush(30);
    expect(urls.length).toBe(seen);
  });
});
