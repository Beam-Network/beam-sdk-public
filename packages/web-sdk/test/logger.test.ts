import { describe, expect, it, vi } from "vitest";

import { createLogger, redact } from "../src/core/logger/index.js";

describe("redaction", () => {
  it("masks values under credential-shaped keys", () => {
    const output = redact({
      access_token: "bwt1.abc.def",
      agentToken: "secret",
      "X-Api-Key": "b1m_live",
      authorization: "Bearer x",
      roomId: "room-1",
    }) as Record<string, unknown>;

    expect(output["access_token"]).toBe("[redacted]");
    expect(output["agentToken"]).toBe("[redacted]");
    expect(output["X-Api-Key"]).toBe("[redacted]");
    expect(output["authorization"]).toBe("[redacted]");
    expect(output["roomId"]).toBe("room-1");
  });

  // Relay capability tokens travel inside URLs, which is where they would
  // otherwise leak into logs unnoticed.
  it("masks credential query parameters inside URLs", () => {
    const output = redact("https://relay.test/t/tun_1/source?sig=abc123def456&region=eu") as string;
    expect(output).toContain("sig=%5Bredacted%5D");
    expect(output).toContain("region=eu");
  });

  it("masks Beam-shaped tokens found loose in strings", () => {
    const output = redact("failed with token bwt1.eyJhIjoxfQ.signature123") as string;
    expect(output).not.toContain("signature123");
    expect(output).toContain("[redacted]");
  });

  it("redacts nested structures", () => {
    const output = redact({ room: { media: { url: "https://r.test", token: "abc" } } }) as Record<
      string,
      Record<string, Record<string, unknown>>
    >;
    expect(output["room"]?.["media"]?.["token"]).toBe("[redacted]");
    // A URL with nothing sensitive in it is passed through untouched.
    expect(output["room"]?.["media"]?.["url"]).toBe("https://r.test");
  });

  it("survives circular references and deep nesting", () => {
    const cyclic: Record<string, unknown> = { name: "loop" };
    cyclic["self"] = cyclic;
    expect(() => redact(cyclic)).not.toThrow();
    expect((redact(cyclic) as Record<string, unknown>)["self"]).toBe("[circular]");

    let deep: Record<string, unknown> = { end: true };
    for (let i = 0; i < 20; i += 1) deep = { nested: deep };
    expect(() => redact(deep)).not.toThrow();
  });

  it("reduces an Error to name and message", () => {
    expect(redact(new TypeError("boom"))).toEqual({ name: "TypeError", message: "boom" });
  });
});

describe("logger", () => {
  it("filters by level", () => {
    const sink = vi.fn();
    const logger = createLogger({ level: "warn", sink });

    logger.debug("debug");
    logger.info("info");
    logger.warn("warn");
    logger.error("error");

    expect(sink).toHaveBeenCalledTimes(2);
    expect(sink.mock.calls.map((call) => call[0])).toEqual(["warn", "error"]);
  });

  it("silent emits nothing", () => {
    const sink = vi.fn();
    createLogger({ level: "silent", sink }).error("nope");
    expect(sink).not.toHaveBeenCalled();
  });

  it("prefixes scope and nests child scopes", () => {
    const sink = vi.fn();
    createLogger({ level: "debug", sink }).child("rooms").child("session").debug("joined");
    expect(sink.mock.calls[0]?.[1]).toBe("[beam:rooms:session] joined");
  });

  it("redacts context by default and honours the opt-out", () => {
    const sink = vi.fn();
    createLogger({ level: "debug", sink }).debug("token", { access_token: "abc" });
    expect(sink.mock.calls[0]?.[2]).toEqual({ access_token: "[redacted]" });

    const raw = vi.fn();
    createLogger({ level: "debug", sink: raw, redact: false }).debug("token", { access_token: "abc" });
    expect(raw.mock.calls[0]?.[2]).toEqual({ access_token: "abc" });
  });

  it("raises the level from localStorage without a rebuild", () => {
    globalThis.localStorage.setItem("beam:debug", "1");
    try {
      expect(createLogger().level).toBe("debug");
    } finally {
      globalThis.localStorage.removeItem("beam:debug");
    }
  });

  it("falls back to the default when storage throws", () => {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      get() {
        throw new Error("blocked in a sandboxed frame");
      },
    });
    try {
      expect(createLogger().level).toBe("warn");
    } finally {
      if (descriptor) Object.defineProperty(globalThis, "localStorage", descriptor);
    }
  });
});
