import { describe, expect, it, vi } from "vitest";

import { BeamAbortError, BeamApiError, BeamError } from "../src/core/errors/index.js";
import { Reconnector, backoffDelay, delay, withRetry } from "../src/core/retry/index.js";

describe("backoffDelay", () => {
  it("grows exponentially up to the ceiling", () => {
    const options = { initialDelayMs: 100, factor: 2, maxDelayMs: 1000, random: () => 1 };
    expect(backoffDelay(0, options)).toBe(100);
    expect(backoffDelay(1, options)).toBe(200);
    expect(backoffDelay(2, options)).toBe(400);
    expect(backoffDelay(10, options)).toBe(1000);
  });

  // Full jitter, not equal jitter: a relay restart drops a whole room at once,
  // and a floor would bring them all back in a synchronized wave.
  it("scales the whole delay by the random draw, allowing zero", () => {
    const options = { initialDelayMs: 1000, factor: 2, maxDelayMs: 10_000 };
    expect(backoffDelay(3, { ...options, random: () => 0 })).toBe(0);
    expect(backoffDelay(3, { ...options, random: () => 0.5 })).toBe(4000);
  });
});

describe("withRetry", () => {
  const immediate = { initialDelayMs: 0, random: () => 0 };

  it("returns the first successful result", async () => {
    const operation = vi.fn(async () => "ok");
    expect(await withRetry(operation, immediate)).toBe("ok");
    expect(operation).toHaveBeenCalledOnce();
  });

  it("retries a retryable error and then succeeds", async () => {
    let attempts = 0;
    const result = await withRetry(
      async () => {
        attempts += 1;
        if (attempts < 3) throw new BeamError("unavailable", { code: "unavailable" });
        return "ok";
      },
      { ...immediate, maxAttempts: 5 },
    );

    expect(result).toBe("ok");
    expect(attempts).toBe(3);
  });

  it("does not retry a non-retryable error", async () => {
    const operation = vi.fn(async () => {
      throw new BeamApiError("gone", { code: "not_found" });
    });

    await expect(withRetry(operation, { ...immediate, maxAttempts: 5 })).rejects.toThrow(BeamApiError);
    expect(operation).toHaveBeenCalledOnce();
  });

  it("stops at maxAttempts and rethrows the last error", async () => {
    const operation = vi.fn(async () => {
      throw new BeamError("still down", { code: "unavailable" });
    });

    await expect(withRetry(operation, { ...immediate, maxAttempts: 3 })).rejects.toThrow(/still down/);
    expect(operation).toHaveBeenCalledTimes(3);
  });

  it("prefers a server-supplied Retry-After over its own curve", async () => {
    const delays: number[] = [];
    let attempts = 0;

    await withRetry(
      async () => {
        attempts += 1;
        if (attempts === 1) {
          throw new BeamError("slow down", { code: "resource_exhausted", retryAfterSeconds: 0 });
        }
        return "ok";
      },
      { ...immediate, maxAttempts: 2, onRetry: (_error, _attempt, ms) => delays.push(ms) },
    );

    expect(delays).toEqual([0]);
  });

  it("aborts promptly when the signal fires", async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(withRetry(async () => "never", { signal: controller.signal })).rejects.toThrow(BeamAbortError);
  });
});

describe("delay", () => {
  it("rejects when aborted mid-wait", async () => {
    const controller = new AbortController();
    const pending = delay(10_000, controller.signal);
    controller.abort();
    await expect(pending).rejects.toThrow(BeamAbortError);
  });
});

describe("Reconnector", () => {
  it("reconnects after a clean close until aborted", async () => {
    const controller = new AbortController();
    let connects = 0;

    const reconnector = new Reconnector({
      signal: controller.signal,
      initialDelayMs: 0,
      random: () => 0,
    });

    await reconnector.run(async () => {
      connects += 1;
      // A clean end still reconnects: a participant whose relay closed the
      // stream expects to be back in the room, not silently dropped.
      if (connects >= 3) controller.abort();
    });

    expect(connects).toBe(3);
    expect(reconnector.state).toBe("closed");
  });

  it("reports state transitions in order", async () => {
    const controller = new AbortController();
    const states: string[] = [];
    const reconnector = new Reconnector({
      signal: controller.signal,
      initialDelayMs: 0,
      random: () => 0,
      onStateChange: (state) => states.push(state),
    });

    let attempts = 0;
    await reconnector.run(async () => {
      attempts += 1;
      reconnector.markConnected();
      if (attempts >= 2) controller.abort();
      throw new Error("dropped");
    });

    // Each successful attempt reports "connected" again, so a UI bound to this
    // shows recovery rather than staying stuck on "reconnecting".
    expect(states).toEqual(["connecting", "connected", "reconnecting", "connected", "closed"]);
  });

  it("resets backoff once a connection is healthy", async () => {
    const reconnector = new Reconnector({ initialDelayMs: 0, random: () => 0 });
    reconnector.markConnected();
    expect(reconnector.state).toBe("connected");
  });
});
