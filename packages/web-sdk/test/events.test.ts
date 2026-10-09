import { describe, expect, it, vi } from "vitest";

import { EventEmitter, eventsToAsyncIterable } from "../src/core/events/index.js";

type TestEvents = { tick: number; done: string };

describe("EventEmitter", () => {
  it("delivers payloads and unsubscribes via the returned function", () => {
    const emitter = new EventEmitter<TestEvents>();
    const seen: number[] = [];
    const off = emitter.on("tick", (value) => seen.push(value));

    emitter.emit("tick", 1);
    off();
    emitter.emit("tick", 2);

    expect(seen).toEqual([1]);
    expect(emitter.listenerCount("tick")).toBe(0);
  });

  it("once fires exactly one time", () => {
    const emitter = new EventEmitter<TestEvents>();
    const listener = vi.fn();
    emitter.once("tick", listener);

    emitter.emit("tick", 1);
    emitter.emit("tick", 2);

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith(1);
  });

  // A listener that removes itself mid-dispatch must not make the emitter skip
  // the next listener in the set.
  it("iterates a snapshot so mutation during dispatch is safe", () => {
    const emitter = new EventEmitter<TestEvents>();
    const seen: string[] = [];

    const first = (): void => {
      seen.push("first");
      emitter.off("tick", first);
    };
    emitter.on("tick", first);
    emitter.on("tick", () => seen.push("second"));

    emitter.emit("tick", 1);

    expect(seen).toEqual(["first", "second"]);
  });

  // One misbehaving handler must not silently kill a media session.
  it("reports a throwing listener and still delivers to the rest", () => {
    const onError = vi.fn();
    const emitter = new EventEmitter<TestEvents>(onError);
    const after = vi.fn();

    emitter.on("tick", () => {
      throw new Error("listener blew up");
    });
    emitter.on("tick", after);
    emitter.emit("tick", 1);

    expect(onError).toHaveBeenCalledOnce();
    expect(onError.mock.calls[0]?.[1]).toBe("tick");
    expect(after).toHaveBeenCalledOnce();
  });

  it("emitting with no listeners is a no-op", () => {
    expect(() => new EventEmitter<TestEvents>().emit("tick", 1)).not.toThrow();
  });
});

describe("eventsToAsyncIterable", () => {
  it("buffers values emitted between iterations", async () => {
    const emitter = new EventEmitter<TestEvents>();
    const iterator = eventsToAsyncIterable<TestEvents, "tick">(emitter, "tick");

    // Emitted before anything awaits: without buffering these are lost.
    emitter.emit("tick", 1);
    emitter.emit("tick", 2);

    expect((await iterator.next()).value).toBe(1);
    expect((await iterator.next()).value).toBe(2);
    await iterator.return?.();
  });

  it("ends when the `until` promise settles", async () => {
    const emitter = new EventEmitter<TestEvents>();
    let finish: () => void = () => undefined;
    const iterator = eventsToAsyncIterable<TestEvents, "tick">(emitter, "tick", {
      until: () => new Promise<void>((resolve) => (finish = resolve)),
    });

    const collected: number[] = [];
    const consumer = (async () => {
      for await (const value of iterator) collected.push(value);
    })();

    emitter.emit("tick", 1);
    await Promise.resolve();
    finish();
    await consumer;

    expect(collected).toEqual([1]);
  });

  it("ends when the abort signal fires", async () => {
    const emitter = new EventEmitter<TestEvents>();
    const controller = new AbortController();
    const iterator = eventsToAsyncIterable<TestEvents, "tick">(emitter, "tick", { signal: controller.signal });

    const consumer = (async () => {
      for await (const _ of iterator) void _;
      return "ended";
    })();

    controller.abort();
    await expect(consumer).resolves.toBe("ended");
  });

  it("drops the oldest value once the buffer is full", async () => {
    const emitter = new EventEmitter<TestEvents>();
    const iterator = eventsToAsyncIterable<TestEvents, "tick">(emitter, "tick", { maxBuffer: 2 });

    emitter.emit("tick", 1);
    emitter.emit("tick", 2);
    emitter.emit("tick", 3);

    // Newest wins: for progress-shaped events a stale value is worth less than
    // the current one.
    expect((await iterator.next()).value).toBe(2);
    expect((await iterator.next()).value).toBe(3);
    await iterator.return?.();
  });

  it("stops listening once the consumer returns", async () => {
    const emitter = new EventEmitter<TestEvents>();
    const iterator = eventsToAsyncIterable<TestEvents, "tick">(emitter, "tick");

    await iterator.return?.();

    expect(emitter.listenerCount("tick")).toBe(0);
  });
});
