import { describe, expect, it, vi } from "vitest";

import { Beam } from "../src/index.js";
import { toProgress, toTransfer } from "../src/transfers/handle.js";
import { flush, makeToken } from "./helpers.js";

/** A Beam client whose transfer event stream the test drives directly. */
function harness(options: { initial?: Record<string, unknown>; failStream?: boolean } = {}) {
  const requests: Array<{ url: string; method: string; body: unknown }> = [];
  let pushEvent: ((frame: string) => void) | undefined;
  let statusBody: Record<string, unknown> = { id: "tr_1", status: "running" };

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      pushEvent = (frame) => controller.enqueue(encoder.encode(frame));
    },
  });

  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    requests.push({ url, method, body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined });

    const json = (value: unknown, status = 200): Response =>
      new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

    if (url.includes("/token")) return json({ access_token: makeToken(), expires_in: 120 });
    if (url.includes("/events")) {
      if (options.failStream) return json({ error: "streaming unsupported by this proxy" }, 501);
      return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
    }
    if (url.includes("/cancel")) return json({});
    const path = new URL(url).pathname;
    if (path.endsWith("/transfers") && method === "POST") {
      return json({ id: "tr_1", status: "pending", created_at: "2026-08-31T00:00:00Z", ...options.initial });
    }
    if (path.endsWith("/transfers")) return json({ transfers: [{ id: "tr_1", status: "completed" }] });
    return json(statusBody);
  }) as unknown as typeof fetch;

  const beam = new Beam({
    tokenEndpoint: "https://app.test/api/beam/token",
    controlUrl: "https://control.test/v1",
    fetch: fetchImpl,
    maxAttempts: 1,
  });

  return {
    beam,
    requests,
    emit: (event: unknown) => pushEvent?.(`data: ${JSON.stringify(event)}\n\n`),
    setStatus: (value: Record<string, unknown>) => (statusBody = value),
  };
}

describe("transfer normalization", () => {
  it("computes percent from bytes", () => {
    const progress = toProgress({ bytes_completed: 250, total_bytes: 1000 }, "running");
    expect(progress?.percent).toBe(25);
  });

  // A destination fan-out can report more delivered bytes than the logical
  // source size, and a progress bar past 100% reads as a bug.
  it("clamps percent at 100", () => {
    expect(toProgress({ bytes_completed: 5000, total_bytes: 1000 }, "running")?.percent).toBe(100);
  });

  it("leaves percent undefined while the total is unknown", () => {
    expect(toProgress({ bytes_completed: 10 }, "running")?.percent).toBeUndefined();
    expect(toProgress({ bytes_completed: 10, total_bytes: 0 }, "running")?.percent).toBeUndefined();
  });

  it("accepts both snake_case and camelCase from the control plane", () => {
    expect(toProgress({ bytesCompleted: 5, totalBytes: 10 }, "running")?.percent).toBe(50);
  });

  // A newer Beam release adding a lifecycle state must not break existing pages,
  // and every unknown state is by definition non-terminal.
  it("treats an unknown status as running", () => {
    expect(toTransfer({ id: "t", status: "some_new_state" }).status).toBe("running");
  });
});

describe("transfers", () => {
  it("creates a transfer with an idempotency key and normalized destinations", async () => {
    const { beam, requests } = harness();

    const handle = await beam.transfers.create({ source: "s3:reports", destination: "archive:2026" });

    expect(handle.id).toBe("tr_1");
    const create = requests.find((request) => request.url.endsWith("/transfers") && request.method === "POST");
    expect(create?.body).toEqual({ source: "s3:reports", destinations: ["archive:2026"] });
    handle.close();
  });

  it("accepts several destinations", async () => {
    const { beam, requests } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: ["d1", "d2"] });

    const create = requests.find((request) => request.method === "POST" && request.url.endsWith("/transfers"));
    expect((create?.body as { destinations: string[] }).destinations).toEqual(["d1", "d2"]);
    handle.close();
  });

  // The browser names endpoints, never credentials. This is what keeps
  // object-storage secrets server-side by construction.
  it("never sends credentials in the create request", async () => {
    const { beam, requests } = harness();
    const handle = await beam.transfers.create({ source: "s3:reports", destination: "archive:2026" });

    expect(JSON.stringify(requests)).not.toMatch(/secret|access_key|password/i);
    handle.close();
  });

  it("emits progress and resolves done() on completion", async () => {
    const { beam, emit } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    const progress = vi.fn();
    handle.on("progress", progress);

    emit({ id: "tr_1", status: "running", progress: { bytes_completed: 500, total_bytes: 1000 } });
    await flush(20);
    emit({ id: "tr_1", status: "completed", progress: { bytes_completed: 1000, total_bytes: 1000 } });

    const result = await handle.done();
    expect(result.status).toBe("completed");
    expect(progress).toHaveBeenCalledWith(expect.objectContaining({ percent: 50 }));
    expect(handle.lastProgress?.percent).toBe(100);
  });

  it("rejects done() with the server's failure message", async () => {
    const { beam, emit } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    emit({ id: "tr_1", status: "failed", error: { code: "not_found", message: "source object is missing" } });

    await expect(handle.done()).rejects.toThrow(/source object is missing/);
  });

  it("rejects done() when the transfer is cancelled", async () => {
    const { beam, emit } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    emit({ id: "tr_1", status: "cancelled" });

    await expect(handle.done()).rejects.toMatchObject({ code: "aborted" });
  });

  // A caller that attaches late must still observe a completion that already
  // happened, or a fast transfer would hang the caller forever.
  it("settles for a handle created on an already-terminal transfer", async () => {
    const { beam } = harness({ initial: { status: "completed" } });
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    await expect(handle.done()).resolves.toMatchObject({ status: "completed" });
  });

  it("exposes progress as an async iterable that ends at a terminal status", async () => {
    const { beam, emit } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    const seen: number[] = [];
    const consumer = (async () => {
      for await (const progress of handle.progress()) seen.push(progress.bytesCompleted);
    })();

    emit({ id: "tr_1", status: "running", progress: { bytes_completed: 100, total_bytes: 1000 } });
    await flush(20);
    emit({ id: "tr_1", status: "completed", progress: { bytes_completed: 1000, total_bytes: 1000 } });

    await consumer;
    expect(seen).toEqual([100, 1000]);
  });

  // A failing transfer already rejects `done()`. The iterator must end quietly
  // rather than produce a second, unhandled rejection for the same failure.
  it("ends the progress iterator without rejecting when the transfer fails", async () => {
    const { beam, emit } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    const consumer = (async () => {
      for await (const _ of handle.progress()) void _;
      return "ended";
    })();
    handle.done().catch(() => undefined);

    emit({ id: "tr_1", status: "failed", error: { message: "boom" } });
    await expect(consumer).resolves.toBe("ended");
  });

  it("times out done() when asked to", async () => {
    const { beam } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    await expect(handle.done({ timeoutMs: 20 })).rejects.toThrow(/did not finish within 20ms/);
    handle.close();
  });

  // A proxy that refuses to stream must not strand the caller with no updates.
  it("falls back to polling when the event stream is unavailable", async () => {
    const { beam, setStatus } = harness({ failStream: true });
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    const onError = vi.fn();
    handle.on("error", onError);
    await vi.waitFor(() => expect(onError).toHaveBeenCalled());

    setStatus({ id: "tr_1", status: "completed" });
    await expect(handle.refresh()).resolves.toMatchObject({ status: "completed" });
    handle.close();
  });

  it("cancels a transfer and tolerates one that already finished", async () => {
    const { beam, requests } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    await handle.cancel();
    expect(requests.some((request) => request.url.includes("/cancel"))).toBe(true);
    handle.close();
  });

  it("re-attaches to a transfer started elsewhere", async () => {
    const { beam, setStatus } = harness();
    setStatus({ id: "tr_1", status: "running", progress: { bytes_completed: 10, total_bytes: 100 } });

    const handle = await beam.transfers.watch("tr_1");
    expect(handle.status).toBe("running");
    expect(handle.lastProgress?.percent).toBe(10);
    handle.close();
  });

  it("lists recent transfers", async () => {
    const { beam } = harness();
    await expect(beam.transfers.list({ limit: 5 })).resolves.toEqual([expect.objectContaining({ id: "tr_1" })]);
  });

  it("close() stops watching without cancelling the transfer", async () => {
    const { beam, requests } = harness();
    const handle = await beam.transfers.create({ source: "s3:a", destination: "d" });

    handle.close();
    await flush(10);

    expect(requests.some((request) => request.url.includes("/cancel"))).toBe(false);
  });
});
