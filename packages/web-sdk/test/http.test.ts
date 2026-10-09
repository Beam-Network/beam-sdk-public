import { describe, expect, it, vi } from "vitest";

import { CachingTokenProvider } from "../src/core/auth/index.js";
import { BeamApiError, BeamAuthError, BeamNetworkError, BeamTimeoutError } from "../src/core/errors/index.js";
import { HttpTransport } from "../src/core/transport/index.js";
import { makeToken, stubFetch } from "./helpers.js";

function transportWith(routes: Parameters<typeof stubFetch>[0], options: { mint?: () => Promise<never> } = {}) {
  const { fetch, requests } = stubFetch(routes);
  const provider = new CachingTokenProvider({
    mint: options.mint ?? (async () => ({ access_token: makeToken(), expires_in: 120 })),
  });
  const transport = new HttpTransport({
    baseUrl: "https://control.test/v1",
    tokenProvider: provider,
    fetch,
    maxAttempts: 2,
  });
  return { transport, requests, provider };
}

describe("HttpTransport", () => {
  it("attaches a bearer token and joins paths against the base URL", async () => {
    const { transport, requests } = transportWith({ "/rooms": { body: { id: "room-1" } } });

    await transport.get("/rooms");

    expect(requests[0]?.url).toBe("https://control.test/v1/rooms");
    expect(requests[0]?.headers["authorization"]).toMatch(/^Bearer bwt1\./);
  });

  it("sends an Idempotency-Key on mutations but not on reads", async () => {
    const { transport, requests } = transportWith({
      "/rooms": { body: { id: "room-1" } },
      "/transfers": { body: { id: "t-1" } },
    });

    await transport.post("/rooms", { name: "standup" });
    await transport.get("/transfers");

    expect(requests[0]?.headers["idempotency-key"]).toBeTruthy();
    expect(requests[1]?.headers["idempotency-key"]).toBeUndefined();
  });

  // The coordinator answers 409 when the same key arrives with a different body,
  // so a retry must reuse the key it already sent, not mint a new one.
  it("reuses the same Idempotency-Key across retries of one operation", async () => {
    const { transport, requests } = transportWith({
      "/rooms": [{ status: 503, body: { error: "no relay available" } }, { body: { id: "room-1" } }],
    });

    await transport.post("/rooms", { name: "standup" }, { idempotencyKey: "fixed-key" });

    expect(requests).toHaveLength(2);
    expect(requests[0]?.headers["idempotency-key"]).toBe("fixed-key");
    expect(requests[1]?.headers["idempotency-key"]).toBe("fixed-key");
  });

  it("re-mints once on a 401 and retries with the new token", async () => {
    let issued = 0;
    const { fetch, requests } = stubFetch({
      "/rooms": [{ status: 401, body: { error: "token expired" } }, { body: { id: "room-1" } }],
    });
    const provider = new CachingTokenProvider({
      mint: async () => ({ access_token: makeToken({ jti: `token-${(issued += 1)}` }), expires_in: 120 }),
    });
    const transport = new HttpTransport({ baseUrl: "https://control.test/v1", tokenProvider: provider, fetch });

    await transport.get("/rooms");

    expect(issued).toBe(2);
    expect(requests[0]?.headers["authorization"]).not.toBe(requests[1]?.headers["authorization"]);
  });

  it("gives up after one re-mint so a persistent 401 does not loop", async () => {
    const { transport, requests } = transportWith({ "/rooms": { status: 401, body: { error: "nope" } } });

    await expect(transport.get("/rooms")).rejects.toThrow(BeamAuthError);
    expect(requests).toHaveLength(2);
  });

  // A Beam-originated code must survive a broker that picks its own status.
  it("prefers the error code in the body over the one implied by the status", async () => {
    const { transport } = transportWith({
      "/rooms": { status: 400, body: { code: "resource_exhausted", error: "room quota reached" } },
    });

    await expect(transport.post("/rooms")).rejects.toMatchObject({ code: "resource_exhausted" });
  });

  it("maps statuses onto Beam error codes when the body carries none", async () => {
    for (const [status, code] of [
      [403, "permission_denied"],
      [404, "not_found"],
      [409, "conflict"],
      [503, "unavailable"],
    ] as const) {
      const { transport } = transportWith({ "/rooms": { status, body: { error: "x" } } });
      await expect(transport.get("/rooms")).rejects.toMatchObject({ code });
    }
  });

  it("surfaces the request id for support", async () => {
    const { transport } = transportWith({
      "/rooms": { status: 500, body: { error: "boom" }, headers: { "x-request-id": "req-42" } },
    });

    await expect(transport.get("/rooms")).rejects.toMatchObject({ requestId: "req-42" });
  });

  it("honours Retry-After instead of its own backoff", async () => {
    const { fetch } = stubFetch({
      "/rooms": [{ status: 429, body: { error: "slow down" }, headers: { "retry-after": "0" } }, { body: { id: "r" } }],
    });
    const transport = new HttpTransport({ baseUrl: "https://control.test/v1", fetch, maxAttempts: 2 });

    await expect(transport.get("/rooms", { authenticated: false })).resolves.toBeDefined();
  });

  it("does not retry a non-retryable error", async () => {
    const { transport, requests } = transportWith({ "/rooms": { status: 404, body: { error: "gone" } } });

    await expect(transport.get("/rooms")).rejects.toThrow(BeamApiError);
    expect(requests).toHaveLength(1);
  });

  // fetch rejects opaquely for CORS, and a browser integration failing against a
  // healthy endpoint is nearly always CORS, so the message has to say so.
  it("names CORS in the network error message", async () => {
    const { transport } = transportWith({ "/rooms": { throws: new TypeError("Failed to fetch") } });

    await expect(transport.get("/rooms")).rejects.toThrow(BeamNetworkError);
    await expect(transport.get("/rooms")).rejects.toThrow(/CORS/);
  });

  it("times out a slow request", async () => {
    const fetchImpl = (() => new Promise(() => undefined)) as unknown as typeof fetch;
    const transport = new HttpTransport({ baseUrl: "https://control.test/v1", fetch: fetchImpl, maxAttempts: 1 });

    await expect(transport.get("/rooms", { authenticated: false, timeoutMs: 10 })).rejects.toThrow(BeamTimeoutError);
  });

  it("serializes query parameters and drops undefined ones", async () => {
    const { transport, requests } = transportWith({ "/transfers": { body: { transfers: [] } } });

    await transport.get("/transfers", { query: { limit: 10, status: undefined, active: true } });

    const url = new URL(requests[0]?.url ?? "");
    expect(url.searchParams.get("limit")).toBe("10");
    expect(url.searchParams.get("active")).toBe("true");
    expect(url.searchParams.has("status")).toBe(false);
  });

  it("returns undefined for a 204 rather than failing to parse an empty body", async () => {
    const { transport } = transportWith({ "/rooms": { status: 204 } });
    await expect(transport.delete("/rooms")).resolves.toBeUndefined();
  });

  it("withBaseUrl keeps auth and can be pointed at a relay", async () => {
    const { transport, requests } = transportWith({ "https://relay.test": { body: { ok: true } } });

    await transport.withBaseUrl("https://relay.test/t/tun_1/webrtc/tok").get("/ice", { authenticated: false });

    expect(requests[0]?.url).toBe("https://relay.test/t/tun_1/webrtc/tok/ice");
    // Media URLs carry their own capability token; adding a bearer would be wrong.
    expect(requests[0]?.headers["authorization"]).toBeUndefined();
  });

  it("does not mint a token for an unauthenticated request", async () => {
    const mint = vi.fn(async () => ({ access_token: makeToken(), expires_in: 120 }));
    const { fetch } = stubFetch({ "/ice": { body: {} } });
    const transport = new HttpTransport({
      baseUrl: "https://relay.test",
      tokenProvider: new CachingTokenProvider({ mint }),
      fetch,
    });

    await transport.get("/ice", { authenticated: false });

    expect(mint).not.toHaveBeenCalled();
  });
});
