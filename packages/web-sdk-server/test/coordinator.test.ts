import { describe, expect, it } from "vitest";

import { coordinatorRooms } from "../src/upstreams/coordinator.js";
import type { BeamSessionContext } from "../src/types.js";

/** A coordinator response with the internals a real one carries. */
const COORDINATOR_ROOM = {
  room_id: "mr_9f2",
  tunnel_id: "tun_abc123",
  relay_id: "relay-dev4",
  lease_id: "lease_77",
  plan_version: 3,
  assignment_token: "assign_supersecret",
  public_token: "pub_tok_1",
  public_webrtc_url: "https://relay.test/t/tun_abc123/webrtc/pub_tok_1/",
  expires_at: "2026-08-31T02:00:00Z",
  routes: [{ relay_id: "relay-dev4", region: "eu", url: "https://relay.test" }],
  federation: [{ relay_id: "relay-dev5", connection_state: "connected" }],
};

const CONTEXT = {
  subject: "u",
  organizationId: undefined,
  scopes: [],
  claims: undefined,
} as unknown as BeamSessionContext;

function stub(responder: (url: string, init?: RequestInit) => Response) {
  const calls: Array<{ url: string; method: string; headers: Record<string, string>; body: unknown }> = [];
  // Derived from `fetch` rather than written as `RequestInfo | URL`: this package
  // compiles without the DOM lib, where that type is not global.
  const fetchImpl = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const url = String(input);
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((value, key) => (headers[key.toLowerCase()] = value));
    calls.push({
      url,
      method: init?.method ?? "GET",
      headers,
      body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
    });
    return responder(url, init);
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

describe("coordinatorRooms", () => {
  it("sends the API key and an idempotency key on create", async () => {
    const { fetchImpl, calls } = stub(() => json(COORDINATOR_ROOM, 201));
    const upstream = coordinatorRooms({
      coordinatorUrl: "https://coordinator.test/",
      apiKey: "b1m_secret",
      fetch: fetchImpl,
    });

    await upstream.createRoom!({ name: "standup", ttlSeconds: 600 }, CONTEXT);

    expect(calls[0]?.url).toBe("https://coordinator.test/media/rooms");
    expect(calls[0]?.headers["x-api-key"]).toBe("b1m_secret");
    // A retry inside the broker must not leave a second room holding a credit
    // reservation.
    expect(calls[0]?.headers["idempotency-key"]).toMatch(/^room-/);
    expect(calls[0]?.body).toEqual({ ttl_seconds: 600 });
  });

  // This is where Beam's internals stop. Forwarding them would make relay ids,
  // leases, and plan versions part of a contract that has to keep working.
  it("strips Beam internals from the room it returns", async () => {
    const { fetchImpl } = stub(() => json(COORDINATOR_ROOM, 201));
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    const room = await upstream.createRoom!({ name: "standup" }, CONTEXT);

    expect(room).toEqual({
      id: "mr_9f2",
      name: "standup",
      createdAt: expect.any(String),
      expiresAt: "2026-08-31T02:00:00Z",
      media: {
        url: "https://relay.test/t/tun_abc123/webrtc/pub_tok_1",
        token: "pub_tok_1",
        whipUrl: "https://relay.test/t/tun_abc123/webrtc/pub_tok_1/whip",
        whepUrl: "https://relay.test/t/tun_abc123/webrtc/pub_tok_1/whep",
      },
    });

    const serialized = JSON.stringify(room);
    for (const leak of ["relay_id", "lease_id", "plan_version", "assignment_token", "federation", "relay-dev4"]) {
      expect(serialized).not.toContain(leak);
    }
  });

  it("rejects a coordinator response with no WebRTC URL", async () => {
    const { fetchImpl } = stub(() => json({ room_id: "mr_1" }, 201));
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    await expect(upstream.createRoom!({}, CONTEXT)).rejects.toThrow(/without an id or a WebRTC URL/);
  });

  // A quota or an invalid key is the customer's problem to see, not a 502.
  it("forwards a 4xx from the coordinator with its status", async () => {
    const { fetchImpl } = stub(() => json({ error: "room quota reached" }, 429));
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    await expect(upstream.createRoom!({}, CONTEXT)).rejects.toMatchObject({ status: 429 });
    await expect(upstream.createRoom!({}, CONTEXT)).rejects.toThrow(/room quota reached/);
  });

  it("reports a 5xx as a bad gateway", async () => {
    const { fetchImpl } = stub(() => json({ error: "internal" }, 500));
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    await expect(upstream.createRoom!({}, CONTEXT)).rejects.toMatchObject({ status: 502 });
  });

  it("reports an unreachable coordinator without hanging", async () => {
    const { fetchImpl } = stub(() => {
      throw new TypeError("fetch failed");
    });
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    await expect(upstream.createRoom!({}, CONTEXT)).rejects.toThrow(/Could not reach the Beam coordinator/);
  });

  it("reads a room back", async () => {
    const { fetchImpl, calls } = stub(() => json(COORDINATOR_ROOM));
    const upstream = coordinatorRooms({ coordinatorUrl: "https://coordinator.test", fetch: fetchImpl });

    const room = await upstream.getRoom!("mr_9f2", CONTEXT);

    expect(calls[0]?.url).toBe("https://coordinator.test/media/rooms/mr_9f2");
    expect(room.id).toBe("mr_9f2");
  });
});
