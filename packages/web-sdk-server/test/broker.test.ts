import { describe, expect, it, vi } from "vitest";

import { createBeamBroker } from "../src/broker.js";
import { mintToken } from "../src/token.js";
import type { BeamScope } from "../src/token.js";
import { memoryUpstream } from "../src/upstreams/memory.js";
import type { AuthorizedSession, BeamUpstream } from "../src/types.js";

const SECRET = "s".repeat(32);
const BASE = "http://localhost:3000/api/beam";

function makeBroker(
  overrides: {
    session?: AuthorizedSession | null;
    upstream?: BeamUpstream;
    allowedOrigins?: string[];
  } = {},
) {
  const session: AuthorizedSession | null =
    overrides.session === undefined
      ? {
          subject: "user-7",
          scopes: ["rooms:create", "room:read", "transfers:create", "transfers:read", "transfers:cancel"],
        }
      : overrides.session;

  return createBeamBroker({
    secret: SECRET,
    authorize: () => session,
    upstream: overrides.upstream ?? memoryUpstream({ totalBytes: 100, bytesPerTick: 50, tickMs: 5 }),
    ...(overrides.allowedOrigins ? { allowedOrigins: overrides.allowedOrigins } : {}),
  });
}

function post(path: string, body?: unknown, headers: Record<string, string> = {}): Request {
  return new Request(`${BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
}

function get(path: string, headers: Record<string, string> = {}): Request {
  return new Request(`${BASE}${path}`, { headers });
}

async function tokenFor(broker: ReturnType<typeof makeBroker>, scopes?: BeamScope[]): Promise<string> {
  const response = await broker.handle(post("/token", scopes ? { scopes } : undefined));
  const body = (await response!.json()) as { access_token: string };
  return body.access_token;
}

function auth(token: string): Record<string, string> {
  return { authorization: `Bearer ${token}` };
}

describe("token endpoint", () => {
  it("mints a token for an authorized session", async () => {
    const broker = makeBroker();
    const response = await broker.handle(post("/token"));

    expect(response?.status).toBe(200);
    const body = (await response!.json()) as Record<string, unknown>;
    expect(body["token_type"]).toBe("Bearer");
    expect(String(body["access_token"]).startsWith("bwt1.")).toBe(true);
    expect(body["expires_in"]).toBe(120);
    // A cached token would outlive its own window and be replayable.
    expect(response?.headers.get("cache-control")).toBe("no-store");
  });

  it("denies an unauthenticated caller", async () => {
    const broker = makeBroker({ session: null });
    const response = await broker.handle(post("/token"));
    expect(response?.status).toBe(401);
  });

  // The browser asks; the server decides. A tampered request can only narrow
  // its own privileges, never widen them.
  it("intersects requested scopes with the granted set", async () => {
    const broker = makeBroker({ session: { subject: "u", scopes: ["room:read"] } });
    const response = await broker.handle(post("/token", { scopes: ["room:read", "transfers:create"] }));

    const body = (await response!.json()) as { scopes: string[] };
    expect(body.scopes).toEqual(["room:read"]);
  });

  it("refuses when no requested scope is permitted", async () => {
    const broker = makeBroker({ session: { subject: "u", scopes: ["room:read"] } });
    const response = await broker.handle(post("/token", { scopes: ["transfers:create"] }));

    expect(response?.status).toBe(403);
  });

  it("binds the token to the requesting origin", async () => {
    const broker = makeBroker({ allowedOrigins: ["https://app.example"] });
    const response = await broker.handle(post("/token", undefined, { origin: "https://app.example" }));
    const body = (await response!.json()) as { access_token: string };

    const claims = broker.verify(get("/rooms/x", { ...auth(body.access_token), origin: "https://app.example" }));
    expect(claims.origin).toBe("https://app.example");
  });

  it("tells the browser where the control plane lives", async () => {
    const broker = makeBroker();
    const body = (await (await broker.handle(post("/token")))!.json()) as { endpoint: string };
    expect(body.endpoint).toBe("/api/beam");
  });
});

describe("authorization on control-plane routes", () => {
  it("rejects a request with no token", async () => {
    const broker = makeBroker();
    const response = await broker.handle(post("/rooms", { name: "standup" }));
    expect(response?.status).toBe(401);
  });

  it("rejects a token signed with the wrong secret", async () => {
    const broker = makeBroker();
    const { token } = mintToken({ secret: "x".repeat(32), scopes: ["rooms:create"] });

    const response = await broker.handle(post("/rooms", {}, auth(token)));
    expect(response?.status).toBe(401);
  });

  it("rejects a token that lacks the route's scope", async () => {
    const broker = makeBroker({ session: { scopes: ["room:read"] } });
    const token = await tokenFor(broker);

    const response = await broker.handle(post("/rooms", {}, auth(token)));
    expect(response?.status).toBe(403);
    expect(await response!.text()).toMatch(/rooms:create/);
  });

  it("rejects an expired token", async () => {
    const broker = makeBroker();
    const { token } = mintToken({
      secret: SECRET,
      scopes: ["rooms:create"],
      now: () => Date.now() - 10 * 60 * 1000,
    });

    const response = await broker.handle(post("/rooms", {}, auth(token)));
    expect(response?.status).toBe(401);
  });
});

describe("rooms", () => {
  it("creates a room and returns only browser-relevant fields", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);

    const response = await broker.handle(post("/rooms", { name: "standup", ttl_seconds: 600 }, auth(token)));

    expect(response?.status).toBe(201);
    const room = (await response!.json()) as Record<string, unknown>;
    expect(room["id"]).toMatch(/^room_/);
    expect(room["name"]).toBe("standup");
    expect(room["media"]).toMatchObject({ url: expect.stringContaining("http") });
  });

  it("reads a room back", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    const created = (await (await broker.handle(post("/rooms", {}, auth(token))))!.json()) as { id: string };

    const response = await broker.handle(get(`/rooms/${created.id}`, auth(token)));
    expect(response?.status).toBe(200);
  });

  it("answers 404 for an unknown room", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);

    const response = await broker.handle(get("/rooms/room_missing", auth(token)));
    expect(response?.status).toBe(404);
  });

  // A broker wired for rooms only should say so clearly rather than fail oddly.
  it("answers 501 for a capability the upstream does not implement", async () => {
    const broker = makeBroker({ upstream: {} });
    const token = await tokenFor(broker);

    const response = await broker.handle(post("/rooms", {}, auth(token)));
    expect(response?.status).toBe(501);
    expect(await response!.text()).toMatch(/createRoom/);
  });
});

describe("transfers", () => {
  it("creates a transfer and validates its input", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);

    const created = await broker.handle(post("/transfers", { source: "s3:a", destinations: ["d1"] }, auth(token)));
    expect(created?.status).toBe(201);

    const invalid = await broker.handle(post("/transfers", { source: "s3:a" }, auth(token)));
    expect(invalid?.status).toBe(400);
  });

  it("streams progress as SSE and closes at a terminal status", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    const created = (await (await broker.handle(
      post("/transfers", { source: "s3:a", destinations: ["d1"] }, auth(token)),
    ))!.json()) as { id: string };

    const response = await broker.handle(get(`/transfers/${created.id}/events`, auth(token)));
    expect(response?.headers.get("content-type")).toBe("text/event-stream");
    // nginx and several CDNs buffer without this, and the browser would see
    // nothing until the transfer finished.
    expect(response?.headers.get("x-accel-buffering")).toBe("no");

    const body = await response!.text();
    expect(body).toMatch(/^id: 1\n/m);
    expect(body).toMatch(/event: transfer/);
    expect(body).toMatch(/"status":"completed"/);
  });

  it("cancels a transfer", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    const created = (await (await broker.handle(
      post("/transfers", { source: "s3:a", destinations: ["d1"] }, auth(token)),
    ))!.json()) as { id: string };

    const response = await broker.handle(post(`/transfers/${created.id}/cancel`, undefined, auth(token)));
    expect(response?.status).toBe(200);
  });

  it("lists transfers", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    await broker.handle(post("/transfers", { source: "s3:a", destinations: ["d"] }, auth(token)));

    const response = await broker.handle(get("/transfers?limit=5", auth(token)));
    const body = (await response!.json()) as { transfers: unknown[] };
    expect(body.transfers).toHaveLength(1);
  });
});

describe("routing and CORS", () => {
  it("ignores paths outside the base path so it can be mounted at the root", async () => {
    const broker = makeBroker();
    expect(await broker.handle(new Request("http://localhost:3000/api/other"))).toBeUndefined();
    expect(await broker.handle(new Request("http://localhost:3000/"))).toBeUndefined();
  });

  it("does not confuse /transfers/:id/cancel with /transfers/:id", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    const created = (await (await broker.handle(
      post("/transfers", { source: "s3:a", destinations: ["d"] }, auth(token)),
    ))!.json()) as { id: string };

    const cancel = await broker.handle(post(`/transfers/${created.id}/cancel`, undefined, auth(token)));
    expect((await cancel!.json()) as unknown).toEqual({ cancelled: true });
  });

  it("answers preflight for an allowed origin only", async () => {
    const broker = makeBroker({ allowedOrigins: ["https://app.example"] });

    const allowed = await broker.handle(
      new Request(`${BASE}/token`, { method: "OPTIONS", headers: { origin: "https://app.example" } }),
    );
    expect(allowed?.status).toBe(204);
    expect(allowed?.headers.get("access-control-allow-origin")).toBe("https://app.example");
    // Without Vary, a shared cache can hand one origin's response to another.
    expect(allowed?.headers.get("vary")).toBe("Origin");

    const denied = await broker.handle(
      new Request(`${BASE}/token`, { method: "OPTIONS", headers: { origin: "https://evil.example" } }),
    );
    expect(denied?.headers.get("access-control-allow-origin")).toBeNull();
  });
});

describe("error handling", () => {
  it("rejects a malformed JSON body", async () => {
    const broker = makeBroker();
    const token = await tokenFor(broker);
    const request = new Request(`${BASE}/rooms`, {
      method: "POST",
      headers: { "content-type": "application/json", ...auth(token) },
      body: "{not json",
    });

    const response = await broker.handle(request);
    expect(response?.status).toBe(400);
  });

  // An upstream failure can carry internal hostnames, credential prefixes, or
  // SQL. The browser gets none of it; the server-side hook gets all of it.
  it("hides upstream detail from the browser but reports it to onError", async () => {
    const onError = vi.fn();
    const broker = createBeamBroker({
      secret: SECRET,
      authorize: () => ({ scopes: ["rooms:create"] }),
      onError,
      upstream: {
        createRoom() {
          throw new Error("connect ECONNREFUSED 10.0.0.5:5432 user=beam password=hunter2");
        },
      },
    });
    const token = await tokenFor(broker);

    const response = await broker.handle(post("/rooms", {}, auth(token)));
    const text = await response!.text();

    expect(response?.status).toBe(500);
    expect(text).not.toMatch(/hunter2|10\.0\.0\.5/);
    expect(onError).toHaveBeenCalledOnce();
  });
});
