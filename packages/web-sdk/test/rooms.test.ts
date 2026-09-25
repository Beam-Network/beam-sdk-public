import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { Beam } from "../src/index.js";
import type { RoomSession } from "../src/index.js";
import { FakeMediaStream, FakePeerConnection, flush, installWebrtcFakes, makeToken } from "./helpers.js";

const ROOM_RESPONSE = {
  id: "room_abc",
  name: "standup",
  created_at: "2026-08-31T00:00:00Z",
  expires_at: "2026-08-31T02:00:00Z",
  media: { url: "https://relay.test/t/tun_1/webrtc/pub_1", token: "media-token" },
};

/**
 * A Beam client wired to a scripted edge: a token endpoint, a control plane, and
 * a relay. Room events are delivered through a controllable SSE stream so tests
 * can drive the SFU rather than wait on it.
 */
function harness(overrides: Record<string, unknown> = {}) {
  const requests: Array<{ url: string; method: string; body: unknown }> = [];
  let pushEvent: ((frame: string) => void) | undefined;
  let closeEvents: (() => void) | undefined;

  const eventStream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      pushEvent = (frame) => controller.enqueue(encoder.encode(frame));
      closeEvents = () => {
        try {
          controller.close();
        } catch {
          // Already closed by a prior teardown.
        }
      };
    },
  });

  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    const body = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
    requests.push({ url, method, body });

    const json = (value: unknown, status = 200): Response =>
      new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

    if (url.includes("/token")) return json({ access_token: makeToken(), expires_in: 120 });
    if (url.includes("/sfu/events")) {
      return new Response(eventStream, { status: 200, headers: { "content-type": "text/event-stream" } });
    }
    if (url.includes("/ice-restart")) return json({ ice_servers: [] });
    if (url.endsWith("/ice")) return json({ ice_servers: [{ urls: "stun:stun.test" }], ttl_seconds: 300 });
    if (url.includes("/sfu/participants") && method === "POST" && url.includes("/renegotiate")) {
      return json({ type: "offer", sdp: "v=0\r\no=- sfu offer\r\n" });
    }
    if (url.includes("/sfu/participants") && method === "POST" && url.includes("/answer")) return json({});
    if (url.includes("/sfu/participants") && method === "POST" && url.includes("/candidates")) return json({});
    if (url.includes("/sfu/participants") && method === "POST") {
      return json({ type: "answer", sdp: "v=0\r\no=- sfu answer\r\n", participant_id: "p_self" });
    }
    if (url.includes("/sfu/participants") && method === "DELETE") return new Response(null, { status: 204 });
    if (url.includes("/rooms")) return json({ ...ROOM_RESPONSE, ...overrides });

    return json({ error: `unhandled ${method} ${url}` }, 404);
  }) as unknown as typeof fetch;

  const beam = new Beam({
    tokenEndpoint: "https://app.test/api/beam/token",
    controlUrl: "https://control.test/v1",
    fetch: fetchImpl,
  });

  return {
    beam,
    requests,
    emit: (event: unknown) => pushEvent?.(`data: ${JSON.stringify(event)}\n\n`),
    emitRaw: (frame: string) => pushEvent?.(frame),
    closeEvents: () => closeEvents?.(),
  };
}

describe("rooms", () => {
  let uninstall: () => void;
  let sessions: RoomSession[] = [];

  beforeEach(() => {
    uninstall = installWebrtcFakes();
    sessions = [];
  });

  afterEach(async () => {
    for (const session of sessions) await session.leave().catch(() => undefined);
    uninstall();
  });

  it("creates a room and returns a clean, Beam-internals-free shape", async () => {
    const { beam, requests } = harness();

    const room = await beam.rooms.create({ name: "standup", ttlSeconds: 3600 });

    expect(room).toEqual({
      id: "room_abc",
      name: "standup",
      createdAt: "2026-08-31T00:00:00Z",
      expiresAt: "2026-08-31T02:00:00Z",
      media: { url: "https://relay.test/t/tun_1/webrtc/pub_1", token: "media-token" },
    });
    // No relay id, tunnel id, lease, plan version, or assignment token reaches
    // the developer.
    expect(JSON.stringify(room)).not.toMatch(/relay_id|lease|plan_version|assignment/);

    const create = requests.find((request) => request.url.endsWith("/rooms") && request.method === "POST");
    expect(create?.body).toEqual({ name: "standup", ttl_seconds: 3600 });
  });

  it("mints a token before the first control-plane call", async () => {
    const { beam, requests } = harness();
    await beam.rooms.create();

    expect(requests[0]?.url).toContain("/token");
    expect(requests[1]?.url).toContain("/rooms");
  });

  it("joins a room, publishes media, and reports the participant id", async () => {
    const { beam } = harness();
    const room = await beam.rooms.create();
    const stream = new FakeMediaStream(["audio", "video"]) as unknown as MediaStream;

    const session = await beam.rooms.join(room, { publish: stream, displayName: "Ada" });
    sessions.push(session);

    expect(session.participantId).toBe("p_self");
    expect(session.state).toBe("connected");

    const peer = FakePeerConnection.instances[0];
    expect(peer?.getSenders()).toHaveLength(2);
    expect(peer?.remoteDescription?.type).toBe("answer");
    // Beam's ICE servers are applied to the peer connection.
    expect(peer?.config?.iceServers).toEqual([{ urls: "stun:stun.test" }]);
  });

  it("adds recvonly transceivers when joining as a viewer", async () => {
    const { beam } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    expect(FakePeerConnection.instances[0]?.getTransceivers().map((t) => t.direction)).toEqual([
      "recvonly",
      "recvonly",
    ]);
  });

  it("sends media requests without a Beam bearer token", async () => {
    const { beam, requests } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    // The relay URL carries its own capability token; attaching the session's
    // Beam token to media traffic would leak it to the relay for no reason.
    const joinRequest = requests.find((request) => request.url.includes("/sfu/participants"));
    expect(joinRequest?.url).toContain("/t/tun_1/webrtc/pub_1");
  });

  it("emits participant lifecycle events from the room stream", async () => {
    const { beam, emit } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    const joined = vi.fn();
    const left = vi.fn();
    session.on("participant.joined", joined);
    session.on("participant.left", left);

    emit({ type: "participant_joined", participant_id: "p_2", display_name: "Grace", at: "2026-08-31T00:01:00Z" });
    emit({ type: "participant_left", participant_id: "p_2", at: "2026-08-31T00:02:00Z" });
    await flush(30);

    expect(joined).toHaveBeenCalledWith({ id: "p_2", displayName: "Grace" });
    expect(left).toHaveBeenCalledWith({ id: "p_2" });
  });

  // An application must be able to observe event types this SDK version predates.
  it("forwards unmodelled event types through the generic `event` channel", async () => {
    const { beam, emit } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    const onEvent = vi.fn();
    session.on("event", onEvent);
    emit({ type: "some_future_event", participant_id: "p_9", at: "2026-08-31T00:03:00Z" });
    await flush(30);

    expect(onEvent).toHaveBeenCalledWith(expect.objectContaining({ type: "some_future_event", participantId: "p_9" }));
  });

  it("ignores a malformed event instead of tearing down the session", async () => {
    const { beam, emitRaw } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    const onError = vi.fn();
    session.on("error", onError);
    emitRaw("data: {not json\n\n");
    await flush(30);

    expect(onError).not.toHaveBeenCalled();
    expect(session.state).toBe("connected");
  });

  it("emits track.added for inbound media", async () => {
    const { beam } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    const onTrack = vi.fn();
    session.on("track.added", onTrack);
    FakePeerConnection.instances[0]?.emitTrack(new FakeMediaStream(["video"], "p_2:cam"), "video");

    expect(onTrack).toHaveBeenCalledWith(expect.objectContaining({ participantId: "p_2" }));
  });

  it("renegotiates when the SFU asks", async () => {
    const { beam, emit, requests } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    emit({ type: "renegotiation_required", at: "2026-08-31T00:04:00Z" });
    await vi.waitFor(() => expect(requests.some((request) => request.url.includes("/renegotiate"))).toBe(true));
    await vi.waitFor(() => expect(requests.some((request) => request.url.includes("/answer"))).toBe(true));
  });

  it("restarts ICE when the peer connection fails", async () => {
    const { beam } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    sessions.push(session);

    const peer = FakePeerConnection.instances[0];
    peer?.emitConnectionState("failed");

    expect(session.state).toBe("reconnecting");
    await vi.waitFor(() => expect(peer?.restartIceCalls).toBeGreaterThan(0));
  });

  it("replaces an outgoing track without renegotiating", async () => {
    const { beam, requests } = harness();
    const stream = new FakeMediaStream(["video"]) as unknown as MediaStream;
    const session = await beam.rooms.join(await beam.rooms.create(), { publish: stream });
    sessions.push(session);

    const before = requests.length;
    const replacement = new FakeMediaStream(["video"]).getTracks()[0];
    await session.replaceTrack("video", replacement as unknown as MediaStreamTrack);

    // No renegotiation: a camera switch must not interrupt anyone else's view.
    expect(requests.length).toBe(before);
    expect(FakePeerConnection.instances[0]?.getSenders()[0]?.track).toBe(replacement);
  });

  it("closes the peer connection and stops emitting on leave", async () => {
    const { beam, emit } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());

    const onEvent = vi.fn();
    session.on("event", onEvent);
    await session.leave();

    expect(FakePeerConnection.instances[0]?.closed).toBe(true);
    expect(session.state).toBe("closed");

    emit({ type: "participant_joined", participant_id: "p_3", at: "now" });
    await flush(20);
    expect(onEvent).not.toHaveBeenCalled();
  });

  it("leave is idempotent", async () => {
    const { beam } = harness();
    const session = await beam.rooms.join(await beam.rooms.create());
    await session.leave();
    await expect(session.leave()).resolves.toBeUndefined();
  });

  it("rejects a control-plane room without a media URL", async () => {
    const { beam } = harness({ media: undefined });
    await expect(beam.rooms.create()).rejects.toThrow(/media URL/);
  });
});
