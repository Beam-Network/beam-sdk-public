import { afterEach, describe, expect, it, vi } from "vitest";
import { WebAgent } from "../src/web-agent/index.js";
import { BeamAbortError, BeamConfigError } from "../src/core/errors/index.js";

class Socket {
  static all: Socket[] = [];
  readyState = 0;
  bufferedAmount = 0;
  binaryType = "";
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  sent: Record<string, any>[] = [];
  constructor(
    readonly url: string,
    readonly protocol: string,
  ) {
    Socket.all.push(this);
  }
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    this.readyState = 3;
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  receive(data: unknown) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }
  reply(data: unknown, request = this.sent.at(-1)!, error?: unknown) {
    this.receive({ type: "response", requestId: request.requestId, data, error });
  }
}
const clients: WebAgent[] = [];
const scope = { roomId: "room", channelId: "channel" };
const credentials = "local-instance-credential";
function client(options: Record<string, unknown> = {}) {
  const result = new WebAgent({
    url: "ws://127.0.0.1/v1/connect",
    getCredential: () => credentials,
    webSocketFactory: (url, protocol) => new Socket(url, protocol) as unknown as WebSocket,
    ...options,
  });
  clients.push(result);
  return result;
}
async function connected(agent = client(), status: Record<string, unknown> = {}) {
  const connection = agent.connect();
  await Promise.resolve();
  await Promise.resolve();
  const socket = Socket.all.at(-1)!;
  socket.open();
  socket.receive({
    type: "session.ready",
    requestId: "auth",
    data: {
      sessionId: "session",
      protocol: "beam.web-agent.v1",
      status: { connected: true, private: "never expose", ...status },
    },
  });
  await connection;
  return { agent, socket };
}
async function subscribed(agent: WebAgent, socket: Socket, kind: "message" | "media" = "message") {
  const result = agent.subscribe(scope, kind);
  const id = `resource-${socket.sent.length}`;
  socket.reply({ resourceId: id, kind, state: "preparing" });
  socket.receive({ type: "subscription.ready", resourceId: id, data: { kind } });
  return { subscription: await result, id };
}
afterEach(() => {
  for (const agent of clients.splice(0)) agent.disconnect();
  Socket.all = [];
  vi.useRealTimers();
});

describe("standalone Web Agent client", () => {
  it("authenticates in the first frame without putting credentials in the URL", async () => {
    const { agent, socket } = await connected();
    expect(socket.url).not.toContain(credentials);
    expect(socket.protocol).toBe("beam.web-agent.v1");
    expect(socket.sent).toEqual([{ type: "session.authenticate", requestId: "auth", token: credentials }]);
    expect(await agent.connect()).toEqual({ id: "session", connected: true });
  });
  it("exposes enrolled identity and process boot while omitting other status fields", async () => {
    const { agent } = await connected(client({ expectedAgentId: "agent-1" }), {
      agentId: "agent-1",
      bootId: "boot-1",
    });
    expect(await agent.connect()).toEqual({ id: "session", connected: true, agentId: "agent-1", bootId: "boot-1" });
  });
  it("rejects missing or changed pinned identity before accepting a session", async () => {
    await expect(connected(client({ expectedAgentId: "agent-1" }))).rejects.toMatchObject({ code: "auth_failed" });
    await expect(connected(client({ expectedAgentId: "agent-1" }), { agentId: "agent-1" })).rejects.toMatchObject({
      code: "auth_failed",
    });
    await expect(
      connected(client({ expectedAgentId: "agent-1" }), { agentId: "agent-2", bootId: "boot-1" }),
    ).rejects.toMatchObject({
      code: "auth_failed",
    });
    expect(Socket.all.every((socket) => socket.readyState === 3)).toBe(true);
  });
  it("closes a pinned session if a later state event changes its identity", async () => {
    const { agent, socket } = await connected(client({ expectedAgentId: "agent-1" }), {
      agentId: "agent-1",
      bootId: "boot-1",
    });
    const errors = vi.fn();
    agent.on("error", errors);
    socket.receive({ type: "connection.state", data: { connected: true, agentId: "agent-2", bootId: "boot-1" } });
    expect(agent.connected).toBe(false);
    expect(errors).toHaveBeenCalledWith(expect.objectContaining({ code: "auth_failed" }));
  });
  it("closes a pinned session if a later state event changes its process boot", async () => {
    const { agent, socket } = await connected(client({ expectedAgentId: "agent-1" }), {
      agentId: "agent-1",
      bootId: "boot-1",
    });
    socket.receive({ type: "connection.state", data: { connected: true, agentId: "agent-1", bootId: "boot-2" } });
    expect(agent.connected).toBe(false);
  });
  it.each(["bm_live_secret", "bm_test_secret", "b1m_secret", "bag_secret", "btrstd1.secret"])(
    "rejects privileged credentials before opening a socket: %s",
    async (token) => {
      const agent = client({ getCredential: () => token });
      await expect(agent.connect()).rejects.toBeInstanceOf(BeamConfigError);
      expect(Socket.all).toHaveLength(0);
    },
  );
  it.each([
    "ws://remote.example/v1/connect",
    "wss://user:pass@example.com/v1/connect",
    "wss://example.com/v1/connect?token=secret",
  ])("rejects unsafe endpoint %s", (url) => {
    expect(() => client({ url })).toThrow(BeamConfigError);
  });
  it("cancels a hanging credential callback and ignores its late result", async () => {
    let give!: (value: string) => void;
    const agent = client({
      getCredential: () =>
        new Promise<string>((resolve) => {
          give = resolve;
        }),
    });
    const abort = new AbortController();
    const connection = agent.connect({ signal: abort.signal });
    await Promise.resolve();
    abort.abort();
    await expect(connection).rejects.toBeInstanceOf(BeamAbortError);
    give(credentials);
    await Promise.resolve();
    await Promise.resolve();
    expect(Socket.all).toHaveLength(0);
  });
  it("correlates out-of-order responses and strips private publication fields", async () => {
    const { agent, socket } = await connected();
    const first = agent.listChannels("room");
    const request = socket.sent.at(-1)!;
    const result = agent.publishMessage(scope, "héllo", { idempotencyKey: "saved-key" });
    expect(socket.sent.at(-1)?.idempotencyKey).toBe("saved-key");
    expect(new TextDecoder().decode(Uint8Array.from(atob(socket.sent.at(-1)?.payload), (c) => c.charCodeAt(0)))).toBe(
      "héllo",
    );
    socket.reply({
      publication_id: "p",
      publisher_sequence: 1,
      online_deliveries: 3,
      accepted_deliveries: 2,
      delivered_deliveries: 1,
      failed_deliveries: 1,
      expired_deliveries: 0,
      skipped_online_only: 0,
      relay_id: "secret",
      attempt_id: "secret",
      receipts: [{ token: "secret" }],
    });
    socket.reply(null, request);
    expect(await first).toEqual([]);
    expect(await result).toEqual({
      id: "p",
      sequence: 1,
      online: 3,
      accepted: 2,
      delivered: 1,
      failed: 1,
      expired: 0,
      skipped: 0,
      duplicate: false,
      queued: false,
    });
  });
  it("owns resources before immediate readiness events and closes only the selected subscription", async () => {
    const { agent, socket } = await connected();
    const first = await subscribed(agent, socket);
    const second = await subscribed(agent, socket);
    await expect(first.subscription.ready).resolves.toBeUndefined();
    expect(first.subscription).not.toHaveProperty("owner");
    const delivered = vi.fn();
    second.subscription.on("message", delivered);
    const closing = first.subscription.close();
    socket.reply({ state: "closed" });
    await closing;
    socket.receive({
      type: "message.delivery",
      resourceId: second.id,
      data: {
        publicationId: "p",
        publisherMemberId: "m",
        sequence: 1,
        payload: btoa("hello"),
        contentType: "text/plain",
        receivedAt: "now",
        token: "private",
      },
    });
    expect(delivered.mock.calls[0]?.[0].payload).toEqual(new TextEncoder().encode("hello"));
    expect(delivered.mock.calls[0]?.[0]).not.toHaveProperty("token");
    expect(first.subscription.state).toBe("closed");
    expect(second.subscription.state).toBe("ready");
  });
  it("cancels one request and releases a late subscription without disconnecting another", async () => {
    const { agent, socket } = await connected();
    const { subscription } = await subscribed(agent, socket);
    const abort = new AbortController();
    const pending = agent.subscribe(scope, "message", { signal: abort.signal });
    const original = socket.sent.at(-1)!;
    abort.abort();
    await expect(pending).rejects.toBeInstanceOf(BeamAbortError);
    expect(socket.sent.at(-1)?.type).toBe("request.cancel");
    socket.reply({ resourceId: "late", kind: "message" }, original);
    expect(socket.sent.at(-1)).toMatchObject({ type: "channel.unsubscribe", resourceId: "late" });
    expect(agent.connected).toBe(true);
    expect(subscription.state).toBe("ready");
  });
  it("closes the session if cancellation cannot be confirmed", async () => {
    vi.useFakeTimers();
    const { agent } = await connected();
    const pending = agent.listChannels("room", { timeoutMs: 10 });
    const rejection = expect(pending).rejects.toMatchObject({ code: "timeout" });
    await vi.advanceTimersByTimeAsync(10);
    await rejection;
    await vi.advanceTimersByTimeAsync(30000);
    expect(agent.connected).toBe(false);
  });
  it("disconnects resources and reconnects without replaying publications or subscriptions", async () => {
    const { agent, socket } = await connected();
    const { subscription } = await subscribed(agent, socket);
    const pending = agent.listChannels("room");
    socket.onclose?.({ code: 1006 });
    await expect(pending).rejects.toMatchObject({ code: "network" });
    expect(subscription.state).toBe("closed");
    const next = await connected(agent);
    expect(next.socket.sent).toHaveLength(1);
    expect(subscription.state).toBe("closed");
  });
  it("redacts server error text and rejects malformed frames", async () => {
    const { agent, socket } = await connected();
    const pending = agent.listChannels("room");
    socket.reply(null, socket.sent.at(-1), { code: "forbidden", message: "secret worker token", retryable: false });
    await expect(pending).rejects.toMatchObject({
      code: "permission_denied",
      message: "The Web Agent could not complete the operation.",
    });
    const errors = vi.fn();
    agent.on("error", errors);
    socket.onmessage?.({ data: "invalid secret" });
    expect(agent.connected).toBe(false);
    expect(JSON.stringify(errors.mock.calls)).not.toContain("secret");
  });
  it("bounds pending requests and outbound buffers", async () => {
    const { agent, socket } = await connected();
    const pending = Array.from({ length: 28 }, () => agent.listChannels("room").catch(() => {}));
    await expect(agent.listChannels("room")).rejects.toMatchObject({ code: "resource_exhausted" });
    agent.disconnect();
    await Promise.all(pending);
    const next = await connected(agent);
    next.socket.bufferedAmount = 1 << 20;
    await expect(agent.listChannels("room")).rejects.toMatchObject({ code: "resource_exhausted" });
    expect(socket.readyState).toBe(3);
  });
});

class Track {
  readyState = "live";
  stopped = false;
  constructor(readonly kind: string) {}
  stop() {
    this.stopped = true;
    this.readyState = "ended";
  }
}
class Stream {
  constructor(readonly tracks: Track[] = []) {}
  getTracks() {
    return this.tracks;
  }
  addTrack(track: Track) {
    this.tracks.push(track);
  }
}
class Peer extends EventTarget {
  static all: Peer[] = [];
  connectionState: RTCPeerConnectionState = "new";
  iceGatheringState: RTCIceGatheringState = "complete";
  localDescription: RTCSessionDescriptionInit | null = null;
  ontrack: ((event: { track: Track }) => void) | null = null;
  onconnectionstatechange: (() => void) | null = null;
  tracks: Track[] = [];
  transceivers: string[] = [];
  failAnswer = false;
  constructor() {
    super();
    Peer.all.push(this);
  }
  addTrack(track: Track) {
    this.tracks.push(track);
  }
  addTransceiver(kind: string) {
    this.transceivers.push(kind);
  }
  async createOffer() {
    return { type: "offer", sdp: "valid local fixture offer" };
  }
  async setLocalDescription(value: RTCSessionDescriptionInit) {
    this.localDescription = value;
  }
  async setRemoteDescription() {
    if (this.failAnswer) throw new Error("sensitive remote SDP");
    if (this.transceivers.length) for (const kind of this.transceivers) this.ontrack?.({ track: new Track(kind) });
  }
  connected() {
    this.connectionState = "connected";
    this.onconnectionstatechange?.();
  }
  close() {
    this.connectionState = "closed";
  }
}
async function tick() {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}
function mediaClient(options: Record<string, unknown> = {}) {
  return client({ peerConnectionFactory: () => new Peer() as unknown as RTCPeerConnection, ...options });
}
function capture() {
  return new Stream([new Track("video"), new Track("audio")]);
}
afterEach(() => {
  Peer.all = [];
  vi.unstubAllGlobals();
});

describe("Web Agent media ownership", () => {
  it("waits for actual connectivity and preserves caller-owned capture on close", async () => {
    const { agent, socket } = await connected(mediaClient());
    const source = capture();
    const pending = agent.publishMedia(scope, source as unknown as MediaStream);
    await tick();
    expect(socket.sent.at(-1)?.type).toBe("media.publish.offer");
    socket.reply({ resourceId: "media", sdp: "answer" });
    const session = await pending;
    let ready = false;
    void session.ready.then(() => {
      ready = true;
    });
    await tick();
    expect(ready).toBe(false);
    Peer.all.at(-1)?.connected();
    await session.ready;
    expect(ready).toBe(true);
    const close = session.close();
    socket.reply({ state: "closed" });
    await close;
    expect(session.state).toBe("closed");
    expect(source.tracks.every((track) => !track.stopped)).toBe(true);
    await session.close();
  });
  it("closes a remote allocation when applying its answer fails", async () => {
    const { agent, socket } = await connected(mediaClient());
    const source = capture();
    const pending = agent.publishMedia(scope, source as unknown as MediaStream, { stopTracksOnClose: true });
    await tick();
    Peer.all.at(-1)!.failAnswer = true;
    socket.reply({ resourceId: "failed-media", sdp: "sensitive" });
    await expect(pending).rejects.toMatchObject({ code: "protocol", message: "Media negotiation failed." });
    expect(socket.sent.at(-1)).toMatchObject({ type: "media.close", resourceId: "failed-media" });
    expect(source.tracks.every((track) => track.stopped)).toBe(true);
  });
  it("closes a cancelled media allocation if its successful response arrives late", async () => {
    const { agent, socket } = await connected(mediaClient());
    const abort = new AbortController();
    const pending = agent.publishMedia(scope, capture() as unknown as MediaStream, { signal: abort.signal });
    await tick();
    const original = socket.sent.at(-1)!;
    abort.abort();
    await expect(pending).rejects.toMatchObject({ code: "aborted" });
    socket.reply({ resourceId: "late-media", sdp: "answer" }, original);
    expect(socket.sent.at(-1)).toMatchObject({ type: "media.close", resourceId: "late-media" });
    expect(Peer.all.at(-1)?.connectionState).toBe("closed");
  });
  it("rejects partial ICE offers and releases the local peer on timeout", async () => {
    vi.useFakeTimers();
    const { agent, socket } = await connected(
      mediaClient({
        iceGatheringTimeoutMs: 10,
        peerConnectionFactory: () => {
          const peer = new Peer();
          peer.iceGatheringState = "gathering";
          return peer as unknown as RTCPeerConnection;
        },
      }),
    );
    const pending = agent.publishMedia(scope, capture() as unknown as MediaStream);
    const failure = expect(pending).rejects.toMatchObject({ code: "timeout" });
    await vi.advanceTimersByTimeAsync(10);
    await failure;
    expect(socket.sent).toHaveLength(1);
    expect(Peer.all.at(-1)?.connectionState).toBe("closed");
  });
  it("ends pending negotiation when disconnected during ICE gathering", async () => {
    const { agent } = await connected(
      mediaClient({
        peerConnectionFactory: () => {
          const peer = new Peer();
          peer.iceGatheringState = "gathering";
          return peer as unknown as RTCPeerConnection;
        },
      }),
    );
    const pending = agent.publishMedia(scope, capture() as unknown as MediaStream);
    await tick();
    agent.disconnect();
    await expect(pending).rejects.toMatchObject({ code: "aborted" });
    expect(Peer.all.at(-1)?.connectionState).toBe("closed");
  });
  it("allows two viewers and closes dependent peers when their subscription closes", async () => {
    vi.stubGlobal("MediaStream", Stream);
    const { agent, socket } = await connected(mediaClient());
    const { subscription } = await subscribed(agent, socket, "media");
    const open = async () => {
      const result = agent.watchMedia(subscription, "broadcast");
      await tick();
      const id = `viewer-${socket.sent.length}`;
      socket.reply({ resourceId: id, sdp: "answer" });
      const session = await result;
      Peer.all.at(-1)?.connected();
      return session;
    };
    const first = await open();
    const second = await open();
    expect(first.stream.getTracks()).toHaveLength(2);
    const closing = first.close();
    socket.reply({ state: "closed" });
    await closing;
    expect(second.state).toBe("connected");
    const unsub = subscription.close();
    socket.reply({ state: "closed" });
    await unsub;
    expect(second.state).toBe("closed");
    expect(second.stream.getTracks().every((track) => track.readyState === "ended")).toBe(true);
    expect(() => agent.watchMedia(subscription)).toThrow(BeamConfigError);
  });
  it("rejects a media subscription owned by another client", async () => {
    const first = await connected(mediaClient());
    const { subscription } = await subscribed(first.agent, first.socket, "media");
    const second = await connected(mediaClient());
    expect(() => second.agent.watchMedia(subscription)).toThrow(BeamConfigError);
  });
});

it("bounds a native SDP operation that never settles", async () => {
  vi.useFakeTimers();
  const { agent, socket } = await connected(
    mediaClient({
      mediaConnectionTimeoutMs: 10,
      peerConnectionFactory: () => {
        const peer = new Peer();
        peer.createOffer = () => new Promise(() => {});
        return peer as unknown as RTCPeerConnection;
      },
    }),
  );
  const pending = agent.publishMedia(scope, capture() as unknown as MediaStream);
  const failure = expect(pending).rejects.toMatchObject({ code: "timeout" });
  await vi.advanceTimersByTimeAsync(10);
  await failure;
  expect(Peer.all.at(-1)?.connectionState).toBe("closed");
  expect(socket.sent).toHaveLength(1);
});

it("can disconnect immediately from the connecting state event", async () => {
  const agent = client();
  agent.on("state", ({ state }) => {
    if (state === "connecting") agent.disconnect();
  });
  await expect(agent.connect()).rejects.toMatchObject({ code: "aborted" });
  await tick();
  expect(Socket.all).toHaveLength(0);
});
