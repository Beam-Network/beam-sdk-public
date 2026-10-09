/**
 * Test doubles.
 *
 * Hand-written rather than mocked: these stand in for `fetch`,
 * `RTCPeerConnection`, and `MediaStream`, and the assertions in the suite depend
 * on their behaviour, so they need to be explicit and readable.
 */

import type { BeamTokenClaims } from "../src/core/auth/types.js";

/** Builds a `bwt1.` token. The signature is filler — nothing verifies it client-side. */
export function makeToken(claims: Partial<BeamTokenClaims> = {}): string {
  const payload: BeamTokenClaims = {
    v: 1,
    iss: "test-broker",
    aud: "beam-web-sdk",
    scopes: ["rooms:create"],
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 120,
    jti: "test-jti",
    ...claims,
  };
  const encoded = btoa(JSON.stringify(payload)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `bwt1.${encoded}.c2ln`;
}

export interface RecordedRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
}

export interface StubResponse {
  status?: number;
  body?: unknown;
  headers?: Record<string, string>;
  /** Returned instead of a response, to simulate a transport-level failure. */
  throws?: Error;
}

/**
 * A `fetch` stub driven by a URL-substring → response map.
 *
 * A handler may be an array, which is consumed one entry per call — that is how
 * the suite expresses "fail once, then succeed" without ad-hoc counters.
 */
export function stubFetch(routes: Record<string, StubResponse | StubResponse[]>): {
  fetch: typeof fetch;
  requests: RecordedRequest[];
} {
  const requests: RecordedRequest[] = [];
  const queues = new Map<string, StubResponse[]>();
  for (const [key, value] of Object.entries(routes)) {
    queues.set(key, Array.isArray(value) ? [...value] : [value]);
  }

  const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((value, key) => {
      headers[key.toLowerCase()] = value;
    });
    requests.push({
      url,
      method: init?.method ?? "GET",
      headers,
      body: typeof init?.body === "string" ? init.body : undefined,
    });

    if (init?.signal?.aborted) throw new DOMException("Aborted", "AbortError");

    const match = [...queues.keys()].sort((a, b) => b.length - a.length).find((pattern) => url.includes(pattern));
    if (match === undefined) {
      return new Response(JSON.stringify({ error: `no stub for ${url}` }), { status: 404 });
    }

    const queue = queues.get(match) as StubResponse[];
    // The last entry repeats, so a single-entry route answers every call.
    const stub = (queue.length > 1 ? queue.shift() : queue[0]) as StubResponse;
    if (stub.throws) throw stub.throws;

    const status = stub.status ?? 200;
    const isText = typeof stub.body === "string";
    return new Response(status === 204 ? null : isText ? (stub.body as string) : JSON.stringify(stub.body ?? {}), {
      status,
      headers: { "content-type": isText ? "text/plain" : "application/json", ...stub.headers },
    });
  };

  return { fetch: fetchImpl as unknown as typeof fetch, requests };
}

/** Builds a streaming `Response` that emits `chunks` as an SSE body. */
export function sseResponse(chunks: string[], options: { closeAfter?: boolean } = {}): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      if (options.closeAfter !== false) controller.close();
    },
  });
  return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
}

class FakeMediaStreamTrack extends EventTarget {
  readonly id: string;
  readonly kind: string;
  enabled = true;
  readyState: "live" | "ended" = "live";
  label = "fake";

  constructor(kind: string, id = `${kind}-${Math.random().toString(36).slice(2, 8)}`) {
    super();
    this.kind = kind;
    this.id = id;
  }

  stop(): void {
    this.readyState = "ended";
    this.dispatchEvent(new Event("ended"));
  }
}

export class FakeMediaStream {
  readonly id: string;
  readonly #tracks: FakeMediaStreamTrack[];

  constructor(kinds: string[] = ["audio", "video"], id = "stream-1") {
    this.id = id;
    this.#tracks = kinds.map((kind) => new FakeMediaStreamTrack(kind));
  }

  getTracks(): FakeMediaStreamTrack[] {
    return [...this.#tracks];
  }
  getAudioTracks(): FakeMediaStreamTrack[] {
    return this.#tracks.filter((track) => track.kind === "audio");
  }
  getVideoTracks(): FakeMediaStreamTrack[] {
    return this.#tracks.filter((track) => track.kind === "video");
  }
}

interface FakeSender {
  track: FakeMediaStreamTrack | null;
  replaceTrack: (track: unknown) => Promise<void>;
}

/**
 * A minimal `RTCPeerConnection`.
 *
 * Models only what the SDK touches, plus the state transitions the tests drive
 * explicitly. Constructed instances are recorded so a test can reach the peer
 * the SDK created without exposing it on the public API.
 */
export class FakePeerConnection extends EventTarget {
  static instances: FakePeerConnection[] = [];

  static reset(): void {
    FakePeerConnection.instances = [];
  }

  readonly config: RTCConfiguration | undefined;
  iceGatheringState: RTCIceGatheringState = "complete";
  signalingState: RTCSignalingState = "stable";
  connectionState: RTCPeerConnectionState = "new";
  localDescription: { type: string; sdp: string } | null = null;
  remoteDescription: { type: string; sdp: string } | null = null;
  closed = false;
  restartIceCalls = 0;

  ontrack: ((event: unknown) => void) | null = null;
  onicecandidate: ((event: unknown) => void) | null = null;
  onconnectionstatechange: (() => void) | null = null;

  readonly #senders: FakeSender[] = [];
  readonly #transceivers: Array<{ direction: string; receiver: { track: { kind: string } | null } }> = [];

  constructor(config?: RTCConfiguration) {
    super();
    this.config = config;
    FakePeerConnection.instances.push(this);
  }

  addTrack(track: FakeMediaStreamTrack): FakeSender {
    const sender: FakeSender = {
      track,
      replaceTrack: async (next: unknown) => {
        sender.track = next as FakeMediaStreamTrack | null;
      },
    };
    this.#senders.push(sender);
    return sender;
  }

  getSenders(): FakeSender[] {
    return [...this.#senders];
  }

  getTransceivers(): Array<{ direction: string; receiver: { track: { kind: string } | null } }> {
    return [...this.#transceivers];
  }

  addTransceiver(
    kind: string,
    init?: { direction?: string },
  ): { direction: string; receiver: { track: { kind: string } } } {
    // Browsers give a new transceiver a receiver whose track already carries the
    // requested kind. The fake must match, or code that reads `receiver.track.kind`
    // to detect existing transceivers looks broken here and works in production.
    const transceiver = { direction: init?.direction ?? "sendrecv", receiver: { track: { kind } } };
    this.#transceivers.push(transceiver);
    return transceiver;
  }

  async createOffer(): Promise<{ type: string; sdp: string }> {
    return { type: "offer", sdp: "v=0\r\no=- fake offer\r\n" };
  }

  async createAnswer(): Promise<{ type: string; sdp: string }> {
    return { type: "answer", sdp: "v=0\r\no=- fake answer\r\n" };
  }

  async setLocalDescription(description: { type: string; sdp: string }): Promise<void> {
    this.localDescription = description;
    this.signalingState = description.type === "offer" ? "have-local-offer" : "stable";
  }

  async setRemoteDescription(description: { type: string; sdp: string }): Promise<void> {
    this.remoteDescription = description;
    this.signalingState = description.type === "answer" ? "stable" : "have-remote-offer";
  }

  restartIce(): void {
    this.restartIceCalls += 1;
  }

  close(): void {
    this.closed = true;
    this.connectionState = "closed";
  }

  /** Drives a connection-state change the way the browser would. */
  emitConnectionState(state: RTCPeerConnectionState): void {
    this.connectionState = state;
    this.onconnectionstatechange?.();
  }

  /** Delivers an inbound track the way the SFU would. */
  emitTrack(stream: FakeMediaStream, kind = "video"): void {
    const track = new FakeMediaStreamTrack(kind);
    this.ontrack?.({ streams: [stream], track, transceiver: {} });
  }
}

/** Installs the WebRTC and media fakes on `globalThis`; returns an uninstaller. */
export function installWebrtcFakes(): () => void {
  const original = {
    RTCPeerConnection: globalThis.RTCPeerConnection,
    MediaStream: globalThis.MediaStream,
  };
  FakePeerConnection.reset();
  (globalThis as Record<string, unknown>)["RTCPeerConnection"] = FakePeerConnection;
  (globalThis as Record<string, unknown>)["MediaStream"] = FakeMediaStream;

  return () => {
    (globalThis as Record<string, unknown>)["RTCPeerConnection"] = original.RTCPeerConnection;
    (globalThis as Record<string, unknown>)["MediaStream"] = original.MediaStream;
    FakePeerConnection.reset();
  };
}

/** Yields to the microtask queue and any pending timers scheduled for now. */
export function flush(ms = 0): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
