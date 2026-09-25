/**
 * The `beam.broadcast` namespace.
 *
 * One-to-many media over WHIP (publish) and WHEP (watch) — the IETF standard
 * for WebRTC ingest and egress, which is what Beam's relays speak. Compared to
 * `beam.rooms`, broadcast has no participant list and no renegotiation: a
 * publisher pushes one stream, and viewers pull it.
 */

import { EventEmitter } from "../core/events/index.js";
import { BeamApiError, BeamNetworkError } from "../core/errors/index.js";
import type { Logger } from "../core/logger/index.js";
import type { HttpTransport } from "../core/transport/index.js";
import { newIdempotencyKey } from "../core/transport/index.js";
import { toRoom } from "../rooms/rooms.js";
import type { Room } from "../rooms/types.js";
import { requirePeerConnection, waitForIceGathering } from "../rooms/webrtc.js";

export type BroadcastEvents = {
  state: RTCPeerConnectionState;
  /** Emitted once per inbound track. Viewer side only. */
  track: { stream: MediaStream; track: MediaStreamTrack };
  error: Error;
};

export interface PublishOptions {
  /** Room to publish into. Omit with `url` to publish to an endpoint directly. */
  roomId?: string;
  stream: MediaStream;
  /** Explicit WHIP endpoint, bypassing room lookup. */
  url?: string;
  token?: string;
  iceServers?: RTCIceServer[];
  iceGatheringTimeoutMs?: number;
  signal?: AbortSignal;
}

export interface WatchOptions {
  roomId?: string;
  /** Explicit WHEP endpoint, bypassing room lookup. */
  url?: string;
  token?: string;
  iceServers?: RTCIceServer[];
  iceGatheringTimeoutMs?: number;
  signal?: AbortSignal;
}

/** A live publish or watch connection. */
export class BroadcastConnection extends EventEmitter<BroadcastEvents> {
  readonly #peer: RTCPeerConnection;
  readonly #fetch: typeof fetch;
  readonly #logger: Logger;
  /**
   * The WHIP/WHEP resource URL from the response `Location` header. DELETEing it
   * is the standard way to tear a session down; without it the relay holds the
   * slot until its lease expires.
   */
  #resourceUrl: string | undefined;
  #token: string | undefined;
  #closed = false;

  constructor(peer: RTCPeerConnection, deps: { fetch: typeof fetch; logger: Logger; token?: string }) {
    super((error, event) => deps.logger.warn(`a "${event}" listener threw`, error));
    this.#peer = peer;
    this.#fetch = deps.fetch;
    this.#logger = deps.logger;
    this.#token = deps.token;

    peer.onconnectionstatechange = () => {
      this.emit("state", peer.connectionState);
      if (peer.connectionState === "failed") {
        this.emit("error", new BeamNetworkError("The broadcast connection failed."));
      }
    };
  }

  get state(): RTCPeerConnectionState {
    return this.#peer.connectionState;
  }

  /** Underlying peer connection, for stats and advanced control. */
  get peerConnection(): RTCPeerConnection {
    return this.#peer;
  }

  /** @internal */
  setResourceUrl(url: string | undefined): void {
    this.#resourceUrl = url;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;

    const resourceUrl = this.#resourceUrl;
    this.#resourceUrl = undefined;
    this.#peer.close();

    if (resourceUrl) {
      await this.#fetch(resourceUrl, {
        method: "DELETE",
        headers: this.#token !== undefined ? { authorization: `Bearer ${this.#token}` } : {},
      }).catch((error: unknown) => this.#logger.debug("WHIP/WHEP resource cleanup failed", error));
    }
    this.removeAllListeners();
  }
}

/**
 * Performs the WHIP/WHEP SDP exchange.
 *
 * The body is `application/sdp`, not JSON, and the answer comes back as a plain
 * SDP body — so this bypasses `HttpTransport`, which is JSON-shaped throughout.
 */
async function exchangeSdp(
  fetchImpl: typeof fetch,
  endpoint: string,
  peer: RTCPeerConnection,
  options: { token?: string; iceGatheringTimeoutMs?: number; signal?: AbortSignal },
): Promise<string | undefined> {
  const offer = await peer.createOffer();
  await peer.setLocalDescription(offer);
  await waitForIceGathering(peer, options.iceGatheringTimeoutMs ?? 1500, options.signal);

  let response: Response;
  try {
    response = await fetchImpl(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/sdp",
        ...(options.token !== undefined ? { authorization: `Bearer ${options.token}` } : {}),
      },
      body: peer.localDescription?.sdp ?? offer.sdp ?? "",
      ...(options.signal ? { signal: options.signal } : {}),
    });
  } catch (cause) {
    throw new BeamNetworkError(`Could not reach the media endpoint at ${endpoint}.`, cause);
  }

  const body = await response.text();
  if (!response.ok) {
    throw new BeamApiError(
      `Media signalling failed (${response.status}): ${body.slice(0, 200) || response.statusText}`,
      {
        code: response.status >= 500 ? "unavailable" : "invalid_argument",
        status: response.status,
      },
    );
  }

  await peer.setRemoteDescription({ type: "answer", sdp: body });

  // Location may be relative to the endpoint, per RFC 9725.
  const location = response.headers.get("location");
  return location === null ? undefined : new URL(location, endpoint).toString();
}

export class Broadcast {
  readonly #control: HttpTransport;
  readonly #fetch: typeof fetch;
  readonly #logger: Logger;

  constructor(deps: { control: HttpTransport; fetch: typeof fetch; logger: Logger }) {
    this.#control = deps.control;
    this.#fetch = deps.fetch;
    this.#logger = deps.logger.child("broadcast");
  }

  async #resolveRoom(roomId: string, signal?: AbortSignal): Promise<Room> {
    const wire = await this.#control.get<Parameters<typeof toRoom>[0]>(`/rooms/${encodeURIComponent(roomId)}`, {
      ...(signal ? { signal } : {}),
    });
    return toRoom(wire);
  }

  /** Publishes a stream. Requires the `broadcast:publish` scope. */
  async publish(options: PublishOptions): Promise<BroadcastConnection> {
    const { endpoint, token } = await this.#resolveEndpoint(options, "whip");

    const PeerConnection = requirePeerConnection();
    const peer = new PeerConnection({ iceServers: options.iceServers ?? [] });
    for (const track of options.stream.getTracks()) peer.addTrack(track, options.stream);

    const connection = new BroadcastConnection(peer, {
      fetch: this.#fetch,
      logger: this.#logger,
      ...(token !== undefined ? { token } : {}),
    });

    try {
      const resourceUrl = await exchangeSdp(this.#fetch, endpoint, peer, {
        ...(token !== undefined ? { token } : {}),
        ...(options.iceGatheringTimeoutMs !== undefined
          ? { iceGatheringTimeoutMs: options.iceGatheringTimeoutMs }
          : {}),
        ...(options.signal ? { signal: options.signal } : {}),
      });
      connection.setResourceUrl(resourceUrl);
    } catch (error) {
      await connection.close();
      throw error;
    }
    this.#logger.info("broadcast publishing", { roomId: options.roomId });
    return connection;
  }

  /** Subscribes to a broadcast. Requires the `broadcast:watch` scope. */
  async watch(options: WatchOptions): Promise<BroadcastConnection> {
    const { endpoint, token } = await this.#resolveEndpoint(options, "whep");

    const PeerConnection = requirePeerConnection();
    const peer = new PeerConnection({ iceServers: options.iceServers ?? [] });
    peer.addTransceiver("video", { direction: "recvonly" });
    peer.addTransceiver("audio", { direction: "recvonly" });

    const connection = new BroadcastConnection(peer, {
      fetch: this.#fetch,
      logger: this.#logger,
      ...(token !== undefined ? { token } : {}),
    });

    // Registered before signalling: the first track can arrive as soon as the
    // remote description is applied.
    peer.ontrack = (event) => {
      connection.emit("track", {
        stream: event.streams[0] ?? new MediaStream([event.track]),
        track: event.track,
      });
    };

    try {
      const resourceUrl = await exchangeSdp(this.#fetch, endpoint, peer, {
        ...(token !== undefined ? { token } : {}),
        ...(options.iceGatheringTimeoutMs !== undefined
          ? { iceGatheringTimeoutMs: options.iceGatheringTimeoutMs }
          : {}),
        ...(options.signal ? { signal: options.signal } : {}),
      });
      connection.setResourceUrl(resourceUrl);
    } catch (error) {
      await connection.close();
      throw error;
    }
    return connection;
  }

  async #resolveEndpoint(
    options: { roomId?: string; url?: string; token?: string; signal?: AbortSignal },
    kind: "whip" | "whep",
  ): Promise<{ endpoint: string; token: string | undefined }> {
    if (options.url !== undefined) {
      return { endpoint: options.url, token: options.token };
    }
    if (options.roomId === undefined) {
      throw new BeamApiError("Provide either `roomId` or an explicit `url` to broadcast.", {
        code: "invalid_argument",
        retryable: false,
      });
    }

    const room = await this.#resolveRoom(options.roomId, options.signal);
    const explicit = kind === "whip" ? room.media.whipUrl : room.media.whepUrl;
    return {
      endpoint: explicit ?? `${room.media.url}/${kind}`,
      token: options.token ?? room.media.token,
    };
  }

  /**
   * Creates a room configured for broadcast and returns it.
   *
   * A convenience over `rooms.create` for the publish-and-share case, where the
   * caller wants a URL to hand out rather than a session to join.
   */
  async createChannel(options: { name?: string; ttlSeconds?: number; signal?: AbortSignal } = {}): Promise<Room> {
    const wire = await this.#control.post<Parameters<typeof toRoom>[0]>(
      "/rooms",
      {
        ...(options.name !== undefined ? { name: options.name } : {}),
        ...(options.ttlSeconds !== undefined ? { ttl_seconds: options.ttlSeconds } : {}),
        mode: "broadcast",
      },
      {
        idempotencyKey: newIdempotencyKey("broadcast-create"),
        ...(options.signal ? { signal: options.signal } : {}),
      },
    );
    return toRoom(wire);
  }
}
