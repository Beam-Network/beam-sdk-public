import {
  BeamAbortError,
  BeamConfigError,
  BeamError,
  BeamNetworkError,
  BeamTimeoutError,
} from "../core/errors/index.js";
import { EventEmitter } from "../core/events/index.js";
import { requirePeerConnection } from "../rooms/webrtc.js";
import {
  array,
  asError,
  broadcast,
  channel,
  duration,
  idempotencyKey,
  identifier,
  inputId,
  membership,
  object,
  publication,
  text,
} from "./protocol.js";
import { MediaSession, Subscription } from "./resources.js";
import { AgentTransport } from "./transport.js";
import type {
  AgentBroadcast,
  AgentChannel,
  AgentChannelScope,
  AgentMediaSession,
  AgentMembership,
  AgentPublication,
  AgentPublishMediaOptions,
  AgentRequestOptions,
  AgentSession,
  AgentSubscription,
  WebAgentEvents,
  WebAgentOptions,
} from "./types.js";

/** Client for one standalone Web Agent. All browsers share that instance's membership. */
export class WebAgent extends EventEmitter<WebAgentEvents> {
  readonly #transport: AgentTransport;
  readonly #options: WebAgentOptions;
  constructor(options: WebAgentOptions) {
    super();
    this.#options = { ...options, iceServers: options.iceServers?.map((server) => ({ ...server })) };
    duration(options.iceGatheringTimeoutMs, 10000);
    duration(options.mediaConnectionTimeoutMs, 30000);
    this.#transport = new AgentTransport(options);
    this.#transport.onState = (state) => this.emit("state", { state });
    this.#transport.onError = (error) => this.emit("error", error);
    this.#transport.onEvent = (type, data) => {
      if (type === "room.membership") this.emit("membership", membership(data));
      else if (type === "connection.state") this.emit("availability", { connected: object(data).connected === true });
    };
  }
  get connected(): boolean {
    return this.#transport.connected;
  }
  /** Reconnect is explicit. It creates a fresh session without replaying operations. */
  connect(options?: AgentRequestOptions): Promise<AgentSession> {
    return this.#transport.connect(options);
  }
  disconnect(): void {
    this.#transport.disconnect();
  }
  joinRoom(
    roomId: string,
    options: AgentRequestOptions & { invitation?: string; idempotencyKey?: string } = {},
  ): Promise<AgentMembership> {
    return this.#transport.request(
      "room.join",
      {
        roomId: inputId(roomId),
        invitation: options.invitation,
        idempotencyKey: idempotencyKey(options.idempotencyKey),
      },
      membership,
      options,
    );
  }
  /** Leaves for the shared instance, affecting every connected browser. */
  leaveRoom(roomId: string, options: AgentRequestOptions & { idempotencyKey?: string } = {}): Promise<AgentMembership> {
    return this.#transport.request(
      "room.leave",
      { roomId: inputId(roomId), idempotencyKey: idempotencyKey(options.idempotencyKey) },
      membership,
      options,
    );
  }
  listChannels(roomId: string, options?: AgentRequestOptions): Promise<AgentChannel[]> {
    return this.#transport.request(
      "channel.list",
      { roomId: inputId(roomId) },
      (value) => array(value, channel),
      options,
    );
  }
  getChannel(scope: AgentChannelScope, options?: AgentRequestOptions): Promise<AgentChannel> {
    return this.#transport.request("channel.get", this.#scope(scope), channel, options);
  }
  subscribe(
    scope: AgentChannelScope,
    kind: "message" | "media",
    options?: AgentRequestOptions,
  ): Promise<AgentSubscription> {
    if (kind !== "message" && kind !== "media")
      throw new BeamConfigError("This client supports message and media subscriptions.");
    const fields = this.#scope(scope);
    return this.#transport.request(
      "channel.subscribe",
      { ...fields, kind },
      (value) => {
        const data = object(value);
        if (data.kind !== kind) throw new BeamConfigError("The channel kind does not match the subscription.");
        return new Subscription(this.#transport, identifier(data.resourceId), fields, kind);
      },
      options,
      (value) => this.#transport.control("channel.unsubscribe", { resourceId: identifier(object(value).resourceId) }),
    );
  }
  publishMessage(
    scope: AgentChannelScope,
    payload: string | Uint8Array,
    options: AgentRequestOptions & { contentType?: string; idempotencyKey?: string } = {},
  ): Promise<AgentPublication> {
    const bytes = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
    if (!(bytes instanceof Uint8Array) || !bytes.length || bytes.length > 65536)
      throw new BeamConfigError("Messages must contain between 1 and 65536 bytes.");
    let binary = "";
    for (const value of bytes) binary += String.fromCharCode(value);
    return this.#transport.request(
      "message.publish",
      {
        ...this.#scope(scope),
        payload: btoa(binary),
        contentType:
          options.contentType ??
          (typeof payload === "string" ? "text/plain; charset=utf-8" : "application/octet-stream"),
        idempotencyKey: idempotencyKey(options.idempotencyKey),
      },
      publication,
      options,
    );
  }
  listMedia(scope: AgentChannelScope, options?: AgentRequestOptions): Promise<AgentBroadcast[]> {
    return this.#transport.request("media.list", this.#scope(scope), (value) => array(value, broadcast), options);
  }
  publishMedia(
    scope: AgentChannelScope,
    stream: MediaStream,
    options: AgentPublishMediaOptions = {},
  ): Promise<AgentMediaSession> {
    if (
      !stream ||
      typeof stream.getTracks !== "function" ||
      !stream.getTracks().length ||
      stream.getTracks().some((track) => track.readyState === "ended")
    )
      throw new BeamConfigError("Provide a live audio or video stream.");
    return this.#media(this.#scope(scope), stream, options, undefined);
  }
  watchMedia(
    subscription: AgentSubscription,
    broadcastId?: string,
    options: AgentRequestOptions = {},
  ): Promise<AgentMediaSession> {
    if (
      !(subscription instanceof Subscription) ||
      !subscription.ownedBy(this.#transport) ||
      subscription.kind !== "media" ||
      subscription.state !== "ready"
    )
      throw new BeamConfigError("Viewing requires this client's ready media subscription.");
    return this.#media(
      {
        ...subscription.scope,
        resourceId: subscription.id,
        ...(broadcastId ? { workloadId: inputId(broadcastId) } : {}),
      },
      new MediaStream(),
      options,
      subscription,
    );
  }
  #scope(scope: AgentChannelScope): AgentChannelScope & Record<string, unknown> {
    return { roomId: inputId(scope.roomId), channelId: inputId(scope.channelId) };
  }

  async #media(
    fields: AgentChannelScope & Record<string, unknown>,
    stream: MediaStream,
    options: AgentPublishMediaOptions,
    parent?: Subscription,
  ): Promise<AgentMediaSession> {
    if (options.signal?.aborted) throw new BeamAbortError();
    if (!this.connected) throw new BeamNetworkError("Connect to the Web Agent before publishing or viewing.");
    const configuration: RTCConfiguration = { iceServers: this.#options.iceServers ?? [] };
    const peer = this.#options.peerConnectionFactory?.(configuration) ?? new (requirePeerConnection())(configuration);
    const session = new MediaSession(
      this.#transport,
      peer,
      stream,
      !!parent || options.stopTracksOnClose === true,
      this.#options.mediaConnectionTimeoutMs ?? 30000,
      parent,
    );
    const controller = new AbortController();
    const abort = (): void => {
      controller.abort();
      session.fail(new BeamAbortError());
    };
    const offClosed = session.on("closed", () => controller.abort(session.reason));
    const offConnection = this.on("state", ({ state }) => {
      if (state === "disconnected") abort();
    });
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      if (parent) {
        peer.addTransceiver("audio", { direction: "recvonly" });
        peer.addTransceiver("video", { direction: "recvonly" });
      } else for (const track of stream.getTracks()) peer.addTrack(track, stream);
      await abortable(
        peer.setLocalDescription(await abortable(peer.createOffer(), controller.signal)),
        controller.signal,
      );
      await gather(peer, this.#options.iceGatheringTimeoutMs ?? 10000, controller.signal);
      if (controller.signal.aborted) throw new BeamAbortError();
      const sdp = peer.localDescription?.sdp;
      if (!sdp || new TextEncoder().encode(sdp).length > 192 << 10)
        throw new BeamConfigError("The media offer is missing or too large.");
      const answer = await this.#transport.request(
        parent ? "media.view.offer" : "media.publish.offer",
        { ...fields, sdp },
        (value) => {
          const data = object(value);
          session.bind(identifier(data.resourceId));
          return text(data.sdp);
        },
        { ...options, signal: controller.signal },
        (value) => this.#transport.control("media.close", { resourceId: identifier(object(value).resourceId) }),
      );
      if (controller.signal.aborted) throw new BeamAbortError();
      await abortable(peer.setRemoteDescription({ type: "answer", sdp: answer }), controller.signal);
      if (controller.signal.aborted) throw new BeamAbortError();
      return session;
    } catch (error) {
      const failure =
        controller.signal.reason instanceof BeamError
          ? controller.signal.reason
          : error instanceof BeamError
            ? error
            : new BeamError("Media negotiation failed.", { code: "protocol", retryable: false });
      session.fail(asError(failure));
      throw failure;
    } finally {
      offClosed();
      offConnection();
      options.signal?.removeEventListener("abort", abort);
    }
  }
}

// No trickle-ICE command exists on v1. Never silently signal a partial offer.
function gather(peer: RTCPeerConnection, timeout: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(new BeamAbortError());
  if (peer.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const finish = (error?: BeamError): void => {
      clearTimeout(timer);
      peer.removeEventListener("icegatheringstatechange", changed);
      signal.removeEventListener("abort", abort);
      if (error) reject(error);
      else resolve();
    };
    const changed = (): void => {
      if (peer.iceGatheringState === "complete") finish();
    };
    const abort = (): void => finish(new BeamAbortError());
    const timer = setTimeout(() => finish(new BeamTimeoutError("Media ICE gathering timed out.")), timeout);
    peer.addEventListener("icegatheringstatechange", changed);
    signal.addEventListener("abort", abort, { once: true });
    changed();
  });
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = (): void => {
      signal.removeEventListener("abort", abort);
      reject(signal.reason instanceof BeamError ? signal.reason : new BeamAbortError());
    };
    signal.addEventListener("abort", abort, { once: true });
    void promise.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
    if (signal.aborted) {
      signal.removeEventListener("abort", abort);
      abort();
    }
  });
}
