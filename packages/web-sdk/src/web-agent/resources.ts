import { BeamAbortError, BeamNetworkError, BeamTimeoutError } from "../core/errors/index.js";
import type { BeamError } from "../core/errors/index.js";
import { EventEmitter } from "../core/events/index.js";
import { array, broadcast, message, object, remoteError } from "./protocol.js";
import type { AgentTransport, WireResource } from "./transport.js";
import type {
  AgentBroadcast,
  AgentChannelScope,
  AgentMediaEvents,
  AgentMediaSession,
  AgentSubscription,
  AgentSubscriptionEvents,
} from "./types.js";

export class Subscription extends EventEmitter<AgentSubscriptionEvents> implements AgentSubscription, WireResource {
  readonly scope: Readonly<AgentChannelScope>;
  readonly kind: "message" | "media";
  readonly ready: Promise<void>;
  readonly children = new Set<MediaSession>();
  readonly id: string;
  readonly #owner: AgentTransport;
  #state: "preparing" | "ready" | "closed" = "preparing";
  #broadcasts: AgentBroadcast[] = [];
  #resolve!: () => void;
  #reject!: (error: BeamError) => void;
  #timer: ReturnType<typeof setTimeout>;
  #closed?: Promise<void>;
  constructor(owner: AgentTransport, id: string, scope: AgentChannelScope, kind: "message" | "media") {
    super();
    this.#owner = owner;
    this.id = id;
    this.scope = Object.freeze({ ...scope });
    this.kind = kind;
    this.ready = new Promise<void>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    void this.ready.catch(() => {});
    this.#timer = setTimeout(() => {
      this.disconnected(new BeamTimeoutError("Channel subscription did not become ready."));
      owner.control("channel.unsubscribe", { resourceId: id });
    }, 35000);
    owner.resources.set(id, this);
  }
  ownedBy(owner: AgentTransport): boolean {
    return this.#owner === owner;
  }
  get state(): "preparing" | "ready" | "closed" {
    return this.#state;
  }
  get broadcasts(): readonly AgentBroadcast[] {
    return this.#broadcasts.map((item) => ({ ...item }));
  }
  event(type: string, data: unknown, error: unknown): void {
    if (this.#state === "closed") return;
    if (type === "resource.error") {
      this.disconnected(remoteError(error));
      return;
    }
    if (type === "subscription.ready") {
      if (this.#state === "preparing") {
        clearTimeout(this.#timer);
        this.#state = "ready";
        this.#resolve();
        this.emit("ready", undefined);
      }
    } else if (type === "message.delivery" && this.kind === "message") {
      this.emit("message", message(data));
    } else if (type === "media.sessions" && this.kind === "media") {
      this.#broadcasts = array(data, broadcast);
      this.emit("broadcasts", this.broadcasts);
    }
  }
  disconnected(reason: BeamError): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    clearTimeout(this.#timer);
    this.#reject(reason);
    this.#owner.resources.delete(this.id);
    for (const child of [...this.children]) child.disconnected(reason);
    this.children.clear();
    if (!(reason instanceof BeamAbortError)) this.emit("error", reason);
    this.emit("closed", undefined);
    this.removeAllListeners();
  }
  close(): Promise<void> {
    if (this.#closed) return this.#closed;
    if (this.#state === "closed") return Promise.resolve();
    this.disconnected(new BeamAbortError("Channel subscription closed."));
    this.#closed = this.#owner.connected
      ? this.#owner.request("channel.unsubscribe", { resourceId: this.id }, () => undefined)
      : Promise.resolve();
    return this.#closed;
  }
}

export class MediaSession extends EventEmitter<AgentMediaEvents> implements AgentMediaSession, WireResource {
  readonly stream: MediaStream;
  readonly ready: Promise<void>;
  readonly #peer: RTCPeerConnection;
  readonly #owner: AgentTransport;
  readonly #parent?: Subscription;
  readonly #stopTracks: boolean;
  #id?: string;
  #resolve!: () => void;
  #reject!: (error: BeamError) => void;
  #timer: ReturnType<typeof setTimeout>;
  #closed?: Promise<void>;
  #ended = false;
  reason?: BeamError;
  constructor(
    owner: AgentTransport,
    peer: RTCPeerConnection,
    stream: MediaStream,
    stopTracks: boolean,
    timeout: number,
    parent?: Subscription,
  ) {
    super();
    this.#owner = owner;
    this.#peer = peer;
    this.stream = stream;
    this.#parent = parent;
    this.#stopTracks = stopTracks;
    parent?.children.add(this);
    this.ready = new Promise<void>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    void this.ready.catch(() => {});
    this.#timer = setTimeout(() => this.fail(new BeamTimeoutError("Media connection timed out.")), timeout);
    peer.ontrack = (event) => {
      if (this.#ended) return;
      if (!stream.getTracks().includes(event.track)) stream.addTrack(event.track);
      this.emit("track", { track: event.track, stream });
    };
    peer.onconnectionstatechange = () => {
      if (this.#ended) return;
      if (peer.connectionState === "connected") {
        clearTimeout(this.#timer);
        this.#resolve();
      }
      if (peer.connectionState === "failed" || peer.connectionState === "closed")
        this.fail(new BeamNetworkError("Media connection ended."));
      else this.emit("state", { state: peer.connectionState });
    };
  }
  get state(): RTCPeerConnectionState {
    return this.#ended ? "closed" : this.#peer.connectionState;
  }
  bind(id: string): void {
    this.#id = id;
    if (this.#ended) {
      this.#owner.control("media.close", { resourceId: id });
      return;
    }
    this.#owner.resources.set(id, this);
  }
  event(type: string, data: unknown, error: unknown): void {
    if (type === "resource.error") this.fail(remoteError(error));
    else if (type === "media.state") {
      const state = object(data).state;
      if (state === "closed" || state === "failed") this.fail(new BeamNetworkError("Media session ended."));
    }
  }
  fail(reason: BeamError): void {
    if (this.#ended) return;
    this.disconnected(reason);
    if (this.#id) this.#owner.control("media.close", { resourceId: this.#id });
  }
  disconnected(reason: BeamError): void {
    if (this.#ended) return;
    this.#ended = true;
    this.reason = reason;
    clearTimeout(this.#timer);
    this.#reject(reason);
    this.#peer.ontrack = this.#peer.onconnectionstatechange = null;
    this.#peer.close();
    if (this.#stopTracks) for (const track of this.stream.getTracks()) track.stop();
    if (this.#id) this.#owner.resources.delete(this.#id);
    this.#parent?.children.delete(this);
    if (!(reason instanceof BeamAbortError)) this.emit("error", reason);
    this.emit("closed", undefined);
    this.removeAllListeners();
  }
  close(): Promise<void> {
    if (this.#closed) return this.#closed;
    if (this.#ended) return Promise.resolve();
    this.disconnected(new BeamAbortError("Media session closed."));
    this.#closed =
      this.#id && this.#owner.connected
        ? this.#owner.request("media.close", { resourceId: this.#id }, () => undefined)
        : Promise.resolve();
    return this.#closed;
  }
}
