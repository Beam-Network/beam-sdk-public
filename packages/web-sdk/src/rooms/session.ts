/**
 * A joined room session.
 *
 * Owns one `RTCPeerConnection` against the room's SFU plus the SSE event stream
 * that keeps it in step with the room's track set.
 */

import { EventEmitter } from "../core/events/index.js";
import { BeamApiError, BeamError } from "../core/errors/index.js";
import type { Logger } from "../core/logger/index.js";
import type { HttpTransport } from "../core/transport/index.js";
import { SseStream } from "../core/transport/index.js";
import type { ReconnectState } from "../core/retry/index.js";
import type {
  JoinRoomOptions,
  ParticipantId,
  Participant,
  RemoteTrack,
  Room,
  RoomSession,
  RoomSessionEvents,
  RoomSessionState,
} from "./types.js";
import {
  RenegotiationQueue,
  ensureReceiveTransceivers,
  normalizeIceServers,
  requirePeerConnection,
  waitForIceGathering,
} from "./webrtc.js";

/** ICE credentials as returned by the relay's `/ice` endpoint. */
interface IceCredentials {
  ice_servers?: RTCIceServer[];
  ttl_seconds?: number;
}

interface JoinResponse {
  type: RTCSdpType;
  sdp: string;
  participant_id: string;
}

interface AnswerResponse {
  type: RTCSdpType;
  sdp: string;
}

/** Room event as it arrives on the wire. */
interface WireRoomEvent {
  seq?: number;
  type: string;
  room_id?: string;
  participant_id?: string;
  display_name?: string;
  track_id?: string;
  metadata?: Record<string, unknown>;
  at?: string;
}

export interface RoomSessionDeps {
  room: Room;
  /** Points at the room's media plane; its URL carries its own capability token. */
  media: HttpTransport;
  logger: Logger;
  options: JoinRoomOptions;
}

export class BeamRoomSession extends EventEmitter<RoomSessionEvents> implements RoomSession {
  readonly room: Room;
  #participantId: ParticipantId | undefined;
  #state: RoomSessionState = "connecting";

  readonly #media: HttpTransport;
  readonly #logger: Logger;
  readonly #options: JoinRoomOptions;
  readonly #controller = new AbortController();

  #peer: RTCPeerConnection | undefined;
  #events: SseStream | undefined;
  #renegotiation: RenegotiationQueue | undefined;
  /** Tracks seen per participant, so `track.removed` can name what went away. */
  readonly #remoteTracks = new Map<string, RemoteTrack>();

  constructor(deps: RoomSessionDeps) {
    super((error, event) => deps.logger.warn(`a "${event}" listener threw`, error));
    this.room = deps.room;
    this.#media = deps.media;
    this.#logger = deps.logger.child("room");
    this.#options = deps.options;
  }

  get participantId(): ParticipantId | undefined {
    return this.#participantId;
  }

  get state(): RoomSessionState {
    return this.#state;
  }

  #setState(state: RoomSessionState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.emit("state", state);
  }

  /** Establishes the peer connection and joins. Called once by `Rooms.join`. */
  async connect(): Promise<void> {
    this.#setState("connecting");

    const external = this.#options.signal;
    if (external) {
      if (external.aborted)
        throw new BeamError("Join was aborted before it started.", { code: "aborted", retryable: false });
      external.addEventListener("abort", () => void this.leave(), { once: true });
    }

    const ice = await this.#media.get<IceCredentials>("/ice", {
      authenticated: false,
      signal: this.#controller.signal,
    });

    const PeerConnection = requirePeerConnection();
    const peer = new PeerConnection({
      iceServers: normalizeIceServers(ice.ice_servers, this.#options.iceServers ?? []),
    });
    this.#peer = peer;
    this.#attachPeerHandlers(peer);

    if (this.#options.publish) {
      for (const track of this.#options.publish.getTracks()) {
        peer.addTrack(track, this.#options.publish);
      }
    }
    if (this.#options.subscribe ?? true) ensureReceiveTransceivers(peer);

    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);
    await waitForIceGathering(peer, this.#options.iceGatheringTimeoutMs ?? 1500, this.#controller.signal);

    const response = await this.#media.post<JoinResponse>(
      "/sfu/participants",
      {
        type: "offer",
        sdp: peer.localDescription?.sdp ?? offer.sdp,
        participant_id: this.#options.participantId,
        display_name: this.#options.displayName,
        publish: this.#options.publish !== undefined,
        subscribe: this.#options.subscribe ?? true,
        metadata: this.#options.metadata,
      },
      { authenticated: false, signal: this.#controller.signal },
    );

    this.#participantId = response.participant_id as ParticipantId;
    await peer.setRemoteDescription({ type: response.type, sdp: response.sdp });

    this.#renegotiation = new RenegotiationQueue({
      run: () => this.#renegotiate(),
      isStable: () => this.#peer?.signalingState === "stable",
      onError: (error) => this.#logger.debug("renegotiation attempt failed", error),
    });

    this.#openEventStream();
    this.#setState("connected");
    this.#logger.info("joined room", { roomId: this.room.id, participantId: this.#participantId });
  }

  #attachPeerHandlers(peer: RTCPeerConnection): void {
    peer.ontrack = (event) => {
      const stream = event.streams[0] ?? new MediaStream([event.track]);
      const remote: RemoteTrack = {
        participantId: participantIdFromStream(stream),
        stream,
        track: event.track,
      };
      this.#remoteTracks.set(event.track.id, remote);
      event.track.addEventListener(
        "ended",
        () => {
          this.#remoteTracks.delete(event.track.id);
          this.emit("track.removed", { participantId: remote.participantId, track: event.track });
        },
        { once: true },
      );
      this.emit("track.added", remote);
    };

    peer.onicecandidate = (event) => {
      if (event.candidate && this.#participantId) {
        void this.#sendCandidate(event.candidate).catch((error: unknown) =>
          this.#logger.debug("trickle candidate rejected", error),
        );
      }
    };

    peer.onconnectionstatechange = () => {
      this.#logger.debug("peer connection state", { state: peer.connectionState });
      switch (peer.connectionState) {
        case "connected":
          this.#setState("connected");
          break;
        case "disconnected":
          // Transient by definition; ICE may recover on its own.
          this.#setState("reconnecting");
          break;
        case "failed":
          this.#setState("reconnecting");
          void this.restartIce().catch((error: unknown) => this.emit("error", toError(error)));
          break;
        case "closed":
          this.#setState("closed");
          break;
        default:
          break;
      }
    };
  }

  #openEventStream(): void {
    this.#events = new SseStream({
      transport: this.#media,
      path: "/sfu/events",
      authenticated: false,
      logger: this.#logger,
      signal: this.#controller.signal,
      onStateChange: (state: ReconnectState) => {
        // The media connection is the source of truth for session state; the
        // event stream dropping is worth reporting only while media is healthy.
        if (state === "reconnecting" && this.#state === "connected") this.#setState("reconnecting");
        if (state === "connected" && this.#peer?.connectionState === "connected") this.#setState("connected");
      },
      onEvent: (event) => this.#handleRoomEvent(event.data),
    });
    void this.#events.start().catch((error: unknown) => {
      if (!this.#controller.signal.aborted) this.emit("error", toError(error));
    });
  }

  #handleRoomEvent(raw: string): void {
    let parsed: WireRoomEvent;
    try {
      parsed = JSON.parse(raw) as WireRoomEvent;
    } catch (error) {
      this.#logger.debug("discarding malformed room event", { raw: raw.slice(0, 200), error });
      return;
    }

    const participantId = parsed.participant_id as ParticipantId | undefined;
    const at = parsed.at ?? new Date().toISOString();

    // Always emitted, so an application can observe event types this SDK
    // version does not model yet.
    this.emit("event", { ...parsed, type: parsed.type, at, ...(participantId ? { participantId } : {}) });

    switch (parsed.type) {
      case "participant_joined": {
        if (!participantId) break;
        const participant: Participant = {
          id: participantId,
          ...(parsed.display_name !== undefined ? { displayName: parsed.display_name } : {}),
          ...(parsed.metadata !== undefined ? { metadata: parsed.metadata } : {}),
        };
        this.emit("participant.joined", participant);
        break;
      }
      case "participant_left":
        if (participantId) this.emit("participant.left", { id: participantId });
        break;
      case "renegotiation_required":
      case "track_added":
        this.#renegotiation?.schedule();
        break;
      default:
        break;
    }
  }

  async #renegotiate(): Promise<void> {
    const peer = this.#peer;
    const participantId = this.#participantId;
    if (!peer || !participantId) return;

    const offer = await this.#media.post<AnswerResponse>(
      `/sfu/participants/${encodeURIComponent(participantId)}/renegotiate`,
      undefined,
      { authenticated: false, signal: this.#controller.signal },
    );
    await peer.setRemoteDescription({ type: offer.type, sdp: offer.sdp });

    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    await waitForIceGathering(peer, this.#options.iceGatheringTimeoutMs ?? 1500, this.#controller.signal);

    await this.#media.post(
      `/sfu/participants/${encodeURIComponent(participantId)}/answer`,
      { type: "answer", sdp: peer.localDescription?.sdp ?? answer.sdp },
      { authenticated: false, signal: this.#controller.signal },
    );
  }

  async #sendCandidate(candidate: RTCIceCandidate): Promise<void> {
    if (!this.#participantId) return;
    await this.#media.post(
      `/sfu/participants/${encodeURIComponent(this.#participantId)}/candidates`,
      candidate.toJSON(),
      { authenticated: false, signal: this.#controller.signal },
    );
  }

  async publish(stream: MediaStream): Promise<void> {
    const peer = this.#peer;
    if (!peer)
      throw new BeamApiError("Join the room before publishing media.", { code: "invalid_argument", retryable: false });
    for (const track of stream.getTracks()) peer.addTrack(track, stream);
    this.#renegotiation?.schedule();
  }

  async replaceTrack(kind: "audio" | "video", track: MediaStreamTrack | null): Promise<void> {
    const sender = this.#peer?.getSenders().find((candidate) => candidate.track?.kind === kind);
    // Replacing in place avoids renegotiation entirely, so camera switches and
    // mute/unmute do not interrupt anyone else's view.
    await sender?.replaceTrack(track);
  }

  async restartIce(): Promise<void> {
    const peer = this.#peer;
    if (!peer) return;
    this.#logger.debug("restarting ICE");
    await this.#media
      .post("/ice-restart", undefined, { authenticated: false, signal: this.#controller.signal })
      .catch((error: unknown) => this.#logger.debug("ice-restart endpoint failed", error));
    peer.restartIce();
    this.#renegotiation?.schedule();
  }

  async leave(): Promise<void> {
    if (this.#controller.signal.aborted) return;
    this.#logger.debug("leaving room");
    this.#controller.abort();
    this.#renegotiation?.close();
    this.#events?.close();

    const participantId = this.#participantId;
    if (participantId) {
      // Best-effort: a failed leave costs the room a participant slot until its
      // lease expires, which is better than throwing during teardown.
      await this.#media
        .delete(`/sfu/participants/${encodeURIComponent(participantId)}`, { authenticated: false })
        .catch((error: unknown) => this.#logger.debug("leave notification failed", error));
    }

    this.#peer?.close();
    this.#peer = undefined;
    this.#remoteTracks.clear();
    this.#setState("closed");
    this.removeAllListeners();
  }
}

/**
 * Recovers the publisher from an SFU stream id.
 *
 * The SFU labels streams `<participantId>` or `<participantId>:<trackLabel>`.
 * Undefined is a normal answer for a relay that does not label streams, so
 * callers treat the participant as unknown rather than dropping the track.
 */
function participantIdFromStream(stream: MediaStream): ParticipantId | undefined {
  const id = stream.id.split(":")[0];
  return id && id !== "default" ? (id as ParticipantId) : undefined;
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
