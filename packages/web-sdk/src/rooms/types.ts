/**
 * Room types.
 *
 * These are the SDK's own vocabulary, not Beam's wire shapes. A developer using
 * this SDK should never encounter a relay id, tunnel id, lease, plan version, or
 * assignment token — the control plane hands those to the SDK and the SDK keeps
 * them to itself.
 */

import type { Emitter } from "../core/events/index.js";
import type { ReconnectState } from "../core/retry/index.js";

/** Opaque room identifier. Branded so it cannot be confused with other ids. */
export type RoomId = string & { readonly __brand: "RoomId" };

/** Opaque participant identifier, assigned by Beam when a participant joins. */
export type ParticipantId = string & { readonly __brand: "ParticipantId" };

/**
 * Everything the browser needs to reach a room's media plane.
 *
 * The control plane returns this; the browser then talks to the media URL
 * directly. The URL carries its own capability token, which is why media traffic
 * needs no Beam credential of its own.
 */
export interface RoomMediaEndpoint {
  /** Base URL for signalling and media. */
  url: string;
  /** Bearer for WHIP/WHEP, when the endpoint requires one. */
  token?: string;
  whipUrl?: string;
  whepUrl?: string;
}

export interface Room {
  id: RoomId;
  name?: string;
  createdAt: string;
  /** When the room's media plane stops accepting connections. */
  expiresAt?: string;
  media: RoomMediaEndpoint;
}

export interface CreateRoomOptions {
  name?: string;
  /** Requested lifetime in seconds. The control plane may clamp this. */
  ttlSeconds?: number;
  /** Preferred region hint, when the deployment offers more than one. */
  region?: string;
  /** Application data stored with the room and echoed back on read. */
  metadata?: Record<string, unknown>;
  signal?: AbortSignal;
}

export interface JoinRoomOptions {
  /** Stable id for this participant. Beam assigns one when omitted. */
  participantId?: string;
  displayName?: string;
  /** Media to publish. Omit to join as a viewer. */
  publish?: MediaStream;
  /** Receive other participants' media. Default true. */
  subscribe?: boolean;
  /** Application data visible to other participants. */
  metadata?: Record<string, unknown>;
  /** Extra ICE servers, merged with the ones Beam supplies. */
  iceServers?: RTCIceServer[];
  /** Deadline for ICE gathering before the offer is sent anyway. Default 1500ms. */
  iceGatheringTimeoutMs?: number;
  signal?: AbortSignal;
}

export interface Participant {
  id: ParticipantId;
  displayName?: string;
  metadata?: Record<string, unknown>;
}

/** A media track published by another participant. */
export interface RemoteTrack {
  participantId: ParticipantId | undefined;
  stream: MediaStream;
  track: MediaStreamTrack;
}

export type RoomSessionState = ReconnectState;

/**
 * Events a room session emits.
 *
 * Names are namespaced (`participant.joined`, not `joined`) so the vocabulary
 * can grow without colliding with future transport or media events.
 */
export type RoomSessionEvents = {
  /** Connection lifecycle: connecting → connected → reconnecting → closed. */
  state: RoomSessionState;
  "participant.joined": Participant;
  "participant.left": { id: ParticipantId };
  "track.added": RemoteTrack;
  "track.removed": { participantId: ParticipantId | undefined; track: MediaStreamTrack };
  /** Any room event, including ones this SDK version does not model. */
  event: { type: string; participantId?: ParticipantId; at: string; [key: string]: unknown };
  /** A non-fatal error. Fatal ones reject the originating call instead. */
  error: Error;
};

export interface RoomSession extends Emitter<RoomSessionEvents> {
  readonly room: Room;
  readonly participantId: ParticipantId | undefined;
  readonly state: RoomSessionState;
  /** Adds tracks to an already-joined session and renegotiates. */
  publish(stream: MediaStream): Promise<void>;
  /** Swaps an outgoing track in place — no renegotiation, no flicker. */
  replaceTrack(kind: "audio" | "video", track: MediaStreamTrack | null): Promise<void>;
  /** Forces an ICE restart. Rarely needed; recovery is automatic. */
  restartIce(): Promise<void>;
  leave(): Promise<void>;
}
