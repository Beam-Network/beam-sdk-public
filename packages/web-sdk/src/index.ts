/**
 * Beam Web SDK.
 *
 * @example Publishable key from the Beam console — no backend required.
 * ```ts
 * import { Beam } from "@beam-network/web-sdk";
 *
 * const beam = new Beam({ clientKey: "bm_pub_…" });
 * const room = await beam.rooms.create({ name: "standup" });
 * const session = await beam.rooms.join(room, { publish: await beam.media.camera() });
 * session.on("track.added", ({ stream }) => attach(stream));
 * ```
 *
 * @example Your own backend mints the tokens.
 * ```ts
 * const beam = new Beam({ tokenEndpoint: "/api/beam/token" });
 * ```
 */

export { Beam, createBeam } from "./client.js";

export { DEFAULT_CONTROL_URL, DEFAULT_TOKEN_URL } from "./config.js";
export type { BeamOptions, BeamAuthConfig, CallbackAuth, ClientKeyAuth, TokenEndpointAuth } from "./config.js";

export {
  BeamAbortError,
  BeamApiError,
  BeamAuthError,
  BeamConfigError,
  BeamError,
  BeamNetworkError,
  BeamTimeoutError,
  BeamUnsupportedError,
} from "./core/errors/index.js";
export type { BeamErrorCode, BeamLocalErrorCode, BeamRemoteErrorCode } from "./core/errors/index.js";

export type { BeamScope, BeamToken, BeamTokenClaims, BeamTokenResponse } from "./core/auth/types.js";
export type { Emitter, Listener, Unsubscribe } from "./core/events/index.js";
export type { LogLevel, LogSink, Logger } from "./core/logger/index.js";
export type { ReconnectState } from "./core/retry/index.js";

export { camera, media, microphone, screen, stopStream } from "./media/index.js";

export { Rooms } from "./rooms/index.js";
export type {
  CreateRoomOptions,
  JoinRoomOptions,
  Participant,
  ParticipantId,
  RemoteTrack,
  Room,
  RoomId,
  RoomMediaEndpoint,
  RoomSession,
  RoomSessionEvents,
  RoomSessionState,
} from "./rooms/index.js";

export { Broadcast, BroadcastConnection } from "./broadcast/index.js";
export type { BroadcastEvents, PublishOptions, WatchOptions } from "./broadcast/index.js";

export { Transfers, TERMINAL_STATUSES } from "./transfers/index.js";
export type {
  CreateTransferOptions,
  Transfer,
  TransferEvents,
  TransferHandle,
  TransferId,
  TransferProgress,
  TransferStatus,
} from "./transfers/index.js";

export * from "./web-agent/index.js";
