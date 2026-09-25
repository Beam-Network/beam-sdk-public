/**
 * Server-side token broker for `@beam-network/web-sdk`.
 *
 * Runs in the customer's backend. It holds the Beam API key, decides which
 * scopes a browser session may hold, and mints short-lived tokens the browser
 * uses instead. The key itself never reaches the browser.
 *
 * @example
 * ```ts
 * import { createBeamBroker, coordinatorRooms } from "@beam-network/web-sdk-server";
 * import { beamExpressMiddleware } from "@beam-network/web-sdk-server/adapters/express";
 *
 * const broker = createBeamBroker({
 *   secret: process.env.BEAM_TOKEN_SECRET!,
 *   upstream: coordinatorRooms({
 *     coordinatorUrl: process.env.BEAM_COORDINATOR_URL!,
 *     apiKey: process.env.BEAM_API_KEY!,
 *   }),
 *   authorize: async (request) => {
 *     const user = await currentUser(request);   // your session, your rules
 *     return user ? { subject: user.id, scopes: ["rooms:create", "rooms:join", "room:read"] } : null;
 *   },
 * });
 *
 * app.use(beamExpressMiddleware(broker));
 * ```
 */

export { createBeamBroker } from "./broker.js";
export type { BeamBroker, BeamBrokerOptions } from "./broker.js";

export {
  BEAM_SCOPES,
  BeamTokenError,
  DEFAULT_TTL_SECONDS,
  MAX_TTL_SECONDS,
  MIN_TTL_SECONDS,
  assertScopes,
  isBeamScope,
  mintToken,
  verifyToken,
} from "./token.js";
export type { BeamScope, BeamTokenClaims, MintOptions, VerifyOptions } from "./token.js";

export { BrokerError } from "./types.js";
export type {
  AuthorizeFn,
  AuthorizedSession,
  BeamSessionContext,
  BeamUpstream,
  CreateRoomInput,
  CreateTransferInput,
  RoomMedia,
  RoomRecord,
  TransferProgressRecord,
  TransferRecord,
} from "./types.js";

export { coordinatorRooms } from "./upstreams/coordinator.js";
export type { CoordinatorUpstreamOptions } from "./upstreams/coordinator.js";

export { beamSdkTransfers, toTransferRecord } from "./upstreams/beam-sdk.js";
export type { BeamSdkTransfersOptions, EndpointResolver } from "./upstreams/beam-sdk.js";

export { memoryUpstream } from "./upstreams/memory.js";
export type { MemoryUpstreamOptions } from "./upstreams/memory.js";
