/**
 * Broker contracts.
 *
 * The broker is deliberately thin: it authenticates the caller in the
 * customer's own model, mints a scoped token, and forwards control-plane calls
 * upstream with the privileged credential attached. Everything Beam-specific
 * lives behind `BeamUpstream`, so the same broker serves a local fake during
 * development and the real Beam edge in production.
 */

import type { BeamScope, BeamTokenClaims } from "./token.js";

/** The media plane descriptor handed to the browser. */
export interface RoomMedia {
  /** Base URL for signalling and media. Carries its own capability token. */
  url: string;
  token?: string;
  whipUrl?: string;
  whepUrl?: string;
}

export interface RoomRecord {
  id: string;
  name?: string;
  createdAt: string;
  expiresAt?: string;
  media: RoomMedia;
}

export interface CreateRoomInput {
  name?: string;
  ttlSeconds?: number;
  region?: string;
  metadata?: Record<string, unknown>;
  /** Set by `beam.broadcast.createChannel`. */
  mode?: string;
}

export interface TransferProgressRecord {
  bytesCompleted: number;
  totalBytes?: number;
  destinationsCompleted?: number;
  destinationsTotal?: number;
  updatedAt?: string;
}

export interface TransferRecord {
  id: string;
  status: "pending" | "preparing" | "running" | "completed" | "failed" | "cancelled";
  name?: string;
  createdAt: string;
  progress?: TransferProgressRecord;
  error?: { code: string; message: string };
}

export interface CreateTransferInput {
  /** Opaque reference the broker resolves to a configured storage endpoint. */
  source: string;
  destinations: string[];
  name?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Identity and authorization for one browser session, as resolved by the
 * customer's `authorize` function and then carried on every upstream call.
 */
export interface BeamSessionContext {
  subject: string | undefined;
  organizationId: string | undefined;
  scopes: readonly BeamScope[];
  claims: BeamTokenClaims | undefined;
  /** The original request, for callers that need headers or cookies. */
  request: Request;
}

/**
 * The Beam-facing half of the broker.
 *
 * Every method is optional except room reads, so a broker can expose only what
 * the application actually uses. A call to a missing capability answers 501
 * rather than failing obscurely.
 */
export interface BeamUpstream {
  createRoom?(input: CreateRoomInput, context: BeamSessionContext): Promise<RoomRecord>;
  getRoom?(roomId: string, context: BeamSessionContext): Promise<RoomRecord>;

  createTransfer?(input: CreateTransferInput, context: BeamSessionContext): Promise<TransferRecord>;
  getTransfer?(transferId: string, context: BeamSessionContext): Promise<TransferRecord>;
  cancelTransfer?(transferId: string, context: BeamSessionContext): Promise<void>;
  listTransfers?(options: { limit?: number; status?: string }, context: BeamSessionContext): Promise<TransferRecord[]>;
  /**
   * Yields a record whenever the transfer changes, ending at a terminal status.
   *
   * BeamCore pushes only a single terminal event, so a real implementation
   * polls `transfer.status` and re-emits. The broker turns whatever this yields
   * into the browser's SSE stream.
   */
  watchTransfer?(transferId: string, context: BeamSessionContext, signal: AbortSignal): AsyncIterable<TransferRecord>;
}

/**
 * Decides who the caller is and what they may do.
 *
 * This is the security boundary. It runs against the customer's own session —
 * a cookie, a header, a JWT — and returns the scopes that session may hold.
 * Returning `null` denies the request.
 */
export type AuthorizeFn = (request: Request) => Promise<AuthorizedSession | null> | AuthorizedSession | null;

export interface AuthorizedSession {
  /** The customer's identifier for the end user. Embedded in the token as `sub`. */
  subject?: string;
  organizationId?: string;
  /** Scopes this session may hold. The browser's request is narrowed to this set. */
  scopes: readonly BeamScope[];
  /** Overrides the broker's default TTL, still capped at 300 seconds. */
  ttlSeconds?: number;
}

export class BrokerError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(message: string, status: number, code: string) {
    super(message);
    this.name = "BrokerError";
    this.status = status;
    this.code = code;
  }
}
