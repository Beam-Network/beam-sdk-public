/**
 * Authentication contracts.
 *
 * The SDK never holds a Beam API key. It holds a short-lived, scoped access
 * token and knows how to get a fresh one. There are three ways to obtain one,
 * and they differ only in who mints it.
 */

/**
 * Scopes a browser session can hold.
 *
 * Deliberately reuses Beam's existing vocabulary (the coordinator's
 * `btrStudioScopes` shape and beam-website's `PermissionAction` names) rather
 * than inventing a parallel one, so a scope means the same thing on both sides
 * of the boundary.
 */
export type BeamScope =
  | "rooms:create"
  | "rooms:join"
  | "room:read"
  | "broadcast:publish"
  | "broadcast:watch"
  | "transfers:create"
  | "transfers:read"
  | "transfers:cancel";

/** The token response every mint path returns. Mirrors an OAuth token response. */
export interface BeamTokenResponse {
  access_token: string;
  token_type?: "Bearer";
  /** Lifetime in seconds. Used to schedule refresh when the token is opaque. */
  expires_in?: number;
  scopes?: BeamScope[];
  /** Where to send control-plane calls carrying this token. */
  endpoint?: string;
}

/** A token plus the bookkeeping the SDK needs to refresh it in time. */
export interface BeamToken {
  value: string;
  /** Epoch milliseconds. */
  expiresAt: number;
  scopes: readonly BeamScope[];
  endpoint: string | undefined;
}

/**
 * Claims the SDK can read from a Beam Web Token without verifying it.
 *
 * The browser never validates the signature — it cannot, and it has no reason
 * to. It decodes only to learn `exp` so it can refresh before expiry. Every
 * authorization decision happens server-side.
 */
export interface BeamTokenClaims {
  v?: number;
  iss?: string;
  aud?: string;
  sub?: string;
  org?: string;
  scopes?: BeamScope[];
  iat?: number;
  exp?: number;
  jti?: string;
  origin?: string;
  /** Control-plane base URL this token is good for. */
  endpoint?: string;
}

export interface TokenProvider {
  /** Returns a valid token, minting or refreshing as needed. */
  getToken(options?: { signal?: AbortSignal }): Promise<BeamToken>;
  /** Drops the cached token so the next call re-mints. Called on a 401. */
  invalidate(): void;
  /** Current token without triggering a fetch. Used for scope preflight. */
  peek(): BeamToken | undefined;
}
