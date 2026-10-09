/**
 * Token decoding and normalization.
 *
 * The SDK treats tokens as opaque strings with one exception: it peeks at `exp`
 * so it can refresh before expiry instead of discovering staleness through a
 * failed request mid-call.
 */

import type { BeamScope, BeamToken, BeamTokenClaims, BeamTokenResponse } from "./types.js";

/** Beam Web Token prefix, mirroring the coordinator's `btrstd1.` delegation format. */
const BWT_PREFIX = "bwt1";

function base64UrlDecode(value: string): string {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/");
  const withPadding = padded.padEnd(padded.length + ((4 - (padded.length % 4)) % 4), "=");
  // `atob` is global in browsers and in Node 16+, which covers the SSR
  // frameworks that import this module while rendering on the server.
  return atob(withPadding);
}

/**
 * Reads claims out of a `bwt1.` or JWT-shaped token.
 *
 * Signature is not checked. That is intentional and load-bearing: a browser
 * cannot hold a verification key that means anything, so verification is the
 * server's job on every request. Returns undefined for opaque tokens, and the
 * caller falls back to `expires_in`.
 */
export function decodeTokenClaims(token: string): BeamTokenClaims | undefined {
  const parts = token.split(".");
  const payload = parts[0] === BWT_PREFIX && parts.length === 3 ? parts[1] : parts.length === 3 ? parts[1] : undefined;
  if (payload === undefined) return undefined;
  try {
    const decoded: unknown = JSON.parse(base64UrlDecode(payload));
    if (decoded === null || typeof decoded !== "object") return undefined;
    return decoded as BeamTokenClaims;
  } catch {
    return undefined;
  }
}

/** Default assumed lifetime when neither `exp` nor `expires_in` is present. */
const FALLBACK_TTL_SECONDS = 120;

/**
 * Turns a mint response into the internal token shape.
 *
 * Expiry resolution order — `exp` from the claims, then `expires_in`, then a
 * conservative fallback. Preferring `exp` matters because it is absolute: a
 * response that sat in a proxy for ten seconds would otherwise be treated as
 * ten seconds fresher than it is.
 */
export function normalizeTokenResponse(response: BeamTokenResponse, now: number): BeamToken {
  const value = response.access_token;
  const claims = decodeTokenClaims(value);

  let expiresAt: number;
  if (claims?.exp !== undefined) {
    expiresAt = claims.exp * 1000;
  } else if (response.expires_in !== undefined) {
    expiresAt = now + response.expires_in * 1000;
  } else {
    expiresAt = now + FALLBACK_TTL_SECONDS * 1000;
  }

  const scopes: readonly BeamScope[] = response.scopes ?? claims?.scopes ?? [];

  return { value, expiresAt, scopes, endpoint: response.endpoint ?? claims?.endpoint };
}

/**
 * Fraction of a token's life to use before refreshing.
 *
 * At 0.75 a 120-second token refreshes with 30 seconds left — comfortably more
 * than a slow mint round-trip, without thrashing.
 */
export const REFRESH_AT_FRACTION = 0.75;

/** Never schedule a refresh closer than this to now, to survive clock skew. */
const MIN_REFRESH_MARGIN_MS = 5_000;

export function isTokenUsable(token: BeamToken, now: number): boolean {
  const lifetimeMs = token.expiresAt - now;
  if (lifetimeMs <= MIN_REFRESH_MARGIN_MS) return false;
  return true;
}

/**
 * Whether the token should be proactively replaced. Distinct from `isTokenUsable`:
 * a token can still work while being due for refresh.
 */
export function isTokenStale(token: BeamToken, now: number, issuedAt: number): boolean {
  const totalMs = token.expiresAt - issuedAt;
  if (totalMs <= 0) return true;
  const refreshAfterMs = totalMs * REFRESH_AT_FRACTION;
  return now - issuedAt >= refreshAfterMs || !isTokenUsable(token, now);
}
