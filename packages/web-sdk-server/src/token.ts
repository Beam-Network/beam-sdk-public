/**
 * Beam Web Tokens.
 *
 * A BWT is a short-lived, scoped bearer credential a customer's backend mints
 * for one browser session. Format and lifetime deliberately mirror the
 * coordinator's own `btrstd1.` Studio delegation — `<prefix>.<claims>.<sig>`,
 * capped at five minutes — so the two read as the same idea at a glance.
 *
 * The broker both mints and verifies. Beam's edge does not know what a BWT is,
 * which makes the broker the policy enforcement point for browser sessions.
 */

import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

export const BWT_PREFIX = "bwt1";

/**
 * Ceiling on token lifetime, matching the coordinator's
 * `btrStudioDelegationMaxTTL`. Short-lived is the entire security argument for
 * putting a token in a browser at all, so this is not configurable upward.
 */
export const MAX_TTL_SECONDS = 300;
export const DEFAULT_TTL_SECONDS = 120;
export const MIN_TTL_SECONDS = 15;

/** Scopes a browser token may carry. Anything outside this list is rejected. */
export const BEAM_SCOPES = [
  "rooms:create",
  "rooms:join",
  "room:read",
  "broadcast:publish",
  "broadcast:watch",
  "transfers:create",
  "transfers:read",
  "transfers:cancel",
] as const;

export type BeamScope = (typeof BEAM_SCOPES)[number];

export function isBeamScope(value: unknown): value is BeamScope {
  return typeof value === "string" && (BEAM_SCOPES as readonly string[]).includes(value);
}

export interface BeamTokenClaims {
  v: 1;
  iss: string;
  aud: string;
  /** The customer's own identifier for the end user. Opaque to Beam. */
  sub?: string;
  org?: string;
  scopes: BeamScope[];
  iat: number;
  exp: number;
  jti: string;
  /** Origin the token is bound to, when the broker enforces one. */
  origin?: string;
}

export interface MintOptions {
  secret: string;
  issuer?: string;
  audience?: string;
  subject?: string;
  organizationId?: string;
  scopes: readonly BeamScope[];
  ttlSeconds?: number;
  origin?: string;
  /** Injected for deterministic tests. */
  now?: () => number;
}

export class BeamTokenError extends Error {
  readonly code: string;
  constructor(message: string, code = "invalid_token") {
    super(message);
    this.name = "BeamTokenError";
    this.code = code;
  }
}

function base64UrlEncode(value: Buffer | string): string {
  return Buffer.from(value).toString("base64url");
}

function sign(secret: string, payload: string): string {
  return createHmac("sha256", secret).update(payload).digest("base64url");
}

/**
 * A short secret makes the HMAC guessable, and this secret is the only thing
 * standing between a stranger and a token that spends the customer's credits.
 */
function assertUsableSecret(secret: string): void {
  if (typeof secret !== "string" || secret.length < 32) {
    throw new BeamTokenError(
      "The signing secret must be at least 32 characters. Generate one with `openssl rand -hex 32`.",
      "weak_secret",
    );
  }
}

export function mintToken(options: MintOptions): { token: string; claims: BeamTokenClaims; expiresIn: number } {
  assertUsableSecret(options.secret);

  const scopes = [...new Set(options.scopes)];
  if (scopes.length === 0) {
    throw new BeamTokenError("A token must carry at least one scope.", "invalid_argument");
  }
  const invalid = scopes.filter((scope) => !isBeamScope(scope));
  if (invalid.length > 0) {
    throw new BeamTokenError(`Unknown scope(s): ${invalid.join(", ")}.`, "invalid_argument");
  }

  const ttlSeconds = Math.min(options.ttlSeconds ?? DEFAULT_TTL_SECONDS, MAX_TTL_SECONDS);
  if (ttlSeconds < MIN_TTL_SECONDS) {
    throw new BeamTokenError(
      `ttlSeconds must be between ${MIN_TTL_SECONDS} and ${MAX_TTL_SECONDS}.`,
      "invalid_argument",
    );
  }

  const now = Math.floor((options.now?.() ?? Date.now()) / 1000);
  const claims: BeamTokenClaims = {
    v: 1,
    iss: options.issuer ?? "beam-web-sdk-server",
    aud: options.audience ?? "beam-web-sdk",
    ...(options.subject !== undefined ? { sub: options.subject } : {}),
    ...(options.organizationId !== undefined ? { org: options.organizationId } : {}),
    scopes,
    iat: now,
    exp: now + ttlSeconds,
    jti: randomBytes(12).toString("base64url"),
    ...(options.origin !== undefined ? { origin: options.origin } : {}),
  };

  const payload = base64UrlEncode(JSON.stringify(claims));
  return { token: `${BWT_PREFIX}.${payload}.${sign(options.secret, payload)}`, claims, expiresIn: ttlSeconds };
}

export interface VerifyOptions {
  secret: string;
  audience?: string;
  /** Rejects a token minted for a different origin. */
  origin?: string;
  /** Tolerance for clock drift between the minting and verifying hosts. */
  clockToleranceSeconds?: number;
  now?: () => number;
}

/**
 * Verifies a token and returns its claims.
 *
 * Order matters: the signature is checked before anything in the payload is
 * trusted, so an attacker cannot steer behaviour with forged claims.
 */
export function verifyToken(token: string, options: VerifyOptions): BeamTokenClaims {
  assertUsableSecret(options.secret);

  const parts = token.split(".");
  if (parts.length !== 3 || parts[0] !== BWT_PREFIX) {
    throw new BeamTokenError("Malformed Beam token.");
  }
  const [, payload, signature] = parts as [string, string, string];

  const expected = Buffer.from(sign(options.secret, payload));
  const provided = Buffer.from(signature);
  // Length is compared first because timingSafeEqual throws on a mismatch, and
  // the comparison itself is constant-time to avoid leaking the signature.
  if (expected.length !== provided.length || !timingSafeEqual(expected, provided)) {
    throw new BeamTokenError("Beam token signature is invalid.");
  }

  let claims: BeamTokenClaims;
  try {
    claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as BeamTokenClaims;
  } catch {
    throw new BeamTokenError("Beam token payload is not valid JSON.");
  }

  if (claims.v !== 1) throw new BeamTokenError("Unsupported Beam token version.");
  if (options.audience !== undefined && claims.aud !== options.audience) {
    throw new BeamTokenError("Beam token audience mismatch.");
  }
  if (!Array.isArray(claims.scopes) || !claims.scopes.every(isBeamScope)) {
    throw new BeamTokenError("Beam token carries unknown scopes.");
  }

  const now = Math.floor((options.now?.() ?? Date.now()) / 1000);
  const tolerance = options.clockToleranceSeconds ?? 30;
  if (typeof claims.exp !== "number" || claims.exp + tolerance <= now) {
    throw new BeamTokenError("Beam token has expired.", "token_expired");
  }
  if (typeof claims.iat !== "number" || claims.iat - tolerance > now) {
    throw new BeamTokenError("Beam token was issued in the future.");
  }
  // Refuses a token minted with a longer life than the ceiling allows, even if
  // some future minter tried to.
  if (claims.exp - claims.iat > MAX_TTL_SECONDS + tolerance) {
    throw new BeamTokenError("Beam token lifetime exceeds the permitted maximum.");
  }
  if (options.origin !== undefined && claims.origin !== undefined && claims.origin !== options.origin) {
    throw new BeamTokenError("Beam token was issued for a different origin.", "origin_mismatch");
  }

  return claims;
}

/** Throws unless the token carries every required scope. */
export function assertScopes(claims: BeamTokenClaims, required: readonly BeamScope[]): void {
  const missing = required.filter((scope) => !claims.scopes.includes(scope));
  if (missing.length > 0) {
    throw new BeamTokenError(
      `This session is missing the required scope(s): ${missing.join(", ")}.`,
      "insufficient_scope",
    );
  }
}
