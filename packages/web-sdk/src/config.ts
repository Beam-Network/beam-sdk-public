/**
 * Client configuration and its validation.
 *
 * The single most important rule the SDK enforces: a Beam API key must never
 * reach browser code. Configuration is shaped so the safe paths are the obvious
 * ones and the unsafe one fails loudly at construction rather than shipping to
 * production and leaking a credit-spending credential to every visitor.
 */

import type { BeamScope, BeamTokenResponse } from "./core/auth/types.js";
import { BeamConfigError } from "./core/errors/index.js";
import type { LogLevel, LogSink } from "./core/logger/index.js";

/** Beam's hosted token endpoint, used by the publishable-key path. */
export const DEFAULT_TOKEN_URL = "https://api.b1m.ai/v1/web-sdk/token";

/**
 * Where control-plane calls go when the token response does not name an
 * endpoint. A broker normally returns its own `endpoint`, so this default
 * matters only for the hosted path.
 */
export const DEFAULT_CONTROL_URL = "https://api.b1m.ai/v1/web-sdk";

/**
 * Publishable key issued by the Beam console.
 *
 * Public by design, in the way a Stripe `pk_` or an Adobe datastream id is
 * public: it identifies a configuration, is bound to an origin allow-list
 * server-side, and can mint nothing but short-lived narrowly-scoped tokens.
 */
export interface ClientKeyAuth {
  clientKey: string;
  /**
   * Stable identifier for the end user, when the app has one. Beam binds issued
   * tokens to it so per-user scopes and audit trails are possible without the
   * app running a backend.
   */
  subject?: string;
  /** Overrides Beam's hosted token endpoint. Used for local development. */
  tokenUrl?: string;
}

/**
 * A token endpoint the customer operates.
 *
 * Preferred whenever the app already has a session: the broker decides who the
 * caller is and which scopes they get, so authorization is expressed in the
 * customer's own model rather than Beam's.
 */
export interface TokenEndpointAuth {
  tokenEndpoint: string;
  /** Defaults to `same-origin` so a session cookie reaches a same-origin broker. */
  credentials?: RequestCredentials;
  headers?: Record<string, string>;
}

/** Fully caller-controlled minting, for apps with their own fetch/auth stack. */
export interface CallbackAuth {
  getToken: (signal?: AbortSignal) => Promise<string | BeamTokenResponse>;
}

export type BeamAuthConfig = ClientKeyAuth | TokenEndpointAuth | CallbackAuth;

export interface BeamOptions extends Partial<ClientKeyAuth & TokenEndpointAuth & CallbackAuth> {
  /** Scopes to request. The issuer may narrow them and is never obliged to grant them. */
  scopes?: readonly BeamScope[];
  /** Control-plane base URL. Normally supplied by the token response instead. */
  controlUrl?: string;
  /** `true` is shorthand for `logLevel: "debug"`. */
  debug?: boolean;
  logLevel?: LogLevel;
  logSink?: LogSink;
  /** Disable only in trusted local debugging: log output will contain tokens. */
  redactLogs?: boolean;
  /** Per-request deadline in milliseconds. Default 30000. */
  timeoutMs?: number;
  /** Attempts per request, including the first. Default 3. */
  maxAttempts?: number;
  fetch?: typeof fetch;
}

export interface ResolvedConfig {
  auth: BeamAuthConfig;
  scopes: readonly BeamScope[];
  controlUrl: string;
  logLevel: LogLevel;
  logSink: LogSink | undefined;
  redactLogs: boolean;
  timeoutMs: number;
  maxAttempts: number;
  fetch: typeof fetch | undefined;
}

/** Prefixes that identify a secret, credit-spending Beam credential. */
const SECRET_KEY_PATTERNS: ReadonlyArray<{ pattern: RegExp; label: string }> = [
  { pattern: /^bm_live_/, label: "beam-website secret key (bm_live_…)" },
  { pattern: /^bm_test_/, label: "beam-website secret key (bm_test_…)" },
  { pattern: /^b1m_/, label: "BeamCore API key (b1m_…)" },
  { pattern: /^bag_/, label: "Beam agent credential (bag_…)" },
  { pattern: /^btrstd1\./, label: "coordinator Studio delegation (btrstd1.…)" },
];

const SECRET_KEY_MESSAGE =
  "Beam API keys can create transfers, start rooms, and spend credits, so anything that reaches a browser is " +
  "readable by every visitor. Use a publishable key from the Beam console (clientKey: 'bm_pub_…'), or mint " +
  "short-lived tokens from your own backend (tokenEndpoint). See https://github.com/Beam-Network/beam-sdk-public#authentication";

/**
 * Rejects a secret credential wherever it was passed.
 *
 * Checked before anything else in `resolveConfig`, and again on the value of
 * `clientKey`, because the most likely mistake is pasting the wrong key from the
 * console into the right-looking field.
 */
export function assertNotSecretCredential(value: unknown, field: string): void {
  if (typeof value !== "string") return;
  const match = SECRET_KEY_PATTERNS.find(({ pattern }) => pattern.test(value));
  if (match) {
    throw new BeamConfigError(`\`${field}\` was given a ${match.label}. ${SECRET_KEY_MESSAGE}`);
  }
}

function isClientKeyAuth(options: BeamOptions): boolean {
  return typeof options.clientKey === "string" && options.clientKey.length > 0;
}

function isTokenEndpointAuth(options: BeamOptions): boolean {
  return typeof options.tokenEndpoint === "string" && options.tokenEndpoint.length > 0;
}

function isCallbackAuth(options: BeamOptions): boolean {
  return typeof options.getToken === "function";
}

export function resolveConfig(options: BeamOptions): ResolvedConfig {
  if (options === null || typeof options !== "object") {
    throw new BeamConfigError("new Beam(...) requires an options object.");
  }

  // `apiKey` is not part of `BeamOptions`, so this only fires when someone
  // ignores the types — which, being the exact mistake that leaks a credential,
  // is worth catching at runtime too.
  const legacyApiKey = (options as Record<string, unknown>)["apiKey"];
  if (legacyApiKey !== undefined) {
    throw new BeamConfigError(`\`apiKey\` is not a supported option. ${SECRET_KEY_MESSAGE}`);
  }
  assertNotSecretCredential(options.clientKey, "clientKey");

  const modes = [isClientKeyAuth(options), isTokenEndpointAuth(options), isCallbackAuth(options)].filter(Boolean);
  if (modes.length === 0) {
    throw new BeamConfigError(
      "Beam needs one way to obtain an access token: `clientKey` (publishable key from the Beam console), " +
        "`tokenEndpoint` (your backend), or `getToken` (your own function).",
    );
  }
  if (modes.length > 1) {
    throw new BeamConfigError(
      "Beam was given more than one authentication mode. Choose exactly one of `clientKey`, `tokenEndpoint`, or `getToken`.",
    );
  }

  let auth: BeamAuthConfig;
  if (isClientKeyAuth(options)) {
    auth = {
      clientKey: options.clientKey as string,
      ...(options.subject !== undefined ? { subject: options.subject } : {}),
      ...(options.tokenUrl !== undefined ? { tokenUrl: options.tokenUrl } : {}),
    };
  } else if (isTokenEndpointAuth(options)) {
    auth = {
      tokenEndpoint: options.tokenEndpoint as string,
      ...(options.credentials !== undefined ? { credentials: options.credentials } : {}),
      ...(options.headers !== undefined ? { headers: options.headers } : {}),
    };
  } else {
    auth = { getToken: options.getToken as CallbackAuth["getToken"] };
  }

  const timeoutMs = options.timeoutMs ?? 30_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new BeamConfigError("`timeoutMs` must be a positive number of milliseconds.");
  }
  const maxAttempts = options.maxAttempts ?? 3;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new BeamConfigError("`maxAttempts` must be an integer of at least 1.");
  }

  return {
    auth,
    scopes: options.scopes ?? [],
    controlUrl: (options.controlUrl ?? DEFAULT_CONTROL_URL).replace(/\/+$/, ""),
    logLevel: options.logLevel ?? (options.debug === true ? "debug" : "warn"),
    logSink: options.logSink,
    redactLogs: options.redactLogs ?? true,
    timeoutMs,
    maxAttempts,
    fetch: options.fetch,
  };
}
