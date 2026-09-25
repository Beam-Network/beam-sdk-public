/**
 * The token provider.
 *
 * One job: hand out a currently-valid access token, refreshing ahead of expiry
 * and never letting two refreshes run at once.
 */

import { BeamAuthError, BeamConfigError, BeamNetworkError, codeFromStatus } from "../errors/index.js";
import type { Logger } from "../logger/index.js";
import { silentLogger } from "../logger/index.js";
import { isTokenStale, isTokenUsable, normalizeTokenResponse } from "./token.js";
import type { BeamScope, BeamToken, BeamTokenResponse, TokenProvider } from "./types.js";

/** Mints a token. Everything else in this file is caching around this. */
export type TokenMinter = (options: { signal?: AbortSignal }) => Promise<BeamTokenResponse>;

export interface TokenProviderOptions {
  mint: TokenMinter;
  logger?: Logger;
  /** Injected for deterministic tests. */
  now?: () => number;
}

export class CachingTokenProvider implements TokenProvider {
  readonly #mint: TokenMinter;
  readonly #logger: Logger;
  readonly #now: () => number;

  #token: BeamToken | undefined;
  #issuedAt = 0;
  /**
   * The in-flight mint, shared by every concurrent caller.
   *
   * Without this, opening a room and starting a transfer in the same tick would
   * mint two tokens and burn two rate-limited broker calls for one session.
   */
  #inFlight: Promise<BeamToken> | undefined;

  constructor(options: TokenProviderOptions) {
    this.#mint = options.mint;
    this.#logger = options.logger ?? silentLogger;
    this.#now = options.now ?? Date.now;
  }

  peek(): BeamToken | undefined {
    return this.#token;
  }

  invalidate(): void {
    this.#logger.debug("token invalidated");
    this.#token = undefined;
    this.#issuedAt = 0;
  }

  async getToken(options: { signal?: AbortSignal } = {}): Promise<BeamToken> {
    const current = this.#token;
    if (current && !isTokenStale(current, this.#now(), this.#issuedAt)) return current;

    // A stale-but-usable token is still returned if a refresh is already running
    // and this caller would otherwise queue behind it for no benefit.
    if (this.#inFlight) {
      if (current && isTokenUsable(current, this.#now())) return current;
      return this.#inFlight;
    }

    this.#inFlight = this.#refresh(options.signal).finally(() => {
      this.#inFlight = undefined;
    });
    return this.#inFlight;
  }

  async #refresh(signal?: AbortSignal): Promise<BeamToken> {
    this.#logger.debug("minting access token");
    const response = await this.#mint({ signal });
    if (typeof response?.access_token !== "string" || response.access_token.length === 0) {
      throw new BeamAuthError("The token endpoint did not return an access_token.");
    }
    const now = this.#now();
    const token = normalizeTokenResponse(response, now);
    if (!isTokenUsable(token, now)) {
      throw new BeamAuthError("The token endpoint returned an already-expired token. Check server clock skew.");
    }
    this.#token = token;
    this.#issuedAt = now;
    this.#logger.debug("access token acquired", {
      scopes: token.scopes,
      expiresInMs: token.expiresAt - now,
    });
    return token;
  }
}

/** Options shared by the HTTP-based minters. */
interface HttpMintOptions {
  fetch: typeof fetch;
  /** Extra fields merged into the request body, e.g. requested scopes. */
  body?: Record<string, unknown>;
  headers?: Record<string, string>;
  /** Sent as `credentials` — `include` lets a broker authenticate by session cookie. */
  credentials?: RequestCredentials;
}

async function requestToken(url: string, options: HttpMintOptions, signal?: AbortSignal): Promise<BeamTokenResponse> {
  let response: Response;
  try {
    response = await options.fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json", ...options.headers },
      body: JSON.stringify(options.body ?? {}),
      credentials: options.credentials ?? "same-origin",
      cache: "no-store",
      ...(signal ? { signal } : {}),
    });
  } catch (cause) {
    throw new BeamNetworkError(`Could not reach the Beam token endpoint at ${url}.`, cause);
  }

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    const code = codeFromStatus(response.status);
    throw new BeamAuthError(`Token request failed (${response.status}): ${detail}`, {
      status: response.status,
      code: code === "auth_failed" ? "auth_failed" : "auth_failed",
      retryable: response.status >= 500 || response.status === 429,
    });
  }

  try {
    return (await response.json()) as BeamTokenResponse;
  } catch (cause) {
    throw new BeamAuthError("The token endpoint returned a body that is not JSON.", { cause });
  }
}

async function readErrorDetail(response: Response): Promise<string> {
  try {
    const text = await response.text();
    if (!text) return response.statusText;
    try {
      const parsed: unknown = JSON.parse(text);
      if (parsed !== null && typeof parsed === "object") {
        const record = parsed as Record<string, unknown>;
        const message = record["error_description"] ?? record["error"] ?? record["message"];
        if (typeof message === "string") return message;
      }
    } catch {
      // Non-JSON body; fall through to the raw text.
    }
    return text.slice(0, 200);
  } catch {
    return response.statusText;
  }
}

/**
 * Mints against a customer-operated broker.
 *
 * The broker decides who the caller is — usually from its own session cookie —
 * and which scopes they may hold. The SDK sends only the scopes it wants; the
 * broker is free to narrow them and is never obliged to honour the request.
 */
export function brokerMinter(url: string, options: HttpMintOptions & { scopes?: readonly BeamScope[] }): TokenMinter {
  return ({ signal }) =>
    requestToken(
      url,
      {
        ...options,
        // A broker keyed on the caller's login session needs the cookie.
        credentials: options.credentials ?? "same-origin",
        body: { ...(options.scopes ? { scopes: options.scopes } : {}), ...options.body },
      },
      signal,
    );
}

/**
 * Mints against Beam's hosted token endpoint using a publishable client key.
 *
 * This is the console-issued embed path — the customer pastes a snippet and
 * ships no backend. The key is public by design; it is safe only because Beam
 * binds it to an origin allow-list and issues nothing but short-lived, narrowly
 * scoped tokens against it. It is not, and must never become, an API key.
 */
export function clientKeyMinter(
  url: string,
  clientKey: string,
  options: HttpMintOptions & { scopes?: readonly BeamScope[]; subject?: string },
): TokenMinter {
  if (!/^bm_pub_/.test(clientKey)) {
    throw new BeamConfigError(
      `clientKey must be a publishable key beginning with "bm_pub_". ` +
        `Secret keys (bm_live_…, b1m_…) grant credit-spending access and must never appear in browser code.`,
    );
  }
  return ({ signal }) =>
    requestToken(
      url,
      {
        ...options,
        // Cross-origin to api.b1m.ai and intentionally cookie-less: the client
        // key is the only credential, so there is nothing for CSRF to abuse.
        credentials: "omit",
        body: {
          client_key: clientKey,
          ...(options.scopes ? { scopes: options.scopes } : {}),
          ...(options.subject !== undefined ? { subject: options.subject } : {}),
          ...options.body,
        },
      },
      signal,
    );
}

/** Wraps a caller-supplied function, for apps that already have their own fetch stack. */
export function callbackMinter(getToken: (signal?: AbortSignal) => Promise<string | BeamTokenResponse>): TokenMinter {
  return async ({ signal }) => {
    const result = await getToken(signal);
    return typeof result === "string" ? { access_token: result } : result;
  };
}
