/**
 * The Beam client.
 *
 * Owns the token provider and the control-plane transport, and exposes the
 * capability namespaces. Constructing it performs no network calls: the first
 * token is minted lazily on the first operation, so creating a client during
 * app bootstrap costs nothing and cannot fail on a flaky network.
 */

import { Broadcast } from "./broadcast/index.js";
import type { TokenMinter } from "./core/auth/index.js";
import { CachingTokenProvider, brokerMinter, callbackMinter, clientKeyMinter } from "./core/auth/index.js";
import type { BeamToken, TokenProvider } from "./core/auth/types.js";
import { BeamNetworkError } from "./core/errors/index.js";
import type { Logger } from "./core/logger/index.js";
import { createLogger } from "./core/logger/index.js";
import { HttpTransport } from "./core/transport/index.js";
import { DEFAULT_TOKEN_URL, resolveConfig } from "./config.js";
import type { BeamOptions, ResolvedConfig } from "./config.js";
import { media } from "./media/index.js";
import { Rooms } from "./rooms/index.js";
import { Transfers } from "./transfers/index.js";

export class Beam {
  readonly #config: ResolvedConfig;
  readonly #logger: Logger;
  readonly #tokenProvider: TokenProvider;
  readonly #control: HttpTransport;

  /** Rooms and live media. */
  readonly rooms: Rooms;
  /** One-to-many publish and watch. */
  readonly broadcast: Broadcast;
  /** Data movement between storage endpoints. */
  readonly transfers: Transfers;
  /** Camera, microphone, and screen capture helpers. */
  readonly media = media;

  constructor(options: BeamOptions) {
    this.#config = resolveConfig(options);
    this.#logger = createLogger({
      level: this.#config.logLevel,
      redact: this.#config.redactLogs,
      ...(this.#config.logSink ? { sink: this.#config.logSink } : {}),
    });

    const fetchImpl = this.#config.fetch ?? globalThis.fetch;
    if (typeof fetchImpl !== "function") {
      throw new BeamNetworkError(
        "No fetch implementation was found. Pass one as `fetch` when running outside a modern browser.",
      );
    }
    const boundFetch = fetchImpl.bind(globalThis);

    this.#tokenProvider = new CachingTokenProvider({
      mint: this.#createMinter(boundFetch),
      logger: this.#logger,
    });

    this.#control = new HttpTransport({
      baseUrl: this.#config.controlUrl,
      tokenProvider: this.#tokenProvider,
      fetch: boundFetch,
      logger: this.#logger,
      timeoutMs: this.#config.timeoutMs,
      maxAttempts: this.#config.maxAttempts,
    });

    this.rooms = new Rooms({ control: this.#control, logger: this.#logger });
    this.broadcast = new Broadcast({ control: this.#control, fetch: boundFetch, logger: this.#logger });
    this.transfers = new Transfers({ control: this.#control, logger: this.#logger });

    this.#logger.debug("Beam client ready", { controlUrl: this.#config.controlUrl, scopes: this.#config.scopes });
  }

  #createMinter(fetchImpl: typeof fetch): TokenMinter {
    const auth = this.#config.auth;
    const scopes = this.#config.scopes;

    if ("clientKey" in auth) {
      return clientKeyMinter(auth.tokenUrl ?? DEFAULT_TOKEN_URL, auth.clientKey, {
        fetch: fetchImpl,
        ...(scopes.length > 0 ? { scopes } : {}),
        ...(auth.subject !== undefined ? { subject: auth.subject } : {}),
      });
    }
    if ("tokenEndpoint" in auth) {
      return brokerMinter(auth.tokenEndpoint, {
        fetch: fetchImpl,
        ...(scopes.length > 0 ? { scopes } : {}),
        ...(auth.credentials !== undefined ? { credentials: auth.credentials } : {}),
        ...(auth.headers !== undefined ? { headers: auth.headers } : {}),
      });
    }
    return callbackMinter(auth.getToken);
  }

  /**
   * Mints a token now rather than on first use.
   *
   * Worth calling when the app can predict an imminent join: it moves a token
   * round trip off the critical path, and surfaces a misconfigured key at
   * startup instead of when a user clicks a button.
   */
  async connect(options: { signal?: AbortSignal } = {}): Promise<BeamToken> {
    return this.#tokenProvider.getToken(options);
  }

  /** Scopes the current token carries, or undefined before the first mint. */
  get scopes(): readonly string[] | undefined {
    return this.#tokenProvider.peek()?.scopes;
  }

  /** Discards the cached token. The next call mints a fresh one. */
  invalidateToken(): void {
    this.#tokenProvider.invalidate();
  }

  /** Escape hatch: an authenticated transport for Beam endpoints the SDK does not model. */
  get transport(): HttpTransport {
    return this.#control;
  }
}

/** Functional alias, for codebases that prefer factories over `new`. */
export function createBeam(options: BeamOptions): Beam {
  return new Beam(options);
}
