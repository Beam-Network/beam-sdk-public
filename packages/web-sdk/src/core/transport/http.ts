/**
 * HTTP transport.
 *
 * Every control-plane call goes through here so that authentication, timeouts,
 * idempotency, retries, and error mapping are applied once rather than at each
 * call site.
 */

import type { TokenProvider } from "../auth/types.js";
import {
  BeamAbortError,
  BeamApiError,
  BeamAuthError,
  BeamNetworkError,
  BeamTimeoutError,
  asRemoteErrorCode,
  codeFromStatus,
} from "../errors/index.js";
import type { Logger } from "../logger/index.js";
import { silentLogger } from "../logger/index.js";
import { withRetry } from "../retry/index.js";

export interface HttpTransportOptions {
  baseUrl: string;
  tokenProvider?: TokenProvider;
  fetch?: typeof fetch;
  logger?: Logger;
  /** Per-request deadline. */
  timeoutMs?: number;
  /** Attempts per request, including the first. */
  maxAttempts?: number;
  defaultHeaders?: Record<string, string>;
}

export interface RequestOptions {
  method?: "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  headers?: Record<string, string>;
  signal?: AbortSignal;
  timeoutMs?: number;
  /**
   * Sends an `Idempotency-Key`. The coordinator requires one on room create,
   * invitation create, and join, and answers 409 if the same key arrives with a
   * different body — so the key is generated once per logical operation and
   * reused across retries of that operation, never regenerated per attempt.
   */
  idempotencyKey?: string;
  /** Set false for endpoints that legitimately take no credential. */
  authenticated?: boolean;
  /** Overrides `baseUrl`, for calls to a relay rather than the control plane. */
  baseUrl?: string;
  /** Skips JSON parsing and returns the raw Response. */
  raw?: boolean;
}

/** Generates an idempotency key. Prefers `randomUUID`, falls back to `getRandomValues`. */
export function newIdempotencyKey(prefix = "beam"): string {
  const cryptoRef = globalThis.crypto;
  if (typeof cryptoRef?.randomUUID === "function") return `${prefix}-${cryptoRef.randomUUID()}`;
  const bytes = new Uint8Array(16);
  cryptoRef.getRandomValues(bytes);
  return `${prefix}-${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

function joinUrl(baseUrl: string, path: string): string {
  if (/^https?:\/\//i.test(path)) return path;
  return `${baseUrl.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}

function buildUrl(baseUrl: string, options: RequestOptions): string {
  const url = new URL(
    joinUrl(options.baseUrl ?? baseUrl, options.path),
    globalThis.location?.href ?? "http://localhost",
  );
  for (const [key, value] of Object.entries(options.query ?? {})) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }
  return url.toString();
}

interface ErrorBody {
  error?: string;
  message?: string;
  code?: string;
  request_id?: string;
  requestId?: string;
}

async function toBeamError(response: Response, url: string): Promise<BeamApiError | BeamAuthError> {
  let body: ErrorBody = {};
  let text = "";
  try {
    text = await response.text();
    if (text) {
      const parsed: unknown = JSON.parse(text);
      if (parsed !== null && typeof parsed === "object") body = parsed as ErrorBody;
    }
  } catch {
    // Non-JSON error bodies are normal from proxies and gateways.
  }

  const message = body.error ?? body.message ?? (text ? text.slice(0, 200) : response.statusText);
  const requestId = body.request_id ?? body.requestId ?? response.headers.get("x-request-id") ?? undefined;
  const retryAfterHeader = response.headers.get("retry-after");
  const retryAfterSeconds = retryAfterHeader !== null ? Number(retryAfterHeader) : undefined;

  // A code in the body wins over the status, so a Beam-originated code keeps its
  // identity even when a broker in the middle picks a different status.
  const code = asRemoteErrorCode(body.code) ?? codeFromStatus(response.status);

  if (response.status === 401) {
    return new BeamAuthError(`Beam rejected the request as unauthenticated: ${message}`, {
      status: 401,
      requestId,
      code: "auth_expired",
      retryable: false,
    });
  }

  return new BeamApiError(`Beam request to ${new URL(url).pathname} failed (${response.status}): ${message}`, {
    code,
    status: response.status,
    requestId,
    ...(retryAfterSeconds !== undefined && Number.isFinite(retryAfterSeconds) ? { retryAfterSeconds } : {}),
  });
}

interface Deadline {
  signal: AbortSignal;
  /** Rejects when the deadline elapses. Never resolves. */
  expiry: Promise<never>;
  timedOut: () => boolean;
  dispose: () => void;
}

/**
 * Combines the caller's signal with a timeout.
 *
 * Returns both an `AbortSignal` and an `expiry` promise, and the caller races
 * the fetch against `expiry`. Aborting alone is not enough: a caller-supplied
 * `fetch` — a wrapper, an instrumented client, a test double — is not obliged to
 * honour the signal, and one that ignores it would otherwise hang the request
 * forever. Racing makes the deadline hold regardless of the implementation,
 * while the abort still lets a well-behaved fetch cancel the socket.
 *
 * `AbortSignal.any` is not yet universal, so composition is manual, and the
 * disposer keeps listeners off a long-lived caller signal.
 */
function withDeadline(timeoutMs: number, signal?: AbortSignal): Deadline {
  const controller = new AbortController();
  let timedOut = false;
  let onExpiry: (error: unknown) => void = () => undefined;

  const expiry = new Promise<never>((_, reject) => {
    onExpiry = reject;
  });
  // Nothing awaits `expiry` when the fetch wins the race; without this the
  // rejection would surface as unhandled.
  expiry.catch(() => undefined);

  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
    onExpiry(new BeamTimeoutError(`The request exceeded ${timeoutMs}ms.`));
  }, timeoutMs);

  const onAbort = (): void => {
    controller.abort();
    onExpiry(new BeamAbortError());
  };
  if (signal) {
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  }

  return {
    signal: controller.signal,
    expiry,
    timedOut: () => timedOut,
    dispose: () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    },
  };
}

export class HttpTransport {
  readonly baseUrl: string;
  readonly #fetch: typeof fetch;
  readonly #logger: Logger;
  readonly #tokenProvider: TokenProvider | undefined;
  readonly #timeoutMs: number;
  readonly #maxAttempts: number;
  readonly #defaultHeaders: Record<string, string>;

  constructor(options: HttpTransportOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    const fetchImpl = options.fetch ?? globalThis.fetch;
    if (typeof fetchImpl !== "function") {
      throw new BeamNetworkError("No fetch implementation is available in this environment.");
    }
    this.#fetch = fetchImpl.bind(globalThis);
    this.#logger = (options.logger ?? silentLogger).child("http");
    this.#tokenProvider = options.tokenProvider;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    this.#maxAttempts = options.maxAttempts ?? 3;
    this.#defaultHeaders = options.defaultHeaders ?? {};
  }

  async request<T>(options: RequestOptions): Promise<T> {
    const url = buildUrl(this.baseUrl, options);
    const method = options.method ?? "GET";
    const authenticated = options.authenticated ?? true;

    // Generated once for the whole operation. Reusing it across attempts is the
    // point: a retry after a timeout must not create a second room.
    const idempotencyKey =
      options.idempotencyKey ?? (method !== "GET" && method !== "DELETE" ? newIdempotencyKey() : undefined);

    let retriedAfterAuthFailure = false;

    const execute = async (attempt: number): Promise<T> => {
      const headers: Record<string, string> = {
        accept: "application/json",
        ...this.#defaultHeaders,
        ...options.headers,
      };
      if (options.body !== undefined && headers["content-type"] === undefined) {
        headers["content-type"] = "application/json";
      }
      if (idempotencyKey !== undefined) headers["Idempotency-Key"] = idempotencyKey;

      if (authenticated && this.#tokenProvider) {
        const token = await this.#tokenProvider.getToken({ ...(options.signal ? { signal: options.signal } : {}) });
        headers["authorization"] = `Bearer ${token.value}`;
      }

      const deadline = withDeadline(options.timeoutMs ?? this.#timeoutMs, options.signal);
      this.#logger.debug(`${method} ${url}`, { attempt });

      let response: Response;
      try {
        response = await Promise.race([
          this.#fetch(url, {
            method,
            headers,
            signal: deadline.signal,
            cache: "no-store",
            ...(options.body !== undefined
              ? { body: typeof options.body === "string" ? options.body : JSON.stringify(options.body) }
              : {}),
          }),
          deadline.expiry,
        ]);
      } catch (cause) {
        if (cause instanceof BeamTimeoutError || cause instanceof BeamAbortError) throw cause;
        if (deadline.timedOut()) {
          throw new BeamTimeoutError(`${method} ${url} exceeded ${options.timeoutMs ?? this.#timeoutMs}ms.`, cause);
        }
        if (options.signal?.aborted) throw new BeamAbortError();
        // fetch rejects opaquely for DNS, TLS, offline, and CORS alike. The
        // message names CORS because that is overwhelmingly the cause when a
        // browser integration fails against a correctly-running Beam endpoint.
        throw new BeamNetworkError(
          `${method} ${url} could not be completed. Check connectivity and that the endpoint allows this origin (CORS).`,
          cause,
        );
      } finally {
        deadline.dispose();
      }

      if (!response.ok) {
        const error = await toBeamError(response, url);

        // One re-mint on 401: tokens are short-lived, so a token that was valid
        // when the request left can legitimately expire in flight.
        if (response.status === 401 && authenticated && this.#tokenProvider && !retriedAfterAuthFailure) {
          retriedAfterAuthFailure = true;
          this.#logger.debug("token rejected, re-minting once");
          this.#tokenProvider.invalidate();
          return execute(attempt);
        }
        throw error;
      }

      if (options.raw === true) return response as unknown as T;
      if (response.status === 204 || response.headers.get("content-length") === "0") return undefined as T;

      const text = await response.text();
      if (!text) return undefined as T;
      try {
        return JSON.parse(text) as T;
      } catch (cause) {
        throw new BeamApiError(`Beam returned a non-JSON body for ${method} ${url}.`, {
          code: "protocol",
          status: response.status,
          cause,
        });
      }
    };

    return withRetry(execute, {
      maxAttempts: this.#maxAttempts,
      ...(options.signal ? { signal: options.signal } : {}),
      onRetry: (error, attempt, delayMs) => this.#logger.debug(`retrying in ${delayMs}ms`, { attempt, error }),
    });
  }

  get<T>(path: string, options: Omit<RequestOptions, "path" | "method" | "body"> = {}): Promise<T> {
    return this.request<T>({ ...options, path, method: "GET" });
  }

  post<T>(path: string, body?: unknown, options: Omit<RequestOptions, "path" | "method" | "body"> = {}): Promise<T> {
    return this.request<T>({ ...options, path, method: "POST", ...(body !== undefined ? { body } : {}) });
  }

  delete<T>(path: string, options: Omit<RequestOptions, "path" | "method" | "body"> = {}): Promise<T> {
    return this.request<T>({ ...options, path, method: "DELETE" });
  }

  /** A transport pointed at a different origin, sharing this one's auth and logging. */
  withBaseUrl(baseUrl: string): HttpTransport {
    return new HttpTransport({
      baseUrl,
      fetch: this.#fetch,
      logger: this.#logger,
      timeoutMs: this.#timeoutMs,
      maxAttempts: this.#maxAttempts,
      defaultHeaders: this.#defaultHeaders,
      ...(this.#tokenProvider ? { tokenProvider: this.#tokenProvider } : {}),
    });
  }
}
