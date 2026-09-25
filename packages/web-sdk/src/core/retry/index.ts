/**
 * Backoff and reconnection.
 *
 * Shared by the SSE room-event stream, WHIP/WHEP renegotiation, and the transfer
 * progress stream, so all three recover with the same, testable policy.
 */

import { BeamAbortError, BeamError } from "../errors/index.js";

export interface BackoffOptions {
  /** Delay before the first retry. */
  initialDelayMs?: number;
  /** Ceiling for a single delay. */
  maxDelayMs?: number;
  /** Multiplier applied per consecutive failure. */
  factor?: number;
  /** Maximum attempts, counting the first try. `Infinity` for reconnect loops. */
  maxAttempts?: number;
  /** Injected for deterministic tests. Must return [0, 1). */
  random?: () => number;
}

const DEFAULTS: Required<Omit<BackoffOptions, "random">> & { random: () => number } = {
  initialDelayMs: 500,
  maxDelayMs: 30_000,
  factor: 2,
  maxAttempts: 4,
  random: Math.random,
};

/**
 * Full jitter: `random() * min(max, initial * factor^attempt)`.
 *
 * Full rather than equal jitter because reconnects here are correlated — a relay
 * restart drops every participant in a room at once, and they must not return in
 * a synchronized wave.
 */
export function backoffDelay(attempt: number, options: BackoffOptions = {}): number {
  const { initialDelayMs, maxDelayMs, factor, random } = { ...DEFAULTS, ...options };
  const ceiling = Math.min(maxDelayMs, initialDelayMs * factor ** Math.max(0, attempt));
  return Math.floor(random() * ceiling);
}

/** Rejects with `BeamAbortError` if the signal fires first. */
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new BeamAbortError());
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(new BeamAbortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export interface RetryOptions extends BackoffOptions {
  signal?: AbortSignal;
  /** Defaults to retrying `BeamError`s flagged retryable. */
  shouldRetry?: (error: unknown, attempt: number) => boolean;
  onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
}

function defaultShouldRetry(error: unknown): boolean {
  return error instanceof BeamError && error.retryable;
}

/**
 * Runs `operation`, retrying on retryable failures with jittered backoff.
 *
 * A server-supplied `Retry-After` overrides the computed delay: the server knows
 * its own recovery window better than the client's curve does.
 */
export async function withRetry<T>(operation: (attempt: number) => Promise<T>, options: RetryOptions = {}): Promise<T> {
  const { maxAttempts } = { ...DEFAULTS, ...options };
  const shouldRetry = options.shouldRetry ?? defaultShouldRetry;
  let attempt = 0;

  for (;;) {
    if (options.signal?.aborted) throw new BeamAbortError();
    try {
      return await operation(attempt);
    } catch (error) {
      attempt += 1;
      if (attempt >= maxAttempts || !shouldRetry(error, attempt)) throw error;

      const retryAfter = error instanceof BeamError ? error.retryAfterSeconds : undefined;
      const waitMs = retryAfter !== undefined ? retryAfter * 1000 : backoffDelay(attempt - 1, options);
      options.onRetry?.(error, attempt, waitMs);
      await delay(waitMs, options.signal);
    }
  }
}

/** True when the browser reports itself offline. Unknown environments count as online. */
export function isOffline(): boolean {
  return typeof navigator !== "undefined" && navigator.onLine === false;
}

/**
 * Resolves once the browser is back online, or immediately if it already is.
 *
 * Reconnecting while `navigator.onLine` is false burns attempts against a
 * network that cannot possibly answer, so long-lived streams gate on this before
 * each attempt.
 */
export function whenOnline(signal?: AbortSignal): Promise<void> {
  if (!isOffline()) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const cleanup = (): void => {
      globalThis.removeEventListener?.("online", onOnline);
      signal?.removeEventListener("abort", onAbort);
    };
    const onOnline = (): void => {
      cleanup();
      resolve();
    };
    const onAbort = (): void => {
      cleanup();
      reject(new BeamAbortError());
    };
    globalThis.addEventListener?.("online", onOnline, { once: true });
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export interface ReconnectorOptions extends BackoffOptions {
  signal?: AbortSignal;
  onStateChange?: (state: ReconnectState) => void;
  logger?: { debug(message: string, context?: unknown): void };
  /**
   * Whether a failure is worth another attempt. Defaults to Beam's retryability
   * flag, so a 403 or 404 stops the loop instead of hammering an endpoint that
   * will never answer.
   */
  shouldRetry?: (error: unknown) => boolean;
}

export type ReconnectState = "connecting" | "connected" | "reconnecting" | "closed";

/**
 * Drives a long-lived connection: run `connect`, and when it ends, wait for the
 * network and a jittered delay, then run it again — until the caller aborts.
 *
 * `connect` is expected to resolve when the connection closes cleanly and to
 * reject when it fails. A clean close still reconnects; only an abort stops the
 * loop, which is what a room participant expects from a dropped relay.
 */
export class Reconnector {
  #state: ReconnectState = "closed";
  #attempt = 0;
  readonly #options: ReconnectorOptions;

  constructor(options: ReconnectorOptions = {}) {
    this.#options = options;
  }

  get state(): ReconnectState {
    return this.#state;
  }

  #setState(state: ReconnectState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.#options.onStateChange?.(state);
  }

  /** Marks the connection healthy and resets backoff. Call on first byte, not on socket open. */
  markConnected(): void {
    this.#attempt = 0;
    this.#setState("connected");
  }

  /**
   * Runs `connect` until the caller aborts, or until reconnecting is pointless.
   *
   * Resolves on a clean stop (abort). Rejects with the last error when the loop
   * gives up on a failure — either because the error is not retryable or because
   * `maxAttempts` is exhausted. That distinction is what lets a caller fall back
   * to another strategy instead of waiting on a stream that will never arrive.
   */
  async run(connect: (signal: AbortSignal | undefined) => Promise<void>): Promise<void> {
    const signal = this.#options.signal;
    const maxAttempts = this.#options.maxAttempts ?? Number.POSITIVE_INFINITY;
    const shouldRetry =
      this.#options.shouldRetry ?? ((error: unknown) => !(error instanceof BeamError) || error.retryable);
    this.#setState("connecting");

    let lastError: unknown;
    try {
      for (;;) {
        if (signal?.aborted) return;
        lastError = undefined;
        try {
          await connect(signal);
        } catch (error) {
          if (signal?.aborted || error instanceof BeamAbortError) return;
          this.#options.logger?.debug("connection attempt failed", error);
          lastError = error;
          if (!shouldRetry(error)) throw error;
        }
        if (signal?.aborted) return;

        this.#setState("reconnecting");
        const waitMs = backoffDelay(this.#attempt, this.#options);
        this.#attempt += 1;
        // Counts attempts, not consecutive failures: a stream that connects and
        // immediately dies is as unusable as one that never connects.
        if (this.#attempt >= maxAttempts && lastError !== undefined) throw lastError;
        if (this.#attempt >= maxAttempts) return;

        try {
          await whenOnline(signal);
          await delay(waitMs, signal);
        } catch {
          return;
        }
      }
    } finally {
      this.#setState("closed");
    }
  }
}
