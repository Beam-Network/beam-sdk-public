/**
 * Server-sent events with resume.
 *
 * Used for relay room events and for broker-relayed transfer progress. Both
 * sources support `Last-Event-ID`, so a dropped connection resumes from the last
 * delivered event instead of replaying or silently skipping.
 *
 * `EventSource` is deliberately not used: it cannot send an `Authorization`
 * header, and every stream here is authenticated. A `fetch` body reader gives
 * headers, resume, and a real `AbortSignal`.
 */

import { BeamAbortError, BeamNetworkError, BeamUnsupportedError } from "../errors/index.js";
import type { Logger } from "../logger/index.js";
import { silentLogger } from "../logger/index.js";
import { Reconnector } from "../retry/index.js";
import type { ReconnectState } from "../retry/index.js";
import type { HttpTransport } from "./http.js";

export interface SseEvent {
  /** The `event:` field, or `message` when absent, matching the SSE default. */
  type: string;
  data: string;
  id: string | undefined;
}

export interface SseStreamOptions {
  transport: HttpTransport;
  path: string;
  baseUrl?: string;
  query?: Record<string, string | number | boolean | undefined>;
  authenticated?: boolean;
  logger?: Logger;
  signal?: AbortSignal;
  onEvent: (event: SseEvent) => void;
  onStateChange?: (state: ReconnectState) => void;
  /** Server-suggested reconnect delay floor, from the stream's `retry:` field. */
  initialDelayMs?: number;
  /**
   * Reconnect attempts before giving up and rejecting `start()`. Defaults to
   * unlimited, which is right for a room a participant is sitting in. Callers
   * with a fallback — polling, say — pass a finite number so they can use it.
   */
  maxAttempts?: number;
}

/**
 * Incremental SSE frame parser.
 *
 * Kept separate from the transport so it can be unit-tested against split
 * chunks — the failure mode this guards against is a frame arriving across two
 * network reads, which a naive split-on-blank-line parser drops.
 */
export class SseParser {
  #buffer = "";

  /** Feeds a chunk and returns whatever complete events it completed. */
  push(chunk: string): SseEvent[] {
    this.#buffer += chunk;
    const events: SseEvent[] = [];

    // Frames are separated by a blank line; normalize CRLF and lone CR first.
    const normalized = this.#buffer.replace(/\r\n|\r/g, "\n");
    const frames = normalized.split("\n\n");
    // The trailing element is either an incomplete frame or "".
    this.#buffer = frames.pop() ?? "";

    for (const frame of frames) {
      const event = this.#parseFrame(frame);
      if (event) events.push(event);
    }
    return events;
  }

  #parseFrame(frame: string): SseEvent | undefined {
    let type = "message";
    let id: string | undefined;
    const dataLines: string[] = [];

    for (const line of frame.split("\n")) {
      if (line === "" || line.startsWith(":")) continue; // comment / keep-alive
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      // A single leading space after the colon is part of the framing, not data.
      const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");

      if (field === "event") type = value;
      else if (field === "data") dataLines.push(value);
      else if (field === "id") id = value;
    }

    if (dataLines.length === 0) return undefined;
    return { type, data: dataLines.join("\n"), id };
  }
}

/**
 * A reconnecting SSE subscription.
 *
 * Reconnection is driven by `Reconnector`, so it shares the SDK's jitter,
 * offline gating, and abort semantics with every other long-lived connection.
 */
export class SseStream {
  readonly #options: SseStreamOptions;
  readonly #logger: Logger;
  readonly #controller = new AbortController();
  #lastEventId: string | undefined;
  #reconnector: Reconnector | undefined;

  constructor(options: SseStreamOptions) {
    this.#options = options;
    this.#logger = (options.logger ?? silentLogger).child("sse");
  }

  get lastEventId(): string | undefined {
    return this.#lastEventId;
  }

  /** Resolves when the stream is closed for good (abort), not on each drop. */
  async start(): Promise<void> {
    const external = this.#options.signal;
    if (external) {
      if (external.aborted) return;
      external.addEventListener("abort", () => this.close(), { once: true });
    }

    this.#reconnector = new Reconnector({
      signal: this.#controller.signal,
      logger: this.#logger,
      maxAttempts: this.#options.maxAttempts ?? Number.POSITIVE_INFINITY,
      ...(this.#options.initialDelayMs !== undefined ? { initialDelayMs: this.#options.initialDelayMs } : {}),
      ...(this.#options.onStateChange ? { onStateChange: this.#options.onStateChange } : {}),
    });

    await this.#reconnector.run((signal) => this.#connectOnce(signal));
  }

  close(): void {
    if (!this.#controller.signal.aborted) this.#controller.abort();
  }

  async #connectOnce(signal: AbortSignal | undefined): Promise<void> {
    const headers: Record<string, string> = { accept: "text/event-stream" };
    if (this.#lastEventId !== undefined) headers["Last-Event-ID"] = this.#lastEventId;

    const response = await this.#options.transport.request<Response>({
      method: "GET",
      path: this.#options.path,
      raw: true,
      headers,
      // Streams have no completion deadline; the reconnector owns liveness.
      timeoutMs: 0x7fffffff,
      ...(this.#options.baseUrl !== undefined ? { baseUrl: this.#options.baseUrl } : {}),
      ...(this.#options.query !== undefined ? { query: this.#options.query } : {}),
      ...(this.#options.authenticated !== undefined ? { authenticated: this.#options.authenticated } : {}),
      ...(signal ? { signal } : {}),
    });

    const body = response.body;
    if (!body) throw new BeamUnsupportedError("This environment does not expose streaming response bodies.");

    this.#reconnector?.markConnected();
    const reader = body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return; // Clean server close; the reconnector decides what next.
        for (const event of parser.push(decoder.decode(value, { stream: true }))) {
          // Recorded before dispatch so a throwing handler cannot cause the same
          // event to be replayed after a reconnect.
          if (event.id !== undefined) this.#lastEventId = event.id;
          this.#options.onEvent(event);
        }
      }
    } catch (cause) {
      if (signal?.aborted) throw new BeamAbortError();
      throw new BeamNetworkError("The event stream was interrupted.", cause);
    } finally {
      reader.cancel().catch(() => undefined);
    }
  }
}
