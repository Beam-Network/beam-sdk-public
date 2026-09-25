/**
 * A live transfer handle.
 *
 * Watches one transfer through the control plane's event stream and exposes it
 * as events, an async iterable, and a promise.
 */

import { BeamAbortError, BeamApiError, BeamError, BeamTimeoutError } from "../core/errors/index.js";
import { EventEmitter, eventsToAsyncIterable } from "../core/events/index.js";
import type { Logger } from "../core/logger/index.js";
import type { HttpTransport } from "../core/transport/index.js";
import { SseStream } from "../core/transport/index.js";
import type {
  Transfer,
  TransferEvents,
  TransferHandle,
  TransferId,
  TransferProgress,
  TransferStatus,
} from "./types.js";
import { TERMINAL_STATUSES } from "./types.js";

/** Transfer as the control plane returns it. */
export interface WireTransfer {
  id: string;
  status: string;
  name?: string;
  created_at?: string;
  createdAt?: string;
  progress?: WireProgress;
  error?: { code?: string; message?: string };
}

interface WireProgress {
  status?: string;
  bytes_completed?: number;
  bytesCompleted?: number;
  total_bytes?: number | null;
  totalBytes?: number | null;
  destinations_completed?: number;
  destinationsCompleted?: number;
  destinations_total?: number | null;
  destinationsTotal?: number | null;
  updated_at?: string;
  updatedAt?: string;
}

const KNOWN_STATUSES: ReadonlySet<string> = new Set<TransferStatus>([
  "pending",
  "preparing",
  "running",
  "completed",
  "failed",
  "cancelled",
]);

function toStatus(value: unknown): TransferStatus {
  // An unrecognized status is treated as `running` rather than thrown on: a
  // newer Beam release adding a lifecycle state must not break existing pages,
  // and every unknown state is by definition non-terminal.
  return typeof value === "string" && KNOWN_STATUSES.has(value) ? (value as TransferStatus) : "running";
}

export function toProgress(wire: WireProgress | undefined, status: TransferStatus): TransferProgress | undefined {
  if (!wire) return undefined;
  const bytesCompleted = wire.bytes_completed ?? wire.bytesCompleted ?? 0;
  const totalRaw = wire.total_bytes ?? wire.totalBytes;
  const totalBytes = typeof totalRaw === "number" && totalRaw > 0 ? totalRaw : undefined;
  const destinationsTotalRaw = wire.destinations_total ?? wire.destinationsTotal;

  return {
    status: toStatus(wire.status ?? status),
    bytesCompleted,
    totalBytes,
    // Clamped because a destination fan-out can report more delivered bytes than
    // the logical source size, and a progress bar past 100% reads as a bug.
    percent: totalBytes === undefined ? undefined : Math.min(100, (bytesCompleted / totalBytes) * 100),
    destinationsCompleted: wire.destinations_completed ?? wire.destinationsCompleted ?? 0,
    destinationsTotal: typeof destinationsTotalRaw === "number" ? destinationsTotalRaw : undefined,
    updatedAt: wire.updated_at ?? wire.updatedAt ?? new Date().toISOString(),
  };
}

export function toTransfer(wire: WireTransfer): Transfer {
  const status = toStatus(wire.status);
  const progress = toProgress(wire.progress, status);
  return {
    id: wire.id as TransferId,
    status,
    ...(wire.name !== undefined ? { name: wire.name } : {}),
    createdAt: wire.created_at ?? wire.createdAt ?? new Date().toISOString(),
    ...(progress ? { progress } : {}),
    ...(wire.error
      ? { error: { code: wire.error.code ?? "internal", message: wire.error.message ?? "The transfer failed." } }
      : {}),
  };
}

export class BeamTransferHandle extends EventEmitter<TransferEvents> implements TransferHandle {
  readonly id: TransferId;
  #transfer: Transfer;
  #stream: SseStream | undefined;
  #closed = false;

  /**
   * Resolved when the transfer reaches a terminal status. Created eagerly so a
   * caller who attaches `done()` late still observes a completion that happened
   * before they attached.
   */
  readonly #settled: Promise<Transfer>;
  #resolve!: (transfer: Transfer) => void;
  #reject!: (error: unknown) => void;

  readonly #control: HttpTransport;
  readonly #logger: Logger;

  constructor(deps: { transfer: Transfer; control: HttpTransport; logger: Logger }) {
    super((error, event) => deps.logger.warn(`a "${event}" listener threw`, error));
    this.id = deps.transfer.id;
    this.#transfer = deps.transfer;
    this.#control = deps.control;
    this.#logger = deps.logger.child(`transfer:${deps.transfer.id}`);

    this.#settled = new Promise<Transfer>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    // Nothing may await this promise; without a no-op handler a rejection would
    // surface as an unhandled rejection and, in some hosts, kill the page.
    this.#settled.catch(() => undefined);

    if (TERMINAL_STATUSES.has(deps.transfer.status)) this.#settle(deps.transfer);
    else this.#watch();
  }

  get status(): TransferStatus {
    return this.#transfer.status;
  }

  get lastProgress(): TransferProgress | undefined {
    return this.#transfer.progress;
  }

  get snapshot(): Transfer {
    return this.#transfer;
  }

  #watch(): void {
    this.#stream = new SseStream({
      transport: this.#control,
      path: `/transfers/${encodeURIComponent(this.id)}/events`,
      logger: this.#logger,
      // Bounded, unlike a room's event stream: polling is a real fallback here,
      // so a stream that will not stay up should surrender to it rather than
      // retry forever while the caller sees no progress at all.
      maxAttempts: 3,
      initialDelayMs: 250,
      onEvent: (event) => this.#handleEvent(event.data),
    });

    void this.#stream.start().catch((error: unknown) => {
      if (this.#closed) return;
      // A dropped stream is not a failed transfer. Report it, then fall back to
      // polling so a proxy that refuses to stream cannot strand the caller.
      this.#logger.debug("progress stream ended, falling back to polling", error);
      this.emit("error", error instanceof Error ? error : new Error(String(error)));
      void this.#pollUntilTerminal();
    });
  }

  #handleEvent(raw: string): void {
    let parsed: WireTransfer;
    try {
      parsed = JSON.parse(raw) as WireTransfer;
    } catch (error) {
      this.#logger.debug("discarding malformed transfer event", { raw: raw.slice(0, 200), error });
      return;
    }
    this.#apply(toTransfer({ ...parsed, id: parsed.id ?? this.id }));
  }

  #apply(next: Transfer): void {
    const statusChanged = next.status !== this.#transfer.status;
    this.#transfer = next;

    if (next.progress) this.emit("progress", next.progress);
    if (statusChanged) this.emit("status", next.status);
    if (TERMINAL_STATUSES.has(next.status)) this.#settle(next);
  }

  #settle(transfer: Transfer): void {
    if (this.#closed) return;
    this.#stream?.close();
    this.#closed = true;

    if (transfer.status === "completed") {
      this.emit("completed", transfer);
      this.#resolve(transfer);
      return;
    }
    const message =
      transfer.error?.message ??
      (transfer.status === "cancelled" ? "The transfer was cancelled." : "The transfer failed.");
    this.#reject(
      new BeamApiError(message, {
        code: transfer.status === "cancelled" ? "aborted" : "internal",
        retryable: false,
      }),
    );
  }

  /** Polling fallback for when the event stream is unavailable end to end. */
  async #pollUntilTerminal(intervalMs = 5_000): Promise<void> {
    while (!this.#closed) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
      if (this.#closed) return;
      try {
        this.#apply(await this.refresh());
      } catch (error) {
        this.#logger.debug("status poll failed", error);
      }
    }
  }

  async refresh(): Promise<Transfer> {
    const wire = await this.#control.get<WireTransfer>(`/transfers/${encodeURIComponent(this.id)}`);
    const transfer = toTransfer(wire);
    this.#transfer = transfer;
    return transfer;
  }

  progress(options: { signal?: AbortSignal } = {}): AsyncIterableIterator<TransferProgress> {
    return eventsToAsyncIterable<TransferEvents, "progress">(this, "progress", {
      ...(options.signal ? { signal: options.signal } : {}),
      // Ends the iteration when the transfer settles, either way. Without the
      // catch, a failed transfer would reject the iterator as well as `done()`,
      // producing a second unhandled rejection for the same failure.
      until: () => this.#settled.catch(() => undefined),
    });
  }

  async done(options: { signal?: AbortSignal; timeoutMs?: number } = {}): Promise<Transfer> {
    const races: Array<Promise<Transfer>> = [this.#settled];

    if (options.signal) {
      const signal = options.signal;
      races.push(
        new Promise<Transfer>((_, reject) => {
          if (signal.aborted) reject(new BeamAbortError());
          else signal.addEventListener("abort", () => reject(new BeamAbortError()), { once: true });
        }),
      );
    }
    if (options.timeoutMs !== undefined) {
      const timeoutMs = options.timeoutMs;
      races.push(
        new Promise<Transfer>((_, reject) => {
          setTimeout(
            () => reject(new BeamTimeoutError(`Transfer ${this.id} did not finish within ${timeoutMs}ms.`)),
            timeoutMs,
          );
        }),
      );
    }
    return Promise.race(races);
  }

  async cancel(): Promise<void> {
    this.#logger.info("cancelling transfer");
    try {
      await this.#control.post(`/transfers/${encodeURIComponent(this.id)}/cancel`, undefined);
    } catch (error) {
      // Cancelling an already-terminal transfer is a no-op, not a failure.
      if (error instanceof BeamError && error.code === "conflict") return;
      throw error;
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#stream?.close();
    this.removeAllListeners();
  }
}
