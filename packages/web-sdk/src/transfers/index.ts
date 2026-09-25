/**
 * The `beam.transfers` namespace.
 *
 * Transfers are brokered: the control plane holds the Beam credential and the
 * object-storage configuration, and the browser refers to endpoints by name.
 * File bytes never pass through the browser or the control plane — Beam's
 * workers move them directly between storage endpoints.
 */

import type { Logger } from "../core/logger/index.js";
import type { HttpTransport } from "../core/transport/index.js";
import { newIdempotencyKey } from "../core/transport/index.js";
import { BeamTransferHandle, toTransfer } from "./handle.js";
import type { WireTransfer } from "./handle.js";
import type { CreateTransferOptions, Transfer, TransferHandle } from "./types.js";

export class Transfers {
  readonly #control: HttpTransport;
  readonly #logger: Logger;

  constructor(deps: { control: HttpTransport; logger: Logger }) {
    this.#control = deps.control;
    this.#logger = deps.logger.child("transfers");
  }

  /**
   * Starts a transfer and returns a handle that is already watching it.
   *
   * Requires the `transfers:create` scope.
   */
  async create(options: CreateTransferOptions): Promise<TransferHandle> {
    const destinations = Array.isArray(options.destination) ? options.destination : [options.destination];

    const wire = await this.#control.post<WireTransfer>(
      "/transfers",
      {
        source: options.source,
        destinations,
        ...(options.name !== undefined ? { name: options.name } : {}),
        ...(options.metadata !== undefined ? { metadata: options.metadata } : {}),
      },
      {
        // A retried create must not start a second transfer of the same data.
        idempotencyKey: newIdempotencyKey("transfer-create"),
        ...(options.signal ? { signal: options.signal } : {}),
      },
    );

    const transfer = toTransfer(wire);
    this.#logger.info("transfer created", { transferId: transfer.id });
    return new BeamTransferHandle({ transfer, control: this.#control, logger: this.#logger });
  }

  /** Reads a transfer without watching it. Requires the `transfers:read` scope. */
  async get(transferId: string, options: { signal?: AbortSignal } = {}): Promise<Transfer> {
    const wire = await this.#control.get<WireTransfer>(`/transfers/${encodeURIComponent(transferId)}`, {
      ...(options.signal ? { signal: options.signal } : {}),
    });
    return toTransfer(wire);
  }

  /** Re-attaches to a transfer started elsewhere — another tab, or a page reload. */
  async watch(transferId: string, options: { signal?: AbortSignal } = {}): Promise<TransferHandle> {
    const transfer = await this.get(transferId, options);
    return new BeamTransferHandle({ transfer, control: this.#control, logger: this.#logger });
  }

  /** Lists recent transfers. Requires the `transfers:read` scope. */
  async list(options: { limit?: number; status?: string; signal?: AbortSignal } = {}): Promise<Transfer[]> {
    const response = await this.#control.get<{ transfers?: WireTransfer[] }>("/transfers", {
      query: {
        ...(options.limit !== undefined ? { limit: options.limit } : {}),
        ...(options.status !== undefined ? { status: options.status } : {}),
      },
      ...(options.signal ? { signal: options.signal } : {}),
    });
    return (response.transfers ?? []).map(toTransfer);
  }

  /** Cancels a transfer by id, without needing a handle. */
  async cancel(transferId: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    await this.#control.post(`/transfers/${encodeURIComponent(transferId)}/cancel`, undefined, {
      ...(options.signal ? { signal: options.signal } : {}),
    });
  }
}

export { BeamTransferHandle, toTransfer } from "./handle.js";
export { TERMINAL_STATUSES } from "./types.js";
export type {
  CreateTransferOptions,
  Transfer,
  TransferEvents,
  TransferHandle,
  TransferId,
  TransferProgress,
  TransferStatus,
} from "./types.js";
