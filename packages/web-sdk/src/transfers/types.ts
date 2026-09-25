/**
 * Transfer types.
 *
 * A browser never names a bucket, a region, or a credential. It refers to
 * sources and destinations by reference — an identifier the control plane
 * resolves against configuration it holds. That is what keeps object-storage
 * credentials server-side by construction rather than by convention.
 */

import type { Emitter } from "../core/events/index.js";

export type TransferId = string & { readonly __brand: "TransferId" };

export type TransferStatus = "pending" | "preparing" | "running" | "completed" | "failed" | "cancelled";

/** Statuses after which nothing further happens. */
export const TERMINAL_STATUSES: ReadonlySet<TransferStatus> = new Set<TransferStatus>([
  "completed",
  "failed",
  "cancelled",
]);

export interface TransferProgress {
  status: TransferStatus;
  bytesCompleted: number;
  /** Undefined until the plan is known, which is why `percent` can be undefined. */
  totalBytes: number | undefined;
  /** 0–100, or undefined while `totalBytes` is unknown. */
  percent: number | undefined;
  destinationsCompleted: number;
  destinationsTotal: number | undefined;
  updatedAt: string;
}

export interface Transfer {
  id: TransferId;
  status: TransferStatus;
  name?: string;
  createdAt: string;
  progress?: TransferProgress;
  /** Present when `status` is `failed`. */
  error?: { code: string; message: string };
}

export interface CreateTransferOptions {
  /** Reference the control plane resolves to a configured source. */
  source: string;
  /** Reference, or references, the control plane resolves to destinations. */
  destination: string | readonly string[];
  name?: string;
  /** Application data stored with the transfer. */
  metadata?: Record<string, unknown>;
  signal?: AbortSignal;
}

export type TransferEvents = {
  progress: TransferProgress;
  status: TransferStatus;
  /** Emitted once, on reaching a terminal status. */
  completed: Transfer;
  error: Error;
};

export interface TransferHandle extends Emitter<TransferEvents> {
  readonly id: TransferId;
  readonly status: TransferStatus;
  /** Latest known progress, or undefined before the first update. */
  readonly lastProgress: TransferProgress | undefined;
  /** Progress as an async iterable, ending when the transfer reaches a terminal status. */
  progress(options?: { signal?: AbortSignal }): AsyncIterableIterator<TransferProgress>;
  /** Resolves on success; rejects on failure or cancellation. */
  done(options?: { signal?: AbortSignal; timeoutMs?: number }): Promise<Transfer>;
  /** Fetches current state without waiting for the next event. */
  refresh(): Promise<Transfer>;
  cancel(): Promise<void>;
  /** Stops watching. Does not cancel the transfer, which continues server-side. */
  close(): void;
}
