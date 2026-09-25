/**
 * Transfers backed by `@beam-network/sdk`.
 *
 * Two things make this server-only, and both are structural rather than
 * incidental:
 *
 *  1. BeamCore's transfer lifecycle runs over NATS, authenticated with the raw
 *     API key as the connection password. A browser holding that key would be
 *     handing every visitor the ability to spend the customer's credits.
 *  2. The SDK presigns storage routes locally and serves a reverse-RPC subject
 *     so BeamCore can call back to re-sign routes that expire mid-transfer.
 *
 * Point 2 has a deployment consequence worth stating plainly: **this upstream
 * must run in a long-lived process.** On a function runtime the process is
 * frozen or torn down between requests, the re-sign callback goes unanswered,
 * and long transfers fail partway with no obvious cause.
 *
 * `@beam-network/sdk` is an optional peer dependency, imported lazily, so an
 * application that only uses rooms never installs `nats` or the AWS SDK.
 */

import { BrokerError } from "../types.js";
import type { BeamUpstream, CreateTransferInput, TransferRecord } from "../types.js";

/**
 * Resolves an opaque reference from the browser to a real storage endpoint.
 *
 * This indirection is the reason a browser can start a transfer safely: it
 * names `"s3:reports"`, and the credentials behind that name never leave the
 * server. Returning `undefined` rejects the reference.
 */
export type EndpointResolver = (
  reference: string,
  context: { subject: string | undefined; organizationId: string | undefined },
) => Promise<unknown> | unknown;

export interface BeamSdkTransfersOptions {
  /** BeamCore API key. Read from the environment; never inline it. */
  apiKey: string;
  /** Maps browser-supplied references to provider configurations. */
  resolveEndpoint: EndpointResolver;
  /** NATS URL override. Defaults to the SDK's own default. */
  natsUrl?: string;
  environment?: string;
  /** Interval between status polls. BeamCore pushes only a terminal event. */
  pollIntervalMs?: number;
}

/** Resolved at runtime only. See the note in `client()`. */
const BEAM_SDK_SPECIFIER = "@beam-network/sdk";

interface BeamSdkClient {
  createTransfer(input: unknown): Promise<{ transfer_id?: string; transferId?: string }>;
  transferStatus(transferId: string): Promise<Record<string, unknown>>;
  cancelTransfer(transferId: string): Promise<unknown>;
  close(): Promise<void>;
}

const TERMINAL = new Set(["completed", "failed", "cancelled"]);

/** Maps a BeamCore status payload onto the browser-facing record. */
export function toTransferRecord(transferId: string, status: Record<string, unknown>): TransferRecord {
  const raw = typeof status["status"] === "string" ? status["status"] : "running";
  const known = ["pending", "preparing", "running", "completed", "failed", "cancelled"];
  const normalized = (known.includes(raw) ? raw : "running") as TransferRecord["status"];

  const bytesCompleted = Number(status["delivery_bytes_completed"] ?? 0);
  const totalBytes = Number(status["delivery_bytes_total"] ?? status["source_bytes_total"] ?? 0);
  const destinationsCompleted = Number(status["destinations_completed"] ?? 0);
  const destinationsTotal = Number(status["destinations_total"] ?? 0);

  return {
    id: transferId,
    status: normalized,
    createdAt: typeof status["created_at"] === "string" ? status["created_at"] : new Date().toISOString(),
    progress: {
      bytesCompleted,
      ...(totalBytes > 0 ? { totalBytes } : {}),
      destinationsCompleted,
      ...(destinationsTotal > 0 ? { destinationsTotal } : {}),
      updatedAt: new Date().toISOString(),
    },
    ...(normalized === "failed"
      ? {
          error: {
            code: typeof status["error_code"] === "string" ? status["error_code"] : "internal",
            message: typeof status["error"] === "string" ? status["error"] : "The transfer failed.",
          },
        }
      : {}),
  };
}

export function beamSdkTransfers(options: BeamSdkTransfersOptions): Pick<
  BeamUpstream,
  "createTransfer" | "getTransfer" | "cancelTransfer" | "watchTransfer"
> & {
  close(): Promise<void>;
} {
  const pollIntervalMs = options.pollIntervalMs ?? 3_000;
  let clientPromise: Promise<BeamSdkClient> | undefined;

  /**
   * One client for the process, created on first use and reused.
   *
   * Reuse is required, not an optimization: the client owns the NATS connection
   * and the route-recovery subscription that BeamCore calls back into.
   */
  async function client(): Promise<BeamSdkClient> {
    clientPromise ??= (async () => {
      let module: { BeamClient: new (options: unknown) => BeamSdkClient };
      try {
        // The specifier is held in a variable so TypeScript does not try to
        // resolve it at build time: `@beam-network/sdk` is an optional peer, and
        // requiring it to be installed just to typecheck this package would drag
        // `nats` and the AWS SDK into every install.
        const specifier = BEAM_SDK_SPECIFIER;
        module = (await import(specifier)) as never;
      } catch {
        throw new BrokerError(
          "`@beam-network/sdk` is not installed. Run `npm install @beam-network/sdk` to enable transfers.",
          501,
          "missing_dependency",
        );
      }
      return new module.BeamClient({
        apiKey: options.apiKey,
        ...(options.natsUrl !== undefined ? { natsUrl: options.natsUrl } : {}),
        ...(options.environment !== undefined ? { environment: options.environment } : {}),
      });
    })();
    return clientPromise;
  }

  async function resolve(
    reference: string,
    context: { subject: string | undefined; organizationId: string | undefined },
  ): Promise<unknown> {
    const resolved = await options.resolveEndpoint(reference, context);
    if (resolved === undefined || resolved === null) {
      throw new BrokerError(`Unknown storage endpoint: ${reference}.`, 400, "invalid_argument");
    }
    return resolved;
  }

  return {
    async createTransfer(input: CreateTransferInput, context): Promise<TransferRecord> {
      const identity = { subject: context.subject, organizationId: context.organizationId };
      const [source, ...destinations] = await Promise.all([
        resolve(input.source, identity),
        ...input.destinations.map((reference) => resolve(reference, identity)),
      ]);

      const beam = await client();
      const created = await beam.createTransfer({
        source,
        destinations,
        ...(input.name !== undefined ? { name: input.name } : {}),
      });

      const transferId = created.transfer_id ?? created.transferId;
      if (!transferId) {
        throw new BrokerError("BeamCore did not return a transfer id.", 502, "upstream_invalid");
      }
      return {
        id: transferId,
        status: "pending",
        ...(input.name !== undefined ? { name: input.name } : {}),
        createdAt: new Date().toISOString(),
      };
    },

    async getTransfer(transferId: string): Promise<TransferRecord> {
      const beam = await client();
      return toTransferRecord(transferId, await beam.transferStatus(transferId));
    },

    async cancelTransfer(transferId: string): Promise<void> {
      const beam = await client();
      await beam.cancelTransfer(transferId);
    },

    /**
     * Polls status and yields on change.
     *
     * BeamCore has no incremental progress push — only one terminal event — so
     * polling is the honest implementation rather than a shortcut. Yielding only
     * on change keeps the browser's SSE stream quiet while nothing is happening.
     */
    async *watchTransfer(transferId: string, _context, signal: AbortSignal): AsyncIterable<TransferRecord> {
      const beam = await client();
      let previous = "";

      while (!signal.aborted) {
        const record = toTransferRecord(transferId, await beam.transferStatus(transferId));
        const fingerprint = `${record.status}:${record.progress?.bytesCompleted ?? 0}`;
        if (fingerprint !== previous) {
          previous = fingerprint;
          yield record;
        }
        if (TERMINAL.has(record.status)) return;

        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, pollIntervalMs);
          signal.addEventListener(
            "abort",
            () => {
              clearTimeout(timer);
              resolve();
            },
            { once: true },
          );
        });
      }
    },

    /** Closes the NATS connection. Call on process shutdown. */
    async close(): Promise<void> {
      const pending = clientPromise;
      clientPromise = undefined;
      if (pending) await (await pending).close();
    },
  };
}
