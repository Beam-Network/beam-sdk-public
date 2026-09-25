/**
 * An in-process upstream for local development.
 *
 * Lets `pnpm dev` and the whole test suite run with no Beam credentials, no
 * network, and no coordinator — which is what makes the examples runnable on a
 * laptop before anyone has been issued a key.
 *
 * It is a development fixture, not a Beam implementation: rooms it returns point
 * at whatever media URL you give it, and transfers advance on a timer. It exists
 * so the browser code paths can be exercised honestly, not to simulate Beam.
 */

import { BrokerError } from "../types.js";
import type { BeamUpstream, CreateRoomInput, CreateTransferInput, RoomRecord, TransferRecord } from "../types.js";

export interface MemoryUpstreamOptions {
  /**
   * Media URL handed back with every room. Point it at a local SFU to exercise
   * real WebRTC, or leave the default to exercise everything up to signalling.
   */
  mediaUrl?: string;
  /** Simulated transfer size, in bytes. */
  totalBytes?: number;
  /** Simulated bytes moved per tick. */
  bytesPerTick?: number;
  tickMs?: number;
  /** Forces every transfer to fail, for exercising the error path. */
  failTransfers?: boolean;
}

export function memoryUpstream(options: MemoryUpstreamOptions = {}): BeamUpstream {
  const mediaUrl = (options.mediaUrl ?? "http://127.0.0.1:8788/t/tun_local/webrtc/pub_local").replace(/\/+$/, "");
  const totalBytes = options.totalBytes ?? 64 * 1024 * 1024;
  const bytesPerTick = options.bytesPerTick ?? 8 * 1024 * 1024;
  const tickMs = options.tickMs ?? 400;

  const rooms = new Map<string, RoomRecord>();
  const transfers = new Map<string, TransferRecord>();
  const cancelled = new Set<string>();

  let counter = 0;
  const nextId = (prefix: string): string => `${prefix}_${(counter += 1).toString().padStart(4, "0")}`;

  return {
    async createRoom(input: CreateRoomInput): Promise<RoomRecord> {
      const id = nextId("room");
      const room: RoomRecord = {
        id,
        ...(input.name !== undefined ? { name: input.name } : {}),
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + (input.ttlSeconds ?? 3600) * 1000).toISOString(),
        media: {
          url: mediaUrl,
          token: `local-media-token-${id}`,
          whipUrl: `${mediaUrl}/whip`,
          whepUrl: `${mediaUrl}/whep`,
        },
      };
      rooms.set(id, room);
      return room;
    },

    async getRoom(roomId: string): Promise<RoomRecord> {
      const room = rooms.get(roomId);
      if (!room) throw new BrokerError(`No such room: ${roomId}.`, 404, "not_found");
      return room;
    },

    async createTransfer(input: CreateTransferInput): Promise<TransferRecord> {
      const id = nextId("tr");
      const record: TransferRecord = {
        id,
        status: "pending",
        ...(input.name !== undefined ? { name: input.name } : {}),
        createdAt: new Date().toISOString(),
        progress: {
          bytesCompleted: 0,
          totalBytes,
          destinationsCompleted: 0,
          destinationsTotal: input.destinations.length,
        },
      };
      transfers.set(id, record);
      return record;
    },

    async getTransfer(transferId: string): Promise<TransferRecord> {
      const record = transfers.get(transferId);
      if (!record) throw new BrokerError(`No such transfer: ${transferId}.`, 404, "not_found");
      return record;
    },

    async listTransfers({ limit }): Promise<TransferRecord[]> {
      return [...transfers.values()].slice(0, limit ?? 50);
    },

    async cancelTransfer(transferId: string): Promise<void> {
      const record = transfers.get(transferId);
      if (!record) throw new BrokerError(`No such transfer: ${transferId}.`, 404, "not_found");
      if (record.status === "completed" || record.status === "failed") {
        throw new BrokerError("This transfer has already finished.", 409, "conflict");
      }
      cancelled.add(transferId);
    },

    async *watchTransfer(transferId, _context, signal): AsyncIterable<TransferRecord> {
      const record = transfers.get(transferId);
      if (!record) throw new BrokerError(`No such transfer: ${transferId}.`, 404, "not_found");

      let moved = record.progress?.bytesCompleted ?? 0;
      const destinationsTotal = record.progress?.destinationsTotal ?? 1;

      while (!signal.aborted) {
        if (cancelled.has(transferId)) {
          const finished: TransferRecord = { ...record, status: "cancelled" };
          transfers.set(transferId, finished);
          yield finished;
          return;
        }

        moved = Math.min(totalBytes, moved + bytesPerTick);
        const done = moved >= totalBytes;
        const status: TransferRecord["status"] = done
          ? options.failTransfers === true
            ? "failed"
            : "completed"
          : "running";

        const next: TransferRecord = {
          ...record,
          status,
          progress: {
            bytesCompleted: moved,
            totalBytes,
            destinationsCompleted: done ? destinationsTotal : 0,
            destinationsTotal,
            updatedAt: new Date().toISOString(),
          },
          ...(status === "failed"
            ? { error: { code: "internal", message: "Simulated failure from the in-memory upstream." } }
            : {}),
        };
        transfers.set(transferId, next);
        yield next;
        if (done) return;

        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, tickMs);
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
  };
}
