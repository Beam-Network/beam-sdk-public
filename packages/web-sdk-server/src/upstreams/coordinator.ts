/**
 * Rooms backed by the Beam coordinator.
 *
 * Calls the coordinator's media-room API with the customer's Beam API key and
 * returns only what a browser needs. The key never leaves this process.
 */

import { BrokerError } from "../types.js";
import type { BeamSessionContext, BeamUpstream, CreateRoomInput, RoomRecord } from "../types.js";

export interface CoordinatorUpstreamOptions {
  /** Coordinator base URL, e.g. `https://coordinator.b1m.ai`. */
  coordinatorUrl: string;
  /**
   * Beam API key. Sent as `X-Api-Key`, which is what the coordinator's room
   * credit check requires. Read it from the environment; never inline it.
   */
  apiKey?: string;
  /** Default room lifetime when the caller does not ask for one. */
  defaultTtlSeconds?: number;
  fetch?: typeof fetch;
  /** Deadline for a single coordinator call. */
  timeoutMs?: number;
}

/** The coordinator's media-room response. Only a few fields are browser-relevant. */
interface CoordinatorMediaRoom {
  room_id?: string;
  id?: string;
  tunnel_id?: string;
  public_webrtc_url?: string;
  whip_url?: string;
  whep_url?: string;
  public_token?: string;
  token?: string;
  expires_at?: string;
  routes?: Array<{ relay_id?: string; region?: string; url?: string }>;
}

/**
 * Reduces a coordinator room to the browser-facing shape.
 *
 * This is where Beam's internals stop. Relay ids, tunnel ids, lease state, plan
 * versions, and federation edges are all dropped: a web developer has no use
 * for them, and forwarding them would make them part of a contract that has to
 * keep working.
 */
function toRoomRecord(payload: CoordinatorMediaRoom, name: string | undefined): RoomRecord {
  const id = payload.room_id ?? payload.id;
  const mediaUrl = payload.public_webrtc_url;

  if (!id || !mediaUrl) {
    throw new BrokerError(
      "The Beam coordinator returned a room without an id or a WebRTC URL.",
      502,
      "upstream_invalid",
    );
  }

  const base = mediaUrl.replace(/\/+$/, "");
  const token = payload.token ?? payload.public_token;

  return {
    id,
    ...(name !== undefined ? { name } : {}),
    createdAt: new Date().toISOString(),
    ...(payload.expires_at !== undefined ? { expiresAt: payload.expires_at } : {}),
    media: {
      url: base,
      ...(token !== undefined ? { token } : {}),
      whipUrl: payload.whip_url ?? `${base}/whip`,
      whepUrl: payload.whep_url ?? `${base}/whep`,
    },
  };
}

export function coordinatorRooms(options: CoordinatorUpstreamOptions): Pick<BeamUpstream, "createRoom" | "getRoom"> {
  const baseUrl = options.coordinatorUrl.replace(/\/+$/, "");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const timeoutMs = options.timeoutMs ?? 15_000;

  async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers: Record<string, string> = {
      accept: "application/json",
      ...(init.body !== undefined ? { "content-type": "application/json" } : {}),
      ...(options.apiKey !== undefined ? { "X-Api-Key": options.apiKey } : {}),
      ...((init.headers as Record<string, string>) ?? {}),
    };

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let response: Response;
    try {
      response = await fetchImpl(`${baseUrl}${path}`, { ...init, headers, signal: controller.signal });
    } catch (cause) {
      const reason = controller.signal.aborted ? `timed out after ${timeoutMs}ms` : String(cause);
      throw new BrokerError(
        `Could not reach the Beam coordinator at ${baseUrl}: ${reason}.`,
        502,
        "upstream_unreachable",
      );
    } finally {
      clearTimeout(timer);
    }

    const text = await response.text();
    if (!response.ok) {
      let message = response.statusText;
      try {
        const parsed = JSON.parse(text) as { error?: string };
        if (parsed.error) message = parsed.error;
      } catch {
        if (text) message = text.slice(0, 200);
      }
      // 4xx from the coordinator is usually the customer's problem (quota, an
      // invalid key, a closed room), so it is forwarded rather than masked as 502.
      const status = response.status >= 400 && response.status < 500 ? response.status : 502;
      throw new BrokerError(`Beam coordinator: ${message}`, status, "upstream_error");
    }
    return (text ? JSON.parse(text) : {}) as T;
  }

  return {
    async createRoom(input: CreateRoomInput, _context: BeamSessionContext): Promise<RoomRecord> {
      const payload = await call<CoordinatorMediaRoom>("/media/rooms", {
        method: "POST",
        // The coordinator supports Idempotency-Key here; a retry inside the
        // broker must not leave a second room holding a credit reservation.
        headers: { "Idempotency-Key": `room-${crypto.randomUUID()}` },
        body: JSON.stringify({
          ttl_seconds: input.ttlSeconds ?? options.defaultTtlSeconds ?? 3600,
          ...(input.region !== undefined ? { region: input.region } : {}),
        }),
      });
      return toRoomRecord(payload, input.name);
    },

    async getRoom(roomId: string): Promise<RoomRecord> {
      const payload = await call<CoordinatorMediaRoom>(`/media/rooms/${encodeURIComponent(roomId)}`);
      return toRoomRecord(payload, undefined);
    },
  };
}
