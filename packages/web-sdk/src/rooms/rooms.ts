/**
 * The `beam.rooms` namespace.
 *
 * Room lifecycle runs through the control plane, which holds the privileged
 * credential. Media runs browser-to-relay, using the capability URL the control
 * plane returns.
 */

import { BeamConfigError } from "../core/errors/index.js";
import type { Logger } from "../core/logger/index.js";
import type { HttpTransport } from "../core/transport/index.js";
import { newIdempotencyKey } from "../core/transport/index.js";
import { BeamRoomSession } from "./session.js";
import type { CreateRoomOptions, JoinRoomOptions, Room, RoomId, RoomSession } from "./types.js";

/** Room as the control plane returns it. */
interface WireRoom {
  id: string;
  name?: string;
  created_at?: string;
  createdAt?: string;
  expires_at?: string;
  expiresAt?: string;
  media: {
    url: string;
    token?: string;
    whip_url?: string;
    whipUrl?: string;
    whep_url?: string;
    whepUrl?: string;
  };
}

export function toRoom(wire: WireRoom): Room {
  if (typeof wire?.id !== "string" || typeof wire?.media?.url !== "string") {
    throw new BeamConfigError(
      "The Beam control plane returned a room without an id or media URL. " +
        "If you are running your own token broker, check that it forwards Beam's room response unchanged.",
    );
  }
  const whipUrl = wire.media.whip_url ?? wire.media.whipUrl;
  const whepUrl = wire.media.whep_url ?? wire.media.whepUrl;
  return {
    id: wire.id as RoomId,
    ...(wire.name !== undefined ? { name: wire.name } : {}),
    createdAt: wire.created_at ?? wire.createdAt ?? new Date().toISOString(),
    ...((wire.expires_at ?? wire.expiresAt) ? { expiresAt: (wire.expires_at ?? wire.expiresAt) as string } : {}),
    media: {
      url: wire.media.url.replace(/\/+$/, ""),
      ...(wire.media.token !== undefined ? { token: wire.media.token } : {}),
      ...(whipUrl !== undefined ? { whipUrl } : {}),
      ...(whepUrl !== undefined ? { whepUrl } : {}),
    },
  };
}

export class Rooms {
  readonly #control: HttpTransport;
  readonly #logger: Logger;

  constructor(deps: { control: HttpTransport; logger: Logger }) {
    this.#control = deps.control;
    this.#logger = deps.logger.child("rooms");
  }

  /** Creates a room. Requires the `rooms:create` scope. */
  async create(options: CreateRoomOptions = {}): Promise<Room> {
    const wire = await this.#control.post<WireRoom>(
      "/rooms",
      {
        ...(options.name !== undefined ? { name: options.name } : {}),
        ...(options.ttlSeconds !== undefined ? { ttl_seconds: options.ttlSeconds } : {}),
        ...(options.region !== undefined ? { region: options.region } : {}),
        ...(options.metadata !== undefined ? { metadata: options.metadata } : {}),
      },
      {
        // One key per call so a network retry cannot create a second room.
        idempotencyKey: newIdempotencyKey("room-create"),
        ...(options.signal ? { signal: options.signal } : {}),
      },
    );
    const room = toRoom(wire);
    this.#logger.info("room created", { roomId: room.id });
    return room;
  }

  /** Looks up an existing room. Requires the `room:read` scope. */
  async get(roomId: string, options: { signal?: AbortSignal } = {}): Promise<Room> {
    const wire = await this.#control.get<WireRoom>(`/rooms/${encodeURIComponent(roomId)}`, {
      ...(options.signal ? { signal: options.signal } : {}),
    });
    return toRoom(wire);
  }

  /**
   * Joins a room and returns a live session.
   *
   * Accepts either a room id or a `Room`. Passing the `Room` returned by
   * `create` saves a round trip, which matters on a join path a user is
   * watching.
   */
  async join(room: string | Room, options: JoinRoomOptions = {}): Promise<RoomSession> {
    const resolved = typeof room === "string" ? await this.get(room, pickSignal(options)) : room;

    const session = new BeamRoomSession({
      room: resolved,
      // Media calls are authorized by the capability token inside the URL, not
      // by the session's Beam token, so this transport sends no Authorization
      // header and applies no token refresh.
      media: this.#control.withBaseUrl(resolved.media.url),
      logger: this.#logger,
      options,
    });

    try {
      await session.connect();
    } catch (error) {
      await session.leave();
      throw error;
    }
    return session;
  }
}

function pickSignal(options: { signal?: AbortSignal }): { signal?: AbortSignal } {
  return options.signal ? { signal: options.signal } : {};
}
