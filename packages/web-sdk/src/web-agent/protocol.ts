import { BeamApiError, BeamConfigError, BeamError } from "../core/errors/index.js";
import type { BeamErrorCode } from "../core/errors/index.js";
import type { AgentBroadcast, AgentChannel, AgentMembership, AgentMessage, AgentPublication } from "./types.js";

export const PROTOCOL = "beam.web-agent.v1";
export function protocolError(): BeamError {
  return new BeamError("The Web Agent returned an invalid response.", { code: "protocol", retryable: false });
}
export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw protocolError();
  return value as Record<string, unknown>;
}
export function text(value: unknown): string {
  if (typeof value !== "string") throw protocolError();
  return value;
}
export function identifier(value: unknown): string {
  const result = text(value);
  if (!result || result.length > 256) throw protocolError();
  return result;
}
export function inputId(value: string): string {
  if (typeof value !== "string" || !value || value.length > 256)
    throw new BeamConfigError("An identifier is required.");
  return value;
}
export function duration(value: number | undefined, fallback: number): number {
  const result = value ?? fallback;
  if (!Number.isFinite(result) || result <= 0 || result > 120_000)
    throw new BeamConfigError("Timeout must be between 1 and 120000 milliseconds.");
  return result;
}
function integer(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw protocolError();
  return value as number;
}
function boolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw protocolError();
  return value;
}
export function membership(value: unknown): AgentMembership {
  const v = object(value);
  return { roomId: identifier(v.roomId), memberId: identifier(v.memberId), state: text(v.state) };
}
export function channel(value: unknown): AgentChannel {
  const v = object(value);
  if (v.kind !== "message" && v.kind !== "media" && v.kind !== "stream") throw protocolError();
  return {
    id: identifier(v.id),
    name: text(v.name),
    kind: v.kind,
    state: text(v.state),
    publish: boolean(v.publish),
    subscribe: boolean(v.subscribe),
    ready: boolean(v.ready),
  };
}
export function array<T>(value: unknown, parse: (entry: unknown) => T): T[] {
  // Empty Go slices can be serialized as null.
  if (value === null) return [];
  if (!Array.isArray(value)) throw protocolError();
  return value.map(parse);
}
export function broadcast(value: unknown): AgentBroadcast {
  const v = object(value);
  return { id: identifier(v.id), updatedAt: text(v.updatedAt) };
}
export function message(value: unknown): AgentMessage {
  const v = object(value);
  const encoded = text(v.payload);
  if (encoded.length > 87384) throw protocolError();
  let decoded: string;
  try {
    decoded = atob(encoded);
  } catch {
    throw protocolError();
  }
  if (decoded.length > 65536) throw protocolError();
  return {
    id: text(v.publicationId),
    senderId: text(v.publisherMemberId),
    sequence: integer(v.sequence),
    contentType: text(v.contentType),
    payload: Uint8Array.from(decoded, (c) => c.charCodeAt(0)),
    receivedAt: text(v.receivedAt),
  };
}
export function publication(value: unknown): AgentPublication {
  const v = object(value);
  // Never spread wire results: they also contain private routing fields.
  return {
    id: identifier(v.publication_id),
    sequence: integer(v.publisher_sequence),
    online: integer(v.online_deliveries),
    accepted: integer(v.accepted_deliveries),
    delivered: integer(v.delivered_deliveries),
    failed: integer(v.failed_deliveries),
    expired: integer(v.expired_deliveries),
    skipped: integer(v.skipped_online_only),
    duplicate: v.duplicate === true,
    queued: v.outbox_pending === true,
  };
}
export function remoteError(value: unknown): BeamError {
  const v = object(value);
  const codes: Record<string, BeamErrorCode> = {
    forbidden: "permission_denied",
    permission_denied: "permission_denied",
    invalid_request: "invalid_argument",
    invalid_argument: "invalid_argument",
    resource_limit: "resource_exhausted",
    resource_exhausted: "resource_exhausted",
    setup_timeout: "timeout",
    cancelled: "aborted",
    keys_pending: "unavailable",
    unavailable: "unavailable",
    leave_pending: "conflict",
    conflict: "conflict",
    not_found: "not_found",
    slow_consumer: "resource_exhausted",
    unsupported_kind: "invalid_argument",
    unsupported_command: "invalid_argument",
  };
  const code = codes[text(v.code)] ?? "internal";
  return new BeamApiError("The Web Agent could not complete the operation.", { code, retryable: v.retryable === true });
}
export function asError(error: unknown): BeamError {
  return error instanceof BeamError ? error : protocolError();
}
export function idempotencyKey(value?: string): string {
  return inputId(value ?? crypto.randomUUID());
}
