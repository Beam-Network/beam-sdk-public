/**
 * Error model.
 *
 * Every failure the SDK surfaces is a `BeamError` with a stable machine-readable
 * `code`. Codes are the union of Beam's own error vocabulary (the coordinator's
 * `btr.Code*` values, which arrive over the wire and keep their identity across
 * the broker hop) and SDK-local conditions that never leave the browser.
 */

/** Error codes that originate in Beam and are forwarded verbatim. */
export type BeamRemoteErrorCode =
  | "invalid_argument"
  | "permission_denied"
  | "not_found"
  | "conflict"
  | "resource_exhausted"
  | "unavailable"
  | "internal";

/** Error codes raised by the SDK itself. */
export type BeamLocalErrorCode =
  | "config_invalid"
  | "auth_failed"
  | "auth_expired"
  | "network"
  | "timeout"
  | "aborted"
  | "unsupported_environment"
  | "protocol";

export type BeamErrorCode = BeamRemoteErrorCode | BeamLocalErrorCode;

const RETRYABLE_CODES: ReadonlySet<BeamErrorCode> = new Set<BeamErrorCode>([
  "unavailable",
  "internal",
  "network",
  "timeout",
  "resource_exhausted",
]);

export interface BeamErrorOptions {
  code: BeamErrorCode;
  /** HTTP status, when the error came from a response. */
  status?: number;
  /** Server-supplied correlation id, surfaced for support requests. */
  requestId?: string;
  /** Overrides the default retryability implied by `code`. */
  retryable?: boolean;
  /** Seconds to wait before retrying, from a `Retry-After` header. */
  retryAfterSeconds?: number;
  cause?: unknown;
}

/**
 * Base class for everything the SDK throws. Subclasses exist so callers can use
 * `instanceof` for coarse handling; `code` is the precise discriminator.
 */
export class BeamError extends Error {
  readonly code: BeamErrorCode;
  readonly status: number | undefined;
  readonly requestId: string | undefined;
  readonly retryable: boolean;
  readonly retryAfterSeconds: number | undefined;

  constructor(message: string, options: BeamErrorOptions) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = new.target.name;
    this.code = options.code;
    this.status = options.status;
    this.requestId = options.requestId;
    this.retryable = options.retryable ?? RETRYABLE_CODES.has(options.code);
    this.retryAfterSeconds = options.retryAfterSeconds;
  }

  /** Safe to log: contains no tokens, headers, or request bodies. */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      message: this.message,
      code: this.code,
      status: this.status,
      requestId: this.requestId,
      retryable: this.retryable,
    };
  }
}

/** The SDK was constructed or called with unusable options. Never retryable. */
export class BeamConfigError extends BeamError {
  constructor(message: string, cause?: unknown) {
    super(message, { code: "config_invalid", retryable: false, cause });
  }
}

/** A token could not be obtained, was rejected, or expired mid-flight. */
export class BeamAuthError extends BeamError {
  constructor(
    message: string,
    options: Omit<BeamErrorOptions, "code"> & { code?: "auth_failed" | "auth_expired" } = {},
  ) {
    const { code = "auth_failed", ...rest } = options;
    super(message, { ...rest, code });
  }
}

/** Beam returned a structured error response. */
export class BeamApiError extends BeamError {}

/** The request never produced a response: DNS, TLS, offline, CORS. */
export class BeamNetworkError extends BeamError {
  constructor(message: string, cause?: unknown) {
    super(message, { code: "network", cause });
  }
}

/** A deadline elapsed before the operation completed. */
export class BeamTimeoutError extends BeamError {
  constructor(message: string, cause?: unknown) {
    super(message, { code: "timeout", cause });
  }
}

/** The caller's `AbortSignal` fired. Not retryable: the caller asked to stop. */
export class BeamAbortError extends BeamError {
  constructor(message = "The operation was aborted.", cause?: unknown) {
    super(message, { code: "aborted", retryable: false, cause });
  }
}

/** The runtime lacks an API the SDK needs (WebRTC, fetch, EventSource). */
export class BeamUnsupportedError extends BeamError {
  constructor(message: string, cause?: unknown) {
    super(message, { code: "unsupported_environment", retryable: false, cause });
  }
}

const REMOTE_CODES: ReadonlySet<string> = new Set<BeamRemoteErrorCode>([
  "invalid_argument",
  "permission_denied",
  "not_found",
  "conflict",
  "resource_exhausted",
  "unavailable",
  "internal",
]);

/**
 * Maps an HTTP status onto Beam's error vocabulary. Used only when the response
 * body carries no `code` of its own — a body-supplied code always wins, so a
 * Beam-originated code survives the trip through a customer's broker even if the
 * broker chose a different status.
 */
export function codeFromStatus(status: number): BeamErrorCode {
  if (status === 400 || status === 422) return "invalid_argument";
  if (status === 401) return "auth_failed";
  if (status === 403) return "permission_denied";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status === 429) return "resource_exhausted";
  if (status >= 500) return status === 503 ? "unavailable" : "internal";
  return "internal";
}

/** Narrows an unknown value to a Beam error code, or returns undefined. */
export function asRemoteErrorCode(value: unknown): BeamRemoteErrorCode | undefined {
  return typeof value === "string" && REMOTE_CODES.has(value) ? (value as BeamRemoteErrorCode) : undefined;
}
