/**
 * Logging and debug mode.
 *
 * Redaction is on by default and applies to every value the logger touches,
 * because the things worth logging in this SDK — token responses, room
 * descriptors, WHIP endpoints — are exactly the things that carry credentials.
 */

export type LogLevel = "silent" | "error" | "warn" | "info" | "debug";

const LEVEL_ORDER: Record<LogLevel, number> = {
  silent: 0,
  error: 1,
  warn: 2,
  info: 3,
  debug: 4,
};

export interface Logger {
  error(message: string, context?: unknown): void;
  warn(message: string, context?: unknown): void;
  info(message: string, context?: unknown): void;
  debug(message: string, context?: unknown): void;
  child(scope: string): Logger;
  readonly level: LogLevel;
}

/** Sink for log records. Swappable so hosts can forward into their own logger. */
export interface LogSink {
  (level: Exclude<LogLevel, "silent">, message: string, context?: unknown): void;
}

// The one place the SDK is allowed to touch the console: this is the default
// sink, and hosts that want their own logging swap it out via `LoggerOptions`.
/* eslint-disable no-console */
const CONSOLE_SINK: LogSink = (level, message, context) => {
  const method = level === "debug" ? "log" : level;
  if (context === undefined) console[method](message);
  else console[method](message, context);
};
/* eslint-enable no-console */

/**
 * Keys whose values are replaced with `[redacted]`. Matched case-insensitively
 * on a substring, so `access_token`, `agentToken`, and `X-Api-Key` all match.
 */
const SENSITIVE_KEY_PATTERN = /(token|secret|password|api[-_]?key|authorization|credential|signature|\bsig\b)/i;

const REDACTED = "[redacted]";

/**
 * Anything that looks like a Beam credential, wherever it appears — including
 * inside a URL query string, which is how relay public tokens travel.
 */
const SENSITIVE_VALUE_PATTERN = /\b(bwt1|btrstd1|bm_live|bm_pub|b1m|bag|agt)_?[A-Za-z0-9._-]{8,}/g;

function redactString(value: string): string {
  return value.replace(SENSITIVE_VALUE_PATTERN, (match) => `${match.slice(0, 6)}…${REDACTED}`);
}

function redactUrl(value: string): string {
  try {
    const url = new URL(value);
    let touched = false;
    for (const key of [...url.searchParams.keys()]) {
      if (SENSITIVE_KEY_PATTERN.test(key)) {
        url.searchParams.set(key, REDACTED);
        touched = true;
      }
    }
    return touched ? url.toString() : redactString(value);
  } catch {
    return redactString(value);
  }
}

/**
 * Deep-redacts a value for logging. Cycles are tolerated; depth is bounded so a
 * pathological object cannot hang the logger.
 */
export function redact(value: unknown, depth = 0, seen = new WeakSet<object>()): unknown {
  if (depth > 6) return "[truncated]";
  if (typeof value === "string") {
    return value.includes("://") ? redactUrl(value) : redactString(value);
  }
  if (value === null || typeof value !== "object") return value;
  if (seen.has(value)) return "[circular]";
  seen.add(value);

  if (Array.isArray(value)) {
    return value.slice(0, 50).map((item) => redact(item, depth + 1, seen));
  }
  if (value instanceof Error) {
    return { name: value.name, message: redactString(value.message) };
  }
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    output[key] = SENSITIVE_KEY_PATTERN.test(key) ? REDACTED : redact(item, depth + 1, seen);
  }
  return output;
}

export interface LoggerOptions {
  level?: LogLevel;
  sink?: LogSink;
  /** Set false only in a trusted local debugging session. */
  redact?: boolean;
  scope?: string;
}

class BeamLogger implements Logger {
  readonly level: LogLevel;
  readonly #sink: LogSink;
  readonly #redact: boolean;
  readonly #scope: string;

  constructor(options: LoggerOptions = {}) {
    this.level = options.level ?? "warn";
    this.#sink = options.sink ?? CONSOLE_SINK;
    this.#redact = options.redact ?? true;
    this.#scope = options.scope ?? "beam";
  }

  #log(level: Exclude<LogLevel, "silent">, message: string, context?: unknown): void {
    if (LEVEL_ORDER[this.level] < LEVEL_ORDER[level]) return;
    const prepared = context === undefined ? undefined : this.#redact ? redact(context) : context;
    this.#sink(level, `[${this.#scope}] ${message}`, prepared);
  }

  error(message: string, context?: unknown): void {
    this.#log("error", message, context);
  }
  warn(message: string, context?: unknown): void {
    this.#log("warn", message, context);
  }
  info(message: string, context?: unknown): void {
    this.#log("info", message, context);
  }
  debug(message: string, context?: unknown): void {
    this.#log("debug", message, context);
  }

  child(scope: string): Logger {
    return new BeamLogger({
      level: this.level,
      sink: this.#sink,
      redact: this.#redact,
      scope: `${this.#scope}:${scope}`,
    });
  }
}

/**
 * Reads the opt-in debug flag from browser storage. Lets a developer raise the
 * log level on a deployed page without a rebuild, which is the only practical
 * way to debug a customer's production integration.
 */
export function storedDebugLevel(): LogLevel | undefined {
  try {
    const raw = globalThis.localStorage?.getItem("beam:debug");
    if (raw === null || raw === undefined) return undefined;
    if (raw === "1" || raw === "true") return "debug";
    if (raw in LEVEL_ORDER) return raw as LogLevel;
  } catch {
    // Storage access throws in sandboxed frames and in privacy modes. Not fatal.
  }
  return undefined;
}

export function createLogger(options: LoggerOptions = {}): Logger {
  return new BeamLogger({ ...options, level: options.level ?? storedDebugLevel() ?? "warn" });
}

/** A logger that discards everything. Used as the default in tests. */
export const silentLogger: Logger = createLogger({ level: "silent", sink: () => undefined });
