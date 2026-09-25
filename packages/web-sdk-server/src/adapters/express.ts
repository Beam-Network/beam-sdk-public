/**
 * Express and Node `http` adapter.
 *
 * Bridges Node's stream-based request/response onto the WHATWG `Request` and
 * `Response` the broker speaks. Kept dependency-free — it is typed structurally
 * against Express rather than importing it, so the package does not pull Express
 * into applications that use Fastify, Next, Hono, or bare `node:http`.
 */

import { Readable } from "node:stream";

import type { BeamBroker } from "../broker.js";

interface NodeRequestLike {
  method?: string;
  url?: string;
  headers: Record<string, string | string[] | undefined>;
  socket?: { encrypted?: boolean };
  on(event: string, listener: () => void): unknown;
}

interface NodeResponseLike {
  statusCode: number;
  setHeader(name: string, value: string): unknown;
  write(chunk: Uint8Array): unknown;
  end(chunk?: Uint8Array): unknown;
  flushHeaders?(): unknown;
}

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value.join(", ") : value;
}

/** Reconstructs the absolute URL Node splits across `host` and `url`. */
function absoluteUrl(request: NodeRequestLike): string {
  const forwardedProto = headerValue(request.headers["x-forwarded-proto"]);
  const protocol = forwardedProto ?? (request.socket?.encrypted === true ? "https" : "http");
  const host = headerValue(request.headers["host"]) ?? "localhost";
  return `${protocol}://${host}${request.url ?? "/"}`;
}

function toWebRequest(request: NodeRequestLike): Request {
  const headers = new Headers();
  for (const [name, value] of Object.entries(request.headers)) {
    const resolved = headerValue(value);
    if (resolved !== undefined) headers.set(name, resolved);
  }

  const method = request.method ?? "GET";
  const hasBody = method !== "GET" && method !== "HEAD";

  // Aborted when the client disconnects, which is what lets an in-flight SSE
  // stream stop polling upstream instead of running to completion unread.
  const controller = new AbortController();
  request.on("aborted", () => controller.abort());
  request.on("close", () => controller.abort());

  return new Request(absoluteUrl(request), {
    method,
    headers,
    signal: controller.signal,
    ...(hasBody
      ? {
          body: Readable.toWeb(request as unknown as Readable) as ReadableStream<Uint8Array>,
          // Required by undici whenever a body is a stream.
          duplex: "half",
        }
      : {}),
  } as RequestInit);
}

async function writeWebResponse(response: Response, target: NodeResponseLike): Promise<void> {
  target.statusCode = response.status;
  response.headers.forEach((value, name) => target.setHeader(name, value));

  if (!response.body) {
    target.end();
    return;
  }

  // Headers go out before the first chunk so the browser starts an SSE stream
  // immediately rather than after the first event arrives.
  target.flushHeaders?.();

  const reader = response.body.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) target.write(value);
    }
  } finally {
    reader.releaseLock();
    target.end();
  }
}

/**
 * Express middleware for the broker.
 *
 * Requests that are not broker routes fall through to `next()`, so it can be
 * mounted at the app root without interfering with anything else.
 *
 * ```ts
 * app.use(beamExpressMiddleware(broker));
 * ```
 */
export function beamExpressMiddleware(broker: BeamBroker) {
  return function beamMiddleware(
    request: NodeRequestLike,
    response: NodeResponseLike,
    next: (error?: unknown) => void,
  ): void {
    void (async () => {
      try {
        const result = await broker.handle(toWebRequest(request));
        if (!result) {
          next();
          return;
        }
        await writeWebResponse(result, response);
      } catch (error) {
        next(error);
      }
    })();
  };
}

/** Handler for a bare `node:http` server, for apps not using Express. */
export function beamNodeHandler(broker: BeamBroker) {
  return async function handle(request: NodeRequestLike, response: NodeResponseLike): Promise<boolean> {
    const result = await broker.handle(toWebRequest(request));
    if (!result) return false;
    await writeWebResponse(result, response);
    return true;
  };
}
