/**
 * The broker request handler.
 *
 * Framework-agnostic: it takes a `Request` and returns a `Response`, so it drops
 * into Next route handlers, Hono, Bun, and Deno unchanged, and into Express or
 * Fastify through the adapters.
 */

import { assertScopes, mintToken, verifyToken } from "./token.js";
import type { BeamScope, BeamTokenClaims } from "./token.js";
import { BeamTokenError } from "./token.js";
import { BrokerError } from "./types.js";
import type { AuthorizeFn, BeamSessionContext, BeamUpstream, CreateRoomInput, CreateTransferInput } from "./types.js";

export interface BeamBrokerOptions {
  /** HMAC signing secret, at least 32 characters. Never sent to the browser. */
  secret: string;
  authorize: AuthorizeFn;
  upstream: BeamUpstream;
  /** Path the routes are mounted under. Default `/api/beam`. */
  basePath?: string;
  /** Default token lifetime in seconds. Capped at 300. Default 120. */
  ttlSeconds?: number;
  /**
   * Origins allowed to call the broker cross-origin.
   *
   * Omit when the browser and broker share an origin, which is the common case
   * and needs no CORS at all. Never set `"*"` here: the broker answers with
   * credentials and a wildcard would let any page mint tokens against a
   * visitor's session.
   */
  allowedOrigins?: readonly string[];
  issuer?: string;
  audience?: string;
  onError?: (error: unknown, request: Request) => void;
}

interface RouteMatch {
  name: string;
  params: Record<string, string>;
}

/** Routes, longest-first so `/transfers/:id/cancel` is not eaten by `/transfers/:id`. */
const ROUTES: Array<{ name: string; method: string; pattern: RegExp }> = [
  { name: "token", method: "POST", pattern: /^\/token$/ },
  { name: "rooms.create", method: "POST", pattern: /^\/rooms$/ },
  { name: "rooms.get", method: "GET", pattern: /^\/rooms\/([^/]+)$/ },
  { name: "transfers.create", method: "POST", pattern: /^\/transfers$/ },
  { name: "transfers.list", method: "GET", pattern: /^\/transfers$/ },
  { name: "transfers.events", method: "GET", pattern: /^\/transfers\/([^/]+)\/events$/ },
  { name: "transfers.cancel", method: "POST", pattern: /^\/transfers\/([^/]+)\/cancel$/ },
  { name: "transfers.get", method: "GET", pattern: /^\/transfers\/([^/]+)$/ },
];

function matchRoute(method: string, path: string): RouteMatch | undefined {
  for (const route of ROUTES) {
    if (route.method !== method) continue;
    const match = route.pattern.exec(path);
    if (match) {
      return { name: route.name, params: { id: match[1] ? decodeURIComponent(match[1]) : "" } };
    }
  }
  return undefined;
}

export interface BeamBroker {
  /** Handles one request. Returns `undefined` when the path is not a broker route. */
  handle(request: Request): Promise<Response | undefined>;
  /** Verifies a browser token. Exposed so an application can reuse the session. */
  verify(request: Request, requiredScopes?: readonly BeamScope[]): BeamTokenClaims;
  readonly basePath: string;
}

export function createBeamBroker(options: BeamBrokerOptions): BeamBroker {
  const basePath = (options.basePath ?? "/api/beam").replace(/\/+$/, "");
  const audience = options.audience ?? "beam-web-sdk";

  function corsHeaders(request: Request): Record<string, string> {
    const origin = request.headers.get("origin");
    if (!origin || !options.allowedOrigins?.includes(origin)) return {};
    return {
      "access-control-allow-origin": origin,
      // Without this a proxy may serve one origin's response to another.
      vary: "Origin",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "authorization, content-type, idempotency-key",
      "access-control-allow-credentials": "true",
      "access-control-max-age": "600",
    };
  }

  function json(request: Request, body: unknown, status = 200): Response {
    return new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json", "cache-control": "no-store", ...corsHeaders(request) },
    });
  }

  function failure(request: Request, error: unknown): Response {
    if (error instanceof BrokerError) {
      return json(request, { error: error.message, code: error.code }, error.status);
    }
    if (error instanceof BeamTokenError) {
      const status = error.code === "insufficient_scope" ? 403 : 401;
      return json(request, { error: error.message, code: error.code }, status);
    }
    options.onError?.(error, request);
    // The message is withheld: an upstream failure can carry internal hostnames,
    // credential prefixes, or SQL. The server-side hook gets the real error.
    return json(request, { error: "The Beam broker encountered an internal error.", code: "internal" }, 500);
  }

  function verify(request: Request, requiredScopes: readonly BeamScope[] = []): BeamTokenClaims {
    const header = request.headers.get("authorization") ?? "";
    if (!header.startsWith("Bearer ")) {
      throw new BeamTokenError("Missing bearer token.", "unauthenticated");
    }
    const claims = verifyToken(header.slice(7).trim(), {
      secret: options.secret,
      audience,
      ...(request.headers.get("origin") ? { origin: request.headers.get("origin") as string } : {}),
    });
    assertScopes(claims, requiredScopes);
    return claims;
  }

  function contextFrom(request: Request, claims: BeamTokenClaims): BeamSessionContext {
    return {
      subject: claims.sub,
      organizationId: claims.org,
      scopes: claims.scopes,
      claims,
      request,
    };
  }

  async function readJson(request: Request): Promise<Record<string, unknown>> {
    try {
      const text = await request.text();
      if (!text) return {};
      const parsed: unknown = JSON.parse(text);
      return parsed !== null && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
    } catch {
      throw new BrokerError("Request body is not valid JSON.", 400, "invalid_argument");
    }
  }

  function requireCapability<T>(method: T | undefined, name: string): NonNullable<T> {
    if (!method) {
      throw new BrokerError(
        `This Beam broker does not implement ${name}. Add it to the \`upstream\` passed to createBeamBroker.`,
        501,
        "not_implemented",
      );
    }
    return method as NonNullable<T>;
  }

  async function handleToken(request: Request): Promise<Response> {
    const session = await options.authorize(request);
    if (!session) {
      throw new BrokerError("Not permitted to obtain a Beam token.", 401, "unauthenticated");
    }

    const body = await readJson(request);
    const requested = Array.isArray(body["scopes"]) ? (body["scopes"] as unknown[]) : undefined;

    // The browser asks; the server decides. Requested scopes are intersected
    // with what `authorize` granted, never unioned, so a tampered request can
    // only ever narrow its own privileges.
    const granted = requested ? session.scopes.filter((scope) => requested.includes(scope)) : [...session.scopes];

    if (granted.length === 0) {
      throw new BrokerError("None of the requested scopes are permitted for this session.", 403, "permission_denied");
    }

    const origin = request.headers.get("origin");
    const { token, claims, expiresIn } = mintToken({
      secret: options.secret,
      scopes: granted,
      ttlSeconds: session.ttlSeconds ?? options.ttlSeconds,
      audience,
      ...(options.issuer !== undefined ? { issuer: options.issuer } : {}),
      ...(session.subject !== undefined ? { subject: session.subject } : {}),
      ...(session.organizationId !== undefined ? { organizationId: session.organizationId } : {}),
      ...(origin ? { origin } : {}),
    });

    return json(request, {
      access_token: token,
      token_type: "Bearer",
      expires_in: expiresIn,
      scopes: claims.scopes,
      endpoint: basePath,
    });
  }

  async function handleTransferEvents(
    request: Request,
    transferId: string,
    context: BeamSessionContext,
  ): Promise<Response> {
    const watch = requireCapability(options.upstream.watchTransfer, "watchTransfer");
    const controller = new AbortController();
    // Fires when the browser navigates away or calls `close()`, so the upstream
    // poll loop stops instead of running for the transfer's whole lifetime.
    request.signal.addEventListener("abort", () => controller.abort(), { once: true });

    const encoder = new TextEncoder();
    let sequence = 0;

    const stream = new ReadableStream<Uint8Array>({
      async start(streamController) {
        try {
          for await (const record of watch.call(options.upstream, transferId, context, controller.signal)) {
            sequence += 1;
            streamController.enqueue(
              encoder.encode(`id: ${sequence}\nevent: transfer\ndata: ${JSON.stringify(record)}\n\n`),
            );
          }
        } catch (error) {
          options.onError?.(error, request);
          streamController.enqueue(
            encoder.encode(`event: error\ndata: ${JSON.stringify({ error: "stream_failed" })}\n\n`),
          );
        } finally {
          controller.abort();
          try {
            streamController.close();
          } catch {
            // Already closed because the client disconnected first.
          }
        }
      },
      cancel() {
        controller.abort();
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-store",
        connection: "keep-alive",
        // Without this, nginx and several CDNs buffer the stream and the browser
        // sees nothing until the transfer finishes.
        "x-accel-buffering": "no",
        ...corsHeaders(request),
      },
    });
  }

  async function handle(request: Request): Promise<Response | undefined> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith(basePath)) return undefined;
    const path = url.pathname.slice(basePath.length) || "/";

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(request) });
    }

    const route = matchRoute(request.method, path);
    if (!route) return undefined;

    try {
      if (route.name === "token") return await handleToken(request);

      switch (route.name) {
        case "rooms.create": {
          const claims = verify(request, ["rooms:create"]);
          const body = await readJson(request);
          const input: CreateRoomInput = {
            ...(typeof body["name"] === "string" ? { name: body["name"] } : {}),
            ...(typeof body["ttl_seconds"] === "number" ? { ttlSeconds: body["ttl_seconds"] } : {}),
            ...(typeof body["region"] === "string" ? { region: body["region"] } : {}),
            ...(typeof body["mode"] === "string" ? { mode: body["mode"] } : {}),
            ...(body["metadata"] !== undefined ? { metadata: body["metadata"] as Record<string, unknown> } : {}),
          };
          const create = requireCapability(options.upstream.createRoom, "createRoom");
          return json(request, await create.call(options.upstream, input, contextFrom(request, claims)), 201);
        }
        case "rooms.get": {
          const claims = verify(request, ["room:read"]);
          const get = requireCapability(options.upstream.getRoom, "getRoom");
          return json(
            request,
            await get.call(options.upstream, route.params["id"] as string, contextFrom(request, claims)),
          );
        }
        case "transfers.create": {
          const claims = verify(request, ["transfers:create"]);
          const body = await readJson(request);
          if (typeof body["source"] !== "string" || !Array.isArray(body["destinations"])) {
            throw new BrokerError("`source` and `destinations` are required.", 400, "invalid_argument");
          }
          const input: CreateTransferInput = {
            source: body["source"],
            destinations: (body["destinations"] as unknown[]).map(String),
            ...(typeof body["name"] === "string" ? { name: body["name"] } : {}),
            ...(body["metadata"] !== undefined ? { metadata: body["metadata"] as Record<string, unknown> } : {}),
          };
          const create = requireCapability(options.upstream.createTransfer, "createTransfer");
          return json(request, await create.call(options.upstream, input, contextFrom(request, claims)), 201);
        }
        case "transfers.list": {
          const claims = verify(request, ["transfers:read"]);
          const list = requireCapability(options.upstream.listTransfers, "listTransfers");
          const limit = Number(url.searchParams.get("limit") ?? "");
          const status = url.searchParams.get("status");
          const transfers = await list.call(
            options.upstream,
            {
              ...(Number.isFinite(limit) && limit > 0 ? { limit } : {}),
              ...(status !== null ? { status } : {}),
            },
            contextFrom(request, claims),
          );
          return json(request, { transfers });
        }
        case "transfers.get": {
          const claims = verify(request, ["transfers:read"]);
          const get = requireCapability(options.upstream.getTransfer, "getTransfer");
          return json(
            request,
            await get.call(options.upstream, route.params["id"] as string, contextFrom(request, claims)),
          );
        }
        case "transfers.events": {
          const claims = verify(request, ["transfers:read"]);
          return await handleTransferEvents(request, route.params["id"] as string, contextFrom(request, claims));
        }
        case "transfers.cancel": {
          const claims = verify(request, ["transfers:cancel"]);
          const cancel = requireCapability(options.upstream.cancelTransfer, "cancelTransfer");
          await cancel.call(options.upstream, route.params["id"] as string, contextFrom(request, claims));
          return json(request, { cancelled: true });
        }
        default:
          return undefined;
      }
    } catch (error) {
      return failure(request, error);
    }
  }

  return { handle, verify, basePath };
}
