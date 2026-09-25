/**
 * Example broker + static server.
 *
 * Runs entirely on localhost with no Beam credentials by default, so the whole
 * flow — sign in, mint a token, create a room, start a transfer, watch progress
 * — can be exercised before anyone has been issued a key.
 *
 * Point it at real Beam by setting BEAM_COORDINATOR_URL and BEAM_API_KEY.
 *
 *   node server/index.js
 *   BEAM_COORDINATOR_URL=http://127.0.0.1:8787 BEAM_API_KEY=b1m_… node server/index.js
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname, extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

import { coordinatorRooms, createBeamBroker, memoryUpstream } from "@beam-network/web-sdk-server";
import { beamNodeHandler } from "@beam-network/web-sdk-server/adapters/express";

const here = dirname(fileURLToPath(import.meta.url));
const publicDir = join(here, "..", "public");
/**
 * The built browser package, served at /vendor/. Serving the whole directory
 * rather than a single file matters: the ESM build is code-split, and its
 * entry point imports sibling chunks by relative path.
 */
const vendorDir = join(here, "..", "..", "..", "packages", "web-sdk", "dist");
const port = Number(process.env.PORT ?? 3000);

/**
 * In production this comes from the environment and is stable across restarts.
 * Generating one per run is fine here and makes the example safe to run as-is:
 * a leftover dev secret can never be mistaken for a real one.
 */
const secret = process.env.BEAM_TOKEN_SECRET ?? randomBytes(32).toString("hex");

const coordinatorUrl = process.env.BEAM_COORDINATOR_URL;
const apiKey = process.env.BEAM_API_KEY;
const live = Boolean(coordinatorUrl && apiKey);

/**
 * Rooms come from the coordinator when credentials are present, and from the
 * in-process fake otherwise. Transfers always use the fake here: the real path
 * needs `@beam-network/sdk` and a long-lived process, which is documented in
 * `docs/architecture.md` rather than demonstrated in a toy example.
 */
const upstream = live
  ? { ...memoryUpstream(), ...coordinatorRooms({ coordinatorUrl, apiKey }) }
  : memoryUpstream({ totalBytes: 256 * 1024 * 1024, bytesPerTick: 16 * 1024 * 1024, tickMs: 300 });

const broker = createBeamBroker({
  secret,
  upstream,
  onError: (error) => console.error("[broker]", error),

  /**
   * The security boundary. A real application checks its own session here — a
   * cookie, a header, a JWT — and returns the scopes that user may hold.
   *
   * This example trusts a header so the flow can be exercised without a login,
   * and says so loudly. Never ship this.
   */
  authorize: (request) => {
    const user = request.headers.get("x-demo-user") ?? "demo-user";
    return {
      subject: user,
      scopes: [
        "rooms:create",
        "rooms:join",
        "room:read",
        "broadcast:publish",
        "broadcast:watch",
        "transfers:create",
        "transfers:read",
        "transfers:cancel",
      ],
    };
  },
});

const handleBeam = beamNodeHandler(broker);

const CONTENT_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".map": "application/json",
};

async function serveStatic(request, response) {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  const pathname = url.pathname === "/" ? "/index.html" : url.pathname;

  const [root, requested] = pathname.startsWith("/vendor/")
    ? [vendorDir, pathname.slice("/vendor".length)]
    : [publicDir, pathname];

  // `normalize` collapses `..` so a crafted path cannot escape the served
  // directory; the prefix check rejects anything that still points outside it.
  const target = normalize(join(root, requested));
  if (!target.startsWith(root)) {
    response.statusCode = 403;
    response.end("Forbidden");
    return;
  }

  try {
    const body = await readFile(target);
    response.statusCode = 200;
    response.setHeader("content-type", CONTENT_TYPES[extname(target)] ?? "application/octet-stream");
    response.end(body);
  } catch {
    if (root === vendorDir) {
      response.statusCode = 404;
      response.end("The browser SDK has not been built yet. Run `pnpm build` at the repository root.");
      return;
    }
    response.statusCode = 404;
    response.end("Not found");
  }
}

createServer(async (request, response) => {
  try {
    if (await handleBeam(request, response)) return;
    await serveStatic(request, response);
  } catch (error) {
    console.error("[server]", error);
    if (!response.headersSent) response.statusCode = 500;
    response.end("Internal error");
  }
}).listen(port, () => {
  console.log(`\n  Beam Web SDK example  →  http://localhost:${port}\n`);
  console.log(
    `  rooms      ${live ? `live coordinator at ${coordinatorUrl}` : "in-process fake (no credentials needed)"}`,
  );
  console.log(`  transfers  in-process fake`);
  if (!process.env.BEAM_TOKEN_SECRET) {
    console.log(`\n  Using a generated signing secret. Set BEAM_TOKEN_SECRET to keep tokens valid across restarts.`);
  }
  console.log(`\n  The authorize() hook in this example trusts an x-demo-user header.`);
  console.log(`  Replace it with your own session check before using any of this for real.\n`);
});
