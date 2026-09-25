import process from "node:process";
import console from "node:console";
// Local-only static server. It never receives or forwards the instance credential.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import { fileURLToPath, URL } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));
const sdk = resolve(here, "../../packages/web-sdk/dist");
const port = Number(process.env.PORT ?? 8088);
const server = createServer(async (request, response) => {
  try {
    const path = decodeURIComponent(new URL(request.url, "http://localhost").pathname);
    const root = path.startsWith("/sdk/") ? sdk : here;
    const name = path.startsWith("/sdk/") ? path.slice(5) : path === "/" ? "index.html" : path.slice(1);
    const file = resolve(root, name);
    if (!file.startsWith(root + sep) || (!path.startsWith("/sdk/") && !["index.html", "app.js"].includes(name))) {
      response.writeHead(404).end();
      return;
    }
    const body = await readFile(file);
    response.writeHead(200, {
      "Content-Type": file.endsWith(".html") ? "text/html; charset=utf-8" : "text/javascript; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    });
    response.end(body);
  } catch {
    response.writeHead(404).end();
  }
});
server.listen(port, "127.0.0.1", () => console.log(`Web Agent SDK example: http://127.0.0.1:${server.address().port}`));
