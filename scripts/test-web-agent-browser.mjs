/* global window, location, sdk, scope, received, sdkSubscription */
import process from "node:process";
import console from "node:console";
// Invoked only by the local Go fixture, with disposable fixture credentials.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { resolve, dirname, sep } from "node:path";
import { fileURLToPath, URL } from "node:url";
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.BEAM_WEB_PLAYWRIGHT_MODULE || "playwright");
const [origin, credential, roomId, channelId] = process.argv.slice(2);
assert.equal(new URL(origin).hostname, "127.0.0.1");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../packages/web-sdk/dist");
const browser = await chromium.launch({
  headless: true,
  ...(process.env.BEAM_WEB_CHROMIUM ? { executablePath: process.env.BEAM_WEB_CHROMIUM } : {}),
  args: ["--disable-background-networking", "--disable-component-update"],
});
try {
  const context = await browser.newContext();
  await context.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith("/sdk/")) return route.continue();
    const path = resolve(root, decodeURIComponent(url.pathname.slice(5)));
    if (!path.startsWith(root + sep)) return route.abort();
    return route.fulfill({ status: 200, contentType: "text/javascript", body: await readFile(path) });
  });
  const pages = await Promise.all([context.newPage(), context.newPage()]);
  const errors = [];
  for (const page of pages) {
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto(`${origin}/example/`);
    await page.evaluate(
      async (config) => {
        const { WebAgent } = await import("/sdk/web-agent/index.js");
        window.sdk = new WebAgent({
          url: `${location.origin.replace("http", "ws")}/v1/connect`,
          getCredential: () => config.credential,
        });
        await sdk.connect();
        window.scope = { roomId: config.roomId, channelId: config.channelId };
        const channels = await sdk.listChannels(config.roomId);
        if (!channels.some((channel) => channel.id === config.channelId)) throw new Error("Missing fixture channel");
        window.received = [];
        window.sdkSubscription = await sdk.subscribe(scope, "message");
        sdkSubscription.on("message", (message) => received.push(Array.from(message.payload)));
        await sdkSubscription.ready;
      },
      { credential, roomId, channelId },
    );
  }
  const payload = Array.from({ length: 65536 }, (_, i) => i % 251);
  await pages[0].evaluate(async (payload) => {
    window.result = await sdk.publishMessage(scope, new Uint8Array(payload));
  }, payload);
  for (const page of pages) {
    await page.waitForFunction(() => received.length === 1);
    assert.deepEqual(await page.evaluate(() => received[0]), payload);
  }
  await pages[0].evaluate(() => sdk.disconnect());
  await pages[1].evaluate(() => sdk.publishMessage(scope, "remaining subscriber"));
  await pages[1].waitForFunction(() => received.length === 2);
  assert.equal(await pages[0].evaluate(() => sdkSubscription.state), "closed");
  await pages[0].evaluate(async () => {
    await sdk.connect();
    window.sdkSubscription = await sdk.subscribe(scope, "message");
    sdkSubscription.on("message", (message) => received.push(Array.from(message.payload)));
    await sdkSubscription.ready;
    await sdk.publishMessage(scope, "after reconnect");
  });
  await pages[0].waitForFunction(() => received.length === 2);
  await pages[1].waitForFunction(() => received.length === 3);
  for (const page of pages)
    await page.evaluate(async () => {
      await sdkSubscription.close();
      sdk.disconnect();
    });
  assert.deepEqual(errors, []);
  console.log(
    JSON.stringify({
      passed: true,
      clients: 2,
      byteExactPayload: payload.length,
      sharedFanout: true,
      independentDisconnect: true,
      explicitReconnect: true,
      scope: "built SDK, actual Go WebSocket API, local participant double",
    }),
  );
  await context.close();
} finally {
  await browser.close();
}
