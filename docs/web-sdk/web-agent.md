# Standalone Web Agent client

`WebAgent` adds the standalone agent's browser protocol to `@beam-network/web-sdk`.
The existing `Beam`, room/broadcast, and storage-transfer APIs remain unchanged.
It has zero runtime dependencies and is available from the package root,
`@beam-network/web-sdk/web-agent`, and the existing script-tag global as
`Beam.WebAgent`.

This client supports room admission, channel discovery, messages, and media.
Byte-stream methods are deliberately absent while Web/normal stream compatibility
is deferred in agent issue #117.
There are no room/channel administration methods.

## Connect and join

```ts
import { WebAgent } from "@beam-network/web-sdk/web-agent";

const agent = new WebAgent({
  url: "wss://web-agent.example/v1/connect",
  getCredential: async (signal) => readInstanceCredentialFromYourTrustedUI(signal),
});
agent.on("error", (error) => showError(error.code, error.message));
await agent.connect();
await agent.joinRoom(roomId, { invitation, idempotencyKey: savedJoinKey });
const channels = await agent.listChannels(roomId);
```

The URL is the standalone Web Agent endpoint, **not** the coordinator or the
existing SDK token broker. Configure the Web Agent's exact allowed browser Origin.
Only loopback endpoints may use `ws:`; other endpoints require `wss:`. Credentials
in URL userinfo, query parameters, or fragments are rejected.

`getCredential` supplies the independent instance API credential. Beam API keys,
agent credentials, and Studio delegations are rejected before opening a socket.
The SDK does not store credentials in browser storage or put them in URLs. This
credential commands the shared instance: supply it only to a trusted browser
application, not an anonymous public bundle. Every connected browser acts with
the same instance identity and room grants.

`leaveRoom(roomId)` leaves for that shared identity and affects other browsers.
`disconnect()` only closes the current client's resources. Closing a tab does not
leave the room automatically.

## Server-side controller use

A long-lived service can use the same message client with a Node WebSocket
implementation supplied through `webSocketFactory`. The standalone Web Agent
requires an exact configured `Origin` even for Node connections; that header is
an origin check, while the instance API credential authenticates the session.
Keep the credential in the service and pin the enrolled identity:

```ts
import WebSocket from "ws";
import { WebAgent } from "@beam-network/web-sdk/web-agent";

const agent = new WebAgent({
  url: webAgentUrl,
  expectedAgentId: storedAgentId,
  getCredential: () => loadInstanceCredential(),
  webSocketFactory: (url, protocol) =>
    new WebSocket(url, protocol, { origin: configuredOrigin }) as unknown as globalThis.WebSocket,
});
const session = await agent.connect();
// session.agentId is the enrolled identity; session.bootId changes on process restart.
```

With `expectedAgentId`, a missing or different `agentId` or `bootId` rejects
authentication, and a later identity or boot change closes the socket. Without
that option, these session fields are optional to preserve existing browser
clients and local fixtures. A controller should persist its expected agent ID,
compare the boot ID after each reconnect, and reconcile its own durable command
journal before it retries uncertain work.

Message subscriptions belong to one WebSocket. A service must reconnect and
resubscribe after interruption; the API has no delivery cursor or replay. A
message event supplies publication ID, publisher member ID and sequence, while
`publishMessage` accepts a caller idempotency key and reports publication ID,
sequence and delivery counts. These transport identities do not themselves
prove a workflow command or reply was accepted. Keep run, task, attempt,
generation, recipient and deadline checks in the server-side workflow contract.
The Web Agent retains Beam identity and MLS state; the SDK receives application
payload bytes but no MLS keys. Closing a browser tab must not close the service's
connection or stop its workflow controller.

## Messages

```ts
const scope = { roomId, channelId: messageChannelId };
const subscription = await agent.subscribe(scope, "message");
subscription.on("message", (message) => {
  render(new TextDecoder().decode(message.payload));
});
subscription.on("error", (error) => showError(error.code, error.message));
await subscription.ready;

const result = await agent.publishMessage(scope, "hello", {
  idempotencyKey: savedPublicationKey,
});
// Inspect counts: accepted, delivered, failed, expired, skipped, and queued.
// Resolving the promise does not mean every intended recipient received it.
await subscription.close();
```

Payloads are strings (UTF-8) or `Uint8Array`, from 1 through 65,536 bytes.
Delivery payloads are always `Uint8Array`. Public results contain publication IDs
and delivery counts; private routing fields and internal receipts are omitted.

## Publish and watch media

```ts
import { media } from "@beam-network/web-sdk";

const scope = { roomId, channelId: mediaChannelId };
const captured = await media.camera(); // or media.screen(), media.microphone()
const publisher = await agent.publishMedia(scope, captured, {
  stopTracksOnClose: true,
});
await publisher.ready; // actual WebRTC connection, not just an SDP answer

const subscription = await agent.subscribe(scope, "media");
subscription.on("broadcasts", (broadcasts) => updateBroadcastPicker(broadcasts));
await subscription.ready;
const broadcasts = await agent.listMedia(scope);
const viewer = await agent.watchMedia(subscription, broadcasts[0]?.id);
video.srcObject = viewer.stream;
await viewer.ready;
await video.play();

await viewer.close();
await publisher.close();
await subscription.close();
```

A viewer requires a ready media subscription owned by this client. Closing that
subscription closes its dependent viewers. Each publisher and viewer has its own
peer connection and can close independently. `stream` also exposes tracks that
arrived before the caller attached a `track` event listener.

Caller-provided capture tracks remain caller-owned by default; set
`stopTracksOnClose: true` to stop them when publishing ends. Viewer tracks always
stop on close. If capture succeeds but calling the SDK fails before ownership is
established, the caller should stop its captured stream in its error handler.

Configure `iceServers` for the browser peers as needed. The Web Agent's own ICE
configuration is independent. Version 1 has no trickle-ICE command: gathering must
complete within `iceGatheringTimeoutMs` (default 10 seconds), otherwise setup fails
without sending a partial offer. `mediaConnectionTimeoutMs` defaults to 30 seconds
and bounds setup/initial connection. Device access needs a secure context and user
permission; autoplay policies still apply to remote audio.

## Cancellation, errors, and reconnect

Requests accept `signal` and `timeoutMs`. Signals cancel setup/request work; they
are not lifetime signals after a media session or subscription is returned. Use
its `close()` method for that. The default request deadline is 30 seconds.

An aborted or timed-out request is not automatically retried: admission or a
publication may already have committed. Pending cancellation responses remain
tracked so late-created resources can be released. If the original result stays
unknown for another 30 seconds, the SDK closes its connection to prevent abandoned
resources from surviving. Error text is sanitized and mapped to existing
`BeamError` codes; raw server messages and private fields are never forwarded.

Reconnect is explicit: call `connect()` after a disconnect, recreate subscriptions
and viewers, and decide whether to republish. Old handles remain closed. Credentials
are requested again on each connection. There is no automatic publication replay,
stream resume, or promise of durable message deduplication. Preserve application
idempotency keys to investigate uncertain results within the server's retention
window (currently one minute in memory, with capacity eviction).

At most 28 requests may be pending, leaving headroom for cancellation/cleanup. The
client stops admitting ordinary requests after 4,000 request IDs; reconnect to
renew the session. Control frames are capped at 256 KiB and socket buffering at
1 MiB. The server's session, rate and resource limits still apply.

## Local example and verification

Build the browser package, then run the local static example:

```sh
npm run build -w @beam-network/web-sdk
node examples/web-agent/server.mjs
```

Open `http://127.0.0.1:8088`. Configure the standalone Web Agent with
`--origins http://127.0.0.1:8088`, provide its WebSocket URL and instance credential,
and join an existing room. The page supports channel discovery, messages, camera
and screen publishing, viewing, and independent cleanup. It does not persist
credentials or invitations. The static server listens only on loopback.

For the actual local Go WebSocket API plus Chromium check, build the package and
run `python3 scripts/test-web-agent-local.py` with a sibling `beam-tunnel-agent`
checkout containing the issue #116 fixtures. Set `BEAM_WEB_BROWSER_NODE`,
`BEAM_WEB_PLAYWRIGHT_MODULE`, and `BEAM_WEB_CHROMIUM` for locally installed tools.
It uses a temporary Go source overlay; the agent repository and installed state
are not modified. Dependencies must already be cached (`GOPROXY=off`). On macOS:

```sh
sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*") (remote unix))' python3 scripts/test-web-agent-local.py
```

This checks the built SDK and real WebSocket API with two Chromium pages,
byte-exact 64 KiB messages, independent disconnect, and explicit reconnect. Its
participant is a local double: passing is not coordinator/Worker acceptance.

## Dev integration prerequisites

Use a dedicated Web Agent state directory enrolled against
`https://coordinator.b1m.ai`, with an ordinary participant identity invited to
an isolated test room. Supply a separate instance API credential and configure
the example's Origin. An eligible room controller must be online to establish
and rotate room keys. Do not reuse a running normal agent's state directory.

Then use two tabs to verify discovery, bidirectional messages, camera/audio or
screen publishing and viewing, independent disconnect, and explicit reconnect.
Include a normal dev participant for media interoperability. Observe actual
Worker allocation and delivery, real membership/key rotation, revocation, and
cleanup. Keep test identities/rooms separate from existing activity. These are
integration acceptance checks, not covered by unit tests or the local doubles.
