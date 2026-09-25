# Architecture

## The shape of the problem

Beam's control plane is not reachable from a browser, and not by accident.

- The **transfer lifecycle runs over NATS**, not HTTP. The transfer runtime
  says so at the top of its transfers route: _"Historical /
  dashboard read path. SDK lifecycle traffic uses NATS transfer-client-control."_
  There is no `POST /transfers` anywhere in that repo.
- That NATS connection **authenticates with the raw API key as the password**
  (`user = apiKey.slice(0,12)`, `pass = apiKey`). A browser speaking NATS over
  WebSocket would still need the key.
- The current transfer flow has the client **presign every storage route locally**
  and then **serve a reverse-RPC subject** so BeamCore can call back to re-sign
  routes that expire mid-transfer. A browser can do neither.

Meanwhile the media plane is genuinely browser-native: the coordinator's media
rooms return a relay URL that carries its own capability token, and the relay
speaks WHIP, WHEP, an SFU signalling API, and SSE — all over ordinary HTTPS, with
CORS already reflecting any origin.

So the two planes get treated differently, because they _are_ different.

## Control plane and data plane

```
                     ┌───────── your backend (Node) ─────────┐
 browser                @beam-network/web-sdk-server
 @beam-network/web-sdk  holds bm_live_… / b1m_…  ─────────────► Beam control plane
    │  POST /token ──────────►  mints a scoped token (≤300 s)     coordinator /media/rooms
    │  ◄──── bwt1.<claims>.<sig>                                  @beam-network/sdk (NATS)
    │  control calls (token) ►  proxies with the real credential
    │
    └── DATA PLANE: browser ↔ relay DIRECTLY ─────────────────────► WHIP / WHEP / SFU / SSE
        (relay URLs already carry their own capability tokens)
```

**Control plane** — create a room, create a transfer, read status. Privileged,
always brokered, never reachable with anything a browser holds.

**Data plane** — media bytes and room events. Browser-direct, because the
credential is already inside the URL the control plane returned:
`/t/{tunnel}/webrtc/{public_token}/…`. Sending the session's Beam token here as
well would leak it to the relay for no benefit, so the SDK deliberately does not.

This is the same split LiveKit, Ably, and Stripe ship, and it needs **no changes
to Beam's backend** to work.

## Why transfers are brokered — and why that is still real

The browser calls your backend; your backend drives a real `BeamClient` against
BeamCore and streams real lifecycle back. Nothing is simulated.

File bytes never touch the browser _or_ the broker. Beam's workers move them
directly between presigned storage endpoints, exactly as they do for the Node
SDK today.

### Two broker profiles

They have different deployment requirements, and conflating them causes failures
that are hard to diagnose.

| Profile      | What it does                          | Where it can run                                                     |
| ------------ | ------------------------------------- | -------------------------------------------------------------------- |
| **Edge**     | Mints tokens; creates and reads rooms | Anywhere, including serverless — Vercel, Lambda, Next route handlers |
| **Transfer** | Owns the `BeamClient`                 | **A long-lived process only**                                        |

The transfer broker holds a NATS connection _and_ answers a route-recovery
callback for the duration of each transfer. On a function runtime the process is
frozen or torn down between requests, that callback goes unanswered, and long
transfers fail partway with no obvious cause. Run it on a container, a VM, or a
persistent worker.

### Endpoint references

The browser never names a bucket, a region, or a credential:

```ts
await beam.transfers.create({ source: "s3:reports", destination: "archive:2026" });
```

Your broker resolves `"s3:reports"` against configuration it holds:

```ts
beamSdkTransfers({
  apiKey: process.env.BEAM_API_KEY,
  resolveEndpoint: (reference, { subject, organizationId }) => {
    const endpoint = endpointsFor(organizationId)[reference];
    return endpoint; // undefined → 400, reference rejected
  },
});
```

This is what keeps object-storage credentials server-side _by construction_
rather than by convention — and it doubles as an authorization point, since
`resolveEndpoint` sees who is asking.

## Progress is polling, and the docs say so

BeamCore publishes exactly one push event per transfer: a terminal signal on
`beam.transfer.client.{env}.events.{key_prefix}.{id}.terminal`. There is no
incremental progress stream.

So `transfer.on("progress")` is your broker polling `transfer.status` and
re-emitting the result as SSE. The terminal event is real and immediate;
intermediate progress arrives at your configured poll interval. The SDK does not
pretend otherwise, and `watchTransfer` in your upstream is where the interval
lives.

## Reliability

One backoff policy is shared by every long-lived connection — the room event
stream, WHIP/WHEP renegotiation, and the transfer progress stream:

- **Full jitter**, not equal jitter. A relay restart drops every participant in a
  room simultaneously; a delay floor would bring them all back in a synchronized
  wave.
- **Offline gating.** Reconnecting while `navigator.onLine` is false burns
  attempts against a network that cannot answer.
- **`Retry-After` wins** over the computed curve. The server knows its own
  recovery window better than the client's math does.
- **Non-retryable errors stop the loop.** A 403 or a 404 is not going to start
  working, and retrying it forever hides the real problem.
- **Bounded where a fallback exists.** A room's event stream retries
  indefinitely, because a participant sitting in a room wants to come back. A
  transfer's progress stream gives up after three attempts and falls back to
  polling, because polling actually works.

### Idempotency

The coordinator _requires_ an `Idempotency-Key` on room create, invitation
create, and join, and answers `409` when the same key arrives with a different
body. The SDK generates one key per logical operation and **reuses it across
retries of that operation**. Generating a fresh key per attempt would turn a
timeout-then-retry into two rooms and two credit reservations.

## Package layout

```
packages/web-sdk/           @beam-network/web-sdk         browser, zero runtime deps
  src/core/                 auth, transport, events, errors, retry, logger
  src/rooms/                SFU signalling, session lifecycle
  src/broadcast/            WHIP / WHEP
  src/transfers/            create, watch, cancel
  src/global.ts             script-tag build (window.Beam)

packages/web-sdk-server/    @beam-network/web-sdk-server  Node
  src/token.ts              mint and verify
  src/broker.ts             Request → Response handler
  src/upstreams/            coordinator (rooms), beam-sdk (transfers), memory (dev)
  src/adapters/             express, node:http
```

`@beam-network/sdk` is an **optional peer dependency** of the server package,
imported lazily through a variable specifier. An application that only uses rooms
never installs `nats` or the AWS SDK, and never pays for typechecking against
them either.

## Build outputs

The browser package ships ESM and CJS per subpath entry, with declarations for
both, plus one IIFE bundle:

- `dist/index.js` / `.cjs` — and the same for `./rooms`, `./broadcast`, `./transfers`
- `dist/beam.min.js` — the script-tag build, global `Beam`

The global name and the CDN path are a **public contract**: the Beam console
hands out a snippet containing both, and every page that pasted it breaks if
either changes. New versions go in a new CDN path; they do not edit the old one.

## Standalone Web Agent transport

The optional `WebAgent` client adds a separate connection model alongside the
existing broker-based `Beam` client. It authenticates a WebSocket with an instance
API credential, commands the instance’s shared membership, and uses browser
WebRTC for media to/from that agent. The agent owns Beam identity, MLS, and remote
workload participation. This does not change existing broker routes or room APIs.
See [the Web Agent guide](web-agent.md) for its implemented surface and limits.
