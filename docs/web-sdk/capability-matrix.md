# Capability matrix

What this SDK exposes today, what it does not, and why. Capabilities that are not
ready are **absent from the API** rather than stubbed — a method that throws
`NotImplemented` is worse than no method, because it survives code review and
fails in production.

## Available

| Capability                         | API                          | Backed by                                |
| ---------------------------------- | ---------------------------- | ---------------------------------------- |
| Create and read rooms              | `beam.rooms.create` / `.get` | Coordinator media rooms, via your broker |
| Join a room, publish and subscribe | `beam.rooms.join`            | Relay SFU signalling, browser-direct     |
| Room lifecycle events              | `session.on(…)`              | Relay SSE, with `Last-Event-ID` resume   |
| Swap a track without renegotiating | `session.replaceTrack`       | `RTCRtpSender.replaceTrack`              |
| Broadcast publish                  | `beam.broadcast.publish`     | Relay WHIP                               |
| Broadcast watch                    | `beam.broadcast.watch`       | Relay WHEP                               |
| Create, watch, cancel transfers    | `beam.transfers.*`           | `@beam-network/sdk` in your broker       |
| Camera, microphone, screen capture | `beam.media.*`               | `getUserMedia` / `getDisplayMedia`       |

## Standalone Web Agent integration

The separate `WebAgent` export now supports existing-room admission, message
channels, media channel discovery, and browser media publishing/viewing through
the standalone agent's authenticated WebSocket API. It uses that instance's
identity and grants; it does not add room administration or change the existing
`Beam` broker client. See [the Web Agent guide](web-agent.md).

The earlier browser-MLS restriction below still applies to direct browser access.
For `WebAgent`, MLS lives in the server-side agent. Protected byte streams remain
deferred because normal-agent compatibility is unresolved (agent issue #117).

## Not available

### `beam.streams` — room data channels

**Blocked on end-to-end encryption the browser cannot participate in.**

BTR channels carry `message`, `stream`, `datagram`, `request-reply`, and `object`
payloads, and every protected channel is an RFC 9420 **MLS group**. Only
authenticated Beam agents hold MLS credentials, leaf private keys, ratchet state,
and exporter secrets. The coordinator's `mls-deliveries` endpoint is a mailbox for
opaque ciphertext — it can be long-polled from a browser, but nothing there can be
decrypted without an MLS implementation and a credential the browser has no way
to obtain.

To unblock: either a browser MLS stack plus an enrollment path that yields real
MLS credentials, or a server-side channel bridge that terminates MLS in the
broker and re-encrypts to the browser over TLS. The second is far simpler and
weakens the guarantee — the broker would see plaintext — so it is a product
decision, not just an engineering one.

Today, room _media_ works and room _data channels_ do not. That distinction is
the whole reason `beam.streams` is missing rather than half-present.

### `beam.tunnels` — private resource exposure

**Blocked on the browser not being able to be an agent.**

A tunnel exposes a private resource — a local file, an HTTP service, a TCP port —
by running an agent process on the machine that holds it. That agent dials out,
maintains a yamux-over-TLS or QUIC session, and serves the resource. A browser
tab cannot do any of that, and should not.

A browser _can_ consume the resulting public URL, but that is an ordinary HTTPS
fetch and needs no SDK support.

To unblock: nothing, for the publishing side. If tunnel _management_ from a
dashboard is wanted — list, create, revoke — that is a control-plane API and
would fit the existing broker pattern.

## Deliberate API departures

The brief sketched an API. Where this SDK differs, it is on purpose.

**`apiKey` → `clientKey` / `tokenEndpoint` / `getToken`.** The example
constructor took an `apiKey`. Accepting one would mean shipping a
credit-spending credential to every visitor. This is the single most important
change in the design.

**No `beam.streams.create({ roomId })`.** In the sketch this created a stream in a
room — which is what `beam.rooms.join({ publish })` already does. A separate
`streams` namespace would either duplicate rooms or imply the MLS channels above
work, and neither is honest.

**Typed events per object, not `on(string, any)`.** `session.on` and
`transfer.on` have distinct, exhaustive event maps, so a typo is a compile error
and payloads are inferred.

**Namespaced event names.** `participant.joined`, not `joined`, so the vocabulary
can grow without collisions.

## Beam internals the SDK never exposes

Not incidental — an explicit boundary. None of these appear in any type, event, or
error the SDK produces:

orchestrators · workers · validators · relays · relay ids · tunnels · tunnel ids ·
leases · plans · plan versions · assignments · assignment tokens · epochs ·
authorization epochs · key epochs · PRISM · shards · NATS subjects · JetStream ·
metagraph · netuid · hotkeys · workload ids · flow ids · path ids · attempt ids ·
federation edges

The broker's coordinator upstream strips these on the way through, and a test
asserts it. If a future capability needs one of them in the browser, it gets an
SDK-level name and an SDK-level type first.
