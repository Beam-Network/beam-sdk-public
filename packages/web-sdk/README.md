# @beam-network/web-sdk

Browser SDK for Beam Network — rooms, live media, broadcast, and data transfers.

```sh
npm install @beam-network/web-sdk
```

```ts
import { Beam } from "@beam-network/web-sdk";

const beam = new Beam({ clientKey: "bm_pub_…" });

const room = await beam.rooms.create({ name: "standup" });
const session = await beam.rooms.join(room, { publish: await beam.media.camera() });

session.on("track.added", ({ stream }) => attachToVideoElement(stream));
```

**Never pass a Beam API key.** `bm_live_…` and `b1m_…` keys spend credits, and
anything in browser code is readable by every visitor. Use a publishable
`bm_pub_…` key from the Beam console, or mint short-lived tokens from your own
backend with [`@beam-network/web-sdk-server`](https://www.npmjs.com/package/@beam-network/web-sdk-server).
The constructor throws if it finds a secret key.

Zero runtime dependencies. ESM and CJS, with subpath exports for `./rooms`,
`./broadcast`, and `./transfers`.

Full documentation: <https://github.com/Beam-Network/beam-sdk-public>

## Standalone Web Agent

Use the new `WebAgent` export for the standalone agent’s WebSocket API: room
participation, messages, and media. This is separate from the existing `Beam`
broker client. See [the Web Agent guide](../../docs/web-sdk/web-agent.md) for
authentication, examples, cleanup, local verification, and dev integration.
