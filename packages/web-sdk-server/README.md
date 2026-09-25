# @beam-network/web-sdk-server

Server-side token broker for [`@beam-network/web-sdk`](https://www.npmjs.com/package/@beam-network/web-sdk).

Runs in your backend. Holds the Beam API key, decides which scopes a browser
session may hold, and mints short-lived tokens the browser uses instead.

```sh
npm install @beam-network/web-sdk-server
```

```ts
import { createBeamBroker, coordinatorRooms } from "@beam-network/web-sdk-server";
import { beamExpressMiddleware } from "@beam-network/web-sdk-server/adapters/express";

const broker = createBeamBroker({
  secret: process.env.BEAM_TOKEN_SECRET, // openssl rand -hex 32
  upstream: coordinatorRooms({
    coordinatorUrl: process.env.BEAM_COORDINATOR_URL,
    apiKey: process.env.BEAM_API_KEY,
  }),
  authorize: async (request) => {
    const user = await currentUser(request); // your session, your rules
    return user ? { subject: user.id, scopes: ["rooms:create", "rooms:join", "room:read"] } : null;
  },
});

app.use(beamExpressMiddleware(broker));
```

`broker.handle(request)` takes a WHATWG `Request` and returns a `Response`, so it
also drops into Next route handlers, Hono, Bun, and Deno unchanged.

Tokens are capped at **300 seconds**, carry explicit scopes with no implication,
and are bound to the requesting origin. Scopes the browser asks for are
intersected with what `authorize` granted — never unioned.

`memoryUpstream()` runs the whole surface in-process, so you can build against it
with no Beam credentials.

**Transfers need a long-lived process.** `beamSdkTransfers()` holds a NATS
connection and answers a route-recovery callback for each transfer's duration; on
a function runtime that callback goes unanswered and long transfers fail partway.
`@beam-network/sdk` is an optional peer dependency, imported lazily.

Full documentation: <https://github.com/Beam-Network/beam-sdk-public>
