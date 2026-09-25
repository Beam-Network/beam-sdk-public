# Example: rooms, broadcast, and transfers

Runs entirely on localhost with **no Beam credentials**, so the whole flow works
before anyone has been issued a key.

```sh
pnpm install
pnpm build                 # the page loads the built browser package
pnpm --filter @beam-network/example-rooms-and-broadcast dev
# → http://localhost:3000
```

## What it shows

- A token broker in `server/index.js` — about 40 lines of real configuration
- A browser app in `public/app.js` that never sees a Beam credential
- Room creation, camera publishing, participant events, transfer progress

## Against real Beam

```sh
BEAM_COORDINATOR_URL=https://coordinator.example \
BEAM_API_KEY=b1m_… \
BEAM_TOKEN_SECRET=$(openssl rand -hex 32) \
pnpm --filter @beam-network/example-rooms-and-broadcast dev
```

Rooms then come from the coordinator. Transfers still use the in-process fake:
the real path needs `@beam-network/sdk` and a long-lived process, which is
covered in [`docs/architecture.md`](../../docs/architecture.md) rather than
demonstrated here.

## Do not copy the authorize hook

```js
authorize: (request) => {
  const user = request.headers.get("x-demo-user") ?? "demo-user";
  return { subject: user, scopes: [...] };
}
```

It trusts a header so the demo runs without a login. In a real application this
is the security boundary: check your own session, and return only the scopes that
user should have.
