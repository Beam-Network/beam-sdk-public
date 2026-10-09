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
