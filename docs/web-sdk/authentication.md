# Authentication

## The rule

**A Beam API key must never reach browser code.**

`bm_live_…` (beam-website) and `b1m_…` (BeamCore) keys are organization-level
bearer secrets. They carry `creditLimit`, `creditsUsed`, `monthlyBudgetCredits`,
and permission rows. They create transfers, start rooms, and spend money. Ship
one to a browser and every visitor can read it from the network tab.

The SDK refuses to start if it finds one, in either the typed field or an
untyped `apiKey` someone added by hand:

```
BeamConfigError: `clientKey` was given a beam-website secret key (bm_live_…).
Beam API keys can create transfers, start rooms, and spend credits, so anything
that reaches a browser is readable by every visitor. Use a publishable key from
the Beam console (clientKey: 'bm_pub_…'), or mint short-lived tokens from your
own backend (tokenEndpoint).
```

Recognized and rejected: `bm_live_`, `bm_test_`, `b1m_`, `bag_`, `btrstd1.`

## Beam Web Tokens

What the browser holds instead. A BWT is short-lived, scoped, and disposable.

```
bwt1.<base64url(claims)>.<base64url(hmac-sha256)>
```

The format deliberately mirrors the coordinator's own `btrstd1.` Studio
delegation — same three-part shape, same five-minute ceiling — so the two read as
the same idea to anyone who has seen either.

### Claims

| Claim         | Meaning                                                        |
| ------------- | -------------------------------------------------------------- |
| `v`           | Format version. Always `1`.                                    |
| `iss`         | Issuer. Defaults to `beam-web-sdk-server`.                     |
| `aud`         | Audience. Defaults to `beam-web-sdk`.                          |
| `sub`         | Your identifier for the end user. Opaque to Beam.              |
| `org`         | Your organization identifier.                                  |
| `scopes`      | Explicit grants. No wildcards, no implication.                 |
| `iat` / `exp` | Issue and expiry, in epoch seconds.                            |
| `jti`         | Unique token id, for audit correlation.                        |
| `origin`      | Origin the token is bound to, when one was present at minting. |

### Lifetime

|             | Seconds |
| ----------- | ------- |
| Minimum     | 15      |
| Default     | 120     |
| **Maximum** | **300** |

The ceiling matches the coordinator's `btrStudioDelegationMaxTTL` and is not
configurable upward. Short lifetime is the entire security argument for putting a
token in a browser at all; a long-lived one is just an API key with extra steps.

Longer requests are clamped rather than rejected, and `verifyToken` refuses a
token whose own `exp - iat` exceeds the ceiling — so a future or modified minter
cannot widen it either.

### Scopes

```
rooms:create   rooms:join   room:read
broadcast:publish   broadcast:watch
transfers:create   transfers:read   transfers:cancel
```

Additive and explicit. `room:read` does not imply `rooms:join`; `transfers:read`
does not imply `transfers:cancel`. This mirrors the coordinator's own
authorization contract, which has no implication, no nesting, and no denies.

## Where verification happens

**Your broker verifies the token. Beam does not.** Beam's edge has no idea what a
BWT is.

That makes your broker the policy enforcement point for browser sessions, and it
is a deliberate v1 boundary, not an oversight.

The browser decodes the token only to read `exp`, so it can refresh in time. It
never validates the signature — it cannot hold a key that would mean anything,
and every real decision happens server-side anyway.

## Three ways to get a token

### Publishable key — no backend

```ts
const beam = new Beam({ clientKey: "bm_pub_…", subject: user.id });
```

The browser posts the key to Beam's hosted token endpoint, which returns a scoped
token. The key is public in the way a Stripe `pk_` key is public: it identifies a
configuration, Beam binds it to an origin allow-list server-side, and it can mint
nothing but short-lived narrowly-scoped tokens.

Get one from the Beam console, which also gives you the embed snippet. It is not,
and must never become, an API key.

### Your own token endpoint

```ts
const beam = new Beam({ tokenEndpoint: "/api/beam/token" });
```

Preferred once your app has a session, because then authorization is expressed in
_your_ model rather than Beam's:

```ts
authorize: async (request) => {
  const user = await currentUser(request);
  if (!user) return null; // 401

  return {
    subject: user.id,
    organizationId: user.orgId,
    scopes:
      user.role === "host"
        ? ["rooms:create", "rooms:join", "room:read", "broadcast:publish"]
        : ["rooms:join", "room:read"],
  };
};
```

### A function you supply

```ts
const beam = new Beam({ getToken: async () => (await myApi.beamToken()).token });
```

## Refresh behaviour

- Refreshed at **75 %** of lifetime — 30 seconds of headroom on a 120-second
  token, comfortably more than a slow mint round trip.
- Concurrent callers **share one in-flight mint**. Opening a room and starting a
  transfer in the same tick makes one broker call, not two.
- A `401` triggers **exactly one** re-mint and retry. A second consecutive `401`
  surfaces, rather than looping.
- A token that arrives already expired is rejected with a message naming clock
  skew — which is nearly always the actual cause, and otherwise looks like an
  unexplained network fault.

## Threat model

| Threat                                    | Mitigation                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------- |
| API key in browser source                 | Constructor rejects every known secret prefix, typed or not.              |
| Stolen token replayed later               | 300-second ceiling; `jti` for audit correlation.                          |
| Stolen token replayed from another site   | `origin` claim, checked on every request.                                 |
| Browser asks for more than it should have | Requested scopes are **intersected** with the granted set, never unioned. |
| Forged or tampered claims                 | HMAC-SHA256 verified **before** any claim is read.                        |
| Signature timing attack                   | Constant-time comparison, with a length check first.                      |
| Weak signing secret                       | Refuses anything under 32 characters, and says `openssl rand -hex 32`.    |
| Clock skew between hosts                  | ±30 s tolerance, configurable.                                            |
| Token cached by a proxy                   | `Cache-Control: no-store` on every mint response.                         |
| Cross-origin token minting                | Explicit origin allow-list; `*` is not accepted.                          |
| Token leaked through logs                 | Redaction on by default, including inside URL query strings.              |
| Upstream error leaking internals          | Browser gets a generic 500; the real error goes to `onError`.             |

### What this does not defend against

Stated plainly, because a threat model that only lists wins is not one:

- **A compromised broker.** It holds the real key. Protect it accordingly.
- **An XSS-compromised page.** An attacker running script on your origin can
  simply ask your broker for a token, exactly as your own code does. Short
  lifetimes bound the damage; they do not prevent it.
- **A malicious end user within their scopes.** If you grant `rooms:create`, that
  user can create rooms until they hit Beam's quota. Grant narrowly, and use
  `authorize` to enforce your own limits.
- **Beam-side authorization.** Beam authorizes your API key, not your end user.
  Per-user policy lives in your broker.

## Rotating the signing secret

The secret is HMAC-only and never leaves your backend, so rotation is
straightforward: deploy the new secret, and tokens minted with the old one fail
verification within 300 seconds. Rotating during a deploy causes at most one
failed request per active session, and the SDK's automatic re-mint on `401`
recovers it without the user noticing.
