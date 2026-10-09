# Beam for Zapier

The Beam integration on the Zapier platform. It lets a Zap **start** when a Beam
transfer finishes, so someone in Zapier can wire a completed transfer to Slack,
Jira, Salesforce or anything else Zapier connects to, without touching Beam.

This is the opposite direction from `@beam/zapier` in Beam Transfer Studio. That
one is a workflow step where Beam calls Zapier. This one puts Beam in Zapier's
own app directory, where Zapier calls Beam.

## What it offers

| Kind | Key | What it does |
| --- | --- | --- |
| Trigger | `transfer_completed` | Polls for transfers that reached a terminal state. Choose succeeded, failed or cancelled. |
| Search | `find_transfer` | Looks up one transfer by id or transfer key. |
| Create | `start_transfer` | Runs a Studio workflow, which starts the transfer it defines. |

## Which half works for which customer

Studio is self-hosted, so the two directions have different requirements.

| Customer's Studio | Transfer finished → Zap | Zap → start a transfer |
| --- | --- | --- |
| Reachable from the internet | yes | yes |
| Private network or VPN only | yes | no |

The trigger polls `beamcore.b1m.ai`, Beam's own hosted service, so it works for
every customer regardless of where -- or whether -- they run Studio.

Start a Transfer is the opposite: Zapier's cloud has to reach *their* Studio, and
there is no Zapier mechanism to get inside a private network. A customer whose
Studio is not internet-facing cannot use it.

They are not stuck, though. Studio's own `@beam/zapier` action calls Zapier
*outbound*, and outbound works from anywhere, so a locked-down customer still
gets "transfer completes → Studio workflow → Slack/Jira/Salesforce". What they
lose is Zapier being able to initiate.

### There is no "Start a Transfer" action

Deliberately. Creating a Beam transfer happens over NATS and involves a callback
where BeamCore asks the client to re-sign expired storage routes part-way
through. That needs a process that stays connected for the life of the transfer,
and a Zapier action is a function invoked for a few seconds. An action that
could not finish what it started would be worse than no action.

Adding one means first adding a REST create endpoint to BeamCore that owns the
long-lived side itself. Until then this integration is read-only.

## Setup

**Requires Node 22.** The platform runs integrations on Node 22 in Lambda, and
`zapier-platform-cli` refuses to start on anything older. The tests here run on
Node 20 fine, so day-to-day development does not need the upgrade — only
`validate`, `register` and `push` do.

The CLI is installed globally rather than as a devDependency, which is both
Zapier's documented path and the smaller one: it pulls in ~780 packages, and
this directory is uploaded on push.

The package is also **not** an npm workspace member. Zapier bundles the app's
own `node_modules`, and workspace hoisting would leave that directory
incomplete.

The binary is `zapier-platform`, not `zapier`. That is the name in the package's
`bin` map, and it does not change when installed globally, despite what most of
Zapier's own documentation shows.

nvm scopes global packages per Node version, so install the CLI *after*
switching, or it will not be on `PATH`.

```bash
nvm use 22
npm install -g zapier-platform-cli
cd packages/zapier
npm install
zapier-platform login       # deploy key -> ~/.zapierrc, outside this repo
zapier-platform register    # creates the integration, writes .zapierapprc here
zapier-platform push
```

`.zapierapprc` holds the integration's app id. It is not a secret and **is**
committed, so a second person can push to the same integration instead of
registering a duplicate. The deploy key in `~/.zapierrc` is the secret, and
never belongs in the repo.

### Pointing a version at dev

The base URL defaults to `https://beamcore.b1m.ai` and is not an auth field —
users connect to Beam's hosted service, and asking for a hostname would invite
both typos and phishing-shaped mistakes. To test a version against dev:

```bash
zapier env:set 1.0.0 BEAM_API_BASE_URL=https://<dev host>
```

## Checks

```bash
npm test              # 27 tests, including the schema check validate runs
npm run lint          # tsc --noEmit over the JS, via checkJs
npm run validate      # the CLI's full check; needs Node 22
```

`validate` currently reports one non-blocking warning, D002, asking for a direct
link to where a user gets an API key. It is deliberately unfixed: the console
page is not public yet -- console.b1m.ai/api-keys redirects to a coming-soon
page -- so any link would dead-end. Add it when the console launches, before the
integration reaches real users.

The suite runs against real `zapier-platform-core`, with `z.request` stubbed. It
covers the mapping, the error translation, and the two properties that are easy
to get silently wrong: that the trigger orders by completion, and that a search
miss returns an empty result rather than halting the Zap.

## Authentication

API key, entered once by the user. Beam keys are long-lived and scoped to one
account, so there is nothing to refresh.

OAuth would be a nicer experience, but Beam's authorization server has no
`client_credentials` grant and no dynamic client registration, so a Zapier app
cannot register itself as a client today. That is the blocker to revisit if the
connection experience matters more later.

The key travels as `x-api-key` rather than a bearer token, so it never lands in
a log that captures `Authorization` headers.

## Why the trigger orders by completion

This is the one piece of correctness worth understanding before changing
anything.

Zapier polls, reads the page it is given, and de-duplicates on `id`. Anything
not near the top of the first page is never seen at all. Ordering by creation
would therefore miss exactly the transfers most worth knowing about: a large
transfer created days ago that finishes today sorts below everything created
since, and its Zap would never fire.

So the trigger asks for `order_by=completed_at`, and Beam's API grew that
parameter for this. It also pins `status` server-side, so in-flight transfers
cannot occupy the dedupe window before they have anything to report.

## Idempotency is opt-in, on the Studio side

Start a Transfer sends `Idempotency-Key`, but Studio only honours it when the
workflow's webhook trigger sets `coalesceWindowSeconds` above zero. De-duplication
lives inside the coalescing path and nowhere else, so with the default of `0` the
header is accepted and ignored.

Verified against a live trigger: three POSTs carrying one key produced three
separate runs with no window, and a single pending event with a 30 second window.

For an action that starts transfers that matters — a Zapier retry after a timeout
would otherwise move the data twice. Set a window on any trigger a Zap calls.

Also worth knowing: editing a trigger's config **rotates its token**, which
changes the webhook URL and breaks any Zap using the old one with a
`404 Webhook not found` that looks exactly like a typo. Useful for revoking a
leaked URL; surprising if you were only changing a window.

## Requirements before a public listing

The integration works immediately as a private, invite-link app — which is what
you would hand an early customer. Zapier's public directory review is a separate
gate that expects live users on the integration, plus branding assets and a help
URL. That is a commercial milestone, not an engineering one.

## Dependency on BeamCore

The trigger needs `order_by`/`completed_at` on `GET /transfers`, and the search
needs `GET /transfers/:transfer_id`. Both are **live in production** as of the
2026-09-07 release, so this app works against `beamcore.b1m.ai` today.

One caveat: migration `109`, which indexes `(client_id, status, completed_at DESC)`,
is written but **not yet applied**. Until it is, every poll sorts a client's
entire transfer history. That is cheap at current volumes and invisible with no
Zaps running, but it should land before the trigger starts polling on a
schedule.
