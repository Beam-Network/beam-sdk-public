# Beam receipts for Chainlink CRE

A Chainlink CRE project that writes receipts for finished Beam transfers and Room
deliveries onchain. A smart contract can then ask "did transfer X finish, how big
was it, and when?" and get a DON-attested Beam report back, without the payload
ever going near a chain.

It can also start the transfer: a contract call asks for one, and Beam Studio runs it.
A contract can ask for a job the same way: Beam delivers its input to a compute agent in a
Room, and the record links that delivery to the delivery of the agent's output
(see [Jobs](#jobs)).

On EVM chains CRE does not serve, such as Bittensor EVM, a relay does the same two jobs
without the DON, and its receipts say so (see [Chains without CRE](#chains-without-cre-the-relay)).

Room members can ask for the same attestation without leaving their Room: a bridge
forwards their request to the workflow and posts the DON-signed answer back as a Room
reply (see [Attestations in a Room](#attestations-in-a-room-the-bridge)).

| Part | Path | What it is |
| --- | --- | --- |
| Contract | `contracts/evm/src/BeamTransferReceipts.sol` | Stores receipts and a bounded set of open requests. Extends Chainlink's `ReceiverTemplate`. |
| Workflow | `workflows/transfer-receipts/` | A CRE TypeScript workflow that reads Beam's evidence by id and writes the finished receipts. |
| Workflow | `workflows/start-transfer/` | A CRE TypeScript workflow that starts a Beam transfer through a Studio webhook when the contract emits `TransferRequested`. |
| Relay | `relay/` | A Bun process for chains without CRE: it starts transfers from `TransferRequested` and writes Beam-reported receipts. |
| Bridge | `bridge/` | A Bun service the operator runs as a Room webhook: Room members ask it for attestations, and it replies in the Room with the DON-signed report. |
| Example | `examples/compute-agent/` | A toy compute member that answers onchain jobs: it computes over a delivered file and has Studio publish the result (see [Jobs](#jobs)). |

The project layout follows the TypeScript starter templates in
[smartcontractkit/cre-templates](https://github.com/smartcontractkit/cre-templates)
(Sports Resolution in particular: an EVM log trigger, HTTP reads, and a signed report
written back to a `ReceiverTemplate` consumer).

Like `packages/zapier`, this directory is not an npm workspace member and is never
published to npm. The CRE CLI compiles each workflow from its own directory with its
own `node_modules`. The Beam SDK is not used: it speaks NATS and needs Node, and CRE
workflows run as WASM under QuickJS. The workflow reaches Beam through its HTTP APIs only.
The relay and the bridge are plain Bun processes that import the workflows' CRE-free modules.

## What gets recorded

**Standard receipt**, keyed by the transfer UUID (`bytes16`):
`requestId`, `status` (completed, failed, cancelled), `totalBytes`, `bytesTransferred`,
`completedAtMs`, `planFingerprint`, `coordinateChecksum` and `level`.

**Room receipt**, keyed by `roomReceiptKey(room, channel, publication)`, which is
`keccak256(abi.encode(room, channel, publication))`:
`status` (completed, partial, failed, cancelled), `fileSizeBytes`, `manifestSha256`,
`targetCount`, `evidenceSha256`, `assignmentsSha256`, `evidenceVersion` and `level`.
A Room receipt that answers a Room delivery request is found with
`roomKeyForRequest(requestId)`.

**Job result**, keyed by the job's `requestId` and read with `getJob(requestId)`:
`inputRoomKey` and `outputRoomKey` (the Room receipts of the input and output deliveries),
`inputManifestSha256`, `outputManifestSha256`, `computeMember`, `status` and `level`.

Every value comes from Beam. A trigger only names *what* to look up; the workflow then
fetches it by id, and only terminal transfers are reported.

## Trust levels

`level` is a bitmask. Each bit is a claim with a limit, and the limits matter as much as
the claims.

| Level | Bit | Proves | Does not prove |
| --- | --- | --- | --- |
| L1 Beam-reported | `0x01` | Beam's API reported this status, size and plan fingerprint | that bytes arrived intact; Beam's honesty |
| L2 Target-signed receipt (Room) | `0x02` | the agent holding receipt key K signed `manifest_sha256` over the range hashes it received | that K belongs to member M (Beam-attested); content correctness; for E2EE, hashes are over ciphertext |
| L3 DON-signed report | `0x04` | each of f+1 nodes fetched Beam's evidence and got the same answer; the onchain write is authentic | that Beam's evidence is authentic. A wrong Beam answer would be attested faithfully |

- **Standard receipts are L1 + L3** (`level = 5`).
- **Room receipts are L1 + L3** (`level = 5`), **plus L2** (`level = 7`) only when every
  target is an agent with a non-null signed `final_receipt`. An object-storage target is
  finalized by its provider and its `storage_finalization` is Beam-reported (L1), so any
  storage target keeps the receipt at 5. A Room receipt always includes L1 data: status,
  size, target count and `assignmentsSha256` are Beam-computed.
- **A relay deployment never records L3.** The deployment mode is fixed in the
  constructor and readable as `donAttested()`. With `donAttested` false, standard receipts
  are `level = 1` and Room receipts 1 or 3, and the contract refuses any report that sets
  `LEVEL_DON_ATTESTED`. `standardReceiptLevel()`, `roomReceiptLevel()` and
  `roomSignedReceiptLevel()` return the levels of the deployment at hand.
- **A Room bridge's attestations are the same L3 reports, delivered offchain.** No
  forwarder checks them on the way: the bridge, and any member with `bridge/verify.ts`,
  checks f+1 signatures against the DON's signers in the Capability Registry.
- **A job result carries the claims both its Room receipts hold** (`level` is the input's
  level AND the output's). Its `status` is the input's, or the output's once the input
  completed. What it claims, exactly: the agent holding receipt key K received input
  manifest A, and output manifest B was delivered from member M (`computeMember`). That K
  belongs to M is Beam-attested. It never claims that B is the correct result of A.
- Neither the contract nor the workflow checks target signatures. `evidenceSha256`
  commits to the exact evidence bytes, so anyone holding that envelope can check them.
- Member identity behind a receipt key is a **Beam-attested member identity** until
  coordinator-signed key bindings ship.
- Receipts attest completion and delivery as Beam reports them, never that the content
  is correct.

The right words are "DON-attested Beam report" and "target-signed receipt".

## Onchain data is public

Everything this contract stores or emits is public, permanently: transfer UUIDs, request
ids, byte counts, completion times, plan fingerprints, Room, channel and publication ids,
manifest and evidence hashes, and the `input` string of `requestTransfer`. Payloads and
credentials never go onchain. Do not put personal data or secrets in `input`, or in Room
and channel ids you request receipts for.

## How a receipt gets recorded

The contract keeps an **open-request set**: a durable outbox of questions that have not
been answered yet. It is bounded at `MAX_OPEN_REQUESTS = 256`; a request beyond that
reverts until one is answered or cancelled.

| Request | Opens | Answered when |
| --- | --- | --- |
| `requestTransfer(requestId, 1, input)` | a standard transfer request; emits `TransferRequested` for the workflow that starts it | a standard receipt for the derived transfer id is recorded with that `requestId` (`RequestAnswered`); reconcile looks it up |
| `requestTransfer(requestId, 2, input)` | a Room delivery request; emits `TransferRequested` | a Room receipt is recorded with that `requestId` (`RoomRequestAnswered`); reconcile finds it through the deployment's `roomDelivery` route, or a push answers it |
| `requestTransfer(requestId, 3, input)` | a job; emits `TransferRequested` | a job result linking its input and output Room receipts is recorded (`JobRecorded`); reconcile finds both through the deployment's `roomDelivery` and `jobOutput` routes |
| `requestReceipt(transferId)` | a receipt request; emits `ReceiptRequested` | the standard receipt is recorded |
| `requestRoomReceipt(room, channel, publication)` | a Room request; emits `RoomReceiptRequested` | the Room receipt is recorded, or Beam reports the publication as bucket-only and the request is closed (`RequestClosed`) |

The second argument of `requestTransfer` is its kind: 1 for a standard transfer, 2 for a
Room delivery, 3 for a job; anything else reverts. A receipt only answers a request of its
own kind: a standard receipt never closes a Room delivery request, a Room receipt never
answers a job, and the other way round.

**Room deliveries are reconciled through their publication key.** A standard transfer's id
derives from its request id, so reconcile looks it up at Beam directly. A Room delivery's
publication id is chosen when it is published. Instead, the Studio Room workflow publishes
it under the key `request:<requestId>`, and the coordinator records that key for the
publishing member. With a `roomDelivery` route in config, reconcile does two reads:
1. It looks the publication up by the route's publisher and that key
   (`GET …/publications/by-key`).
2. It reads its canonical Room receipt and records it with the `requestId`.

Without a route, reconcile skips these requests; only a push answers them, and one that
never arrives shows in `staleRequests()`.

**Jobs are reconciled through both publications.** With `roomDelivery` and `jobOutput`
routes, reconcile does four reads for a job: it looks up the input by the source member and
the output by the compute member, both under `request:<requestId>`, then reads their two
Room receipts. Once both are terminal, one report records the two Room receipts and the job
result that links them. No push answers a job.

Only the owner and accounts allowed with `setRequester` can open requests, because each
one spends the operator's Beam reads and CRE quota. A repeated request, or a request for
a receipt that is already recorded, is a no-op. A repeated report is a no-op too: a
recorded receipt is never overwritten. `cancelRequest(key)` (requester or owner) removes
a request that can never be answered, such as one naming a transfer that was never created;
`transferRequestKey`, `receiptRequestKey` and `roomRequestKey` compute the key.

`staleRequests()` lists open requests older than `staleAfter()` (set at deploy, changed
with `setStaleAfter`). A non-empty list means the workflow is not keeping up, or a request
needs cancelling. Choose a value longer than your longest transfer plus one full
reconcile rotation.

The workflow has three handlers. `cre workflow simulate --trigger-index` uses these
indexes:

| Index | Trigger | Does |
| --- | --- | --- |
| 0 | EVM log on `ReceiptRequested` and `RoomReceiptRequested` | answers an on-demand request right away, if the transfer has finished |
| 1 | Cron, every `reconcileIntervalMinutes` (default 5) | reads a window of the open-request set and records every finished one |
| 2 | HTTP | a push from a workflow that saw a transfer finish, or attestation requests from a Room bridge |

A push names up to 14 receipts:

```json
{
  "transfers": [{ "requestId": "0x…64 hex" }, { "transferId": "uuid" }],
  "rooms": [{ "room": "…", "channel": "…", "publication": "…", "requestId": "0x…64 hex" }]
}
```

A Room item's `requestId` is optional; it names the Room delivery request the receipt
answers. A [Room bridge](#attestations-in-a-room-the-bridge) sends `attest` instead.

### Why receipts are fetched by id

`completed_at` is the transfer's event time, and nothing in Beam records when a row was
written. A row written late, for example after a database outage, would sort behind
rows a cursor has already passed, so any page-scanning reader can skip it for good.
Every lookup here is by id, and the open-request set is what makes "no push arrived"
recoverable: reconcile keeps asking until Beam answers.

### Transfer ids from request ids

BeamCore issues `transfer_key` on the server, so a requester can never know it in
advance. Idempotency runs on the client-supplied `transfer_id` instead, which the Beam
SDK derives from an idempotency key. Reconcile derives the same id from the request id:

```
transferId = uuid(sha256("beam-transfer:<clientId>:<requestId>")[0..16])
             with the version nibble set to 5 and RFC 4122 variant bits
```

- `requestId` is the lowercase 0x-prefixed hex of the `bytes32`, exactly as the
  workflow that starts the transfer must pass it as the idempotency key.
- `clientId` is `beamClientId` in config: the `user_id` that `GET /auth/me` returns for
  `BEAM_API_KEY`. Before any request-id lookup, the workflow calls `GET /auth/me` and skips
  those lookups if the two disagree. A mismatch would otherwise look up ids that never exist.
- That `user_id` equals the `client_id` the SDK derives transfer ids under is inferred
  from how BeamCore scopes transfers to the caller, not yet observed. The first DEV
  simulation of a `requestTransfer` confirms it: the transfer's id must equal the derived one.

`bun run check` checks this derivation against Node's `crypto` with the SDK's formula.

### Room evidence

Room receipts come from BeamCore's canonical Room receipt route, specified as
`beam-room-receipt-evidence/v1` in the [Beam documentation](https://docs.b1m.ai/docs/integrations/chainlink-cre):

```
GET {roomApiBaseUrl}/studio/v1/rooms/{room}/channels/{channel}/objects/{publication}/receipt
Authorization: Bearer <BEAM_API_KEY>
X-Beam-Organization-ID: <BEAM_ORG_ID>
```

One GET returns the bytes; the key needs `room:objects:read` for the Room's organization,
and no delegation is involved. The body is the canonical `beam-room-receipt-evidence/v1`
envelope (RFC 8785 JSON), and the workflow hashes the raw response bytes; it never
re-serializes them:

```
evidenceSha256 = sha256("beam:room-receipt-evidence:v1\0" || response body bytes)
```

From the envelope it records `status`, `file_size_bytes` (a decimal string),
`manifest_sha256` (zero when `null`), `assignments_sha256` and the number of `targets`.
It refuses an envelope whose fields are not exactly the v1 set, whose targets do not have
exactly `final_receipt`, `kind` (`agent` or `object_storage`), `member_id`,
`storage_finalization` and `verification_basis`, or that names a different publication.
`assignmentsSha256` is recorded as Beam reports it; the assignment pages are not fetched.

| Answer | The workflow |
| --- | --- |
| `200` | records the receipt |
| `409 not_terminal` | treats it as pending and asks again on a later run |
| `409 bucket_only_publication` | records no Room receipt: the publication ran as a standard transfer. The report closes the Room receipt request instead (below) |
| `404` | treats the publication as unknown for now |

**Bucket-only closures.** When the Room route answers `409 bucket_only_publication`, the
same report that carries receipts also carries a closure for that Room receipt request,
part of the DON-attested projection like any receipt. The contract removes the request,
stores nothing in the Room receipts, and emits
`RequestClosed(key, CLOSURE_BUCKET_ONLY_PUBLICATION, transferId)`, where `transferId` is
the standard transfer named in the `409` body (zero if Beam named none). The requester
follows up with `requestReceipt(transferId)` to get that transfer's standard receipt. A
closure only ever removes an open Room receipt request; for any other request, or one
that is not open, it does nothing.

`bun run check` pins the hashing and the projection to the spec's published test vector
(`evidenceSha256 = 940313b5…3b7d`).

### Consensus

Every lookup runs on each node, which projects the response to a fixed set of fields in a
fixed order. The DON then needs identical answers from a quorum. If nodes disagree, for
example while a transfer is finishing, that lookup counts as unavailable for this run and
the next run asks again. GETs are never served from CRE's response cache, so every node
really asks Beam.

## Starting a transfer from an onchain request

No Beam HTTP route creates a transfer: creation needs a client that stays connected over
NATS, which a CRE workflow cannot be. `start-transfer` therefore starts it through a Beam
Studio workflow's inbound webhook, the path the Zapier integration uses. The source and
destination stay in the Studio step's locked configuration; the request carries only data.

1. A requester calls `requestTransfer(requestId, 1, input)` (kind 1 is a standard
   transfer). The contract opens the request and emits `TransferRequested`.
2. On that log, every node POSTs `{"requestId", "requester", "input"}` to the Studio
   webhook with `Idempotency-Key: <requestId>`, signed with the trigger's secret. Studio
   records the key in the same transaction that creates the run, so every repeat answers
   with that run: one request, one run. The nodes agree on the run id; a first delivery
   (202) and a repeat (200, `deduplicated`) report the same one.
3. The Studio run's `@beam/transfer` step uses the request id as its idempotency key, so
   the transfer id is `transferIdForRequest(clientId, requestId)`.
4. The receipts workflow records the receipt: `reconcile` derives that id from the open
   request, so no push is needed, and a push only makes it sooner.

### Studio setup

- A workflow with a webhook trigger that **requires signatures**, and one
  `@beam/transfer` step (1.5.6 or later) whose configuration fixes the source and
  destination. Bind the step's `idempotencyKey` **input** to `${workflow.input.requestId}`.
  Studio resolves `${…}` expressions only in input bindings; written into the step's
  configuration, the expression would be used as literal text, giving every request the
  same key. Other fields of `workflow.input` (`input`, `requester`) are available to other
  input bindings.
- A Studio release that deduplicates every webhook delivery on `Idempotency-Key`, which
  is the first release after v1.0.19. On v1.0.19 each node's delivery starts its own run,
  so the nodes report different run ids and the execution fails consensus. The extra runs
  fail at transfer creation with HTTP 409 `request_id_conflict`, because each run signs
  fresh URLs under the same create request, so a request still starts only one transfer.
- Studio's Beam key must belong to the same Beam user as the receipts workflow's
  `BEAM_API_KEY` and `beamClientId`, because transfer ids and transfer reads are scoped
  to that user.
- Two secrets per webhook, declared in `workflows/start-transfer/secrets.yaml` so the
  receipts workflow never needs them:
  - `STUDIO_WEBHOOK_URL`, the full `https://<studio>/hooks/workflows/<workflowId>/<triggerId>/<token>` URL;
  - `STUDIO_WEBHOOK_SIGNING_SECRET`, the trigger's signing secret.

  The Room workflow's are `STUDIO_ROOM_WEBHOOK_URL` and `STUDIO_ROOM_WEBHOOK_SIGNING_SECRET`.
  CRE requires every declared secret to exist, so a deployment without Room deliveries
  sets those two to any non-empty value; they are never read. `signDeliveries` must be
  `true` whenever the triggers require signatures.

### Room deliveries

A Room delivery request (`requestTransfer(requestId, 2, input)`) delivers a file to Room
members, such as private agents or compute nodes, through a second Studio workflow.
- **The Studio Room workflow:**
  - a webhook trigger that requires signatures;
  - one `@beam/room-transfer` step whose configuration fixes the room, the object
    channel, the source agent and its file, and the target members;
  - the step's `publicationKey` **input** bound to `${workflow.input.requestId}`, so it
    publishes under `request:<requestId>`. This needs the Studio release that carries
    that input (the first after v1.0.19).
- **The two workflows:**
  - set `startRoomDeliveries: true` in the `start-transfer` config and the Room
    webhook's secrets;
  - set the same room, channel and source member as `roomDelivery` in the
    `transfer-receipts` config.
- **What the receipt claims, exactly:** the configured targets received the file whose
  manifest it names. With a signed receipt from every target it is L2. It says nothing
  about whether the content is correct.
- **Scope:** agent sources only. A delivery from or to object storage runs as a Studio
  storage transfer and is not found this way.

### Jobs

A job (`requestTransfer(requestId, 3, input)`) delivers its input to a compute member, such
as a private AI agent, and is answered once that member's output is delivered too.
- **Input:** the Room workflow of [Room deliveries](#room-deliveries) delivers it, with its
  target set to the compute member. Set `startJobs: true` in the `start-transfer` config.
- **Output:** a second Studio Room workflow publishes it from the compute member to an
  object channel of the same Room. Its `@beam/room-transfer` step's `publicationKey` input is
  bound to the job's request id, so the output is published under the same key as the input.
  The compute agent starts that workflow through its webhook when the output is ready,
  passing the request id. The step's source path is fixed configuration, so the agent writes
  every result to the same file, and must not replace it while a publication is sending it.
- **The compute member** is a Studio agent (`beam studio connect … --rooms=true`) whose
  `--allow-root` covers the output file, and a member of the Room with `subscribe` on the
  input channel and `publish` on the output channel.
- **Receipts:** set `jobOutput` in the `transfer-receipts` config to that channel and the
  compute member's id, next to `roomDelivery`.
- **What the result claims, exactly:** the agent holding receipt key K received input
  manifest A, and output manifest B was delivered from member M. That K belongs to M is
  Beam-attested. Nothing claims that the output is correct.

**The compute agent learns the request id from the chain.** A Room target never sees a
publication's key: the agent receives the input into its Room inbox,
`<state dir>/room-inbox/objects/<sha256(room id)>/<sha256(transfer id)>.object`, without
the key and without a notification. `examples/compute-agent/` therefore follows the
contract's `TransferRequested` logs of kind 3, which carry the request id and the
instruction in `input`. It serves jobs one at a time, in request order: it takes the next
input that reaches its inbox, computes, writes the output file, and starts the output
workflow with a signed `{"requestId"}` delivery. It uses only Python's standard library;
copy `agent.example.json` to `agent.json` beside it and fill in the contract, Room and
paths.

**Channel TTL.** Both object channels need a publication TTL at least as long as the steps'
`ttlSeconds`. A channel created with the CLI allows 60 seconds unless it is created or
updated with `--publication-ttl`, and a longer step TTL fails the run with
`btr: invalid resource limit`.

**A failed input delivery** cannot be retried: Studio retries only an active publication,
and a repeated delivery of the same request id returns the failed run. Cancel the request
with `cancelRequest(transferRequestKey(requestId))` and request the job again under a new
id.

### Limits

- Studio must be reachable from the internet over HTTPS, as for Zapier.
- Every DON node sends the request, so every node operator sees the webhook URL and its
  token. The signing secret never travels in a request; it reaches the nodes from the
  Vault DON inside the execution. With `signDeliveries`, a URL taken from a log cannot
  start a run, and a captured signed delivery is accepted only within Studio's 300-second
  window, where its `Idempotency-Key` returns the existing run. A node operator can still
  sign a new request with the secret its own execution holds. Such a request controls
  only the request id and `input`, because the source and destination stay fixed in
  Studio.
- Room delivery requests (kind 2) are started only with `startRoomDeliveries`, and jobs
  (kind 3) only with `startJobs`.
- A refusal (401 rejected signature, 404 unknown webhook) or an unavailable Studio fails the
  execution, which shows in CRE's execution history. The request stays open, and
  `staleRequests()` lists it once it is older than `staleAfter`.

## Chains without CRE: the relay

CRE serves a fixed set of chains. Bittensor EVM is not one of them: its chain selectors
exist, but `cre workflow simulate` fails with "no compatible capability found". On such
a chain, `relay/` does the work of both workflows from one process.

- **Starts.** It follows `TransferRequested` logs up to the `finalized` block and delivers
  each standard request to the Studio webhook as `start-transfer` does, with the same
  body, signature and `Idempotency-Key`. A cursor file holds the next block to scan.
  - When Studio is unavailable, the cursor stays at that request's block, and the next
    pass delivers from there again.
  - A Studio release after v1.0.19 returns the existing run for a repeated key. On
    v1.0.19 a repeat becomes a second run that fails with `409 request_id_conflict`;
    it never starts a second transfer.
- **Receipts.** Every pass looks up each open request at Beam, as `reconcile` does, and
  writes the finished ones through `onReport`, at most 14 per report. The contract
  ignores receipts it already holds, so a repeat costs only gas.
- **Room delivery requests** (kind 2): with a `roomDelivery` route in its config, the relay
  starts them through the Room workflow's webhook and records their receipts as `reconcile`
  does, through the publication lookup. Without one, it skips them.
- **Jobs** (kind 3): with `roomDelivery` and `jobOutput` routes, the relay starts them through
  the Room workflow's webhook and records their results as `reconcile` does. Without both, it
  skips them.
- **Room attestations.** A [bridge](#on-a-chain-without-cre) with `contract` answers lets
  Room members ask about publications and transfers; the relay records the receipts it
  opens requests for.
- **Receipts only.** With `starts: false` in the config, the relay skips the start pass and
  needs no Studio. Contracts then ask for receipts of transfers and Room deliveries that
  already happened (`requestReceipt`, `requestRoomReceipt`). Transfer requests stay open
  until something else starts them, and `signDeliveries` must be false.
- **Trust.** A relay is one operator, so its receipts are Beam-reported only. Deploy the
  contract with `donAttested` false and the relay's account as the forwarder. Readers
  see the difference in `level`, and `donAttested()` says which deployment it is.
- **Customers.** With a `customers` block, the relay also records the receipts of
  organizations set up in the Beam console, and charges each one credit. See
  [Customers mode](#customers-mode).

### Running the relay

```bash
cd packages/cre/contracts/evm
forge create src/BeamTransferReceipts.sol:BeamTransferReceipts \
  --rpc-url https://test.chain.opentensor.ai \
  --private-key "$PRIVATE_KEY" --broadcast \
  --constructor-args <relay account> 86400 false

# The relay imports modules from both workflows, which resolve their own dependencies.
(cd ../../workflows/transfer-receipts && bun install)
(cd ../../workflows/start-transfer && bun install)
cd ../../relay
bun install
bun run typecheck && bun run check
cp config.bittensor-testnet.json relay.json   # contract, start block, Beam hosts, client id
bun main.ts relay.json                        # --once runs a single pass
```

Secrets come from the environment; Bun also loads `.env` from the working directory, which
git ignores.

| Variable | What it is |
| --- | --- |
| `RELAY_PRIVATE_KEY` | The contract's forwarder account. It pays the gas for receipts. |
| `BEAM_API_KEY` | A key of the `beamClientId` user with `transfers:read` (and `room:objects:read` for Room receipts). |
| `BEAM_ORG_ID` | The organization that owns the Rooms; needed only for Room receipt requests. |
| `STUDIO_WEBHOOK_URL` | The Studio inbound webhook URL, when `starts` is true. |
| `STUDIO_WEBHOOK_SIGNING_SECRET` | The trigger's signing secret, when `starts` and `signDeliveries` are true. |
| `STUDIO_ROOM_WEBHOOK_URL` | The Room workflow's webhook URL, when `starts` is true and the config has a `roomDelivery` route. |
| `STUDIO_ROOM_WEBHOOK_SIGNING_SECRET` | Its signing secret, with `starts`, a route and `signDeliveries`. |

Before its first pass the relay checks the chain id, that the contract is a relay
deployment whose forwarder is the relay's account, and that `GET /auth/me` returns
`beamClientId`. It refuses to start otherwise.

### Customers mode

Organizations set up onchain receipts in the Beam console. They get them
two ways, one credit each, charged to the read-only key they registered:
- **Every transfer.** Each transfer the organization created while its switch was on.
- **Contract requests.** Each `requestReceipt` or `requestRoomReceipt` from an EVM address
  of theirs that Beam approved. The address pays its own request gas.

The relay pays the recording gas either way. Each pass, before receipts:
1. **Registry.** It reads `GET <registryUrl>/api/onchain-receipts/accounts` and opens each
   organization's key with `RELAY_SEALING_PRIVATE_KEY`. The console seals keys to its public
   half, and `sealed-box.ts` matches the console's format and test vector. A key that does not
   open is logged and its organization skipped.
2. **Allowlist.** It calls `setRequester(address, true)` for approved addresses and `false` for
   revoked ones still allowed, then reports each transaction. Only the contract's owner can,
   so the relay's account must own it.

Then, for receipts:
- **Who owns a request.**
  - A request the relay's own account opened is Beam's: it is read with `BEAM_API_KEY` and
    never charged.
  - A request from an approved address is read with its organization's key and charged to it.
  - Any other address's request stays open.
- **Every transfer.** The relay lists each organization's finished transfers
  (`GET /transfers?order_by=completed_at`) after a per-organization cursor in
  `customers.cursorPath`. It keeps those created inside one of the organization's periods and
  records them with no request, at most `maxPerOrganization` per pass. It lists an
  organization while its switch is on and for 7 days after, for transfers still running.
- **Charging.** Each customer receipt's credit is held before the report is written
  (`charges/reserve`) and committed once it is mined (`charges/commit`). The website keys the
  charge by the receipt, so a receipt asked for both ways is charged once.
  - A refused charge (out of credits, say) writes nothing: the request stays open, and the
    listing cursor waits before the transfer.
  - A failed write keeps the hold, and the next pass writes again.
  - A receipt the contract already holds is committed too: it is onchain.
  - An already committed charge is never written again.

To set it up:
1. Run `bun run sealing-key /opt/beam-relay/relay.env`. It appends the private key to the
   env file and prints `ONCHAIN_RELAY_PUBLIC_KEY=…`; set that on the Beam console.
2. Set the same random `ONCHAIN_RELAY_SECRET` on the Beam API and in the relay's env.
3. Add the block to the config:

```json
"customers": {
  "registryUrl": "https://api.b1m.ai",
  "cursorPath": "./customers-cursor.json",
  "maxPerOrganization": 50
}
```

| Variable | What it is |
| --- | --- |
| `ONCHAIN_RELAY_SECRET` | Sent as `x-onchain-relay-secret` to the website's relay routes; the same value as the Beam API's. |
| `RELAY_SEALING_PRIVATE_KEY` | The X25519 key customer keys are sealed to, raw base64url. Replacing it orphans every registered key. |

### Bittensor EVM

- **Chains:** testnet is chain 945 (`https://test.chain.opentensor.ai`) and mainnet is
  chain 964 (`https://lite.chain.opentensor.ai`).
  - Both serve the `finalized` tag a few blocks behind the head.
  - Contract deployment is permissionless.
- **Gas** is paid in TAO. To fund an EVM address from a Bittensor wallet, send TAO to its
  mirrored SS58 account: `blake2b_256("evm:" ‖ address)` with SS58 prefix 42. The
  `0x…080C` precompile returns that account id.
- **Foundry** prints `missing field mixHash` errors on Bittensor blocks; transactions
  still go through.
- **Ed25519:** Bittensor's precompile at `0x…0402` verifies messages of 32 bytes only.
  Room target receipts sign their full text, so their signatures cannot be checked
  onchain yet.

## Attestations in a Room: the bridge

A Room member, such as a private agent, can ask about a transfer or a Room publication and
get the answer back in the Room. Ordinary Room traffic stays member-to-member; only these
requests leave it. The bridge gets its answers from one of two places (`answers.from`):

- **`cre`:** the receipts workflow on a Chainlink DON, as DON-signed reports. The steps
  below describe this mode.
- **`contract`:** a `BeamTransferReceipts` contract that a relay serves, on a chain CRE
  does not reach. See [On a chain without CRE](#on-a-chain-without-cre).

1. **Ask.** A member publishes a JSON message on a message channel that the bridge's
   webhook member listens to:

   ```json
   {"op": "attest_room_publication", "channel": "btr_channel_…", "publication": "btr_pub_…"}
   {"op": "attest_transfer", "transfer_id": "uuid"}
   ```

   `publish_onchain: true` also records the answer in the contract. A publication is
   always looked up in the asking member's own Room, and only in channels the operator
   lists. Transfers are attested only when the operator allows it.
2. **Accept.** The Room host delivers the message to the bridge with an HMAC signature
   (see the agent's Room webhook hosting docs). The bridge answers `202` with
   `X-Beam-Reply: v1` and a `Location` on its own origin, and the host polls there.
3. **Forward.** At most once per `paceSeconds` (the HTTP trigger takes one call per 30
   seconds), the bridge sends the oldest requests, up to 7, to the receipts workflow's HTTP
   trigger:

   ```json
   {"attest": [{"op": "attest_room_publication", "room": "…", "channel": "…", "publication": "…"},
               {"op": "attest_transfer", "transferId": "uuid"}],
    "publishOnchain": false}
   ```

4. **Attest.** The workflow reads each subject from Beam as it reads a receipt. For
   every finished one, it sends the bridge one DON-signed report, to
   `attestationReplyUrl`, in CRE's offchain format (`report`, `context` and
   `signatures`, hex). One report per subject means a reply in one Room carries nothing
   about another. An unfinished or unknown subject gets no report.
5. **Verify and reply.** The bridge checks the report's workflow id and owner, then f+1
   signatures from distinct signers of the report's DON, read from the Capability
   Registry on Ethereum mainnet. It turns the report into the reply the host publishes in
   the Room, one hop after the request. A request that gets no report is forwarded again
   after `retrySeconds`; after `maxAttempts` it is answered `not_attested`.

Other `op` values get a `refused` reply. A result is not attestable as a message: the
DON can only attest what it reads from Beam. Publish the result as a Room transfer,
then ask about that publication.

An attested reply:

```json
{"type": "beam.cre.attestation.v1", "outcome": "attested",
 "request": {"op": "attest_room_publication", "room": "…", "channel": "…", "publication": "…"},
 "answer": {"kind": "room_publication", "status": "completed", "file_size_bytes": "8388608",
            "manifest_sha256": "0x…", "evidence_sha256": "0x…", "level": 7,
            "level_names": ["beam_reported", "target_signed", "don_attested"], "…": "…"},
 "signatures": {"verified": true, "source": "registry", "f": 3, "signers": ["0x…", "…"]},
 "workflow": {"id": "…", "owner": "0x…", "execution_id": "…", "don_id": 1, "…": "…"},
 "report": {"report": "…", "context": "…", "signatures": ["…"]},
 "claim": "Each signing Chainlink node read this answer from Beam and signed the same report. …"}
```

Members need not trust the bridge. `bun verify.ts reply.json --rpc <Ethereum mainnet RPC>
--workflow-id <id> --owner <address>` checks the signatures against the registry itself
and decodes the answer from the signed bytes. The trust levels above apply unchanged: the
DON attests what Beam reported, not that Beam is right.

### On a chain without CRE

With `answers.from` set to `contract`, the bridge answers from the receipts contract
instead:

1. **Read.** Every `pollSeconds` it reads the contract. A receipt the contract already
   holds is the answer at once.
2. **Request.** Otherwise it opens the request on the contract, once per subject:
   `requestRoomReceipt(room, channel, publication)` or `requestReceipt(transferId)`, from
   `BRIDGE_EVM_PRIVATE_KEY`'s account, which must be the owner or an allowed requester. The
   relay records the receipt like any open request's.
3. **Reply.** Once the contract holds the receipt, the reply carries the contract's record,
   the chain, the contract and the transaction that recorded it. A Room publication that ran
   as a standard transfer is answered with the transfer to ask about, from its
   `RequestClosed` event. A request the relay does not answer within `answerWithinSeconds`
   is answered `not_recorded`.

```json
{"type": "beam.chain.receipt.v1", "outcome": "recorded",
 "request": {"op": "attest_room_publication", "room": "…", "channel": "…", "publication": "…"},
 "answer": {"kind": "room_publication", "status": "completed", "level": 3,
            "level_names": ["beam_reported", "target_signed"], "…": "…"},
 "chain": {"chain_id": 945, "contract": "0x…", "don_attested": false,
           "transaction": "0x…", "block": "…"},
 "claim": "A relay operator read this answer from Beam and recorded it on this contract, …"}
```

**What it proves:** what the relay operator recorded, which anyone can read from the
contract. On a relay deployment (`don_attested: false`) it is Beam-reported: no DON or
other independent party attests it. Pointed at a CRE deployment, the same mode answers
with receipts whose DON signatures Chainlink's forwarder checked, and the claim says so.
`bun verify.ts reply.json --rpc <RPC of that chain>` reads the receipt from the contract and
checks that the reply says the same.

### Running the bridge

```bash
(cd ../workflows/transfer-receipts && bun install)
cd bridge
bun install
bun run typecheck && bun run check
cp config.example.json bridge.json   # or config.bittensor-testnet.json; routes, rooms, answers
bun main.ts bridge.json
```

| Key | Meaning |
| --- | --- |
| `listen` | `hostname` and `port` to serve on, behind an HTTPS reverse proxy |
| `basePath` | prefix of every route: the webhook URL is `https://<host><basePath>/hooks` |
| `rooms` | for each Room id, the object `channels` whose publications its members may ask about |
| `allowTransferAttestations` | whether members may ask about standard transfers of the deployment's account |
| `answers` | where answers come from: `cre` or `contract`, with the keys below |
| `statePath` | file holding open requests and replies, so polls survive a restart |

With `answers.from: "cre"`:

| Key | Meaning |
| --- | --- |
| `forward` | `gateway`: `gatewayUrl` and the deployed `workflowId`. `command`: `argv` and `cwd` of a command run once per batch with the input file's path appended, such as `cre workflow simulate … --http-payload` before deploy access |
| `workflow` | the `id` and `owner` that every accepted report's header must name |
| `signers` | `registry`: an Ethereum mainnet `rpcUrl` and the Capability Registry `registryAddress` (`0x76c9cf548b4179F8901cda1f8623568b58215E62`). `unverified`: for the CRE simulator only, whose reports carry throwaway keys; its replies say `"verified": false` |
| `paceSeconds` | seconds between workflow calls, at least 30 |
| `retrySeconds`, `maxAttempts` | when to ask again for a request that got no report, and how many times |

With `answers.from: "contract"`:

| Key | Meaning |
| --- | --- |
| `chain` | `rpcUrl`, `chainId`, the `receiptsAddress` the relay records on, and `logRangeBlocks` per `eth_getLogs` call when finding the recording transaction |
| `pollSeconds` | seconds between reads of the contract |
| `answerWithinSeconds` | how long the relay has to record a receipt after the request is opened |

| Variable | What it is |
| --- | --- |
| `BRIDGE_WEBHOOK_SECRET` | the Room webhook's signing secret |
| `BRIDGE_WEBHOOK_PREVIOUS_SECRET` | the previous secret, during a rotation overlap |
| `BRIDGE_EVM_PRIVATE_KEY` | with `forward.mode` `gateway`, signs gateway requests, and its address must be in the workflow's `httpTriggerAuthorizedKeys`; with `contract` answers, opens requests on the contract, and its account must be the owner or an allowed requester, with gas |

Bun also loads `.env` from the working directory, which git ignores.

- **Room setup.** Create a webhook member hosted by an agent of your account, with the
  URL `https://<host><basePath>/hooks` and the bridge's secret. Grant it the message channel
  members ask on. It needs no object channel: the bridge reads messages only.
- **Workflow setup** (`cre` answers). Set `attestationReplyUrl` in the `transfer-receipts`
  config to `https://<host><basePath>/cre/results`. Every DON node posts its copy of each
  report there.
- **Relay setup** (`contract` answers). Allow the bridge's account with
  `setRequester(account, true)` on the contract the relay serves.
- **State.** Requests and replies are kept 25 hours, past the longest reply window a
  host polls for (24 hours).

## Quotas

| CRE limit | Value | How this workflow fits |
| --- | --- | --- |
| HTTP requests per execution | 15 | at most 14 lookups, plus one `GET /auth/me`; a Room delivery request costs two and a job four; an attestation costs two (its read and its report's delivery), so 7 per execution |
| Consensus observation | 25 KB | each lookup is a projection of under 1 KB |
| Report payload | 50 KB | 14 receipts encode to at most about 12 KB |
| EVM reads per execution | 15 | reconcile uses at most 3 |
| Secrets fetches per execution | 5 | one batched fetch |
| HTTP trigger rate | 1 per 30 s | batch receipts into one push; the bridge calls at most once per `paceSeconds` |
| Log trigger rate | 10 per 6 s | one request per log |
| Workflows per organization (private registry) | 3 | `transfer-receipts` and `start-transfer` are two |

**Reconcile capacity is about 14 transfers per run: 168 per hour at the default 5 minutes.**
The window advances by `reconcileBatchSize` each run and wraps around the set, so every
open request is checked at least once every `ceil(open / 14)` runs. A full set of 256
takes 19 runs, about 95 minutes. Long-running transfers do not block newer ones.

A full report of 14 entries costs the receiver about 2.3 million gas for standard
receipts, 1.5 million for closures, and up to 4.0 million for Room receipts with 128-byte
ids that each close a Room request and a Room delivery request (`forge test -vv` prints
all three). Four jobs with their eight Room receipts cost about 2.0 million. `gasLimit` in the config files is 5,000,000, and `forge test` fails if a full
report needs more than 4,500,000 in the receiver.

## Setup

Requires [Bun](https://bun.sh/) 1.2.21 or later, [Foundry](https://getfoundry.sh/), the
[CRE CLI](https://docs.chain.link/cre/getting-started/cli-installation), a Beam API key,
and Sepolia ETH for the deploy.

```bash
cd packages/cre/workflows/transfer-receipts
bun install
bun run typecheck
bun run check

cd ../../contracts/evm
forge build
forge test
```

Dependencies are pinned to exact versions, and `bun.lock` is committed.

### Deploy the contract

For simulation, deploy with the CRE mock forwarder of your chain as the forwarder. On
Sepolia that is `0x15fC6ae953E024d975e77382eEeC56A9101f9F88`; `cre workflow
supported-chains` lists the addresses for your tenant. The second argument is
`staleAfter` in seconds; the third, `donAttested`, is `true` for a CRE deployment.

```bash
cd packages/cre/contracts/evm
forge create src/BeamTransferReceipts.sol:BeamTransferReceipts \
  --rpc-url https://ethereum-sepolia-rpc.publicnode.com \
  --private-key "$PRIVATE_KEY" --broadcast \
  --constructor-args 0x15fC6ae953E024d975e77382eEeC56A9101f9F88 86400 true
```

Never set the forwarder to the zero address: the vendored `ReceiverTemplate` treats that
as "accept reports from anyone".

### Configure

`config.staging.json` targets Beam DEV and Sepolia; `config.production.json` targets
Beam production.

| Key | Meaning |
| --- | --- |
| `evms[0].chainSelectorName` | CRE chain name, e.g. `ethereum-testnet-sepolia` |
| `evms[0].receiptsAddress` | the deployed `BeamTransferReceipts` |
| `evms[0].gasLimit` | gas for one receipt write |
| `beamApiBaseUrl` | BeamCore public API (`https://beamcore.b1m.ai` in production) |
| `roomApiBaseUrl` | Beam coordinator, for Room receipts (`https://coordinator.b1m.ai` in production) |
| `beamClientId` | `user_id` from `GET /auth/me` for `BEAM_API_KEY` |
| `reconcileIntervalMinutes` | reconcile schedule; must divide 60 |
| `reconcileBatchSize` | open requests checked per run, 1 to 14 (1 to 7 with `roomDelivery`, and 1 to 3 with `jobOutput`, since each Room delivery request costs two reads and each job four) |
| `roomDelivery` | the Room route of the deployment's Studio Room workflow: `room`, `channel` and `publisherMemberId`, the source member that publishes; `null` when it starts no Room deliveries |
| `jobOutput` | where the deployment's jobs publish their output: `channel`, an object channel of the `roomDelivery` Room, and `computeMemberId`, the compute member that publishes there; needs `roomDelivery`; `null` when it runs no jobs |
| `httpTriggerAuthorizedKeys` | EVM addresses allowed to call the HTTP trigger; required for deployment |
| `attestationReplyUrl` | the Room bridge's results endpoint, where attestation reports are sent; `null` when no bridge asks |

Secrets are declared in `secrets.yaml`. For simulation, copy `.env.example` to
`packages/cre/.env` and fill in `BEAM_API_KEY` and `BEAM_ORG_ID`. `.env` is ignored by git.

### Simulate

Simulation needs a CRE account and `cre login`. Run from `packages/cre`:

```bash
# Log trigger: after calling requestReceipt or requestRoomReceipt
cre workflow simulate workflows/transfer-receipts --target staging-settings \
  --non-interactive --trigger-index 0 \
  --evm-tx-hash 0xTX_THAT_EMITTED_THE_REQUEST --evm-event-index 0

# Reconcile
cre workflow simulate workflows/transfer-receipts --target staging-settings \
  --non-interactive --trigger-index 1

# HTTP push
cre workflow simulate workflows/transfer-receipts --target staging-settings \
  --non-interactive --trigger-index 2 \
  --http-payload '{"transfers":[{"transferId":"<uuid>"}]}'

# HTTP attestation, as a Room bridge sends it (needs attestationReplyUrl)
cre workflow simulate workflows/transfer-receipts --target staging-settings \
  --non-interactive --trigger-index 2 \
  --http-payload '{"attest":[{"op":"attest_transfer","transferId":"<uuid>"}]}'
```

Add `--broadcast` to write the receipt through the mock forwarder.

```bash
# start-transfer: after calling requestTransfer(requestId, 1, input)
cre workflow simulate workflows/start-transfer --target staging-settings \
  --non-interactive --trigger-index 0 \
  --evm-tx-hash 0xTX_THAT_EMITTED_THE_REQUEST --evm-event-index 0
```

`start-transfer` writes nothing onchain; it posts to the Studio webhook in
`CRE_STUDIO_WEBHOOK_URL`.

### Deploy the workflow

Deploying to the CRE DON needs deploy access approved by Chainlink (`cre account access`);
simulation does not. Before deploying:

1. Deploy the contract with the production `KeystoneForwarder` for your chain, or call
   `setForwarderAddress` on the simulation one.
2. Restrict reports to your workflow: `setExpectedAuthor(<workflow owner>)` and
   `setExpectedWorkflowId(<workflow id>)` (`cre workflow hash` shows the id).
3. Allow requesters with `setRequester`.
4. Store the secrets in the Vault DON: `cre secrets create secrets.yaml --target production-settings`,
   and for `start-transfer` also `cre secrets create workflows/start-transfer/secrets.yaml --target production-settings`.
5. Fill `httpTriggerAuthorizedKeys`; a deployed HTTP trigger refuses requests without them.

```bash
cre workflow deploy workflows/transfer-receipts --target production-settings
cre workflow deploy workflows/start-transfer --target production-settings
```

## API key scopes

The workflows, the relay and the bridge's lookups only read. `GET /transfers/:id` needs
`transfers:read`; `GET /auth/me` needs any valid key; the Room receipt and publication
lookup routes need `room:objects:read`. Create `BEAM_API_KEY` as an API key (`b1m_…`) on
the console's **API keys** page, then apply the **"Read-only: transfers + Room evidence"**
preset in that key's permission matrix. The preset holds exactly `transfers:read` and
`room:objects:read`: the key cannot create transfers or change Rooms. A service account
credential (`bm_sa_…`) does not work here: it administers keys and billing through
`api.b1m.ai`, and BeamCore accepts only API keys.
Create it in the organization whose key starts the transfers (the Studio credential): Beam
attributes every key an organization issues to one identity, so both keys share the
`beamClientId` that transfer ids derive from, and `GET /transfers/:id` only returns that
identity's transfers.

- **Where the read-only key works:** Beam DEV and production both accept it. A key without
  the preset holds `transfers:create` and, as an organization key, administers Rooms; treat
  such a key as a full credential.
- The workflow only ever sends the key to `beamApiBaseUrl` and `roomApiBaseUrl`. On a
  deployed workflow it lives in the Vault DON and is fetched per execution.

## Dependency on Beam

- `GET /transfers/:id` and `GET /auth/me` are live on BeamCore today. `planFingerprint` and
  `coordinateChecksum` come from the transfer's `metadata` and are zero when Beam reported
  none.
- The Room receipt route and the `beam-room-receipt-evidence/v1` spec (envelope, error codes
  and test vector) are described in the [Beam documentation](https://docs.b1m.ai/docs/integrations/chainlink-cre).
  Until that route is deployed, Room lookups fail and Room requests stay open.

## Reading a receipt from another contract

```solidity
BeamTransferReceipts.StandardReceipt memory receipt = receipts.getReceipt(transferId);
bool finished = receipt.status == BeamTransferReceipts.Status.Completed;
bool attested = (receipt.level & receipts.LEVEL_DON_ATTESTED()) != 0;

BeamTransferReceipts.JobResult memory job = receipts.getJob(requestId);
bool jobDone = job.status == BeamTransferReceipts.Status.Completed;
```

## Report format

`_processReport` decodes version 2 as

```
abi.encode(uint8 version, StandardReceiptReport[] standard, RoomReceiptReport[] rooms,
           RequestClosure[] closures, JobResultReport[] jobs)
```

with the structs declared in the contract. Jobs are recorded last, so one report can carry a
job with its two Room receipts. Any change to the layout of a deployed contract bumps the version.
`bun run check` and `forge test` both pin the hash of one shared fixture, so the
workflow's viem encoding and the contract's decoding cannot drift apart unnoticed.

## Vendored contracts

`contracts/evm/src/vendor/` holds unmodified MIT-licensed sources, each with its origin
URL in its header and the upstream `LICENSE` beside it:

- `chainlink/`: `ReceiverTemplate.sol`, `IReceiver.sol` and `IERC165.sol` from
  `smartcontractkit/documentation` at `940d83c5c72d729db292186df64a3e68b8d9eb61`
  (`public/samples/CRE/`), the version the CRE docs publish. It accepts the 64-byte
  metadata the production `KeystoneForwarder` passes; some template copies require
  exactly 62 bytes once workflow checks are enabled.
- `openzeppelin/`: `Ownable.sol` and `Context.sol` from OpenZeppelin Contracts v5.4.0
  (`c64a1edb67b6e3f4a15cca8909c9482ad33a02b0`).

There are no Foundry library installs: the tests declare the few cheatcodes they use.

## Files

```
packages/cre/
├── project.yaml                    CRE project settings (RPCs per target)
├── secrets.yaml                    BEAM_API_KEY, BEAM_ORG_ID (transfer-receipts)
├── .env.example
├── contracts/evm/
│   ├── foundry.toml
│   ├── src/BeamTransferReceipts.sol
│   ├── src/vendor/
│   └── test/BeamTransferReceipts.t.sol
├── workflows/transfer-receipts/
│   ├── workflow.yaml               targets, config and secrets paths
│   ├── config.staging.json         Beam DEV, Sepolia
│   ├── config.production.json      Beam production
│   ├── main.ts                     Runner entry point
│   ├── workflow.ts                 config schema, handlers, EVM reads and writes
│   ├── observe.ts                  node-mode Beam lookups and report delivery to a bridge
│   ├── beam.ts                     projections of Beam's answers, including the publication lookup
│   ├── subjects.ts                 what to look up and what to report, at a deployment's levels
│   ├── receipts.ts                 contract ABI, levels and report encoding
│   ├── attestations.ts             a Room bridge's requests and the reports that answer them
│   ├── transfer-id.ts              transfer id derivation
│   └── check.ts                    offline checks (`bun run check`)
├── workflows/start-transfer/
│   ├── workflow.yaml
│   ├── secrets.yaml                the transfer and Room workflows' webhook URLs and signing secrets
│   ├── config.staging.json         Sepolia
│   ├── config.production.json      Ethereum mainnet
│   ├── main.ts
│   ├── workflow.ts                 config schema, the TransferRequested handler, node-mode POST
│   ├── studio.ts                   webhook body, signature and response projection
│   └── check.ts                    offline checks (`bun run check`)
├── relay/
│   ├── config.bittensor-testnet.json   example config
│   ├── main.ts                     entry point and pass loop
│   ├── config.ts                   config schema and secrets from the environment
│   ├── relay.ts                    starts and receipts, over an IO interface
│   ├── io.ts                       chain, Studio, Beam and cursor file
│   └── check.ts                    offline checks (`bun run check`)
├── bridge/
│   ├── config.example.json         example config, answers from CRE
│   ├── config.bittensor-testnet.json   example config, answers from a relay's contract
│   ├── main.ts                     HTTP routes and the forwarding loop
│   ├── config.ts                   config schema and secrets from the environment
│   ├── bridge.ts                   requests, batching, report intake and replies, over an IO interface
│   ├── webhook.ts                  Room webhook signatures, envelopes and what a message asks
│   ├── cre.ts                      gateway JWT and JSON-RPC, and the command forwarder
│   ├── report.ts                   report header, DON signatures and Capability Registry reads
│   ├── chain.ts                    receipts contract reads, request transactions and recording logs
│   ├── state.ts                    state file
│   ├── verify.ts                   checks a reply without trusting the bridge (`bun run verify`)
│   └── check.ts                    offline checks (`bun run check`)
└── examples/compute-agent/
    ├── compute_agent.py            a toy compute member that answers jobs (Python, standard library)
    └── agent.example.json          its configuration
```
