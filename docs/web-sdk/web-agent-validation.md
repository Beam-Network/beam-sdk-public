# Web Agent SDK validation

Local implementation and verification, 2026-09-16. The existing broker-based
`Beam` API remains unchanged. No normal-agent code or remote Beam actor was
modified. Package artifacts were built locally; nothing was published.

## Results

| Check                                         | Outcome                                                                                                                                                                |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser SDK suite                             | 178 tests passed, including 26 new Web Agent tests.                                                                                                                    |
| Companion server suite                        | 49 tests passed.                                                                                                                                                       |
| Browser and server TypeScript checks          | Passed.                                                                                                                                                                |
| Repository ESLint                             | Passed.                                                                                                                                                                |
| Formatting of changed files                   | Passed.                                                                                                                                                                |
| Full repository formatting                    | Flags the existing, untouched `HUGGINGFACE_PROVIDER.md`. Not changed as part of this work.                                                                             |
| Browser and server builds                     | Passed; browser ESM/CJS/declarations and IIFE include the new client.                                                                                                  |
| Both package checks                           | `publint` and `attw --profile node16` passed. Browser publint has an advisory about its existing missing Node engines field; no new engines restriction was added.     |
| Browser example                               | Loads the built SDK without JavaScript errors in Chromium.                                                                                                             |
| Real Chromium + actual local Go WebSocket API | Passed: two clients, byte-exact 65,536-byte messages, shared delivery, independent disconnect, explicit reconnect, and cleanup. Go fixture ran with the race detector. |

The new tests cover authentication credential restrictions, safe endpoints,
request correlation and limits, private-field/error redaction, independent
resources, cancellation and late allocation cleanup, uncertain cancellation,
no replay on reconnect, media readiness, capture-track ownership, failed/aborted
SDP setup, ICE timeout, hanging native negotiation, concurrent viewers,
subscription ownership, and dependent cleanup.

Go/browser and unit tests ran with outbound networking denied except localhost
and Unix sockets. Dependency installation used the package registry beforehand.
The real API fixture uses a participant double; it does not establish real MLS,
coordinator authorization, Worker scheduling, or end-to-end dev media delivery.
SDK unit-level media negotiation/lifecycle uses hand-written peer connection
fakes. Earlier agent-runtime browser media tests are separate evidence. The
subsequent authorized live dev run below adds SDK end-to-end media coverage.

## Reproduction

From the SDK repository, with its Node dependencies installed:

```sh
npm run typecheck -w @beam-network/web-sdk
npm run typecheck -w @beam-network/web-sdk-server
npm run test -w @beam-network/web-sdk
npm run test -w @beam-network/web-sdk-server
npm run build:web
npm run package:check
npx eslint .
```

For the network-isolated Chromium/Go check, use the instructions in
[the client guide](web-agent.md#local-example-and-verification). The driver uses
an overlay of the sibling `beam-tunnel-agent` test fixture, so that repository's
working tree is unchanged. Node 24.19.0, Playwright 1.62.1, Chromium headless shell,
and Go 1.26.5 were used here. Command Line Tools clang supplied the race toolchain.

## Authorized live dev run

On 2026-09-16 the user explicitly authorized their normal local `beam-dev` to
manage room `btr_room_25y7gerldah6k25emca2abtkd4` and requested an OBS test app.
The former local dev example connected the built SDK to a separate invited Web
Agent identity, with the real dev coordinator, channel grants, MLS keys, and
remote media worker allocation. The application and its historical validation
record now live in the private
streaming showcase repository.
This report describes that earlier local run; it does not qualify the showcase's
new gateway or hosted deployment.

The synthetic WHIP source negotiates H.264 and Opus against the same local
normal-agent ingest endpoint prepared for OBS. Actual video frames and Opus
samples are decoded by the SDK browser viewer. Simultaneous viewers and
independent viewer cleanup passed, as did explicit SDK reconnect. The source
is cleaned up after each run. Runtime evidence and a screenshot are saved in
`~/.beam-web-sdk-dev/obs-test/verification.json` and `app-live.png`.

Message delivery is **blocked** in this dev environment: a normal-agent
publication timed out in provisioning, and the orchestrator logged
`room_workload_relay_unavailable` while redeeming its source path. Local
fixture message coverage does not establish dev message delivery. The
media-only live test explicitly records message checks as skipped.

The actual OBS application remains a user-operated final check. No remote
service configuration or normal-agent code was changed. Source-stop/restart
results are recorded in the live report; a named publisher can reuse its
workload ID, so acceptance checks newly decoded frames rather than assuming
that ID changes. Full permission-revocation/failover acceptance remains outside
this run. Byte streams remain deferred under agent issue #117 and are absent
from the SDK surface.
