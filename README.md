# BEAM SDK

BEAM is a decentralized transfer network. This repository is a multi-language SDK monorepo focused on transfer creation and transfer lifecycle management.

## Packages

| Language | Directory | Registry target | Status |
| --- | --- | --- | --- |
| Python | `sdks/python` | PyPI: `beam-network-sdk` | Transfer SDK |
| TypeScript | `sdks/typescript` | npm: `@beam-network/sdk` | Transfer SDK |
| CLI | `packages/cli` | npm: `@beam-network/cli` | `beam-send` binary |
| Go | `sdks/go` | Go module: `github.com/Beam-Network/beam-sdk-public/sdks/go` | Transfer SDK |
| Rust | `sdks/rust` | crates.io: `beam-network-sdk` | Transfer SDK |
| Browser | `packages/web-sdk` | npm: `@beam-network/web-sdk` | Rooms, broadcast, transfers |
| Token broker | `packages/web-sdk-server` | npm: `@beam-network/web-sdk-server` | Server-side token minting |
| Zapier | `packages/zapier` | Zapier platform (not npm) | Beam in Zapier's app directory |

Shared API contracts live in `specs/`. Package-specific docs and release metadata live inside each SDK directory.

The browser SDK is a different shape from the language SDKs: `@beam-network/sdk` is a
Node client that holds a Beam API key and speaks NATS, while `@beam-network/web-sdk` runs
in a page and never sees a credential. `@beam-network/web-sdk-server` is what bridges
them, minting short-lived scoped tokens from a backend. See `docs/web-sdk/`.

## What You Can Do

- Create transfers from provider-agnostic HTTP or object-storage definitions.
- Prepare provider-aware transfers while keeping S3, R2, Hippius, or Hugging Face credentials local to the SDK.
- Attach signed destination routes, distribute transfers, cancel transfers, and wait on owned terminal signals with authoritative status reconciliation.
- Use `beam-send` for transfer-oriented CLI workflows.

Signed-route clients use `transfer-client-control/v6`, with Runtime/transport epoch monitoring, `transfer.resume`, and a UUID route generation on prepare and route streaming. Explicit provider resume calls use a fresh prepare generation per invocation while reusing the same request envelope for transport retries. The in-memory recovery lease is installed after prepare and before route-stream begin; initial streaming and replay are coalesced per transfer, and foreground cancellation, deadline expiry, or a Runtime state-loss response leaves background recovery active. Multipart uploads are not aborted while that lease can resume the transfer. Provider signing inputs stay in memory so routes can be regenerated after a Runtime restart; manual route APIs require a recovery factory. Signed routes and credentials are never journaled locally. Clients retain terminal reconciliation and explicit close/shutdown behavior, which also abandons and clears active recovery leases.

## Install

Python:

```bash
pip install beam-network-sdk
```

TypeScript:

```bash
npm install @beam-network/sdk
```

Go:

```bash
go get github.com/Beam-Network/beam-sdk-public/sdks/go
```

Rust:

```toml
beam-network-sdk = "0.1"
```

Python extras:

```bash
pip install beam-network-sdk[s3]       # S3 provider signing
pip install beam-network-sdk[r2]       # Cloudflare R2 provider signing
pip install beam-network-sdk[gcs]      # GCS provider models
```

## Quick Start

```python
import asyncio

from beam_network_sdk import BeamSDK
from beam_network_sdk.models import DestConfig, SourceConfig


async def main() -> None:
    async with BeamSDK(api_key="b1m_your_key", environment="dev") as beam:
        transfer = await beam.transfers.create(
            sources=[
                SourceConfig(
                    type="http",
                    url="https://downloads.example.com/report.parquet",
                )
            ],
            destinations=[
                DestConfig(
                    type="http",
                    url="https://storage.example.com/ingest/report.parquet",
                )
            ],
            total_size=104_857_600,
            name="daily-report",
        )

        await beam.transfers.distribute(transfer.transfer_id)
        status = await beam.transfers.wait_complete(transfer.transfer_id)
        print(status.status)


asyncio.run(main())
```

## CLI

Create and monitor a transfer:

```bash
beam-send create \
  --source '{"type":"http","url":"https://downloads.example.com/report.parquet"}' \
  --destination '{"type":"http","url":"https://storage.example.com/ingest/report.parquet"}' \
  --total-size 104857600 \
  --wait
```

Only `beam-send` is packaged.

The npm CLI package exposes the same binary name for `npx`:

```bash
npx -p @beam-network/cli beam-send --help
```

## Repository Layout

- `sdks/python/` contains the Python transfer SDK and examples.
- `sdks/typescript/` contains the npm package scaffold and TypeScript client.
- `packages/cli/` contains the npm CLI package exposing `beam-send`.
- `sdks/go/` contains the Go module scaffold and Go client.
- `sdks/rust/` contains the Rust crate scaffold and Rust client.
- `packages/web-sdk/` contains the browser SDK (rooms, broadcast, transfers).
- `packages/web-sdk-server/` contains the Node token broker for the browser SDK.
- `packages/zapier/` contains the Beam integration for the Zapier platform. It is
  deliberately not an npm workspace member: Zapier bundles the app's own
  `node_modules` on push, and workspace hoisting would ship an incomplete one. It is
  deployed with `zapier push`, never published to npm.
- `examples/rooms-and-broadcast/` is a runnable browser-SDK example.
- `specs/` contains shared API contracts used across language packages.
- `docs/web-sdk/` documents the browser SDK's architecture, authentication, and capabilities.
- `docs/RELEASE.md` explains how to build and publish each package.

## License

MIT
