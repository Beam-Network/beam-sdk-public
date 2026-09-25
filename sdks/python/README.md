# BEAM SDK for Python

The Python SDK for BEAM 0.6.0 is now transfer-only. It exposes `BeamSDK.transfers` for transfer creation, provider-aware preparation, distribution, cancellation, status reconciliation, and event-driven completion waiting.

## Install

```bash
pip install beam-network-sdk
```

Optional extras:

```bash
pip install beam-network-sdk[s3]        # S3 provider signing
pip install beam-network-sdk[r2]        # Cloudflare R2 provider signing
pip install beam-network-sdk[gcs]       # GCS provider models
pip install beam-network-sdk[dev]       # lint/typecheck tooling
```

## Configuration

| Setting | Purpose |
| ------- | ------- |
| `BEAM_NATS_URL` | Beam NATS lifecycle endpoint when neither `nats_url` nor `environment` is passed. |
| `BEAM_API_KEY` | API key authentication. |

Pass `api_key` explicitly or set `BEAM_API_KEY`. Provide either `nats_url` or `environment`, not both.

## Basic Transfer

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
            progressive_mode=True,
        )

        await beam.transfers.distribute(transfer.transfer_id)
        status = await beam.transfers.wait_complete(transfer.transfer_id)
        print(status.status)


asyncio.run(main())
```

## Provider-Aware Transfer

```python
import asyncio

from beam_network_sdk import BeamSDK
from beam_network_sdk.models import S3ProviderDestination, S3ProviderSource


async def main() -> None:
    async with BeamSDK(api_key="b1m_your_key", environment="dev") as beam:
        transfer = await beam.transfers.prepare_provider_transfer(
            sources=[
                S3ProviderSource(
                    bucket="source-bucket",
                    key="exports/report.parquet",
                    region="us-east-1",
                    access_key_id="...",
                    secret_access_key="...",
                )
            ],
            destinations=[
                S3ProviderDestination(
                    bucket="dest-bucket",
                    key="imports/report.parquet",
                    region="us-east-1",
                    access_key_id="...",
                    secret_access_key="...",
                )
            ],
            distribute=True,
        )
        print(transfer.transfer_id)


asyncio.run(main())
```

Provider credentials stay in the SDK process. BeamCore receives prepared HTTP source URLs and signed destination routes.

## Hugging Face Hub

`HuggingFaceProviderSource` and `HuggingFaceProviderDestination` move a file in or
out of a Hub repo. The token stays in this process: a source resolves to the
presigned CDN URL the Hub redirects to, and a destination uses the presigned part
URLs the Hub's LFS batch endpoint issues.

```python
import asyncio

from beam_network_sdk import BeamSDK
from beam_network_sdk.models import HuggingFaceProviderSource, S3ProviderDestination


async def main() -> None:
    async with BeamSDK(api_key="b1m_your_key", environment="dev") as beam:
        transfer = await beam.transfers.prepare_provider_transfer(
            sources=[
                HuggingFaceProviderSource(
                    repo_id="org/dataset",
                    path="data/train.parquet",
                    repo_type="dataset",
                    token="hf_...",
                )
            ],
            destinations=[
                S3ProviderDestination(
                    bucket="archive",
                    key="datasets/train.parquet",
                    access_key_id="...",
                    secret_access_key="...",
                )
            ],
            distribute=True,
        )
        # wait_complete commits any Hugging Face destination once the parts land.
        await beam.transfers.wait_complete(transfer.transfer_id)


asyncio.run(main())
```

The CLI covers the same flow with `hf://` URIs:

```bash
beam-send hf-transfer \
  hf://datasets/org/dataset@main/data/train.parquet \
  hf://datasets/org/mirror@main/data/train.parquet
```

Constraints, all of them the Hub's rather than Beam's — see
[`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md) for the full protocol:

- **Sources are large-file (LFS or Xet) content.** Only those get the
  credential-free CDN redirect; a small regular file is served inline and is
  rejected with that reason. Hub **buckets** work as sources too, but cannot be
  written to — they expose no LFS endpoint.
- **A destination needs the object's sha256 up front**, because the LFS batch
  endpoint will not issue upload URLs without it. When the source is not itself a
  Hub file, set `allow_source_rehash=True` to let the SDK read the source once to
  compute it.
- **One source per Hugging Face destination**, because the Hub dictates the part
  size and a plan carries a single chunk size.
- If you do not call `wait_complete`, call
  `finalize_huggingface_uploads(transfer_id)` yourself once the transfer completes
  — without it the parts are uploaded but no commit is made and the file does not
  appear in the repo.

## Route Streaming And Payload Size

Provider transfers use `transfer-client-control/v6`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. S3 and R2 retain direct multipart UploadPart/ListParts/HEAD handling. Hippius and Hugging Face destinations take a plain PUT per chunk instead, so they carry no multipart group manifest.

The SDK signs up to 64 routes concurrently by default and emits 2,048-route logical batches, split only when encoded MessagePack requests exceed the 8 MiB default guard. Explicit positive overrides remain supported. A single route larger than the configured limit fails before publication with both sizes in the error. The stream ID derives from the transfer, selected flow, and immutable plan identity, and each ordered batch ID includes its route-coordinate checksum. Manual multi-destination attachment requires `delivery_index` on every route. Lifecycle mutations retry transient failures up to three times with the same request identity. Completion waits subscribe to the API-key-owned, at-most-once terminal signal before the first status read and reconcile every signal through authoritative status; subscription failure degrades to the same jittered 15-to-30-second status fallback. `async with BeamSDK(...)` closes signing workers, terminal waiters, and NATS resources; callers not using the context manager must call `await beam.close()`.

Hippius uses canonical non-multipart `signed_url` routes, so its manifest is empty and group-level final HEAD verification is not available from that provider flow. GCS and Azure provider signing remain unimplemented.

NATS requires this guard because the broker rejects messages above its configured `max_payload`. This limit applies only to lifecycle/control metadata; transfer file bytes do not flow through NATS.

## Restart Recovery

The SDK keeps one in-memory recovery lease per active transfer and one `runtime.hello` monitor per active shard. The lease is installed before route-stream begin, and a per-transfer lock coalesces initial streaming with replay. Multipart recovery retains the existing upload IDs and compact group state, then re-signs only the expiring route and commit controls. Runtime epoch changes invalidate cached auth, coalesce `transfer.resume`, and regenerate routes under a fresh generation; transport-only changes replay only when Runtime reports routes missing or expired. Provider inputs are released on terminal status or `close()`. Manual `attach_signed_urls` requires `route_generation_id`, plan fingerprint/checksum, and an async `recovery_factory`. No signed URL, credential, or recovery journal is written locally. Foreground cancellation, deadline expiry, and Runtime state-loss responses keep the background lease and retained multipart upload alive.

## CLI

`beam-send` creates, distributes, monitors, and waits for transfers:

```bash
beam-send create \
  --source '{"type":"http","url":"https://downloads.example.com/report.parquet"}' \
  --destination '{"type":"http","url":"https://storage.example.com/ingest/report.parquet"}' \
  --total-size 104857600 \
  --wait
```

Convenience commands for object storage remain under `beam-send`, for example:

```bash
beam-send s3-transfer s3://source-bucket/file.bin s3://dest-bucket/file.bin \
    --server http://127.0.0.1:8000 \
    --api-key b1m_your_key \
    --aws-access-key YOUR_KEY \
    --aws-secret-key YOUR_SECRET \
    --aws-region us-east-1
```

Only `beam-send` is packaged.

## Multi-Language Repository

The Python package lives alongside TypeScript and Go SDKs under `sdks/`. Shared API contracts live in `../../specs/`, and release instructions live in `../../docs/RELEASE.md`.
