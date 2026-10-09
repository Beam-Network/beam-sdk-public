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

`tls://` lifecycle URLs, including the production gateway `tls://orch-gateway.b1m.ai:4222`,
connect with TLS handshake-first and verify the gateway hostname. The gateway sends no
plaintext `INFO`, so this is required, and matches the TypeScript SDK.

`BeamSDK` options mirror the TypeScript `BeamClientOptions`:

| Option | Default | Purpose |
| ------ | ------- | ------- |
| `timeout` | `30.0` | Seconds per lifecycle request. |
| `route_signing_concurrency` | `64` | Routes signed concurrently; also bounds recovery signing and integrity grants. |
| `multipart_control_concurrency` | `2` (`BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY`) | Concurrent multipart create/abort calls. |
| `max_payload_bytes` | `8 MiB` | Hard guard on encoded lifecycle messages. |
| `transfer_client_subject_prefix` | `beam.transfer.client` | NATS subject prefix. |
| `transfer_runtime_shard_count` | `TRANSFER_RUNTIME_SHARD_COUNT` or `1` | Transfer runtime shard count. |

All counts must be positive integers.

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

Source and destination credentials must not be restricted to specific IP addresses or
networks. When storage refuses access, `wait_complete` raises `BeamStorageAccessError`
(`code` is `source_access_denied` or `destination_access_denied`).

`create_transfer(...)` is the TypeScript `createTransfer` name for the same call. It
distributes by default. `prepare_provider_transfer` keeps its `distribute=False` default.

S3 and R2 sources record the object's `etag`, `version_id`, `last_modified` and
`content_length` from the HEAD request. Integrity audits use them to pin source reads
with `If-Match`/`VersionId`.

### S3-compatible storage

`S3CompatibleProviderSource` and `S3CompatibleProviderDestination` cover any S3 API
(MinIO, Wasabi, Backblaze B2, and similar):

```python
from beam_network_sdk import S3CompatibleProviderDestination

destination = S3CompatibleProviderDestination(
    provider="wasabi",                      # reported to BeamCore, lower-cased
    bucket="archive",
    key="imports/report.parquet",
    endpoint_url="https://s3.us-east-1.wasabisys.com",
    access_key_id="...",
    secret_access_key="...",
    # region=..., session_token=..., force_path_style=..., account_id=...
)
```

`endpoint_url` is required except for `s3` (AWS regional endpoint) and `r2` (derived
from `account_id`). The region defaults to `auto` for R2 and `us-east-1` otherwise.
Path-style addressing is on by default for custom endpoints. S3 clients are cached per
provider, endpoint, region, and credentials, and retry each call up to 5 attempts in
total.

### Hooks, cancellation, and ownership

```python
from beam_network_sdk import BeamCancellationToken

ownership = BeamCancellationToken()
groups = []

prepared = await beam.transfers.prepare_provider_transfer(
    sources=[...],
    destinations=[...],
    on_before_transfer_prepare=lambda: None,   # sources resolved, before transfer.prepare
    on_prepared=save_transfer_id,              # before any route is streamed
    on_multipart_group_ready=groups.append,    # after each upload is created
    throw_if_cancelled=check_deadline,         # def check_deadline(transfer_id=None): ...
    ownership=ownership,
)
```

Hooks can be sync or async.

- `on_multipart_group_ready` receives a `ProviderMultipartGroupIdentity` holding the
  transfer, group, source, destination, object key, upload ID, size, part count, and
  expiry. It contains no credentials, headers, or URLs. If the hook raises, that upload
  is aborted and the transfer fails closed.
- If `on_prepared` or `throw_if_cancelled` raises, or the calling task is cancelled
  with `asyncio`, the foreground call stops. The recovery lease keeps the prepared
  transfer going in the background.
- `ownership.cancel(reason)` may be called from any thread. It fences this owner:
  signing, route replay, and multipart creation stop, and this owner's recovery lease
  and signer are released. It does **not** cancel the transfer, and it never aborts
  uploads that a replacement owner may use.

### Resuming in another process

Persist the identities from `on_multipart_group_ready`. A new process can then take
the transfer over without creating replacement uploads:

```python
prepared = await beam.transfers.resume_provider_transfer(
    transfer_id=saved_transfer_id,
    multipart_groups=saved_identities,         # ProviderMultipartGroupIdentity or dicts
    sources=[...],
    destinations=[...],
    ownership=new_owner_token,
)
```

`resume_provider_transfer` re-prepares the transfer under a fresh
`transfer:{id}:prepare:resume:{uuid}` key and gets a new route generation. It checks
that BeamCore returned the same transfer, then reuses the given upload IDs. The
identities must cover every multipart group of the plan exactly once. Otherwise it
raises a `provider_multipart_recovery_*` error, and nothing is created. As in the
TypeScript SDK, `distribute` defaults to `True`.

### Failures

A transfer failure that cannot be recovered raises `BeamProviderTransferError`, after
the SDK has cancelled the transfer and aborted the uploads it created:

- If the route stream never began, uploads are aborted first. A failed abort is retried
  once after cancellation.
- Otherwise the transfer is cancelled first, so that no worker finishes a part after
  its upload is aborted.

`errors` lists the cause, then any cancellation or cleanup failure.
`transfer_cancelled` and `multipart_cleanup_complete` report the outcome of each step.
Messages carry only error types and HTTP statuses, never signed URLs. The recovery lease
is released only after cleanup finishes, because cleanup still needs the retained
credentials.

### Hybrid helpers

These are provider metadata operations. None of them reads or proxies object bytes:

- `sign_destination_url(destination, object_key=..., upload_id=..., part_number=..., content_md5=...)`.
  A `content_md5` binds the upload to a checksum, and requires S3-compatible storage.
- `list_multipart_parts(...)`, which follows pagination.
- `complete_multipart_upload(...)`.
- `inspect_destination_object(...)`.
- `prepare_provider_source_for_plan(...)`, which returns a `PlanningHttpSource` without
  a signed URL.
- `sign_source_read_range(..., if_match=..., version_id=...)`.

Each accepts `cancellation=BeamCancellationToken`. The token is checked before every
provider call, but an S3 request that is already in flight cannot be interrupted.

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

Provider transfers use `transfer-client-control/v7`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. S3 and R2 retain direct multipart UploadPart/ListParts/HEAD handling. Hippius and Hugging Face destinations take a plain PUT per chunk instead, so they carry no multipart group manifest.

The SDK signs up to 64 routes concurrently by default and emits 1,024-route logical batches. A batch is split to target `min(max_payload_bytes, 4 MiB)` of encoded MessagePack, with room reserved for the auth token. A single route above that target but within `max_payload_bytes` (8 MiB by default) is sent alone. A route above `max_payload_bytes` fails before publication, with both sizes in the error. The stream ID derives from the transfer, selected flow, and immutable plan identity, and each ordered batch ID includes its route-coordinate checksum. Manual multi-destination attachment requires `delivery_index` on every route. Lifecycle mutations retry transient failures up to three times with the same request identity. Completion waits subscribe to the API-key-owned, at-most-once terminal signal before the first status read and reconcile every signal through authoritative status; subscription failure degrades to the same jittered 15-to-30-second status fallback. `async with BeamSDK(...)` closes signing workers, terminal waiters, and NATS resources; callers not using the context manager must call `await beam.close()`.

Hippius uses canonical non-multipart `signed_url` routes, so its manifest is empty and group-level final HEAD verification is not available from that provider flow. GCS and Azure provider signing remain unimplemented.

NATS requires this guard because the broker rejects messages above its configured `max_payload`. This limit applies only to lifecycle/control metadata; transfer file bytes do not flow through NATS.

## Restart Recovery

The SDK keeps one in-memory recovery lease per active transfer and one `runtime.hello` monitor per active shard. The lease is installed before route-stream begin, and a per-transfer lock coalesces initial streaming with replay. Multipart recovery retains the existing upload IDs and compact group state, then re-signs only the expiring route and commit controls. Route replay re-streams the retained plan. It does not prepare again, repeat the source HEAD, or re-hash for Hugging Face. When Runtime asks to sign routes for an upload this process has not signed yet, the SDK rebuilds its controls from the requested upload ID. Recovery requests for Hippius and Hugging Face destinations are signed as plain PUTs. Under `transfer-client-control/v7` every source chunk owns one consecutive part, so the requested part number must equal `source_chunk_index + 1` and `attempt_slot` must be 0. Recovery signing also mirrors the TypeScript SDK's staged multipart recovery: on Runtime request it renews the original upload's list, complete, abort and HEAD controls, or signs a per-attempt staged recovery object (PUT, HEAD, DELETE) plus the `UploadPartCopy` that copies it into the original upload's part, and signs listing and deletion of staged objects for cleanup. `cancel()` always releases the lease and stops recovery signing and integrity grants. Runtime epoch changes invalidate cached auth, coalesce `transfer.resume`, and regenerate routes under a fresh generation; transport-only changes replay only when Runtime reports routes missing or expired. Provider inputs are released on terminal status or `close()`. Manual `attach_signed_urls` requires `route_generation_id`, plan fingerprint/checksum, and an async `recovery_factory`. No signed URL, credential, or recovery journal is written locally. Foreground cancellation, deadline expiry, and Runtime state-loss responses keep the background lease and retained multipart upload alive.

## Integrity Audits

`status()` answers a transfer's `integrity_audit_challenge` with signed source and
destination read grants. Concurrent polls share one submission per `audit_id`. If
submission fails, the status call still succeeds:

- `integrity_audit_submission_error` holds a short, sanitized reason, and the next poll
  retries.
- The reason is `integrity audit signer unavailable` when this process did not prepare
  the transfer.
- If the error message mentions a URL, `x-amz`, or a secret, token, authorization, or
  credential, only the error type is reported. Otherwise the message is truncated to
  200 characters.

## Differences From The TypeScript SDK

The Python SDK follows the TypeScript SDK's wire behavior. The differences are in API
shape and in Python-only features:

- Names are snake_case, and identities are pydantic models (`ProviderMultipartGroupIdentity`) rather than camelCase objects.
- `prepare_provider_transfer` defaults to `distribute=False` for compatibility. `create_transfer` and `resume_provider_transfer` default to `True`, as in TypeScript.
- `BeamCancellationToken` replaces `AbortSignal`. Provider calls run in worker threads, so cancellation is checked between calls rather than interrupting one in flight.
- Cancelling the calling asyncio task keeps the recovery lease running, like `throw_if_cancelled`.
- Lifecycle `request_id`s are derived as UUIDs from `beam:{message_type}:{idempotency_key}`, while TypeScript uses `idempotent-<sha256>`. The idempotency keys themselves match.
- Python-only features: the `beam-send` CLI, environment variable configuration (`BEAM_API_KEY`, `BEAM_NATS_URL`, `BEAM_ENV`, `TRANSFER_RUNTIME_SHARD_COUNT`), GCS and Azure placeholder models, and the extra exception types.

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
    --api-key b1m_your_key \
    --aws-access-key YOUR_KEY \
    --aws-secret-key YOUR_SECRET \
    --aws-region us-east-1
```

Only `beam-send` is packaged.

## Multi-Language Repository

The Python package lives alongside TypeScript and Go SDKs under `sdks/`. Shared API contracts live in `../../specs/`.

### Preparation diagnostics

Route streaming flushes at 1,024 routes, with no timer, and sends the final partial batch at stream completion. Signing overlaps acknowledged publication through bounded buffering; existing encoded-payload limits still apply. A source grant is reused across destinations only within the same signing generation and actual expiry.

The optional `on_diagnostics` callback receives bounded preparation measurements and source-grant reuse counts. Callbacks are best effort, may be dropped under load, and do not affect transfer outcomes. Durations may overlap; do not sum them into elapsed transfer time. Transfer status also exposes a typed `performance` summary when supported by Core.

Optional `storage_location` (`StorageLocation` in Go) describes the physical
storage location. It is separate from the region used to sign provider requests.
Leave it unset when unknown; a signing region such as R2's `auto` is not a location.

When supported by Core, diagnostics use `sdk-performance/v2`: bounded histograms,
preparation milestones, concurrency high-water marks, and separate provider,
callback, manifest, signing, and transport waits. Histogram bins are noncumulative,
with upper bounds in milliseconds of 0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250,
500, 1000, 2500, 5000, 10000, 30000, 120000, then overflow. `unmeasured` explicitly
identifies unavailable measurements. Process CPU includes other concurrent work
in the SDK process; it is not transfer-exclusive CPU. Detailed reporting can be
disabled with `BEAM_SDK_PERFORMANCE_DETAILS=false`. Older peers retain v1 reports.
No credentials or storage grants are included in these diagnostics.

`sdk.producer_wait` measures waiting for a signed route separately from publication
backpressure. Configured-limit gauges preserve starting limits; effective-limit
gauges report the highest limit reached. Optional `source_renewals` counts source
grants recreated in later preparation generations. A bounded 16 KiB history tracks
131,072 chunk indices; the counter is omitted after takeover or beyond that bound.
It does not include separate recovery-control signing operations.
