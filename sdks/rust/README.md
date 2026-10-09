# BEAM SDK for Rust

Install:

```toml
beam-network-sdk = "0.7"
```

The crate talks to BeamCore over NATS (`tls://orch-gateway.b1m.ai:4222` by default) and signs
provider URLs locally: provider credentials never leave your process.

This release requires BeamCore's `transfer-client-control/v7` contract. Multipart uploads use
consecutive parts (source chunk `i` is part `i + 1`); BeamCore may request retained staging for
recovery through the SDK's signing connection, while workers receive ordinary upload URLs. Keep
the client connected until the transfer reaches a terminal state so BeamCore can renew grants,
promote recovered data and clean up staging. Drain active transfers before upgrading BeamCore and
SDK consumers.

## Provider transfer

`prepare_provider_transfer` resolves every source, asks BeamCore for a plan, creates multipart
uploads, signs every route and streams the routes to BeamCore. Sources and destinations may mix
providers.

```rust
use beam_network_sdk::{
    BeamClient, BeamClientOptions, ProviderDestinationConfig, ProviderSourceConfig,
    ProviderTransferCreateInput, R2ProviderDestination, S3ProviderSource, WaitForTransferOptions,
};

# async fn example() -> Result<(), beam_network_sdk::BeamApiError> {
let beam = BeamClient::new(BeamClientOptions {
    api_key: "b1m_your_key".to_string(),
    ..Default::default()
})?;

let source = S3ProviderSource {
    bucket: "source-bucket".into(),
    key: "exports/report.parquet".into(),
    region: Some("us-east-1".into()),
    access_key_id: "AKIA...".into(),
    secret_access_key: "...".into(),
    ..Default::default()
}
.create()?;
let destination = R2ProviderDestination {
    bucket: "destination-bucket".into(),
    key: "imports/report.parquet".into(),
    account_id: Some("cloudflare-account-id".into()),
    access_key_id: "...".into(),
    secret_access_key: "...".into(),
    ..Default::default()
}
.create()?;

let prepared = beam
    .prepare_provider_transfer(ProviderTransferCreateInput {
        sources: vec![ProviderSourceConfig::S3(source)],
        destinations: vec![ProviderDestinationConfig::R2(destination)],
        idempotency_key: Some("daily-report-2026-07-15".into()),
        ..Default::default()
    })
    .await?;

let status = beam
    .wait_for_transfer_with_options(&prepared.transfer_id, WaitForTransferOptions::default())
    .await?;
println!("{}", status.status);
beam.close().await?;
# Ok(())
# }
```

A complete program lives in [`examples/provider_transfer.rs`](examples/provider_transfer.rs).

| Provider | Config | Source | Destination |
| --- | --- | --- | --- |
| AWS S3 | `S3ProviderSource` / `S3ProviderDestination` | SigV4 presigned `GetObject` | multipart `UploadPart` |
| Cloudflare R2 | `R2ProviderSource` / `R2ProviderDestination` | same, region `auto` | same |
| Any S3 API store (MinIO, Wasabi, B2, ...) | `S3CompatibleProviderSource` / `S3CompatibleProviderDestination` | same | same |
| Hippius | `HippiusProviderSource` / `HippiusProviderDestination` | Hippius presigned URL | per-chunk `PUT` |
| Hugging Face Hub | `HuggingFaceProviderSource` / `HuggingFaceProviderDestination` | CDN redirect | the Hub's own LFS part URLs |
| GCS, Azure | `GCS*` / `Azure*` | described to BeamCore only; no signing adapter | same |

Every config has public fields, `Default`, and a validating `create()` (plus `validate()`), mirroring
the TypeScript SDK's `*ProviderConfig.create()`. S3-compatible configs require `endpoint_url`
(R2 accepts `account_id` instead); R2 and S3-compatible stores use path-style addressing unless
`force_path_style: Some(false)`, AWS S3 uses regional virtual hosting.

Signing is a hand-rolled AWS Signature Version 4 (`hmac` + `sha2`) validated against AWS's
published S3 and SigV4 test vectors; presigning is purely local and the SDK makes only a handful
of S3 control calls itself (create/abort/list/complete multipart and `HEAD`), each retried up to
five times on throttling and 5xx responses. An ignored test exercises every signed operation
against MinIO:

```sh
BEAM_MINIO_ENDPOINT=http://127.0.0.1:9000 cargo test -- --ignored minio
```

### Storage credentials

Source and destination credentials must not be restricted to specific IP addresses or networks.
When storage refuses access, `wait_for_transfer` returns `BeamApiError::StorageAccessDenied`
(`code` is `source_access_denied` or `destination_access_denied`).

### Failure, recovery and resume

- A recovery lease is registered before the route stream begins. A recoverable transport failure
  returns `BeamApiError::RouteRecoveryPending` while replay continues in the background; dropping
  the future also hands the transfer to background recovery.
- Any other failure cancels the transfer, aborts every multipart upload the call created (before
  cancelling when the stream never began) and releases the lease only after cleanup, returning
  `BeamApiError::ProviderTransfer { transfer_cancelled, multipart_cleanup_complete, cause, errors }`.
  Error messages never include signed URLs or credentials; `BeamApiError::safe_code()` gives a
  loggable code such as `SlowDown:status=503`.
- `cancellation: Some(CancellationToken)` fences ownership: cancelling stops signing, replay and
  the recovery signer (`BeamApiError::Aborted`) without cancelling the transfer, so a replacement
  owner can take over.
- `on_multipart_group_ready` receives each upload's durable `ProviderMultipartGroupIdentity`
  (no credentials or URLs) before its routes are streamed; an error aborts that upload and fails
  closed. `resume_provider_transfer` takes the full set of identities, reuses the uploads and
  never creates replacements (`provider_multipart_recovery_*` errors otherwise).
- `on_before_transfer_prepare`, `on_prepared` and `throw_if_cancelled` follow the TypeScript
  callbacks; a `throw_if_cancelled` error after prepare leaves the transfer to background
  recovery.
- Multipart recovery requests from BeamCore are signed locally with
  `provider_signing::sign_multipart_recovery`: staging objects live under
  `{final_object_key}.beam-recovery/{transfer_id}/{group}/{part}/{attempt}` and are promoted into
  the original upload with `UploadPartCopy` (bound to the staged ETag on AWS S3); renewals re-sign
  the complete, abort, final HEAD and every `ListParts` page of the original upload.
- While a transfer runs, the client answers Runtime's `transfer.route_recovery.sign` requests and
  integrity audit challenges (`transfer_status` reports a sanitized
  `integrity_audit_submission_error` if answering fails). S3 audit reads are bound to the planned
  source ETag/version and the delivered object's ETag with `If-Match`.

`prepare_hippius_provider_transfer` and `prepare_huggingface_provider_transfer` remain as thin
wrappers. Hugging Face uploads are committed by `wait_for_transfer` once the transfer completes
(or explicitly with `finalize_huggingface_uploads`); see
[`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md).

### Provider signing helpers

`beam_network_sdk::provider_signing` exposes the building blocks: `prepare_provider_source`,
`prepare_provider_source_for_plan`, `prepare_provider_destination`, `create_multipart_upload`,
`abort_multipart_upload`, `list_multipart_parts`, `complete_multipart_upload`,
`inspect_destination_object`, `sign_final_object_head`, `sign_complete_multipart_upload`,
`sign_abort_multipart_upload`, `sign_list_multipart_upload`, `sign_multipart_recovery`,
`sign_destination_route`, `sign_destination_url` (optional `content_md5`) and `sign_source_read_range` /
`sign_destination_read_range` (optional `if_match` and `version_id`, S3-compatible only). Network
helpers accept a `tokio_util::sync::CancellationToken` through `ProviderSigningOptions`.
`multipart_limits` holds the part-number layout (`multipart_part_number`: consecutive parts, one
attempt slot per chunk, at most 10,000 chunks per multipart destination).

## Raw lifecycle

```rust
use beam_network_sdk::{BeamClient, BeamClientOptions, TransferCreateRequest};

# async fn example(beam: BeamClient) -> Result<(), beam_network_sdk::BeamApiError> {
let created = beam
    .create_raw_transfer(TransferCreateRequest {
        idempotency_key: Some("daily-report-2026-07-15".into()),
        sources: vec![/* raw source configs */],
        destinations: vec![/* raw destination configs */],
        total_size: 104_857_600,
        ..Default::default()
    })
    .await?;
beam.distribute_transfer(&created.transfer_id).await?;
# Ok(())
# }
```

`plan_transfer`, `prepare_transfer_with_options` and `attach_signed_urls_with_options` cover the
manual signed-URL flow; `attach_signed_urls_with_options` requires a `ManualRouteRecovery` and
accepts `auto_distribute`. Idempotency follows the TypeScript SDK: the caller key derives the
transfer id, and lifecycle keys are `transfer:{id}:create` / `transfer:{id}:prepare` (with the
route generation derived from the prepare key).

## Transport

- `tls://` URLs start TLS before the NATS `INFO` exchange (the production gateway runs
  `handshake_first`); `nats://` is plaintext. NATS reconnects indefinitely.
- Auth tokens refresh 30 seconds before expiry. Each retry rebuilds the envelope with the current
  token under the same request id, and a `401 auth_token_expired` reply is retried with a fresh
  token. Other lifecycle failures surface as `BeamApiError::Lifecycle { status, code,
  message, body }`.
- Route batches hold at most 1,024 routes and target 4 MiB of encoded MessagePack (the full
  envelope with a reserved token allowance); a larger single route is sent alone if it fits
  `max_payload_bytes` (8 MiB by default) and fails before publication otherwise.
- `wait_for_transfer_with_options` subscribes to the terminal signal and backs the status poll off
  by 1.5x from `poll_interval` (15 s) up to `max_poll_interval` (30 s). `close()` stops recovery,
  drains the connection and drops retained provider state.

## Changes in 0.4.0

See [`CHANGELOG.md`](CHANGELOG.md). Breaking changes:

- `BeamApiError` moved to its own module, gained variants (`Lifecycle`, `InvalidArgument`,
  `ProviderRequest`, `ProviderTransfer`, `Aborted`, `Multiple`) and is `#[non_exhaustive]`.
  Lifecycle failures are `Lifecycle` instead of `HttpStatus`.
- `ProviderSourceConfig` / `ProviderDestinationConfig` gained `S3Compatible` variants.
- Public structs gained fields (`BeamClientOptions`, `TransferCreateRequest`,
  `TransferStatusInfo`, `TransferCreateResponse`, integrity challenge types); build them with
  `..Default::default()` where available.
- `create_transfer` is deprecated in favour of `create_raw_transfer` (raw configs) and
  `create_provider_transfer` / `prepare_provider_transfer` (provider signing).
- Lifecycle idempotency keys no longer reuse the caller's key directly (see above), and the
  Hippius destination no longer sends `mode: "http_chunks"`.
- `TransferTerminalSignalWaiter::wait` returns `Ok(None)` after `close()` and rejects a zero
  timeout.

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
