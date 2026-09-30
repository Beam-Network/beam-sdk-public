# BEAM SDK for Go

Install:

```bash
go get github.com/Beam-Network/beam-sdk-public/sdks/go
```

Example:

```go
package main

import (
	"context"
	"fmt"

	beam "github.com/Beam-Network/beam-sdk-public/sdks/go"
)

func main() {
	ctx := context.Background()
	client, err := beam.New(beam.WithAPIKey("b1m_your_key"))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	source, err := beam.NewR2ProviderSource(beam.R2ProviderSource{
		Bucket:          "source-bucket",
		Key:             "exports/report.parquet",
		AccountID:       "cloudflare-account-id",
		AccessKeyID:     "r2-access-key",
		SecretAccessKey: "r2-secret-key",
	})
	if err != nil {
		panic(err)
	}
	destination, err := beam.NewS3ProviderDestination(beam.S3ProviderDestination{
		Bucket:          "destination-bucket",
		Key:             "imports/report.parquet",
		Region:          "us-east-1",
		AccessKeyID:     "aws-access-key",
		SecretAccessKey: "aws-secret-key",
	})
	if err != nil {
		panic(err)
	}

	transfer, err := client.CreateProviderTransfer(ctx, beam.ProviderTransferOptions{
		Sources:      []beam.ProviderSource{source},
		Destinations: []beam.ProviderDestination{destination},
		Name:         "r2-to-s3-report",
	})
	if err != nil {
		panic(err)
	}

	status, err := client.WaitForTransfer(ctx, transfer.TransferID, 0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(status.Status)
}
```

The provider-aware API covers S3, R2, S3-compatible, Hippius, and Hugging Face configs. GCS and Azure models exist for configuration and raw transfers, but provider signing for them is not implemented; passing them to a provider transfer fails before anything is created.

## Storage Credentials

Source and destination credentials must not be restricted to specific IP addresses or networks (for example Cloudflare R2 API-token client IP filtering, S3 bucket policies with `aws:SourceIp`, or VPC-only endpoints). Beam moves data through many workers on different networks, so restricted credentials make the transfer fail.

## Failed Transfers

`WaitForTransfer` returns `*TransferFailedError` for a failed transfer; its `ErrorMessage` is BeamCore's `error_message` verbatim. When the source or destination storage refused Beam's requests it returns `*StorageAccessError`, whose `Code` is `StorageAccessSourceDenied` (`source_access_denied`) or `StorageAccessDestinationDenied` (`destination_access_denied`). `errors.As` matches a `*StorageAccessError` as a `*TransferFailedError` too:

```go
status, err := client.WaitForTransfer(ctx, transfer.TransferID, 0, 0)
var storageErr *beam.StorageAccessError
if errors.As(err, &storageErr) {
	// For example: "destination_access_denied: The destination storage refused Beam's requests (403 AccessDenied). ..."
	log.Fatal(storageErr.Code, ": ", storageErr.ErrorMessage)
}
```

`NewTransferFailedError(transferID, errorMessage)` gives the same classification for a status you polled yourself.

## Client Configuration

`New` validates configuration like the TypeScript constructor: the API key is required, the lifecycle URL must be `nats://` or `tls://`, and numeric options must be positive. `NewClient` keeps its original lenient behaviour (invalid option values are ignored and a missing API key fails at the first request).

Configuration falls back to the environment: `BEAM_API_KEY`, `BEAM_NATS_URL`, `BEAM_ENV` (`prod` by default; other environments use `BEAM_DEV_NATS_URL` or `nats://127.0.0.1:4222`), and `TRANSFER_RUNTIME_SHARD_COUNT`.

| Option | Default |
| --- | --- |
| `WithAPIKey`, `WithNATSURL`, `WithEnvironment` | environment |
| `WithHTTPClient` | `http.DefaultClient` (Hippius and Hugging Face calls) |
| `WithRouteSigningConcurrency` | 64, adaptive up to 256 unless set |
| `WithMultipartControlConcurrency` | `DefaultMultipartControlConcurrency` (2) |
| `WithMaxPayloadBytes` | 8 MiB |
| `WithSubjectPrefix` | `beam.transfer.client` |
| `WithTransferRuntimeShardCount` | `TRANSFER_RUNTIME_SHARD_COUNT` or 1 |
| `WithRequestTimeout` | 30s per lifecycle attempt |

The production gateway `tls://orch-gateway.b1m.ai:4222` performs the TLS handshake before sending NATS `INFO`. For every `tls://` URL the client connects with TLS handshake-first and presents the URL hostname for SNI and certificate verification.

## S3-Compatible Providers

Use `S3CompatibleProviderSource` and `S3CompatibleProviderDestination` for S3-compatible providers with custom endpoints, such as Wasabi, MinIO, Backblaze B2 S3 API, DigitalOcean Spaces, Scaleway Object Storage, Hippius S3, and self-hosted S3-compatible systems.

```go
source, err := beam.NewS3CompatibleProviderSource(beam.S3CompatibleProviderSource{
	Provider:        "wasabi",
	Bucket:          "my-bucket",
	Key:             "input/file.bin",
	Region:          "us-east-1",
	EndpointURL:     "https://s3.us-east-1.wasabisys.com",
	AccessKeyID:     os.Getenv("WASABI_ACCESS_KEY_ID"),
	SecretAccessKey: os.Getenv("WASABI_SECRET_ACCESS_KEY"),
})
destination, err := beam.NewS3CompatibleProviderDestination(beam.S3CompatibleProviderDestination{
	Provider:        "minio",
	Bucket:          "archive",
	Key:             "file.bin",
	EndpointURL:     "https://minio.example.com",
	ForcePathStyle:  beam.Bool(true),
	AccessKeyID:     os.Getenv("MINIO_ACCESS_KEY_ID"),
	SecretAccessKey: os.Getenv("MINIO_SECRET_ACCESS_KEY"),
})
```

Endpoint, region, and addressing resolve like the TypeScript `s3Compatible*` helpers (exported as `S3CompatibleEndpoint`, `S3CompatibleRegion`, and `S3CompatibleForcePathStyle`):

- An explicit `EndpointURL` wins; R2 derives `https://<account>.r2.cloudflarestorage.com` from `AccountID`; AWS S3 uses the SDK endpoint; any other provider requires `EndpointURL`.
- The region defaults to `auto` for R2 and `us-east-1` otherwise.
- `ForcePathStyle` wins when set. AWS S3 otherwise keeps the SDK default (virtual-hosted, or path style for IP endpoints); every other provider uses path style whenever an endpoint is set. `S3ProviderSource`/`S3ProviderDestination` also accept `ForcePathStyle`, so set it for MinIO-style endpoints configured through the S3 types.

The same addressing is used for SDK-signed operations and for the SDK's own presigned CompleteMultipartUpload, AbortMultipartUpload, and ListParts URLs. S3 clients are cached per configuration and make up to five attempts for transient provider failures.

Every provider has a validating constructor (`NewS3ProviderSource`, `NewR2ProviderDestination`, `NewS3CompatibleProviderSource`, `NewHippiusProviderSource`, `NewHuggingFaceProviderDestination`, and so on) matching the TypeScript `create` helpers, including Hugging Face `repo_id` and `repo_type` checks. Struct literals keep working without them.

## Hugging Face Hub

Use `HuggingFaceProviderSource` to pull a file out of a Hub repo or `HuggingFaceProviderDestination` to push one into one. The token stays in this process: a source resolves to the presigned CDN URL the Hub redirects to, and a destination uses the presigned part URLs the Hub's LFS batch endpoint issues.

```go
transfer, err := client.CreateProviderTransfer(ctx, beam.ProviderTransferOptions{
	Sources: []beam.ProviderSource{beam.HuggingFaceProviderSource{
		RepoID:   "org/dataset",
		Path:     "data/train.parquet",
		RepoType: "dataset",
		Token:    os.Getenv("HF_TOKEN"),
	}},
	Destinations: []beam.ProviderDestination{destination},
	Name:         "Hugging Face pull",
})
// WaitForTransfer commits any Hugging Face destination once the parts land.
_, err = client.WaitForTransfer(ctx, transfer.TransferID, 0, 0)
```

Constraints, all of them the Hub's rather than Beam's; see [`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md):

- Sources are large-file (LFS or Xet) content. Hub buckets work as sources but cannot be written to.
- A destination needs the object's sha256 up front. When the source is not itself a Hub file, set `AllowSourceRehash: true` to let the SDK read the source once.
- One source per Hugging Face destination, because the Hub dictates the part size.
- If you do not call `WaitForTransfer`, call `FinalizeHuggingFaceUploads(ctx, transferID)` once the transfer completes; without it no commit is made.

Route replay after a Runtime epoch change reuses the negotiated Hub plan; it never re-hashes the source or re-requests upload URLs.

## Provider Transfer Options

`CreateProviderTransfer` (the TypeScript `createTransfer`) and `PrepareProviderTransferWithOptions` take `ProviderTransferOptions`: `Sources`, `Destinations`, `Name`, `ExpiresIn` (1 hour), `Distribute` (true when nil), `ChunkSize`, `IdempotencyKey`, `RouteGenerationID`, `Ownership`, and the callbacks below. The positional `PrepareProviderTransfer` remains and behaves as before.

- `OnBeforeTransferPrepare` runs after sources are signed and before `transfer.prepare`.
- `OnPrepared` runs after prepare and before routes stream, including during resume.
- `OnMultipartGroupReady` runs after the SDK creates a multipart upload and before that group's routes stream. Its `ProviderMultipartGroupIdentity` carries transfer, group, source, destination, object key, upload id, size, part count, and expiry only; never credentials, headers, or signed URLs. An error aborts the new upload and fails the transfer closed.
- `ThrowIfCancelled` is polled before prepare and before each planned chunk of the initial stream.

When `OnPrepared` or `ThrowIfCancelled` fails, or the foreground `ctx` is cancelled or times out after prepare, the call returns but the recovery lease and retained multipart uploads stay active and background recovery continues.

`Ownership` is an ownership fence, separate from the foreground `ctx`. Cancelling it (for example with `context.WithCancelCause`) stops initial routes, multipart creation, route replay, and route recovery signing, and releases the lease; it does not cancel the transfer or abort a replacement owner's uploads. A fenced owner releases only its own lease and signers, never a replacement owner's on the same `Client`, and it sends no further `transfer.resume` or route replay. Operations then return `context.Cause(Ownership)`. User cancellation still goes through `CancelTransfer`.

## Route Streaming And Payload Size

Provider transfers use `transfer-client-control/v7`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. S3, R2, and S3-compatible destinations use direct multipart UploadPart/ListParts/HEAD handling with consecutive part numbers: source chunk N is part N + 1, every retry reuses that part, and each group manifest's `max_part_number` equals its `expected_part_count`. Hippius and Hugging Face destinations take a plain PUT per chunk, so they carry no multipart group manifest, and their routes carry only per-attempt metadata (Hugging Face routes omit `part_number`).

The client signs up to 64 routes concurrently by default and emits 1,024-route logical batches. Encoded MessagePack requests target 4 MiB physical batches under `maxPayloadBytes` (8 MiB by default). The splitter reserves control-envelope headroom for the live auth token and request identity so the encoded request stays below the guard. A single route above the 4 MiB target but within the guard publishes alone; one larger than the guard fails before publication with both sizes in the error. Each multipart group manifest is published as its own request. Multipart create and abort requests use `WithMultipartControlConcurrency` (2 by default), independent from route signing.

Lifecycle requests retry transient failures up to three times with the same request identity and payload. Cached SDK auth refreshes 30 seconds before expiry, and an `auth_token_expired` reply is retried with a fresh token and the same request identity. Lifecycle idempotency keys derive from the transfer id (`transfer:<id>:create`, `transfer:<id>:prepare`), and the route generation derives from the prepare key. Non-success replies surface as `*LifecycleRequestError` with `Status`, `Code`, `Message`, and `Body`.

A non-recoverable provider setup or route failure cancels the transfer, aborts the multipart uploads this owner created, and returns `*ProviderTransferError`. Its `TransferCancelled` and `MultipartCleanupComplete` fields report the outcome, `Errors` (via `Unwrap() []error`) keeps the original, cancellation, and cleanup failures, and its message never echoes provider errors. The recovery lease, whose credentials the cleanup needs, is released only after cleanup finishes.

`WaitForTransfer` subscribes to the API-key-owned, at-most-once terminal signal before its first status read and reconciles every signal through authoritative status; subscription failure degrades to a jittered 15-to-30-second status fallback. `WaitForTransferWithOptions` accepts `Timeout`, `PollInterval`, and `MaxPollInterval` (negative values are rejected). On completion it commits Hugging Face uploads before returning. Always call `Close`; it also closes terminal waiters and recovery signers.

During the pre-completion `integrity_check` phase, Runtime may include an `integrity_audit_challenge` in status. The client signs exact read-only source and final-destination GET ranges and submits `transfer.integrity_audit_grants`. S3-compatible sources report `driver`, `endpoint_url`, `content_length`, `etag`, `last_modified`, and `version_id`, so source grants are pinned with `If-Match` and `VersionId`, and destination grants use the Runtime-verified final-object ETag as `If-Match`. Concurrent polls share one submission per audit. Failures retry on the next status poll and appear as a URL- and credential-safe `IntegrityAuditSubmissionError` on that status.

NATS enforces `max_payload`, so this guard applies to lifecycle metadata only; transfer file bytes never flow through NATS.

## Restart Recovery

The client keeps one in-memory recovery lease per active transfer and one `runtime.hello` monitor (every 5 seconds) per active shard. The lease is installed before route-stream begin, and a per-transfer lock serializes the initial stream with replay. Replay reuses the prepared plan and existing multipart upload ids and re-signs only the expiring route and group controls. A Runtime epoch change invalidates cached auth, coalesces one `transfer.resume`, and replays routes under a fresh generation when Runtime reports `route_replay_required`. Provider credentials are retained only in memory and released on terminal status, cancellation, ownership loss, or `Close`. `AttachSignedURLs` requires a `ManualRouteRecovery` factory and the plan fingerprint/checksum; signed URLs are never journaled to disk.

While a provider transfer is active the client also answers Runtime's route recovery signing requests on `<prefix>.<env>.sdk.<key_prefix>.transfer.<id>.route_recovery_sign`. It validates each request against the plan (attempt slot 0 and part number `source_chunk_index + 1`), re-signs the route, and rebuilds multipart group controls from the existing upload id; it never creates an upload. When Runtime attaches a multipart `recovery` directive to an S3, R2, or S3-compatible route, the signer applies it to the signed route like the TypeScript `signMultipartRecovery`: a `staged` attempt uploads to its own staging object under `<final key>.beam-recovery/<transfer>/<group>/<part>/<attempt>` and receives presigned HEAD, DELETE, and UploadPartCopy grants (with `x-amz-copy-source-if-match` on AWS S3 when Runtime supplies an ETag) that copy it into the original upload; `list` and `delete` sign the staging listing and cleanup; `renew` re-signs the group's complete, abort, final HEAD, and ListParts controls. Hippius and Hugging Face routes keep their per-chunk plain PUT.

`ResumeProviderTransfer` re-attaches after a process restart. Pass the complete identities saved from `OnMultipartGroupReady` in `MultipartGroups`. Each call re-prepares the transfer by explicit id with a fresh request (`transfer:<id>:prepare:resume:<uuid>`) and route generation, validates every transfer/source/destination/object/size/part coordinate, and streams routes that reuse those upload ids. Missing, duplicate, or changed identities fail with a `provider_multipart_recovery_*` error before any upload or route is created. A nil `MultipartGroups` is the same as an empty list, so plans whose destinations are all Hippius or Hugging Face resume without identities. If a process died before recording an identity, treat it as uncertain cleanup rather than inventing a replacement.

## Planning

`PlanTransfer` sends `transfer.plan` and returns the compact plan BeamCore would use, without creating a transfer. Build its sources with `PrepareProviderSourceForPlan`, which uses metadata requests only.

## Hybrid Endpoint Signing

Credential adapters can sign single provider operations with the same configs, cached clients, and addressing as the transfer flow. Every helper takes a `context.Context`; cancellation stops waiting and network activity but cannot undo a provider operation already accepted.

- `PrepareProviderSource`, `PrepareProviderSourceForPlan`, `PrepareProviderDestination`.
- `SignDestinationURL` signs an upload URL without a source descriptor and reads no source data. `ContentMD5` binds the worker-computed checksum into the signature (S3-compatible only).
- `SignDestinationRoute` signs a full worker route.
- `SignSourceReadRange` accepts `IfMatch` and `VersionID` for frozen S3-compatible sources; replay the returned headers unchanged. Other providers reject these conditions instead of dropping them. `SignDestinationReadRange` accepts `IfMatch`.
- `CreateMultipartUpload`, `AbortMultipartUpload`, `SignFinalObjectHead`, `SignCompleteMultipartUpload`, `SignAbortMultipartUpload`, and `SignListMultipartUpload`.
- `ListMultipartParts` walks every page, `CompleteMultipartUpload` submits provider-verified parts in order, and `InspectDestinationObject` HEADs the final object. They return metadata only. Retain the upload and verified manifest durably before completing so restart recovery cannot infer delivery from size alone; after an interrupted completion, inspect the object before retrying.

The multipart limits are exported as `MultipartMaxPartNumber` (10,000), `MultipartAttemptSlotCount` (1), `MultipartMaxSourceChunks` (10,000), and `MultipartPartNumber(chunkIndex, attemptSlot)`, which returns `chunkIndex + 1` and rejects any slot other than 0.

## Breaking And Behavior Changes

- **Breaking:** the SDK speaks `transfer-client-control/v7`. Multipart destinations use consecutive part numbers (`MultipartAttemptSlotCount` is 1, `MultipartMaxSourceChunks` is 10,000, and `MultipartPartNumber` rejects any slot other than 0); a retried chunk reuses its part, and Runtime-directed staged recovery copies a new attempt into the original upload with UploadPartCopy.
- **Breaking:** `SignedChunkRoute.DeliveryIndex` changed from `int` to `*int` so delivery index 0 is distinguishable from unset. Code that sets or reads it must take or dereference a pointer.
- Options now record invalid values instead of dropping them silently. `NewClient` still ignores the recorded errors and keeps defaults; the new `New` constructor returns them (joined) and fails.
- `Close` also stops every route recovery and integrity audit signer, and releases the retained provider credentials.
- A non-recoverable `PrepareProviderTransfer` failure now cancels the transfer, aborts the multipart uploads it created, and returns `*ProviderTransferError`. Its message no longer contains provider or lifecycle error text (which can carry signed URLs); use `errors.As`/`errors.Is` on the error, whose `Unwrap() []error` exposes the original failure.
- A failed transfer makes `WaitForTransfer` return `*TransferFailedError` (or `*StorageAccessError`) instead of an untyped error; the text is now `transfer <id> failed: <error_message>`.
- A rejected first NATS connect (for example `nats: authorization violation` for an invalid API key) fails immediately instead of retrying; reconnects after a successful connect stay unbounded.

## Differences From The TypeScript SDK

- `CreateTransfer` is the raw transfer in Go; `CreateRawTransfer` is its TypeScript-named alias, and it still derives a default chunk size. The TypeScript provider-aware `createTransfer` is `CreateProviderTransfer`.
- Optional TypeScript inputs are options structs: `ProviderTransferOptions`, `ProviderTransferResumeOptions`, `TransferPrepareRequest` (`PrepareTransferWithRequest`, which also accepts an explicit `TransferID`), `TransferPlanRequest`, and `WaitForTransferOptions`. The positional methods remain.
- The TypeScript `signal` is `ProviderTransferOptions.Ownership`; the foreground `ctx` keeps continue-recovery semantics.
- Callbacks return `error` and receive the foreground `ctx`.
- `SignedChunkRoute.DeliveryIndex` is `*int`, so delivery index 0 is representable; a numeric `metadata["delivery_index"]` still takes precedence.
- Go keeps GCS and Azure models and environment-based configuration; the TypeScript SDK has neither.

### Preparation diagnostics

Route streaming flushes at 1,024 routes, with no timer, and sends the final partial batch at stream completion. Signing overlaps acknowledged publication through bounded buffering; existing encoded-payload limits still apply. A source grant is reused across destinations only within the same signing generation and actual expiry.

The optional `WithDiagnostics` callback receives bounded preparation measurements and source-grant reuse counts. Callbacks are best effort, may be dropped under load, and do not affect transfer outcomes. Durations may overlap; do not sum them into elapsed transfer time. Transfer status also exposes a typed `performance` summary when supported by Core.

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
