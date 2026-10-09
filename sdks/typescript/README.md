# BEAM SDK for TypeScript

This release requires Core's `transfer-client-control/v7` contract. Multipart uploads use consecutive parts;
Core may request retained staging for recovery through the existing SDK signing connection. Workers receive
ordinary upload URLs. Keep the client connected until the transfer reaches a terminal state so Core can renew
grants, promote recovered data, and clean up staging. Drain active transfers before upgrading Core and SDK consumers.

Install:

```bash
npm install @beam-network/sdk
```

Example:

```ts
import { BeamClient, R2ProviderConfig, S3ProviderConfig } from "@beam-network/sdk";

const beam = new BeamClient({
  apiKey: "b1m_your_key"
});

const transfer = await beam.createTransfer({
  sources: [
    R2ProviderConfig.create({
      bucket: "source-bucket",
      key: "exports/report.parquet",
      account_id: "cloudflare-account-id",
      access_key_id: "r2-access-key",
      secret_access_key: "r2-secret-key"
    })
  ],
  destinations: [
    S3ProviderConfig.create({
      bucket: "destination-bucket",
      key: "imports/report.parquet",
      region: "us-east-1",
      access_key_id: "aws-access-key",
      secret_access_key: "aws-secret-key"
    })
  ],
  name: "r2-to-s3-report"
});

const status = await beam.waitForTransfer(transfer.transfer_id);
console.log(status.status);
await beam.close();
```

The main `createTransfer` API is provider-aware and strictly typed for S3, R2, S3-compatible, Hippius, and Hugging Face configs.

## Storage Credentials

Source and destination credentials must not be restricted to specific IP addresses or networks. When storage refuses access, `waitForTransfer` rejects with `BeamStorageAccessError` (`code` is `source_access_denied` or `destination_access_denied`).

## S3-Compatible Providers

Use `S3CompatibleProviderConfig` for S3-compatible providers with custom
endpoints, such as Wasabi, MinIO, Backblaze B2 S3 API, DigitalOcean Spaces,
Scaleway Object Storage, and self-hosted S3-compatible systems.

```ts
import {
  BeamClient,
  S3CompatibleProviderConfig
} from "@beam-network/sdk";

const beam = new BeamClient({ apiKey: process.env.BEAM_API_KEY! });

await beam.prepareProviderTransfer({
  sources: [
    S3CompatibleProviderConfig.create({
      provider: "wasabi",
      bucket: "my-bucket",
      key: "input/file.bin",
      region: "us-east-1",
      endpoint_url: "https://s3.us-east-1.wasabisys.com",
      access_key_id: process.env.WASABI_ACCESS_KEY_ID!,
      secret_access_key: process.env.WASABI_SECRET_ACCESS_KEY!
    })
  ],
  destinations: [
    S3CompatibleProviderConfig.create({
      provider: "minio",
      bucket: "archive",
      key: "file.bin",
      endpoint_url: "https://minio.example.com",
      force_path_style: true,
      access_key_id: process.env.MINIO_ACCESS_KEY_ID!,
      secret_access_key: process.env.MINIO_SECRET_ACCESS_KEY!
    })
  ],
  name: "S3-compatible transfer"
});
```

Use `S3ProviderConfig` for ordinary AWS S3 if preferred, and keep using
`R2ProviderConfig` for existing Cloudflare R2 code. Use
`S3CompatibleProviderConfig` when a provider needs a custom S3-compatible
endpoint.

## Hugging Face Hub

Use `HuggingFaceProviderConfig` to pull a file out of a Hub repo or push one into
one. The token stays in this process: a source resolves to the presigned CDN URL
the Hub redirects to, and a destination uses the presigned part URLs the Hub's
LFS batch endpoint issues.

```ts
import { BeamClient, HuggingFaceProviderConfig, S3ProviderConfig } from "@beam-network/sdk";

const beam = new BeamClient({ apiKey: process.env.BEAM_API_KEY! });

const transfer = await beam.prepareProviderTransfer({
  sources: [
    HuggingFaceProviderConfig.create({
      repo_id: "org/dataset",
      path: "data/train.parquet",
      repo_type: "dataset",
      token: process.env.HF_TOKEN!
    })
  ],
  destinations: [
    S3ProviderConfig.create({
      bucket: "archive",
      key: "datasets/train.parquet",
      access_key_id: process.env.AWS_ACCESS_KEY_ID!,
      secret_access_key: process.env.AWS_SECRET_ACCESS_KEY!
    })
  ],
  name: "Hugging Face pull"
});

// waitForTransfer commits any Hugging Face destination once the parts land.
await beam.waitForTransfer(transfer.transfer_id);
```

Constraints, all of them the Hub's rather than Beam's — see
[`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md) for the full protocol:

- **Sources are large-file (LFS or Xet) content.** Only those get the
  credential-free CDN redirect; a small regular file is served inline and is
  rejected with that reason. Hub **buckets** work as sources too, but cannot be
  written to — they expose no LFS endpoint.
- **A destination needs the object's sha256 up front**, because the LFS batch
  endpoint will not issue upload URLs without it. When the source is not itself a
  Hub file, set `allow_source_rehash: true` to let the SDK read the source once to
  compute it.
- **One source per Hugging Face destination**, because the Hub dictates the part
  size and a plan carries a single chunk size.
- If you do not call `waitForTransfer`, call `finalizeHuggingFaceUploads(transferId)`
  yourself once the transfer completes — without it the parts are uploaded but no
  commit is made and the file does not appear in the repo.

For low-level raw transfer configs, use `createRawTransfer`; lifecycle transport still goes through NATS.

## Route Streaming And Payload Size

Provider transfers use `transfer-client-control/v7`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. S3, R2, and S3-compatible destinations retain direct multipart UploadPart/ListParts/HEAD handling. Hippius and Hugging Face destinations take a plain PUT per chunk instead, so they carry no multipart group manifest.

The client signs up to 64 routes concurrently by default and emits 1,024-route logical batches. Encoded MessagePack requests target 4 MiB physical batches under `maxPayloadBytes` (8 MiB by default; explicit positive overrides remain supported). The splitter reserves control-envelope headroom for the live auth token and stable request identity so the final encoded request remains below the configured guard. Lifecycle control refreshes cached SDK auth before the final 30 seconds of token lifetime and retries `auth_token_expired` replies with the same request identity and payload but a fresh token, preserving Core request-conflict protection during large route streams. A single route above the 4 MiB target but within the configured guard publishes alone; a single route larger than the configured limit fails before publication with both sizes in the error. Multipart create and abort requests use a separate `multipartControlConcurrency` limit of 2 by default, with explicit positive overrides supported, and S3-compatible control requests make up to five attempts for transient provider failures. This keeps URL signing throughput independent from provider control-plane pressure.

Callers that need durable recovery accounting can provide `onMultipartGroupReady`. The SDK invokes it after creating a multipart upload and before streaming that group’s signed routes. Its payload contains transfer, source, destination, object-key, upload, size, part-count, and expiry identities only; it never includes credentials, request headers, or signed URLs. A rejected callback aborts the newly created upload and fails preparation closed.

`resumeProviderTransfer` requires the complete saved callback payloads in `multipartGroups`. It validates every transfer/source/destination/object/size/part coordinate, then uses the same route-stream implementation as creation with those upload IDs. Missing, duplicate, or changed identities fail before new provider uploads or route publication. If a process died before recording an upload identity, treat it as uncertain cleanup instead of inventing a replacement. The `onPrepared` callback also runs during resume, before routes are streamed.

Create and resume accept an ownership `signal`. Aborting it stops initial routes, multipart creation, and recovery signing/replay; it does not cancel the transfer or abort a replacement owner's uploads. Recovery leases, signers, and background resume/replay are keyed by owner identity: a fenced-off owner never replays into, releases, or stops the signers of a replacement owner registered for the same transfer, and a replacement always gets its own recovery. User cancellation still uses `cancelTransfer` and verified provider cleanup. `prepareProviderSource` accepts the same signal for metadata requests.

The stream ID derives from the transfer, selected flow, and immutable plan identity, and each ordered batch ID includes its route-coordinate checksum. Manual multi-destination attachment requires `delivery_index` on every route. Lifecycle mutations retry transient failures up to three times with the same request identity. A non-recoverable provider setup or route failure raises `BeamProviderTransferError`, whose `errors`, `transferCancelled`, and `multipartCleanupComplete` fields retain the original provider, cancellation, and cleanup outcomes. `waitForTransfer` subscribes to the API-key-owned, at-most-once terminal signal before its first status read and reconciles every signal through authoritative status; subscription failure degrades to the same jittered 15-to-30-second status fallback. Call `close()` when the client is no longer needed; it also closes outstanding terminal waiters.

During the pre-completion `integrity_check` phase, Runtime may include an `integrity_audit_challenge` in status. The TypeScript client signs exact read-only source and final-destination GET ranges and submits `transfer.integrity_audit_grants`. S3-compatible source and destination grants use the prepared source ETag and Runtime-verified final-object ETag as `If-Match` conditions. Submission failures retry on the next status poll and appear as a URL-safe `integrity_audit_submission_error` on that status response; the transfer completes with a warning if the check cannot finish by its deadline.

Hippius uses canonical non-multipart `signed_url` routes (initial and recovered routes both PUT to the planned `<final>/<plan_nonce>/chunk-NNNNNN` key), so its manifest is empty and group-level final HEAD verification is not available from that provider flow.

NATS requires this guard because the broker rejects messages above its configured `max_payload`. This limit applies only to lifecycle/control metadata; transfer file bytes do not flow through NATS.

## Restart Recovery

The client keeps one in-memory recovery lease per active transfer and one `runtime.hello` monitor per active shard. The lease is installed before route-stream begin, and a per-transfer lock coalesces initial streaming with replay. Multipart recovery retains the existing upload IDs and compact group state, then re-signs only the expiring route and commit controls. Each `resumeProviderTransfer` invocation re-prepares an existing transfer by explicit transfer id with a fresh request and route generation, while transport retries within that invocation reuse the same request. It then reattaches route-recovery signing from the current provider configs without creating duplicate multipart uploads. A Runtime epoch change invalidates cached auth, coalesces one `transfer.resume`, and regenerates routes under a fresh generation; a transport-only epoch change replays only when Runtime reports routes missing or expired. Provider credentials and signing inputs are retained only in memory and released on terminal status or `close()`. Low-level `attachSignedUrls` requires `routeGenerationId`, the plan fingerprint/checksum, and a `recoveryFactory`; signed URLs are never journaled to disk. Foreground cancellation, deadline expiry, and Runtime state-loss responses keep the background lease and retained multipart upload alive.

## Hybrid endpoint signing

Credential adapters can use `signDestinationUrl` independently of `signDestinationRoute`, using the same provider configuration and multipart signer. Destination-only signing requires no synthetic source descriptor and reads no source data. Optional `contentMd5` binds the worker-computed checksum to the provider upload without a separate source checksum scan.

`signSourceReadRange` accepts `ifMatch` and `versionId` for frozen S3-compatible sources. Replay the returned headers unchanged. The ETag condition is signed and the version is included in the signed request. Unsupported non-S3 credential modes reject these conditions instead of dropping them. These helpers support the existing S3-compatible profiles used for R2, Hippius S3 and Hugging Face Storage Buckets; Hub repository tokens are a different provider mode.

Only short-lived range/upload routes go to workers. Multipart creation, verification, completion and abort remain in the existing control path. A worker upload response alone is not final-object completion.

`listMultipartParts`, `completeMultipartUpload`, and `inspectDestinationObject` reuse the same provider client and return metadata only. ListParts walks every page; completion submits the selected provider-verified parts. Adapters must durably retain the upload and verified manifest before completion so restart recovery cannot infer delivery from size alone.

Manual releases from `dev` require a committed `-dev.N` version and publish only to npm tag `dev`; the stable tag is unchanged.

Multipart control helpers (`createMultipartUpload`, `listMultipartParts`, `completeMultipartUpload`, `inspectDestinationObject`, and `abortMultipartUpload`) accept an optional `AbortSignal` and cancel through the existing provider HTTP transport. Pass it as `signal` in object arguments or as the last positional argument. Cancellation stops waiting and network activity; it cannot undo a provider operation already accepted. After an interrupted completion, inspect the durable upload/object identity before retrying or reporting cleanup complete.

### Preparation diagnostics

Route streaming flushes at 1,024 routes, with no timer, and sends the final partial batch at stream completion. Signing overlaps acknowledged publication through bounded buffering; existing encoded-payload limits still apply. A source grant is reused across destinations only within the same signing generation and actual expiry.

The optional `onDiagnostics` callback receives bounded preparation measurements and source-grant reuse counts. Callbacks are best effort, may be dropped under load, and do not affect transfer outcomes. Durations may overlap; do not sum them into elapsed transfer time. Transfer status also exposes a typed `performance` summary when supported by Core.

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
