# Changelog

## 0.7.4

- Worker source reads are pinned to the source object version captured at prepare.

## 0.7.3

- Packaging: no library changes.

## 0.7.2

- Docs: minimal storage-credential text.

## 0.7.1

- Docs: shorter storage-credential guidance.

## 0.7.0

### Removed

- `chunk_size` on every transfer input; Beam chooses the chunk size.

## 0.6.0

### Added

- `BeamApiError::StorageAccessDenied`, `StorageAccessCode` and `BeamApiError::transfer_failed`.
- Docs: storage credentials must not be restricted to specific IP addresses or networks.

## 0.4.0

Feature parity with the TypeScript SDK on `transfer-client-control/v7`.

### Added

- Multipart recovery signing: `provider_signing::sign_multipart_recovery` (with
  `MultipartRecoverySignInput`, `MultipartRecoveryRequest`, `MultipartRecoveryOperation` and
  `MultipartRecoveryMode`) answers BeamCore's `recovery` requests on
  `transfer.route_recovery.sign` chunks: staged `PutObject`, HEAD, `UploadPartCopy` (signed
  `x-amz-copy-source` headers, `x-amz-copy-source-if-match` on AWS S3 only) and `DeleteObject`
  grants, `ListObjectsV2` pages of the staging prefix, and renewal of the complete, abort, final
  HEAD and every `ListParts` URL of the original upload. The provider recovery signer applies it
  to every re-signed route, as the TypeScript SDK does.
- `etag_required` is kept in compact route attempt metadata and signed part route metadata.
- Generic provider flow: `prepare_provider_transfer` / `create_provider_transfer` with mixed
  sources and destinations, multipart manifests (`multipart_control_concurrency`, default 2),
  callbacks (`on_before_transfer_prepare`, `on_prepared`, `on_multipart_group_ready`,
  `throw_if_cancelled`), an ownership `CancellationToken`, background recovery and the
  `transfer.route_recovery.sign` responder.
- `resume_provider_transfer` with multipart identity validation.
- S3, R2 and S3-compatible signing (SigV4, validated against AWS test vectors) and the
  `provider_signing` module helpers; `S3CompatibleProviderSource` / `S3CompatibleProviderDestination`.
- `plan_transfer`, `create_raw_transfer`, `prepare_transfer_with_options`,
  `attach_signed_urls_with_options` (`auto_distribute`), `wait_for_transfer_with_options`
  (`max_poll_interval`).
- Validating `create()` / `validate()` for every provider config.
- `multipart_limits` module, `TransferPlanResponse`, `PlanningHttpSource`,
  `ProviderMultipartGroupIdentity`, `SignedUrlFlow`, `TransferCreateResponse.transfer_key` and
  related fields, `TransferStatusInfo.name` and `integrity_audit_submission_error`.
- `BeamApiError::{Lifecycle, InvalidArgument, ProviderRequest, ProviderTransfer, Aborted,
  Multiple}` and `BeamApiError::safe_code()`.

### Fixed

- `tls://` connections start TLS before `INFO` (required by the production gateway).
- Auth tokens refresh 30 s before expiry; retries rebuild the envelope with a fresh token and
  retry `auth_token_expired` under the same request id.
- Route batches target 4 MiB with a full-envelope estimate; oversize-but-legal routes are sent
  alone.
- Hugging Face transfers register a recovery lease, and `wait_for_transfer` commits Hugging Face
  uploads on completion (waiting for the part-ETag pass).
- `transfer.resume` replays on `recovery: "route_replay_required"`; `runtime.hello` polls every
  5 s with key `runtime:hello:{shard}`.
- Multipart manifests are sent one group per request and validated with the TypeScript bounds;
  route metadata is compacted only for multipart routes.
- `cancel_transfer` always releases recovery state once BeamCore answers; `close()` drains the
  connection and drops retained provider state.
- Integrity audits are keyed by source/destination id, coalesce per audit and report failures.
- Hippius routes match the TypeScript wire format.
- Terminal waiters return `Ok(None)` after `close()` and accept additional event fields.
- `attach_signed_urls` forwards `transfer_key` into `AttachSignedUrlsInput` instead of dropping
  it.
- Route recovery for direct-PUT destinations (Hippius, Hugging Face) signs the planned per-chunk
  object key; a recovered Hippius chunk no longer overwrites the final object.
- Recovery leases and signers are fenced by owner: a fenced-off owner's in-flight recovery, stream
  failure or cancellation never replays into, releases, or stops a replacement owner's lease,
  route recovery signer or integrity context, and pending recovery is handed to the replacement.
- `Debug` output of provider source/destination configs and Hugging Face config/upload types
  redacts credentials, tokens and presigned URLs (`<redacted>`).
- S3/R2/S3-compatible object keys with a `.` or `..` path segment fail with
  `BeamApiError::InvalidArgument` instead of signing a URL that HTTP clients normalize to a
  different object.

### Breaking

- `BeamApiError` is `#[non_exhaustive]`; lifecycle failures are `Lifecycle` instead of
  `HttpStatus`.
- Provider config enums gained `S3Compatible` variants; several public structs gained fields.
- `create_transfer` is deprecated; lifecycle idempotency keys are transfer-scoped
  (`transfer:{id}:create`, `transfer:{id}:prepare`).

## 0.3.0

- `transfer-client-control/v7`: consecutive multipart part numbers
  (`part_number = source_chunk_index + 1`), a single attempt slot
  (`multipart_attempt_slots == 1`), and manifests whose `max_part_number` equals
  `expected_part_count`.
