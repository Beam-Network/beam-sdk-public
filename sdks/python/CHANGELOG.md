# Changelog

## 0.13.4

- Worker source reads are pinned to the source object version captured at prepare.

## 0.13.3

- Packaging: the sdist contains only the hermetic test suite.

## 0.13.2

- Docs: minimal storage-credential text.

## 0.13.1

- Docs: shorter storage-credential guidance.

## 0.13.0

Removals:

- `chunk_size` and `beam-send --chunk-size`; Beam chooses the chunk size.

## 0.12.0

Additions:

- `BeamTransferFailedError`, its `BeamStorageAccessError` subclass and `transfer_failed_error`.
- Docs: storage credentials must not be restricted to specific IP addresses or networks.

Changes:

- `wait_complete` raises `BeamTransferFailedError` instead of `BeamAPIError` for a failed transfer.
- `beam-send` prints a failed transfer's error and exits with status 1.

## 0.10.0

This release brings the Python SDK to parity with the TypeScript SDK, on top of the `transfer-client-control/v7` multipart contract released in 0.9.0.

Fixes:

- `tls://` lifecycle URLs, including the production gateway, now connect with TLS handshake-first and verify the gateway hostname. Before this, the Python SDK could not connect to `tls://orch-gateway.b1m.ai:4222`.
- When a provider transfer failed and could not be recovered, releasing the recovery lease cleared the multipart state before cleanup read it, so created uploads were never aborted. The transfer is now cancelled without releasing the lease, and the lease is released only after cleanup. Cleanup follows the TypeScript order: it aborts first if the route stream never began, retries a failed early abort after cancellation, drops aborted entries, and runs with bounded concurrency.
- Route-recovery signing failed for Hippius and Hugging Face destinations. It now signs plain PUTs for them, and rebuilds multipart controls from the upload ID the Runtime requests.
- Recovery part numbers are checked against the v7 mapping `source_chunk_index + 1`, and `attempt_slot` must be 0. `multipart_part_number(index, attempt_slot)` now takes the attempt slot instead of a logical attempt index and rejects any slot other than 0 instead of ignoring it.
- Route recovery signing, now served by a dedicated signer that also covers resumed transfers and Hippius and Hugging Face destinations, applies the v7 multipart recovery signing (`sign_multipart_recovery`) to every recovered route, as the TypeScript SDK does.
- S3 and R2 source metadata now also records `last_modified`, `content_length`, `driver` and `endpoint_url`, alongside the `etag` and `version_id` pinned since 0.7.8, so integrity audits can pin source reads. Destinations record `driver` and `endpoint_url`.
- Route batches target 4 MiB, with room reserved for the auth token. A single route larger than that but within `max_payload_bytes` is sent alone.
- The auth token is refreshed 30 s before it expires.
- A `recovery: "route_replay_required"` reply now triggers route replay.
- Route replay re-streams the retained plan. It no longer prepares again, repeats the source HEAD, or re-hashes for Hugging Face.
- Lifecycle idempotency keys match TypeScript: `transfer:{id}:create` and `transfer:{id}:prepare`, and the route generation is derived from the prepare key.
- `cancel()` always releases the recovery lease and stops recovery signing and integrity grants.
- Recovery signing is bounded by `route_signing_concurrency`.

Additions:

- `resume_provider_transfer` takes over a transfer in a new process, reusing its existing multipart uploads.
- `create_transfer`, the TypeScript-named alias of `prepare_provider_transfer`. It distributes by default.
- New `prepare_provider_transfer` hooks: `on_before_transfer_prepare`, `on_prepared`, `on_multipart_group_ready` (which receives a credential-free `ProviderMultipartGroupIdentity`) and `throw_if_cancelled`.
- An `ownership` fence (`BeamCancellationToken`). Cancelling it stops this owner's signing and replay without cancelling the transfer or aborting a replacement owner's uploads. A fenced owner's background recovery never replays into, or releases, a replacement owner's lease; if the calling task is itself cancelled, `CancelledError` still propagates.
- `BeamProviderTransferError`, which carries `errors`, `transfer_cancelled` and `multipart_cleanup_complete`, with sanitized messages.
- `BeamSDK` options: `max_payload_bytes`, `multipart_control_concurrency` (default 2), `transfer_client_subject_prefix` and `transfer_runtime_shard_count`.
- `S3CompatibleProviderSource` and `S3CompatibleProviderDestination`, for MinIO, Wasabi and other S3 APIs.
- Hybrid helpers: `sign_destination_url` (with `content_md5`), `list_multipart_parts`, `complete_multipart_upload`, `inspect_destination_object` and `prepare_provider_source_for_plan`.
- `TransferStatusInfo.name` and `integrity_audit_submission_error`. Grant failures are now reported in the status (sanitized) rather than only logged, and concurrent polls share one submission per audit.
- Typed `IntegrityAuditChallenge`, which still supports `challenge["audit_id"]`. `TransferCreateResponse.transfer_key`.
- Package exports: the multipart limits under their TypeScript names (`MULTIPART_ATTEMPT_SLOT_COUNT`, now 1, with `MULTIPART_ATTEMPT_SLOTS` kept as an alias), the provider signing helpers, and the Hugging Face models.
- S3 clients are cached per configuration and make up to 5 attempts per call.

Changes:

- `TransferStatusInfo.destination_groups` is now optional.
- Route metadata keeps `etag_required` in the per-route attempt fields.
- A custom boto3 `client_factory` is now called as `factory("s3", **kwargs)` for every provider, including R2 (previously R2 passed `service_name="s3"` as a keyword).
- R2 presigned URLs now use path-style addressing, because `force_path_style` defaults to on for custom endpoints, as in TypeScript.
- `TransferPrepareResponse` and `TransferPlanResponse` parse failed responses that have no plan. Their plan fields are therefore typed optional; a successful response still requires them.
- `MultipartGroupManifest.final_object_metadata` must be exactly `{"beam-transfer-id": ...}`, and `expected_part_count` is capped at 10000.
- An explicit `idempotency_key` no longer replaces the create or prepare lifecycle key. It still derives the transfer ID.

## 0.9.0

- Use `transfer-client-control/v7`. Multipart uploads use one attempt slot, so each source chunk owns one consecutive part: `part_number = source_chunk_index + 1`, `attempt_slot` is always 0, and a multipart group's `max_part_number` equals its `expected_part_count`. Compact plans must declare `multipart_attempt_slots: 1` and the `source_chunk_index + 1` part-number formula.
- Keep `etag_required` in compact route attempt metadata.
- Add `sign_multipart_recovery`, invoked for every route recovery request: it renews direct multipart controls, or signs a staged per-attempt recovery object with its HEAD, DELETE and `UploadPartCopy` into the original upload, plus listing and deletion of staged objects.

## 0.7.8

- Retain the S3 and R2 source HEAD `ETag` and `VersionId` in prepared source metadata, so integrity grants pin the prepared source object.

## 0.7.6

- Submit source and destination signed range grants for a transfer's pre-completion integrity challenge.

## 0.7.5

- Report the installed distribution version from `__version__` and `beam-send --version`. 0.7.4 reported 0.7.3, because the version was declared both in `pyproject.toml` and as a literal in `__init__.py` and the two drifted.

## 0.7.4

- Use the `b1m_` API key prefix in documentation and examples, matching the keys the Beam Console issues.

## 0.7.2

- Reuse the same default route generation for idempotent prepare retries.

## 0.7.1

- Default encoded route messages to 8 MiB while preserving explicit overrides.
- Expose `BeamRouteRecoveryPendingError` when process-local route replay remains active.

## 0.6.0

- Simplified the Python SDK to transfer creation and transfer lifecycle management.
- Kept `beam-send` as the only packaged CLI entry point.
- Removed local receiver, sender, worker, sink, API wrapper, and MCP modules.
- Removed stream, destination-manager, network, orchestrator, worker, and security managers from `BeamSDK`.
- Kept provider-aware transfer preparation for S3, R2, GCS, Azure, and Hippius models.
