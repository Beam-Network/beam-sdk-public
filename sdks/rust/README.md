# BEAM SDK for Rust

Install:

```toml
beam-network-sdk = "0.1"
```

Example:

```rust
use beam_network_sdk::{BeamClient, BeamClientOptions, TransferCreateRequest};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let beam = BeamClient::new(BeamClientOptions {
    api_key: "b1m_your_key".to_string(),
    nats_url: None,
    environment: Some("dev".to_string()),
    http_client: None,
    transfer_runtime_shard_count: None,
    request_timeout: None,
    max_payload_bytes: None,
    route_signing_concurrency: None,
})?;

let status = beam.transfer_status("transfer_id").await?;
println!("{}", status.status);
beam.close().await?;
# Ok(())
# }
```

This crate provides the core transfer lifecycle over NATS: create, distribute, cancel, status, wait, prepare, and streaming signed-route submission.
It also includes Hippius provider signing via `prepare_hippius_provider_transfer` and Hugging Face Hub signing via `prepare_huggingface_provider_transfer`; S3/R2/GCS/Azure provider-signing adapters are not implemented yet.

Signed-route submission uses `transfer-client-control/v6`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. Raw grouped attachment remains validated against its manifest and source-local `max_part_number`.

Hippius and Hugging Face use genuine non-multipart v1 routes with an empty manifest. Their built-in provider flows therefore have no group-level final HEAD verification. A Hugging Face destination carries the Hub's own presigned part URLs and is committed by `finalize_huggingface_uploads` once the transfer completes; see [`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md). Routes sign concurrently (64 by default) and emit 2,048-route logical batches, split only when encoded MessagePack requests exceed the 8 MiB default guard. `BeamClientOptions.max_payload_bytes` permits an explicit positive override. A single route larger than the configured limit fails before publication with both sizes in the error. Stream IDs derive from the transfer, selected flow, and immutable plan identity, while each ordered batch ID includes its route-coordinate checksum; manual multi-destination attachment requires `delivery_index` on every route. Completion waits subscribe to the API-key-owned, at-most-once terminal signal before the first status read and reconciles every signal through authoritative status; subscription failure degrades to the same jittered 15-to-30-second status fallback unless the caller supplies a different initial interval. Callers should finish with `beam.close().await`, which also closes terminal waiters.

The Rust client reconnects NATS indefinitely and keeps one in-memory recovery lease per active transfer plus one hello monitor per shard. The lease is installed before route-stream begin, and a per-transfer async lock coalesces initial streaming with replay. Its current built-in provider is non-multipart; manual multipart callers retain compact group state inside the required recovery factory. Epoch changes coalesce `transfer.resume`; route replay uses a fresh generation only when Runtime requests it. `attach_signed_urls` requires `ManualRouteRecovery` and the plan fingerprint/checksum. Dropping or timing out the foreground future, or receiving a Runtime state-loss response, leaves background recovery active. Retained provider inputs are released on terminal status or `close`, and no local recovery journal is written.
