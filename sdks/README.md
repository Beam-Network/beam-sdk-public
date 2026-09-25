# BEAM SDK Packages

This directory contains the language packages shipped from this repository.

| Language | Package directory | Registry target | Status |
| --- | --- | --- | --- |
| Python | `sdks/python` | PyPI: `beam-network-sdk` | Transfer SDK moved from the previous `python/` directory |
| TypeScript | `sdks/typescript` | npm: `@beam-network/sdk` | Transfer SDK |
| CLI | `packages/cli` | npm: `@beam-network/cli` | `beam-send` binary for npm/npx |
| Go | `sdks/go` | Go module: `github.com/Beam-Network/beam-sdk-public/sdks/go` | Transfer SDK |
| Rust | `sdks/rust` | crates.io: `beam-network-sdk` | Transfer SDK |

Each SDK owns its package manifest, examples, changelog, and release process. Shared API contracts live in `specs/` so new languages can be added without copying protocol notes between packages.

## NATS Lifecycle Transport

Transfer lifecycle and restart recovery use Core NATS request/reply through `transfer-client-control/v6`. All four SDKs install the in-memory recovery lease before route-stream begin, monitor Runtime/transport epochs, coalesce initial streaming with `transfer.resume`, and regenerate expiring routes under a fresh UUID generation. Foreground cancellation, deadline expiry, and Runtime state-loss responses do not abandon the background lease or abort retained multipart uploads. No SDK persists credentials, signed URLs, or a recovery journal.

Provider transfers sign up to 64 routes concurrently by default and stream 2,048-route logical batches, split only when encoded payload size requires it. Route stream IDs derive from the transfer, selected flow, and immutable plan identity; batch IDs bind the batch index to its route-coordinate checksum. Lifecycle mutations reuse those stable identities across up to three transient retries. Every SDK splits by encoded size under its 8 MiB default guard; the TypeScript splitter also reserves control-envelope headroom for the live auth token and request identity. BeamCore NATS uses a 32 MiB `max_payload`. Callers may explicitly choose a different positive SDK guard.

Completion waits subscribe first to `beam.transfer.client.<environment>.events.<key-prefix>.<transfer-id>.terminal`, which is authorized only for the owning API-key prefix. This at-most-once signal is advisory: a valid signal wakes an immediate authoritative status read, while subscription or delivery failure degrades to jittered 15-to-30-second status reconciliation. Closing the client closes every outstanding terminal subscription. Manual multi-destination route attachment must include the immutable `delivery_index` on every route; single-destination attachment derives it from the global chunk index.

The payload limit is required because NATS rejects a message that exceeds broker `max_payload`. A single encoded route larger than the configured SDK guard fails before publication and reports both encoded and configured sizes. It is not a transfer throughput limit: file bytes move directly between storage endpoints and workers, while NATS carries only lifecycle metadata, signed routes, task offers, acknowledgements, and results.

## R2 to S3 Transfer Snippets

### Python

```python
import asyncio

from beam_network_sdk import BeamSDK
from beam_network_sdk.models import R2ProviderSource, S3ProviderDestination


async def main() -> None:
    async with BeamSDK(api_key="b1m_your_key", environment="dev") as beam:
        transfer = await beam.transfers.prepare_provider_transfer(
            sources=[
                R2ProviderSource(
                    bucket="source-bucket",
                    key="exports/report.parquet",
                    account_id="cloudflare-account-id",
                    access_key_id="r2-access-key",
                    secret_access_key="r2-secret-key",
                )
            ],
            destinations=[
                S3ProviderDestination(
                    bucket="destination-bucket",
                    key="imports/report.parquet",
                    region="us-east-1",
                    access_key_id="aws-access-key",
                    secret_access_key="aws-secret-key",
                )
            ],
            name="r2-to-s3-report",
            distribute=True,
        )
        print(transfer.transfer_id)


asyncio.run(main())
```

### TypeScript

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

console.log(transfer.transfer_id);
```

### Go

```go
package main

import (
	"context"
	"fmt"
	"time"

	beamnetworksdk "github.com/Beam-Network/beam-sdk-public/sdks/go"
)

func main() {
	client := beamnetworksdk.NewClient(beamnetworksdk.WithAPIKey("b1m_your_key"))
	defer client.Close()

	transfer, err := client.PrepareProviderTransfer(
		context.Background(),
		[]beamnetworksdk.ProviderSource{
			beamnetworksdk.R2ProviderSource{
				Bucket:          "source-bucket",
				Key:             "exports/report.parquet",
				AccountID:       "cloudflare-account-id",
				AccessKeyID:     "r2-access-key",
				SecretAccessKey: "r2-secret-key",
			},
		},
		[]beamnetworksdk.ProviderDestination{
			beamnetworksdk.S3ProviderDestination{
				Bucket:          "destination-bucket",
				Key:             "imports/report.parquet",
				Region:          "us-east-1",
				AccessKeyID:     "aws-access-key",
				SecretAccessKey: "aws-secret-key",
			},
		},
		"r2-to-s3-report",
		false,
		time.Hour,
		true,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(transfer.TransferID)
}
```

### Rust

The Rust SDK currently exposes the transfer lifecycle, signed-URL attach flow, and Hippius and Hugging Face provider signing. S3/R2/GCS/Azure provider-signing adapters are still pending for Rust.

```rust
use beam_network_sdk::{BeamClient, BeamClientOptions, TransferCreateRequest};
use serde_json::json;
use std::collections::HashMap;

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

let mut source = HashMap::new();
source.insert("type".to_string(), json!("r2"));
source.insert("bucket".to_string(), json!("source-bucket"));
source.insert("key".to_string(), json!("exports/report.parquet"));
source.insert("account_id".to_string(), json!("cloudflare-account-id"));

let mut destination = HashMap::new();
destination.insert("type".to_string(), json!("s3"));
destination.insert("bucket".to_string(), json!("destination-bucket"));
destination.insert("key".to_string(), json!("imports/report.parquet"));
destination.insert("region".to_string(), json!("us-east-1"));

let transfer = beam
    .create_transfer(TransferCreateRequest {
        transfer_id: None,
        idempotency_key: Some("daily-report-2026-07-15".to_string()),
        sources: vec![source],
        destinations: vec![destination],
        total_size: 104_857_600,
        chunk_size: None,
        name: Some("r2-to-s3-report".to_string()),
        merkle_root: None,
        chunk_hashes: None,
        callbacks: None,
        test_mode: false,
        progressive_mode: false,
    })
    .await?;

beam.distribute_transfer(&transfer.transfer_id).await?;
println!("{}", transfer.transfer_id);
beam.close().await?;
# Ok(())
# }
```

## Adding Another Language

1. Create `sdks/<language>/`.
2. Add a language-native manifest and README.
3. Keep generated clients and handwritten helpers inside that SDK directory.
4. Add the package to root CI and root release docs.
5. Use `specs/beamcore.openapi.yaml` as the source of truth for generated API shapes.
