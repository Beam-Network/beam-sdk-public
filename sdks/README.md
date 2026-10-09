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

Transfer lifecycle and restart recovery use Core NATS request/reply through `transfer-client-control/v7`. All four SDKs install the in-memory recovery lease before route-stream begin, monitor Runtime/transport epochs, coalesce initial streaming with `transfer.resume`, and regenerate expiring routes under a fresh UUID generation. Foreground cancellation, deadline expiry, and Runtime state-loss responses do not abandon the background lease or abort retained multipart uploads. No SDK persists credentials, signed URLs, or a recovery journal.

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
		time.Hour,
		true,
		"",
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(transfer.TransferID)
}
```

### Rust

The Rust SDK covers the transfer lifecycle, the signed-URL attach flow, and local provider
signing for S3, R2, S3-compatible stores, Hippius and the Hugging Face Hub through
`prepare_provider_transfer` (with background recovery and `resume_provider_transfer`). GCS and
Azure configs can be described to BeamCore but have no Rust signing adapter. See
[`rust/README.md`](rust/README.md).

```rust
use beam_network_sdk::{
    BeamClient, BeamClientOptions, ProviderDestinationConfig, ProviderSourceConfig,
    ProviderTransferCreateInput, R2ProviderSource, S3ProviderDestination,
};

# async fn example() -> Result<(), beam_network_sdk::BeamApiError> {
let beam = BeamClient::new(BeamClientOptions {
    api_key: "b1m_your_key".to_string(),
    ..Default::default()
})?;

let source = R2ProviderSource {
    bucket: "source-bucket".into(),
    key: "exports/report.parquet".into(),
    account_id: Some("cloudflare-account-id".into()),
    access_key_id: "...".into(),
    secret_access_key: "...".into(),
    ..Default::default()
}
.create()?;
let destination = S3ProviderDestination {
    bucket: "destination-bucket".into(),
    key: "imports/report.parquet".into(),
    region: Some("us-east-1".into()),
    access_key_id: "...".into(),
    secret_access_key: "...".into(),
    ..Default::default()
}
.create()?;

let prepared = beam
    .prepare_provider_transfer(ProviderTransferCreateInput {
        sources: vec![ProviderSourceConfig::R2(source)],
        destinations: vec![ProviderDestinationConfig::S3(destination)],
        name: Some("r2-to-s3-report".into()),
        idempotency_key: Some("daily-report-2026-07-15".into()),
        ..Default::default()
    })
    .await?;
println!("{}", prepared.transfer_id);
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
