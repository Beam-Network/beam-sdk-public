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

	beamnetworksdk "github.com/Beam-Network/beam-sdk-public/sdks/go"
)

func main() {
	client := beamnetworksdk.NewClient(beamnetworksdk.WithAPIKey("b1m_your_key"))
	defer client.Close()

	transfer, err := client.CreateTransfer(context.Background(), beamnetworksdk.TransferCreateRequest{
		Sources: []beamnetworksdk.SourceConfig{
			{"type": "http", "url": "https://downloads.example.com/report.parquet"},
		},
		Destinations: []beamnetworksdk.DestConfig{
			{"type": "http", "url": "https://storage.example.com/report.parquet"},
		},
		TotalSize: 104_857_600,
	})
	if err != nil {
		panic(err)
	}

	if _, err := client.DistributeTransfer(context.Background(), transfer.TransferID); err != nil {
		panic(err)
	}

	status, err := client.WaitForTransfer(context.Background(), transfer.TransferID, 0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(status.Status)
}
```

Lifecycle traffic uses Beam NATS request/reply. The SDK includes transfer creation, streaming signed-route distribution, cancellation, event-driven terminal waiting with status reconciliation, prepare helpers, and provider-aware helpers for S3, R2, Hippius, and Hugging Face. GCS and Azure provider models are present; signing adapters are placeholders.

Provider transfers use `transfer-client-control/v6`. Every reply carries Runtime and transport epochs; prepare and every route-stream message carry a UUID route generation. S3 and R2 retain direct multipart UploadPart/ListParts/HEAD handling.

The SDK signs up to 64 routes concurrently by default and emits 2,048-route logical batches, split only when encoded MessagePack requests exceed the 8 MiB default guard. `WithMaxPayloadBytes` permits an explicit positive override. A single route larger than the configured limit fails before publication with both sizes in the error. The stream ID derives from the transfer, selected flow, and immutable plan identity, and each ordered batch ID includes its route-coordinate checksum. Manual multi-destination attachment requires `delivery_index` on every route. Lifecycle mutations retry transient failures up to three times with the same request identity. `WaitForTransfer` subscribes to the API-key-owned, at-most-once terminal signal before the first status read and reconciles every signal through authoritative status; subscription failure degrades to the same jittered 15-to-30-second status fallback unless the caller supplies a different initial interval. Always call `Close`; it also closes outstanding terminal waiters.

Hippius and Hugging Face use canonical non-multipart `signed_url` routes, so their manifest is empty and group-level final HEAD verification is not available from those provider flows. A Hugging Face destination carries the Hub's own presigned part URLs and is committed by `FinalizeHuggingFaceUploads` once the transfer completes; see [`HUGGINGFACE_PROVIDER.md`](../../HUGGINGFACE_PROVIDER.md). GCS and Azure provider signing remain unimplemented.

The Go client reconnects NATS indefinitely and keeps one in-memory recovery lease per active transfer plus one hello monitor per shard. The lease is installed before route-stream begin, and a per-transfer lock coalesces initial streaming with replay. Multipart recovery retains the existing upload IDs and compact group state, then re-signs only the expiring route and commit controls. Epoch changes coalesce `transfer.resume`; route replay uses a fresh generation only when Runtime requests it. `AttachSignedURLs` requires a `ManualRouteRecovery` factory and plan fingerprint/checksum. Foreground cancellation, deadline expiry, and Runtime state-loss responses keep background recovery and retained multipart uploads active. Provider signing inputs are released on terminal status or `Close`, and no local recovery journal is written.
