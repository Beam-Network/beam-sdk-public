# BEAM Transfer Workbench

Create, monitor, distribute, and cancel BEAM transfers from VS Code.

## MVP Features

- Store the BEAM API key in VS Code SecretStorage.
- Use the BEAM sidebar Workbench to choose the Beam NATS endpoint, API key, test mode, sources, and destinations.
- Pick HTTP, S3-compatible, R2, or Hippius for each source and destination.
- Import and export transfer JSON without storing the BEAM API key in the file.
- Run HTTP-only transfers with automatic size inference.
- Run provider-only transfers through the SDK provider-aware path.
- Run and follow a transfer with live progress.
- Check transfer status.
- Distribute transfers.
- Cancel transfers.
- Keep a recent-transfer list in the BEAM activity-bar view.

## Development

```bash
npm install
npm run build -w @beam-network/vscode
```

Open this package in VS Code and press `F5` to launch an Extension Development Host.

## Transfer JSON

```json
{
  "nats_url": "tls://orch-gateway.b1m.ai:4222",
  "name": "daily-report",
  "test_mode": true,
  "sources": [
    {
      "kind": "http",
      "fields": {
        "url": "https://downloads.example.com/report.parquet",
        "headers": ""
      }
    }
  ],
  "destinations": [
    {
      "kind": "http",
      "fields": {
        "url": "https://storage.example.com/ingest/report.parquet",
        "headers": ""
      }
    }
  ]
}
```
