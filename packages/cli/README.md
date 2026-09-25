# BEAM CLI

Command line tools for BEAM transfers.

```bash
npx @beam-network/cli --help
npx -p @beam-network/cli beam-send create \
  --source '{"type":"http","url":"https://downloads.example.com/report.parquet"}' \
  --destination '{"type":"http","url":"https://storage.example.com/ingest/report.parquet"}' \
  --total-size 104857600 \
  --wait
```

The package is configured for private npm publication with restricted access.
