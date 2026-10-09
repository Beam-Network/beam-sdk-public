#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../sdks/go"
gofmt -w .
go test ./...
go list ./...

if [[ "${PUBLISH:-0}" == "1" ]]; then
  echo "Copy sdks/go into Beam-Network/beam-sdk-public and push a module-scoped tag there (see docs/RELEASE.md)."
fi
