#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../sdks/go"
gofmt -w .
go test ./...
go list ./...

if [[ "${PUBLISH:-0}" == "1" ]]; then
  echo "Push a module-scoped semver tag, for example: git tag sdks/go/v0.1.0 && git push origin sdks/go/v0.1.0"
fi
