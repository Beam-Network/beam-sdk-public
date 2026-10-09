#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
npm install
npm run build -w @beam-network/sdk
npm pack -w @beam-network/sdk

if [[ "${PUBLISH:-0}" == "1" ]]; then
  npm publish -w @beam-network/sdk --access public
fi
