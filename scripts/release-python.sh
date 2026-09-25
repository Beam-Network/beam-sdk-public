#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../sdks/python"
python -m pip install --upgrade build twine
python -m build
twine check dist/*

if [[ "${PUBLISH:-0}" == "1" ]]; then
  twine upload dist/*
fi
