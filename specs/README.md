# BEAM API Specs

This directory is the shared contract layer for all BEAM SDK packages.

- `beamcore.openapi.yaml` is the portable HTTP contract used by future code generators.
- Language-specific hand-written helpers should wrap generated or typed HTTP operations rather than redefining request and response shapes from scratch.
- Breaking API changes should update this spec first, then each SDK package.

The current spec is intentionally small and covers the common package bootstrap surface. Expand it as endpoints stabilize.
