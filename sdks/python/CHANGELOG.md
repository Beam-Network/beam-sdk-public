# Changelog

## 0.7.5

- Report the installed distribution version from `__version__` and `beam-send --version`. 0.7.4 reported 0.7.3, because the version was declared both in `pyproject.toml` and as a literal in `__init__.py` and the two drifted.

## 0.7.4

- Use the `b1m_` API key prefix in documentation and examples, matching the keys the Beam Console issues.

## 0.7.2

- Reuse the same default route generation for idempotent prepare retries.

## 0.7.1

- Default encoded route messages to 8 MiB while preserving explicit overrides.
- Expose `BeamRouteRecoveryPendingError` when process-local route replay remains active.

## 0.6.0

- Simplified the Python SDK to transfer creation and transfer lifecycle management.
- Kept `beam-send` as the only packaged CLI entry point.
- Removed local receiver, sender, worker, sink, API wrapper, and MCP modules.
- Removed stream, destination-manager, network, orchestrator, worker, and security managers from `BeamSDK`.
- Kept provider-aware transfer preparation for S3, R2, GCS, Azure, and Hippius models.
