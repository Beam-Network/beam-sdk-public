# Streaming showcase moved

The OBS streaming application now lives in the private
beam-streaming-showcase repository.

That repository owns the responsive gallery, Beam assets, viewer access gateway,
operator-only OBS configuration, application tests and deployment configuration.
It consumes a pinned SDK build and the separately versioned Web Agent runtime.

For a minimal SDK integration example, use [examples/web-agent](../web-agent).
The SDK and its protocol-level fixtures remain in this repository.
