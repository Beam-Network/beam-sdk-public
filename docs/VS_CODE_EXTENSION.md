# BEAM VS Code Extension

## Vision

The BEAM VS Code extension should make transfer creation and lifecycle management feel native inside a developer workspace. It is inspired by provider-style extensions such as Chutes AI for VS Code, but BEAM's core workflow is operational rather than conversational: configure credentials, create transfers, monitor progress, distribute work, and debug failures without leaving the editor.

The extension should be distributed as `@beam-network/vscode` from this monorepo under `packages/vscode`. Keeping it in the SDK monorepo lets the extension depend on the local TypeScript SDK, reuse shared API contracts, and evolve with the CLI and language SDKs.

## Target Users

- Developers integrating BEAM transfer flows into applications.
- Operators who need to create, distribute, inspect, and cancel transfers.
- SDK maintainers who need a fast manual testing surface for BeamCore.
- Teams that prefer config-driven workflows checked into project repositories.

## Core Use Cases

### First-run Setup

- Store the BEAM API key in VS Code SecretStorage.
- Configure the BeamCore endpoint through VS Code settings.
- Validate credentials with a simple status call or lightweight health command when available.
- Keep secrets out of workspace files, settings JSON, logs, and generated examples.

### Transfer Creation

- Create raw transfers from provider-agnostic source and destination JSON.
- Support HTTP and object-storage definitions exposed by the TypeScript SDK.
- Prompt for `total_size`, optional transfer name, optional `chunk_size`, test mode, and progressive mode.
- Offer a one-click `create + distribute` path.

### Transfer Monitoring

- Check transfer status by ID.
- Show progress as `chunks_completed / total_chunks`.
- Surface BeamCore error messages clearly.
- Persist recent transfer IDs in VS Code global state for quick refreshes.

### Lifecycle Actions

- Distribute a transfer.
- Cancel a transfer.
- Copy transfer IDs.
- Open raw JSON status output in an editor tab.

### Config-first Workflow

Future versions should support `.beam.json` or `.beam.yaml` files with schema validation, snippets, CodeLens actions, and generation of SDK examples in Python, TypeScript, Go, and Rust.

### Provider-aware Workflow

Future versions should expose S3, R2, Hippius, and S3-compatible preparation flows while keeping provider credentials local. SecretStorage should hold credentials, and BeamCore should only receive prepared routes or signed URLs.

The current Workbench exposes provider-specific forms for HTTP, S3-compatible, R2, and Hippius. Provider-only transfers use the SDK provider-aware path. HTTP-only transfers infer `total_size` from source `HEAD` responses because raw BeamCore creation still requires a size. Mixed HTTP/provider transfers are intentionally blocked until the extension has a full resolution layer for mixed routes.

### Chat Integration

Chat should be optional and come after the operational MVP. A future `@beam` participant could answer:

- `@beam status transfer_123`
- `@beam create transfer from this config`
- `@beam explain this BeamCore error`

## MVP Scope

The initial MVP in `packages/vscode` includes:

- Command Palette commands:
  - `Beam: Manage API Key`
  - `Beam: Clear API Key`
  - `Beam: Create Transfer`
  - `Beam: Check Transfer Status`
  - `Beam: Distribute Transfer`
  - `Beam: Cancel Transfer`
  - `Beam: Refresh Transfers`
- SecretStorage-backed BEAM API key.
- `beam.endpoint` setting with default `https://beamcore.b1m.ai`.
- `beam.pollIntervalMs` setting reserved for future polling.
- A BEAM activity-bar view listing recent transfers.
- A BEAM Workbench sidebar view for endpoint, API key, test mode, provider-specific source/destination fields, and run controls.
- Import/export of transfer JSON files containing `beamcore_url`, `name`, `test_mode`, `sources`, and `destinations`.
- Live transfer progress tracking after `Run Transfer`.
- Output channel named `Beam`.
- Direct dependency on `@beam-network/sdk`.

## Suggested Package Layout

```txt
packages/vscode/
  package.json
  tsconfig.json
  README.md
  src/
    extension.ts
    commands/
    services/
      beamService.ts
      credentialService.ts
      transferStore.ts
    views/
      beamWorkbenchView.ts
      beamTransfersProvider.ts
```

## Release Strategy

1. Keep the extension in this monorepo until the product surface becomes large enough to justify a separate repository.
2. Version it independently from the SDK, but test it against the workspace SDK before release.
3. Publish to the Visual Studio Marketplace once the create/status/distribute/cancel loop is stable.
4. Add config-file workflows before investing in chat or richer webviews.

## Non-goals For The MVP

- No custom webview form yet.
- No provider credential wizard yet.
- No Copilot language model provider behavior.
- No chat participant yet.
- No telemetry.
