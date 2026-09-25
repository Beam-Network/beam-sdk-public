# Release Guide

This repository is structured so each language SDK can be released independently.

## Python

Package: `beam-network-sdk`

Directory: `sdks/python`

Build and publish:

```bash
cd sdks/python
python -m pip install --upgrade build twine
python -m build
twine check dist/*
twine upload dist/*
```

## TypeScript

Package: `@beam-network/sdk`

Directory: `sdks/typescript`

Automatic publish:

- Push to `prod` or a `prod/*` branch after changing files under `sdks/typescript/**`.
- The GitHub Actions workflow builds and tests `@beam-network/sdk`.
- If the version in `sdks/typescript/package.json` already exists on npm, the workflow bumps the patch version in CI until it finds an unpublished version.
- The workflow publishes to npm with npm trusted publishing, so it does not need an `NPM_TOKEN` secret.

Build and publish:

```bash
npm install
npm run build -w @beam-network/sdk
npm publish -w @beam-network/sdk --access public
```

## CLI

Package: `@beam-network/cli`

Directory: `packages/cli`

The CLI exposes the `beam-send` binary for `npx` and global installs. It depends on the TypeScript SDK package, so publish `@beam-network/sdk` first when releasing both packages.

Build and publish:

```bash
npm install
npm run build -w @beam-network/sdk
npm run build -w @beam-network/cli
npm publish -w @beam-network/cli --access public
```

## Go

Module: `github.com/Beam-Network/beam-sdk-public/sdks/go`

Directory: `sdks/go`

Go packages are published by pushing a semver Git tag. Prefer module-scoped tags:

```bash
git tag sdks/go/v0.1.0
git push origin sdks/go/v0.1.0
```

Consumers can then install:

```bash
go get github.com/Beam-Network/beam-sdk-public/sdks/go@v0.1.0
```

## Rust

Crate: `beam-network-sdk`

Directory: `sdks/rust`

Build and publish:

```bash
cd sdks/rust
cargo test
cargo publish --dry-run
cargo publish
```

## Web SDK

Packages: `@beam-network/web-sdk`, `@beam-network/web-sdk-server`

Directories: `packages/web-sdk`, `packages/web-sdk-server`

Unlike `@beam-network/sdk`, these are **not** published by a branch push. Release is the
`Publish Web SDK` workflow, run manually with a package choice and a `dry_run` input that
defaults to true. Publishing is deliberate because customer applications consume both
packages, and the browser bundle is additionally served from a CDN path that pages
hard-code.

Verify locally before dispatching the workflow:

```bash
npm run lint:web
npm run test:web
npm run build:web
npm run package:check
```

The browser package's script-tag build exposes the global `Beam` from
`packages/web-sdk/dist/beam.min.js`. That global name and the CDN path are frozen public
contracts — pages that pasted the console's embed snippet break if either changes. CI
asserts both on every run.

## Versioning

SDK packages can version independently while sharing the same API spec. When the BeamCore API changes:

1. Update `specs/beamcore.openapi.yaml`.
2. Update the affected language SDKs.
3. Release only the packages whose public surface changed.
