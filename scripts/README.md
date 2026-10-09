# Scripts

Release scripts live here as thin wrappers around each registry's native tooling.

- `release-python.sh` builds and validates the PyPI package.
- `release-typescript.sh` builds the npm package.
- `release-go.sh` verifies the Go module.

The scripts do not publish by default unless `PUBLISH=1` is set.
