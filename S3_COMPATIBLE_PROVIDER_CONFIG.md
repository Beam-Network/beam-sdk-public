# S3CompatibleProviderConfig implementation prompt spec

This document describes the desired SDK design for supporting many
S3-compatible providers through one shared implementation.

Use it as the implementation brief for `beam-sdk`, starting with the TypeScript
SDK.

## Goal

Add a generic `S3CompatibleProviderConfig` model and signer path so providers
such as AWS S3, Cloudflare R2, Wasabi, MinIO, Backblaze B2 S3 API,
DigitalOcean Spaces, Scaleway Object Storage, Linode/Akamai Object Storage,
Vultr Object Storage, IBM Cloud Object Storage, OVHcloud, Oracle S3
Compatibility API, Ceph RGW, Cloudian, Garage, and other S3-compatible storage
systems can be configured without adding one SDK type and one signing branch per
provider.

The SDK should keep BeamCore and Beam workers provider-agnostic:

- the SDK signs provider-specific URLs;
- BeamCore receives only prepared HTTP sources, prepared destinations, and
  signed chunk routes;
- workers receive `source_urls` and `dest_urls` and perform HTTP GET/PUT;
- credentials never leave the SDK/Studio process.

## Current TypeScript SDK shape

Relevant files:

- `sdks/typescript/src/models.ts`
- `sdks/typescript/src/provider-signing.ts`
- `sdks/typescript/src/client.ts`
- `sdks/typescript/src/index.ts`
- `sdks/typescript/test/provider-signing.functional.test.mjs`
- `sdks/typescript/test/client.functional.test.mjs`
- `sdks/typescript/README.md`

Current model:

- `S3ProviderConfig` and `R2ProviderConfig` are separate public types.
- Both are signed with `@aws-sdk/client-s3`.
- `provider-signing.ts` has separate `createS3Client` and `createR2Client`
  helpers.
- R2 mostly differs by endpoint resolution and region:
  `https://<account_id>.r2.cloudflarestorage.com` and region `"auto"`.
- AWS S3 differs mostly by default region and lack of custom endpoint.

Problem:

- Adding Wasabi, MinIO, Backblaze B2, DigitalOcean Spaces, etc. should not
  require another public config type plus another signing branch.
- The real abstraction is "S3-compatible driver with provider profile
  defaults", not "one technical implementation per provider".

## Target concept

Separate:

- provider identity: user-facing provider string, for example `"s3"`, `"r2"`,
  `"wasabi"`, `"minio"`, `"b2"`, `"spaces"`;
- driver: technical implementation, for example `"s3-compatible"`.

The provider string should be preserved in metadata and status surfaces. The
driver should be used to route signing logic.

## New TypeScript model

Add this public interface in `sdks/typescript/src/models.ts`:

```ts
export interface S3CompatibleProviderConfig {
  provider: string;
  driver?: "s3-compatible";
  id?: string;
  bucket: string;
  key: string;
  region?: string;
  endpoint_url?: string;
  access_key_id: string;
  secret_access_key: string;
  session_token?: string;
  force_path_style?: boolean;
  account_id?: string;
}
```

Field notes:

- `provider` is a free string and should identify the product/profile.
- `driver` should be optional for ergonomic input but set to
  `"s3-compatible"` by the factory.
- `endpoint_url` is the main customization field for non-AWS providers.
- `account_id` is mainly a compatibility helper for R2.
- `force_path_style` is required for some self-hosted or S3-compatible systems.
- `bucket`, `key`, `access_key_id`, and `secret_access_key` are always
  required.

Add input type and factory:

```ts
export type S3CompatibleProviderConfigInput = Omit<
  S3CompatibleProviderConfig,
  "driver"
>;

export const S3CompatibleProviderConfig = Object.freeze({
  create(input: S3CompatibleProviderConfigInput): S3CompatibleProviderConfig {
    requireFields(input.provider || "s3-compatible", input, [
      "provider",
      "bucket",
      "key",
      "access_key_id",
      "secret_access_key"
    ]);

    const provider = input.provider.trim().toLowerCase();

    if (provider === "r2" && !hasText(input.endpoint_url) && !hasText(input.account_id)) {
      throw new Error("r2 config requires account_id or endpoint_url.");
    }

    if (provider !== "s3" && provider !== "r2" && !hasText(input.endpoint_url)) {
      throw new Error(`${provider} config requires endpoint_url.`);
    }

    return { driver: "s3-compatible", ...input, provider };
  }
});
```

Keep the existing factories:

- `S3ProviderConfig.create(...)`
- `R2ProviderConfig.create(...)`

They must remain public and backwards compatible.

## Backwards compatibility

Do not remove or rename:

- `S3ProviderConfig`
- `S3ProviderConfigInput`
- `R2ProviderConfig`
- `R2ProviderConfigInput`
- `HippiusProviderConfig`
- `HippiusProviderConfigInput`

Existing code using this must still compile:

```ts
S3ProviderConfig.create({
  bucket: "bucket",
  key: "file.bin",
  region: "us-east-1",
  access_key_id: "...",
  secret_access_key: "..."
});

R2ProviderConfig.create({
  bucket: "bucket",
  key: "file.bin",
  account_id: "...",
  access_key_id: "...",
  secret_access_key: "..."
});
```

The existing S3/R2 config objects can stay structurally separate, but signing
should route them through the shared S3-compatible code path.

## ProviderConfig union

Update:

```ts
export type ProviderConfig =
  | S3ProviderConfig
  | R2ProviderConfig
  | S3CompatibleProviderConfig
  | HippiusProviderConfig;
```

TypeScript caveat:

`S3CompatibleProviderConfig.provider` is a free `string`, so it can reduce
discriminated-union narrowing. Avoid relying only on `switch
(config.provider)` for the full union. Prefer helper predicates.

Add a helper, either exported or local to `provider-signing.ts`:

```ts
type AnyS3CompatibleProviderConfig =
  | S3ProviderConfig
  | R2ProviderConfig
  | S3CompatibleProviderConfig;

function isS3CompatibleProvider(
  config: ProviderSourceConfig | ProviderDestinationConfig
): config is AnyS3CompatibleProviderConfig {
  return (
    config.provider === "s3" ||
    config.provider === "r2" ||
    config.driver === "s3-compatible"
  );
}
```

If TypeScript complains because not all union members have `driver`, use:

```ts
"driver" in config && config.driver === "s3-compatible"
```

## Shared S3-compatible client helpers

In `sdks/typescript/src/provider-signing.ts`, replace the separate S3 and R2
client construction with a shared helper.

```ts
function createS3CompatibleClient(config: AnyS3CompatibleProviderConfig): S3Client {
  const endpoint = s3CompatibleEndpoint(config);

  return new S3Client({
    region: s3CompatibleRegion(config),
    endpoint,
    forcePathStyle: s3CompatibleForcePathStyle(config, endpoint),
    credentials: {
      accessKeyId: config.access_key_id,
      secretAccessKey: config.secret_access_key,
      sessionToken: "session_token" in config ? config.session_token : undefined
    }
  });
}

function s3CompatibleEndpoint(config: AnyS3CompatibleProviderConfig): string | undefined {
  if (config.endpoint_url) {
    return config.endpoint_url;
  }

  if (config.provider === "r2" && "account_id" in config && config.account_id) {
    return `https://${config.account_id}.r2.cloudflarestorage.com`;
  }

  return undefined;
}

function s3CompatibleRegion(config: AnyS3CompatibleProviderConfig): string {
  if ("region" in config && config.region) {
    return config.region;
  }

  if (config.provider === "r2") {
    return "auto";
  }

  return "us-east-1";
}

function s3CompatibleForcePathStyle(
  config: AnyS3CompatibleProviderConfig,
  endpoint?: string
): boolean | undefined {
  if ("force_path_style" in config && typeof config.force_path_style === "boolean") {
    return config.force_path_style;
  }

  if (config.provider === "s3") {
    return undefined;
  }

  return Boolean(endpoint);
}
```

Notes:

- R2 with `account_id` should still work without `endpoint_url`.
- AWS S3 should not force path-style by default.
- Custom providers with `endpoint_url` should default to path-style unless the
  caller sets `force_path_style: false`.
- If a provider needs virtual-hosted style with a custom endpoint, the caller can
  set `force_path_style: false`.

## Refactor signing paths

In `provider-signing.ts`, all S3-like operations should use
`isS3CompatibleProvider`.

Affected functions:

- `prepareProviderSource`
- `prepareProviderDestination`
- `createMultipartUpload`
- `signCompleteMultipartUpload`
- `signAbortMultipartUpload`
- `abortMultipartUpload`
- `signDestinationRoute`
- internal `signDestinationUrl`

Desired behavior:

### Source preparation

For any S3-compatible config:

- create S3-compatible client;
- `HeadObject` to get size;
- sign `GetObject`;
- return `PreparedHttpSource` with:
  - `provider: source.provider`
  - `url`
  - `size`
  - `filename`
  - `expires_at`
  - metadata including `driver: "s3-compatible"`, `bucket`, `key`, `region`,
    and `endpoint_url` when present.

### Destination preparation

For any S3-compatible config:

- return `PreparedDestination` with:
  - `destination_id`
  - `provider: destination.provider`
  - `logical_prefix: destination.key`
  - metadata including `driver: "s3-compatible"`, `bucket`, `key`, `region`,
    and `endpoint_url` when present.

### Multipart

For any S3-compatible config:

- `CreateMultipartUpload`
- sign direct final-object `UploadPart`
- sign paged `ListParts`, complete, abort, and final HEAD controls
- sign `CompleteMultipartUpload`
- sign `AbortMultipartUpload`
- sign final-object `HEAD` verification

There is no direct-to-final-object `PutObject` fallback and no legacy multipart route path.

Hippius behavior should remain unchanged.

## Error handling

Keep useful validation messages:

- missing core fields:
  `s3-compatible config requires bucket, key, access_key_id, secret_access_key`
  or provider-specific equivalent through `requireFields`.
- R2 without `account_id` or `endpoint_url`:
  `r2 config requires account_id or endpoint_url.`
- custom S3-compatible without endpoint:
  `<provider> config requires endpoint_url.`

Do not require `endpoint_url` for `provider: "s3"` or `provider: "r2"`.

## Public exports

Update `sdks/typescript/src/index.ts` if needed so these are exported:

- `S3CompatibleProviderConfig`
- `S3CompatibleProviderConfigInput`
- `S3CompatibleProviderConfig` interface/factory value

Be careful with TypeScript's merged interface/value pattern already used for
`S3ProviderConfig`.

## README example

Add a TypeScript README example:

```ts
import {
  BeamClient,
  S3CompatibleProviderConfig
} from "@beam-network/sdk";

const beam = new BeamClient({ apiKey: process.env.BEAM_API_KEY! });

await beam.transfers.prepareProviderTransfer({
  sources: [
    S3CompatibleProviderConfig.create({
      provider: "wasabi",
      bucket: "my-bucket",
      key: "input/file.bin",
      region: "us-east-1",
      endpoint_url: "https://s3.us-east-1.wasabisys.com",
      access_key_id: process.env.WASABI_ACCESS_KEY_ID!,
      secret_access_key: process.env.WASABI_SECRET_ACCESS_KEY!
    })
  ],
  destinations: [
    S3CompatibleProviderConfig.create({
      provider: "minio",
      bucket: "archive",
      key: "file.bin",
      endpoint_url: "https://minio.example.com",
      force_path_style: true,
      access_key_id: process.env.MINIO_ACCESS_KEY_ID!,
      secret_access_key: process.env.MINIO_SECRET_ACCESS_KEY!
    })
  ],
  name: "S3-compatible transfer"
});
```

Also mention:

- use `S3ProviderConfig` for ordinary AWS S3 if preferred;
- use `R2ProviderConfig` for existing R2 code if preferred;
- use `S3CompatibleProviderConfig` for every S3-compatible provider with a
  custom endpoint.

## Tests to add/update

Prefer focused TypeScript tests.

Add tests for model validation:

1. `S3ProviderConfig.create()` still works.
2. `R2ProviderConfig.create()` still works with `account_id`.
3. `R2ProviderConfig.create()` still works with `endpoint_url`.
4. `S3CompatibleProviderConfig.create({ provider: "wasabi", endpoint_url, ... })`
   works and returns `driver: "s3-compatible"`.
5. `S3CompatibleProviderConfig.create({ provider: "minio", endpoint_url, force_path_style: true, ... })`
   preserves `force_path_style`.
6. Custom provider without `endpoint_url` throws.
7. R2 without `account_id` or `endpoint_url` throws.
8. Provider string is normalized to lowercase if desired by implementation.

Add or update signing tests:

1. S3-compatible custom provider uses the shared S3 client path.
2. R2 endpoint is derived from `account_id`.
3. Custom provider endpoint is passed as `endpoint`.
4. Custom provider defaults `forcePathStyle` to true when `endpoint_url` is
   present.
5. Metadata returned by `prepareProviderSource` preserves `provider:
   "wasabi"` and includes `driver: "s3-compatible"`.
6. Metadata returned by `prepareProviderDestination` preserves custom provider.

If tests cannot hit real provider endpoints, mock `S3Client` commands or add
unit-testable pure helper exports for endpoint, region, and force-path-style
resolution.

## Beam signed-URL contract

Provider prepare and route attachment use canonical `signed_url` over `transfer-client-control/v6`. S3-compatible destinations create the final multipart upload with `beam-transfer-id` object metadata before any route batch is sent. Each route signs direct `UploadPart` into that upload and carries the required finalization controls.

- the upload ID and final object key;
- expected object size and expected part count;
- the source-local maximum part number (`source.chunk_count`);
- complete, abort, final HEAD, and the ListParts page for the route's source-local part;
- a presigned final-object HEAD URL and the metadata it must expose; and
- one expiry timestamp for the group controls.

2,048-route logical batches carry direct finalization metadata for v1 routes and split only when encoded payload size requires it. Workers still receive only HTTP URLs and provider credentials remain local to the SDK process. TypeScript, Python, and Go share this S3/R2 path. Rust exposes the manifest type for raw v2 attachment but does not yet implement S3-compatible provider signing.

Each source uses `part_number = source_chunk_index + 1`. A retry signs the same source-local part number and overwrites that part; it does not allocate a new multipart slot.

## Acceptance criteria

The change is complete when:

- Existing S3 and R2 TypeScript SDK usage still compiles and works.
- A new S3-compatible provider can be configured with only:
  `provider`, `endpoint_url`, `bucket`, `key`, `access_key_id`,
  `secret_access_key`, and optional `region` / `force_path_style`.
- Signing source URLs works through the shared S3-compatible path.
- Signing direct `UploadPart`, multipart lifecycle, repair, and final HEAD URLs works through the shared
  S3-compatible path.
- Prepared source/destination metadata preserves the provider identity and
  marks `driver: "s3-compatible"`.
- Route `part_number` values are bounded by the referenced manifest's `max_part_number`.
- Final verification checks the expected part count against the source-local part range.
- Hippius remains a canonical non-multipart `signed_url` path with an empty manifest and no group-level HEAD verification.
