import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListPartsCommand,
  PutObjectCommand,
  S3Client,
  UploadPartCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  huggingFaceEndpoint,
  huggingFaceFileMetadata,
  huggingFaceRepoType,
  huggingFaceRevision
} from "./huggingface.js";
import type {
  ChunkDestinationSigningTarget,
  ChunkSigningPlanItem,
  HippiusProviderConfig,
  HuggingFaceProviderConfig,
  PlanningHttpSource,
  PreparedDestination,
  PreparedHttpSource,
  ProviderDestinationConfig,
  ProviderSourceConfig,
  R2ProviderConfig,
  S3CompatibleProviderConfig,
  S3ProviderConfig,
  SignedChunkRoute
} from "./models.js";

export type AnyS3CompatibleProviderConfig =
  | S3ProviderConfig
  | R2ProviderConfig
  | S3CompatibleProviderConfig;

export function nowIso(): string {
  return new Date().toISOString();
}

export function expiresAtIso(expiresIn: number): string {
  return new Date(Date.now() + expiresIn * 1000).toISOString();
}

export async function prepareProviderSource(
  source: ProviderSourceConfig,
  options: { index?: number; expiresIn?: number; fetchImpl?: typeof fetch; signal?: AbortSignal } = {}
): Promise<PreparedHttpSource> {
  const index = options.index ?? 0;
  const expiresIn = options.expiresIn ?? 3600;
  options.signal?.throwIfAborted();
  const baseFetch = options.fetchImpl ?? globalThis.fetch;
  const fetchImpl: typeof fetch = options.signal
    ? (input, init) => baseFetch(input, { ...init, signal: AbortSignal.any([options.signal!, ...(init?.signal ? [init.signal] : [])]) })
    : baseFetch;

  if (isS3CompatibleProvider(source)) {
    const endpoint = s3CompatibleEndpoint(source);
    const client = createS3CompatibleClient(source, endpoint);
    const head = await client.send(new HeadObjectCommand({ Bucket: source.bucket, Key: source.key }), { abortSignal: options.signal });
    const getUrl = await getSignedUrl(
      client,
      new GetObjectCommand({ Bucket: source.bucket, Key: source.key }),
      { expiresIn }
    );
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: source.provider,
      url: getUrl,
      size: Number(head.ContentLength ?? 0),
      filename: filename(source.key),
      expires_at: expiresAtIso(expiresIn),
      metadata: {
        ...s3CompatibleMetadata(source, endpoint),
        content_length: Number(head.ContentLength ?? 0),
        ...(head.ETag ? { etag: head.ETag } : {}),
        ...(head.LastModified ? { last_modified: head.LastModified.toISOString() } : {}),
        ...(head.VersionId ? { version_id: head.VersionId } : {})
      }
    };
  }

  if (isHippiusProvider(source)) {
    const baseUrl = source.base_url ?? "https://api.hippius.com";
    const size = await hippiusObjectSize(fetchImpl, baseUrl, source.api_token, source.bucket, source.key);
    const url = await hippiusPresign(fetchImpl, baseUrl, source.api_token, source.bucket, source.key, "get", expiresIn);
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: "hippius",
      url,
      size,
      filename: filename(source.key),
      expires_at: expiresAtIso(expiresIn),
      metadata: { bucket: source.bucket, key: source.key, base_url: baseUrl }
    };
  }

  if (isHuggingFaceProvider(source)) {
    const metadata = await huggingFaceFileMetadata(fetchImpl, source);
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: "huggingface",
      url: metadata.url,
      size: metadata.size,
      filename: filename(source.path),
      metadata: huggingFaceMetadata(source, metadata.etag, metadata.commitHash)
    };
  }

  return unsupportedProviderConfig(source);
}

export async function prepareProviderSourceForPlan(
  source: ProviderSourceConfig,
  options: { index?: number; fetchImpl?: typeof fetch } = {}
): Promise<PlanningHttpSource> {
  const index = options.index ?? 0;

  if (isS3CompatibleProvider(source)) {
    const endpoint = s3CompatibleEndpoint(source);
    const client = createS3CompatibleClient(source, endpoint);
    const head = await client.send(new HeadObjectCommand({ Bucket: source.bucket, Key: source.key }));
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: source.provider,
      size: Number(head.ContentLength ?? 0),
      filename: filename(source.key),
      metadata: {
        ...s3CompatibleMetadata(source, endpoint),
        content_length: Number(head.ContentLength ?? 0),
        ...(head.ETag ? { etag: head.ETag } : {}),
        ...(head.LastModified ? { last_modified: head.LastModified.toISOString() } : {}),
        ...(head.VersionId ? { version_id: head.VersionId } : {})
      }
    };
  }

  if (isHippiusProvider(source)) {
    const fetchImpl = options.fetchImpl ?? globalThis.fetch;
    const baseUrl = source.base_url ?? "https://api.hippius.com";
    const size = await hippiusObjectSize(fetchImpl, baseUrl, source.api_token, source.bucket, source.key);
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: "hippius",
      size,
      filename: filename(source.key),
      metadata: { bucket: source.bucket, key: source.key, base_url: baseUrl }
    };
  }

  if (isHuggingFaceProvider(source)) {
    const metadata = await huggingFaceFileMetadata(options.fetchImpl ?? globalThis.fetch, source);
    return {
      source_id: source.id ?? `src_${index}`,
      type: "http",
      provider: "huggingface",
      size: metadata.size,
      filename: filename(source.path),
      metadata: huggingFaceMetadata(source, metadata.etag, metadata.commitHash)
    };
  }

  return unsupportedProviderConfig(source);
}

export function prepareProviderDestination(
  destination: ProviderDestinationConfig,
  options: { index?: number } = {}
): PreparedDestination {
  const index = options.index ?? 0;
  const destinationId = destination.id ?? `dst_${index}`;

  if (isS3CompatibleProvider(destination)) {
    const endpoint = s3CompatibleEndpoint(destination);
    return {
      destination_id: destinationId,
      provider: destination.provider,
      logical_prefix: destination.key,
      metadata: {
        ...s3CompatibleMetadata(destination, endpoint)
      }
    };
  }

  if (isHippiusProvider(destination)) {
    return {
      destination_id: destinationId,
      provider: "hippius",
      logical_prefix: destination.key.replace(/\/+$/, ""),
      metadata: {
        bucket: destination.bucket,
        key: destination.key,
        base_url: destination.base_url ?? "https://api.hippius.com"
      }
    };
  }

  if (isHuggingFaceProvider(destination)) {
    return {
      destination_id: destinationId,
      provider: "huggingface",
      logical_prefix: destination.path,
      metadata: huggingFaceMetadata(destination)
    };
  }

  return unsupportedProviderConfig(destination);
}

export async function createMultipartUpload(input: {
  destination: ProviderDestinationConfig;
  objectKey: string;
  metadata: Record<string, string>;
  signal?: AbortSignal;
}): Promise<string> {
  if (isS3CompatibleProvider(input.destination)) {
    const client = createS3CompatibleClient(input.destination);
    const response = await client.send(new CreateMultipartUploadCommand({
      Bucket: input.destination.bucket,
      Key: input.objectKey,
      Metadata: input.metadata
    }), { abortSignal: input.signal });
    if (!response.UploadId) {
      throw new Error(`provider did not return UploadId for ${input.objectKey}`);
    }
    return response.UploadId;
  }
  throw new Error(`multipart upload is not supported for ${input.destination.provider}`);
}

export async function signFinalObjectHead(
  destination: ProviderDestinationConfig,
  objectKey: string,
  expiresIn: number
): Promise<string> {
  if (isS3CompatibleProvider(destination)) {
    const client = createS3CompatibleClient(destination);
    return getSignedUrl(
      client,
      new HeadObjectCommand({ Bucket: destination.bucket, Key: objectKey }),
      { expiresIn }
    );
  }
  throw new Error(`final object HEAD signing is not supported for ${destination.provider}`);
}

export async function signCompleteMultipartUpload(
  destination: ProviderDestinationConfig,
  objectKey: string,
  uploadId: string,
  expiresIn: number
): Promise<string> {
  if (isS3CompatibleProvider(destination)) {
    const client = createS3CompatibleClient(destination);
    return getSignedUrl(
      client,
      new CompleteMultipartUploadCommand({ Bucket: destination.bucket, Key: objectKey, UploadId: uploadId }),
      { expiresIn }
    );
  }
  throw new Error(`multipart completion is not supported for ${destination.provider}`);
}

export async function signAbortMultipartUpload(
  destination: ProviderDestinationConfig,
  objectKey: string,
  uploadId: string,
  expiresIn: number
): Promise<string> {
  if (isS3CompatibleProvider(destination)) {
    const client = createS3CompatibleClient(destination);
    return getSignedUrl(
      client,
      new AbortMultipartUploadCommand({ Bucket: destination.bucket, Key: objectKey, UploadId: uploadId }),
      { expiresIn }
    );
  }
  throw new Error(`multipart abort is not supported for ${destination.provider}`);
}

export async function signListMultipartUpload(
  destination: ProviderDestinationConfig,
  objectKey: string,
  uploadId: string,
  expiresIn: number,
  options: { maxParts?: number; partNumberMarker?: number } = {}
): Promise<string> {
  if (isS3CompatibleProvider(destination)) {
    const client = createS3CompatibleClient(destination);
    return getSignedUrl(
      client,
      new ListPartsCommand({
        Bucket: destination.bucket,
        Key: objectKey,
        UploadId: uploadId,
        MaxParts: options.maxParts,
        PartNumberMarker: options.partNumberMarker === undefined ? undefined : String(options.partNumberMarker)
      }),
      { expiresIn }
    );
  }
  throw new Error(`multipart list-parts is not supported for ${destination.provider}`);
}

export async function abortMultipartUpload(
  destination: ProviderDestinationConfig,
  objectKey: string,
  uploadId: string,
  signal?: AbortSignal
): Promise<void> {
  if (isS3CompatibleProvider(destination)) {
    await createS3CompatibleClient(destination).send(
      new AbortMultipartUploadCommand({ Bucket: destination.bucket, Key: objectKey, UploadId: uploadId }),
      { abortSignal: signal }
    );
    return;
  }
  throw new Error(`multipart abort is not supported for ${destination.provider}`);
}

/** Provider metadata only. These operations never read or proxy object bytes. */
export async function listMultipartParts(destination: ProviderDestinationConfig, objectKey: string, uploadId: string, signal?: AbortSignal) {
  if (!isS3CompatibleProvider(destination)) return unsupportedProviderConfig(destination);
  const parts: { partNumber: number; etag: string; size: number }[] = [];
  let marker: string | undefined;
  do {
    const page = await createS3CompatibleClient(destination).send(new ListPartsCommand({
      Bucket: destination.bucket, Key: objectKey, UploadId: uploadId, MaxParts: 1000, PartNumberMarker: marker
    }), { abortSignal: signal });
    for (const part of page.Parts ?? []) {
      if (!part.PartNumber || !part.ETag || !Number.isSafeInteger(part.Size)) throw new Error("invalid multipart part metadata");
      parts.push({ partNumber: part.PartNumber, etag: part.ETag, size: part.Size! });
    }
    const next = page.IsTruncated ? page.NextPartNumberMarker : undefined;
    if (page.IsTruncated && (!next || next === marker)) throw new Error("invalid multipart pagination");
    marker = next;
  } while (marker);
  return parts;
}

export async function completeMultipartUpload(input: {
  destination: ProviderDestinationConfig; objectKey: string; uploadId: string;
  parts: { partNumber: number; etag: string }[];
  signal?: AbortSignal;
}) {
  if (!isS3CompatibleProvider(input.destination)) return unsupportedProviderConfig(input.destination);
  const result = await createS3CompatibleClient(input.destination).send(new CompleteMultipartUploadCommand({
    Bucket: input.destination.bucket, Key: input.objectKey, UploadId: input.uploadId,
    MultipartUpload: { Parts: [...input.parts].sort((a, b) => a.partNumber - b.partNumber)
      .map((part) => ({ PartNumber: part.partNumber, ETag: part.etag })) }
  }), { abortSignal: input.signal });
  return { etag: result.ETag, versionId: result.VersionId };
}

export async function inspectDestinationObject(destination: ProviderDestinationConfig, objectKey: string, signal?: AbortSignal) {
  if (!isS3CompatibleProvider(destination)) return unsupportedProviderConfig(destination);
  const result = await createS3CompatibleClient(destination).send(new HeadObjectCommand({ Bucket: destination.bucket, Key: objectKey }), { abortSignal: signal });
  return { size: result.ContentLength, etag: result.ETag, versionId: result.VersionId, metadata: result.Metadata ?? {} };
}

export async function signDestinationRoute(input: {
  chunk: ChunkSigningPlanItem;
  target: ChunkDestinationSigningTarget;
  source?: ProviderSourceConfig;
  destination: ProviderDestinationConfig;
  expiresIn: number;
  partNumber?: number;
  uploadId?: string;
  completeUrl?: string;
  abortUrl?: string;
  listPageUrl?: string;
  finalHeadUrl?: string;
  finalObjectKey?: string;
  expectedObjectSize?: number;
  expectedPartCount?: number;
  maxPartNumber?: number;
  finalObjectMetadata?: Record<string, string>;
  multipartGroupId?: string;
  /** Pre-issued destination URL, used by providers that sign their own upload targets. */
  destUrl?: string;
  fetchImpl?: typeof fetch;
}): Promise<SignedChunkRoute> {
  const targetObjectKey = input.target.object_key;
  if (!targetObjectKey) {
    throw new Error("destination signing target is missing object_key");
  }
  const [destUrl, sourceRoute] = await Promise.all([
    input.destUrl ?? signDestinationUrl({
      destination: input.destination,
      objectKey: targetObjectKey,
      uploadId: input.uploadId,
      partNumber: input.partNumber,
      expiresIn: input.expiresIn,
      fetchImpl: input.fetchImpl,
    }),
    signSourceRoute({
      source: input.source,
      fallbackUrl: input.chunk.source_url,
      chunk: input.chunk,
      expiresIn: input.expiresIn,
      fetchImpl: input.fetchImpl
    })
  ]);

  return {
    source_id: input.chunk.source_id,
    destination_id: input.target.destination_id,
    chunk_index: input.chunk.chunk_index,
    source_url: sourceRoute.url,
    dest_url: destUrl,
    source_offset: input.chunk.source_offset,
    chunk_size: input.chunk.chunk_size,
    expires_at: expiresAtIso(input.expiresIn),
    headers: sourceRoute.headers,
    metadata: {
      ...(input.target.metadata ?? {}),
      ...(input.multipartGroupId ? { multipart_group_id: input.multipartGroupId } : {}),
      ...(input.uploadId ? { upload_id: input.uploadId } : {}),
      ...(input.completeUrl ? { complete_url: input.completeUrl } : {}),
      ...(input.abortUrl ? { abort_url: input.abortUrl } : {}),
      ...(input.listPageUrl ? { list_page_url: input.listPageUrl } : {}),
      ...(input.finalHeadUrl ? { final_head_url: input.finalHeadUrl } : {}),
      ...(input.finalObjectKey ? { final_object_key: input.finalObjectKey } : {}),
      ...(input.expectedObjectSize !== undefined ? { expected_object_size: input.expectedObjectSize } : {}),
      ...(input.expectedPartCount !== undefined ? { expected_part_count: input.expectedPartCount } : {}),
      ...(input.maxPartNumber !== undefined ? { max_part_number: input.maxPartNumber } : {}),
      ...(input.finalObjectMetadata ? { final_object_metadata: input.finalObjectMetadata } : {}),
      ...(input.partNumber ? { part_number: input.partNumber } : {})
    }
  };
}

function rangeHeaderForChunk(chunk: Pick<ChunkSigningPlanItem, "source_offset" | "chunk_size">): string {
  return rangeHeaderForRange(chunk.source_offset, chunk.chunk_size);
}

function rangeHeaderForRange(sourceOffset: number, chunkSize: number): string {
  const start = Math.max(0, Math.floor(sourceOffset));
  const size = Math.max(1, Math.floor(chunkSize));
  return `bytes=${start}-${start + size - 1}`;
}

export async function signSourceReadRange(input: {
  source: ProviderSourceConfig;
  ifMatch?: string;
  versionId?: string;
  offset: number;
  length: number;
  expiresIn: number;
  fetchImpl?: typeof fetch;
}): Promise<{ url: string; headers: Record<string, string> }> {
  const range = rangeHeaderForRange(input.offset, input.length);
  if (isS3CompatibleProvider(input.source)) {
    const client = createS3CompatibleClient(input.source);
    return {
      url: await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: input.source.bucket,
          Key: input.source.key,
          Range: range,
          IfMatch: input.ifMatch,
          VersionId: input.versionId
        }),
        { expiresIn: input.expiresIn }
      ),
      headers: { Range: range, ...(input.ifMatch ? { "If-Match": input.ifMatch } : {}) }
    };
  }

  if (input.ifMatch || input.versionId) throw new Error("conditional source ranges require S3-compatible storage");

  if (isHippiusProvider(input.source)) {
    return {
      url: await hippiusPresign(
        input.fetchImpl ?? globalThis.fetch,
        input.source.base_url ?? "https://api.hippius.com",
        input.source.api_token,
        input.source.bucket,
        input.source.key,
        "get",
        input.expiresIn
      ),
      headers: { Range: range }
    };
  }

  if (isHuggingFaceProvider(input.source)) {
    const metadata = await huggingFaceFileMetadata(input.fetchImpl ?? globalThis.fetch, input.source);
    return { url: metadata.url, headers: { Range: range } };
  }

  return unsupportedProviderConfig(input.source);
}

export async function signDestinationReadRange(input: {
  destination: ProviderDestinationConfig;
  objectKey: string;
  offset: number;
  length: number;
  expiresIn: number;
  fetchImpl?: typeof fetch;
}): Promise<{ url: string; headers: Record<string, string> }> {
  const range = rangeHeaderForRange(input.offset, input.length);
  if (isS3CompatibleProvider(input.destination)) {
    const client = createS3CompatibleClient(input.destination);
    return {
      url: await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: input.destination.bucket,
          Key: input.objectKey,
          Range: range
        }),
        { expiresIn: input.expiresIn }
      ),
      headers: { Range: range }
    };
  }

  if (isHippiusProvider(input.destination)) {
    return {
      url: await hippiusPresign(
        input.fetchImpl ?? globalThis.fetch,
        input.destination.base_url ?? "https://api.hippius.com",
        input.destination.api_token,
        input.destination.bucket,
        input.objectKey,
        "get",
        input.expiresIn
      ),
      headers: { Range: range }
    };
  }

  if (isHuggingFaceProvider(input.destination)) {
    // Read-back resolves the committed file. Before the commit lands the bytes exist only as
    // uncommitted LFS parts, which the Hub does not expose.
    const metadata = await huggingFaceFileMetadata(input.fetchImpl ?? globalThis.fetch, {
      ...input.destination,
      path: input.objectKey
    });
    return { url: metadata.url, headers: { Range: range } };
  }

  return unsupportedProviderConfig(input.destination);
}

async function signSourceRoute(input: {
  source?: ProviderSourceConfig;
  fallbackUrl: string;
  chunk: ChunkSigningPlanItem;
  expiresIn: number;
  fetchImpl?: typeof fetch;
}): Promise<{ url: string; headers: Record<string, string> }> {
  const range = rangeHeaderForChunk(input.chunk);
  if (!input.source) {
    return { url: input.fallbackUrl, headers: { Range: range } };
  }

  if (isS3CompatibleProvider(input.source)) {
    const client = createS3CompatibleClient(input.source);
    return {
      url: await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: input.source.bucket,
          Key: input.source.key,
          Range: range
        }),
        { expiresIn: input.expiresIn }
      ),
      headers: { Range: range }
    };
  }

  if (isHippiusProvider(input.source)) {
    return {
      url: await hippiusPresign(
        input.fetchImpl ?? globalThis.fetch,
        input.source.base_url ?? "https://api.hippius.com",
        input.source.api_token,
        input.source.bucket,
        input.source.key,
        "get",
        input.expiresIn
      ),
      headers: { Range: range }
    };
  }

  if (isHuggingFaceProvider(input.source)) {
    const metadata = await huggingFaceFileMetadata(input.fetchImpl ?? globalThis.fetch, input.source);
    return { url: metadata.url, headers: { Range: range } };
  }

  return unsupportedProviderConfig(input.source);
}

export async function signDestinationUrl(input: {
  contentMd5?: string;
  destination: ProviderDestinationConfig;
  objectKey: string;
  uploadId?: string;
  partNumber?: number;
  expiresIn: number;
  fetchImpl?: typeof fetch;
}): Promise<string> {
  if (isS3CompatibleProvider(input.destination)) {
    const client = createS3CompatibleClient(input.destination);
    if (input.uploadId && input.partNumber) {
      return getSignedUrl(
        client,
        new UploadPartCommand({
          Bucket: input.destination.bucket,
          Key: input.objectKey,
          UploadId: input.uploadId,
          PartNumber: input.partNumber,
          ContentMD5: input.contentMd5
        }),
        { expiresIn: input.expiresIn }
      );
    }
    return getSignedUrl(
      client,
      new PutObjectCommand({ Bucket: input.destination.bucket, Key: input.objectKey, ContentMD5: input.contentMd5 }),
      { expiresIn: input.expiresIn }
    );
  }

  if (input.contentMd5) throw new Error("checksum-bound uploads require S3-compatible storage");

  if (isHippiusProvider(input.destination)) {
    return hippiusPresign(
      input.fetchImpl ?? globalThis.fetch,
      input.destination.base_url ?? "https://api.hippius.com",
      input.destination.api_token,
      input.destination.bucket,
      input.objectKey,
      "put",
      input.expiresIn
    );
  }

  return unsupportedProviderConfig(input.destination);
}

const s3ClientCache = new Map<string, S3Client>();

function s3ClientCacheKey(source: AnyS3CompatibleProviderConfig, endpoint: string | undefined): string {
  return JSON.stringify({
    provider: source.provider,
    endpoint,
    region: s3CompatibleRegion(source),
    forcePathStyle: s3CompatibleForcePathStyle(source, endpoint),
    accessKeyId: source.access_key_id,
    secretAccessKey: source.secret_access_key,
    sessionToken: "session_token" in source ? source.session_token : undefined
  });
}
function createS3CompatibleClient(
  source: AnyS3CompatibleProviderConfig,
  endpoint = s3CompatibleEndpoint(source)
): S3Client {
  const cacheKey = s3ClientCacheKey(source, endpoint);
  const cached = s3ClientCache.get(cacheKey);
  if (cached) return cached;
  const client = new S3Client({
    region: s3CompatibleRegion(source),
    endpoint,
    forcePathStyle: s3CompatibleForcePathStyle(source, endpoint),
    maxAttempts: 5,
    credentials: {
      accessKeyId: source.access_key_id,
      secretAccessKey: source.secret_access_key,
      sessionToken: "session_token" in source ? source.session_token : undefined
    }
  });
  s3ClientCache.set(cacheKey, client);
  return client;
}

export function isS3CompatibleProvider(
  config: ProviderSourceConfig | ProviderDestinationConfig
): config is AnyS3CompatibleProviderConfig {
  if (config.provider === "s3" || config.provider === "r2") {
    return true;
  }
  if ("driver" in config && config.driver === "s3-compatible") {
    return true;
  }
  return "access_key_id" in config && "secret_access_key" in config;
}

export function s3CompatibleEndpoint(source: AnyS3CompatibleProviderConfig): string | undefined {
  if (hasText(source.endpoint_url)) {
    return source.endpoint_url;
  }

  if (source.provider === "r2") {
    if ("account_id" in source && hasText(source.account_id)) {
      return `https://${source.account_id}.r2.cloudflarestorage.com`;
    }
    throw new Error("r2 config requires account_id or endpoint_url.");
  }

  if (source.provider !== "s3") {
    throw new Error(`${source.provider} config requires endpoint_url.`);
  }

  return undefined;
}

export function s3CompatibleRegion(source: AnyS3CompatibleProviderConfig): string {
  if ("region" in source && hasText(source.region)) {
    return source.region;
  }

  if (source.provider === "r2") {
    return "auto";
  }

  return "us-east-1";
}

export function s3CompatibleForcePathStyle(
  source: AnyS3CompatibleProviderConfig,
  endpoint = s3CompatibleEndpoint(source)
): boolean | undefined {
  if ("force_path_style" in source && typeof source.force_path_style === "boolean") {
    return source.force_path_style;
  }

  if (source.provider === "s3") {
    return undefined;
  }

  return Boolean(endpoint);
}

function s3CompatibleMetadata(
  source: AnyS3CompatibleProviderConfig,
  endpoint = s3CompatibleEndpoint(source)
): Record<string, unknown> {
  return compactMetadata({
    driver: "s3-compatible",
    bucket: source.bucket,
    key: source.key,
    region: s3CompatibleRegion(source),
    endpoint_url: endpoint,
    account_id: "account_id" in source ? source.account_id : undefined
  });
}

function isHippiusProvider(
  config: ProviderSourceConfig | ProviderDestinationConfig
): config is HippiusProviderConfig {
  return config.provider === "hippius" && "api_token" in config;
}

export function isHuggingFaceProvider(
  config: ProviderSourceConfig | ProviderDestinationConfig
): config is HuggingFaceProviderConfig {
  return config.provider === "huggingface" && "repo_id" in config;
}

function huggingFaceMetadata(
  config: HuggingFaceProviderConfig,
  etag?: string,
  commitHash?: string
): Record<string, unknown> {
  return compactMetadata({
    driver: "huggingface",
    repo_id: config.repo_id,
    repo_type: huggingFaceRepoType(config),
    revision: huggingFaceRevision(config),
    path: config.path,
    endpoint: huggingFaceEndpoint(config),
    // For an LFS blob the Hub's linked ETag is the object's sha256.
    sha256: etag,
    commit_hash: commitHash
  });
}

async function hippiusPresign(
  fetchImpl: typeof fetch,
  baseUrl: string,
  token: string,
  bucket: string,
  key: string,
  action: "get" | "put",
  expiresIn: number
): Promise<string> {
  const url = new URL(`/api/objectstore/buckets/${bucket}/presigned-url/`, baseUrl);
  url.searchParams.set("key", key);
  url.searchParams.set("action", action);
  url.searchParams.set("expires_in", String(expiresIn));
  const response = await fetchImpl(url, { headers: { Authorization: `Token ${token}` } });
  if (!response.ok) {
    throw new Error(`Hippius presigned URL failed status=${response.status}`);
  }
  return String((await response.json()).url);
}

async function hippiusObjectSize(
  fetchImpl: typeof fetch,
  baseUrl: string,
  token: string,
  bucket: string,
  key: string
): Promise<number> {
  const url = new URL(`/api/objectstore/buckets/${bucket}/objects/`, baseUrl);
  url.searchParams.set("prefix", key);
  url.searchParams.set("max_keys", "1");
  const response = await fetchImpl(url, { headers: { Authorization: `Token ${token}` } });
  if (!response.ok) {
    throw new Error(`Hippius object lookup failed status=${response.status}`);
  }
  const payload = await response.json();
  const contents = payload.Contents ?? payload.contents ?? [];
  if (!contents.length) {
    throw new Error("Hippius object not found");
  }
  return Number(contents[0].Size ?? contents[0].size);
}

function filename(key: string): string {
  return key.split("/").filter(Boolean).at(-1) ?? key;
}

function compactMetadata(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
}

function hasText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function unsupportedProviderConfig(value: unknown): never {
  throw new Error(`Unsupported provider config: ${JSON.stringify(value)}`);
}

function copySourceHeader(destination: AnyS3CompatibleProviderConfig, objectKey: string): string {
  return `/${destination.bucket}/${objectKey.split("/").map(encodeURIComponent).join("/")}`;
}
