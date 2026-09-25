import type { HuggingFaceProviderConfig, HuggingFaceRepoType } from "./models.js";

export const HUGGINGFACE_DEFAULT_ENDPOINT = "https://huggingface.co";
export const HUGGINGFACE_DEFAULT_REVISION = "main";
export const HUGGINGFACE_DEFAULT_REPO_TYPE: HuggingFaceRepoType = "model";

const LFS_CONTENT_TYPE = "application/vnd.git-lfs+json";

/** Mirrors `constants.REPO_TYPES_URL_PREFIXES`; models carry no prefix. */
const REPO_TYPE_URL_PREFIXES: Record<HuggingFaceRepoType, string> = {
  model: "",
  dataset: "datasets/",
  space: "spaces/",
  kernel: "kernels/",
  bucket: "buckets/"
};

export interface HuggingFaceFileMetadata {
  /** Credential-free presigned CDN URL the workers read from. */
  url: string;
  size: number;
  /** sha256 for an LFS blob, git sha1 otherwise. */
  etag?: string;
  commitHash?: string;
}

export interface HuggingFaceLfsUploadPlan {
  oid: string;
  size: number;
  /** Absent when the Hub already stores this content and no upload is needed. */
  upload?: {
    /** Completion endpoint for multipart, or the single-part PUT target. */
    href: string;
    /** Part size the Hub requires. Absent for a single-part upload. */
    chunkSize?: number;
    /** Presigned part PUT URLs, ordered by part number. Empty for a single-part upload. */
    partUrls: string[];
  };
  verifyHref?: string;
}

export function huggingFaceEndpoint(config: HuggingFaceProviderConfig): string {
  return (config.endpoint ?? HUGGINGFACE_DEFAULT_ENDPOINT).replace(/\/+$/, "");
}

export function huggingFaceRepoType(config: HuggingFaceProviderConfig): HuggingFaceRepoType {
  return config.repo_type ?? HUGGINGFACE_DEFAULT_REPO_TYPE;
}

export function huggingFaceRevision(config: HuggingFaceProviderConfig): string {
  return config.revision ?? HUGGINGFACE_DEFAULT_REVISION;
}

/**
 * `{endpoint}/{prefix}{repo_id}/resolve/{revision}/{path}`, as built by `hf_hub_url`.
 *
 * Buckets are unversioned and take no revision segment, and the Hub escapes their whole key
 * as one component — see `HfApi.get_bucket_file_metadata`.
 */
export function huggingFaceResolveUrl(config: HuggingFaceProviderConfig): string {
  const repoType = huggingFaceRepoType(config);
  const prefix = REPO_TYPE_URL_PREFIXES[repoType];
  if (repoType === "bucket") {
    return `${huggingFaceEndpoint(config)}/${prefix}${config.repo_id}/resolve/${encodeURIComponent(config.path)}`;
  }
  const revision = encodeURIComponent(huggingFaceRevision(config));
  return `${huggingFaceEndpoint(config)}/${prefix}${config.repo_id}/resolve/${revision}/${encodePath(config.path)}`;
}

/** `{endpoint}/api/{repo_type}s/{repo_id}`. */
export function huggingFaceApiBase(config: HuggingFaceProviderConfig): string {
  return `${huggingFaceEndpoint(config)}/api/${huggingFaceRepoType(config)}s/${config.repo_id}`;
}

/** `{endpoint}/{prefix}{repo_id}.git/info/lfs/objects/batch`. */
export function huggingFaceLfsBatchUrl(config: HuggingFaceProviderConfig): string {
  const prefix = REPO_TYPE_URL_PREFIXES[huggingFaceRepoType(config)];
  return `${huggingFaceEndpoint(config)}/${prefix}${config.repo_id}.git/info/lfs/objects/batch`;
}

/**
 * HEAD the resolve URL and require the Hub to redirect to its CDN. The redirect target is
 * presigned and carries no credential, so it is the only form of this URL that may be handed
 * to BeamCore and the workers.
 */
export async function huggingFaceFileMetadata(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig
): Promise<HuggingFaceFileMetadata> {
  const resolveUrl = huggingFaceResolveUrl(config);
  const response = await fetchImpl(resolveUrl, {
    method: "HEAD",
    redirect: "manual",
    headers: {
      Authorization: `Bearer ${config.token}`,
      // Compression would report a transformed length instead of the real object size.
      "Accept-Encoding": "identity"
    }
  });

  if (response.status >= 400) {
    throw new Error(
      `Hugging Face file lookup failed status=${response.status} for ${describe(config)}`
    );
  }

  const location = response.headers.get("location");
  if (!location || sameHost(resolveUrl, location)) {
    throw new Error(
      `Hugging Face did not redirect ${describe(config)} to a presigned CDN URL. `
        + "Beam reads this file over plain HTTP without forwarding your token, and the Hub only "
        + "redirects for large-file (LFS or Xet) content. A small regular file is served inline "
        + "from the Hub instead and cannot be transferred."
    );
  }

  const linkedSize = response.headers.get("x-linked-size");
  const size = Number(linkedSize ?? response.headers.get("content-length") ?? Number.NaN);
  if (!Number.isFinite(size) || size <= 0) {
    throw new Error(`Hugging Face did not report a size for ${describe(config)}`);
  }

  return {
    url: location,
    size,
    etag: normalizeEtag(response.headers.get("x-linked-etag") ?? response.headers.get("etag")),
    commitHash: response.headers.get("x-repo-commit") ?? undefined
  };
}

/**
 * Ask the Hub whether a path is stored as an LFS blob or as a regular git blob.
 * `sample` is the base64 of the first 512 bytes, exactly as `_fetch_upload_modes` sends it.
 */
export async function huggingFacePreupload(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig,
  input: { size: number; sample: string }
): Promise<{ uploadMode: "lfs" | "regular"; shouldIgnore: boolean; oid?: string }> {
  const url = new URL(`${huggingFaceApiBase(config)}/preupload/${encodeURIComponent(huggingFaceRevision(config))}`);
  if (config.create_pr) url.searchParams.set("create_pr", "1");

  const response = await fetchImpl(url, {
    method: "POST",
    headers: jsonHeaders(config),
    body: JSON.stringify({ files: [{ path: config.path, sample: input.sample, size: input.size }] })
  });
  const payload = await readJson(response, "preupload", config);

  const file = Array.isArray(payload.files) ? payload.files[0] : undefined;
  if (!file || (file.uploadMode !== "lfs" && file.uploadMode !== "regular")) {
    throw new Error(`Hugging Face preupload returned a malformed response for ${describe(config)}`);
  }
  return {
    uploadMode: file.uploadMode,
    shouldIgnore: Boolean(file.shouldIgnore),
    oid: typeof file.oid === "string" ? file.oid : undefined
  };
}

/**
 * Request upload instructions for one object. The Hub answers with either a single-part PUT
 * target or a completion endpoint plus one presigned PUT URL per part.
 */
export async function huggingFaceLfsBatch(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig,
  input: { oid: string; size: number }
): Promise<HuggingFaceLfsUploadPlan> {
  const response = await fetchImpl(huggingFaceLfsBatchUrl(config), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      Accept: LFS_CONTENT_TYPE,
      "Content-Type": LFS_CONTENT_TYPE
    },
    body: JSON.stringify({
      operation: "upload",
      transfers: ["basic", "multipart"],
      hash_algo: "sha256",
      ref: { name: huggingFaceRevision(config) },
      objects: [{ oid: input.oid, size: input.size }]
    })
  });
  const payload = await readJson(response, "LFS batch", config);

  const object = Array.isArray(payload.objects) ? payload.objects[0] : undefined;
  if (!object || typeof object.oid !== "string") {
    throw new Error(`Hugging Face LFS batch returned a malformed response for ${describe(config)}`);
  }
  if (object.error) {
    throw new Error(
      `Hugging Face LFS batch rejected ${describe(config)}: ${object.error.message ?? "unknown error"}`
    );
  }

  const uploadAction = object.actions?.upload;
  if (!uploadAction) {
    // No actions means the Hub already stores this content; only the commit is left to do.
    return { oid: object.oid, size: input.size };
  }
  if (typeof uploadAction.href !== "string") {
    throw new Error(`Hugging Face LFS batch returned no upload href for ${describe(config)}`);
  }

  const header: Record<string, string> = uploadAction.header ?? {};
  const rawChunkSize = header.chunk_size;
  const chunkSize = rawChunkSize === undefined ? undefined : Number(rawChunkSize);
  if (chunkSize !== undefined && (!Number.isInteger(chunkSize) || chunkSize <= 0)) {
    throw new Error(
      `Hugging Face LFS batch returned a malformed chunk_size '${rawChunkSize}' for ${describe(config)}`
    );
  }

  const partUrls = Object.entries(header)
    .filter(([key]) => /^\d+$/.test(key))
    .sort(([left], [right]) => Number(left) - Number(right))
    .map(([, value]) => value);

  if (chunkSize !== undefined && partUrls.length !== Math.ceil(input.size / chunkSize)) {
    throw new Error(
      `Hugging Face returned ${partUrls.length} part URLs for ${describe(config)}, `
        + `expected ${Math.ceil(input.size / chunkSize)} at chunk_size ${chunkSize}`
    );
  }

  return {
    oid: object.oid,
    size: input.size,
    upload: { href: uploadAction.href, chunkSize, partUrls },
    verifyHref: typeof object.actions?.verify?.href === "string" ? object.actions.verify.href : undefined
  };
}

/** Close a multipart LFS upload: `{oid, parts:[{partNumber, etag}]}` to the completion href. */
export async function huggingFaceCompleteLfsUpload(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig,
  input: { href: string; oid: string; etags: string[] }
): Promise<void> {
  const response = await fetchImpl(input.href, {
    method: "POST",
    headers: { Accept: LFS_CONTENT_TYPE, "Content-Type": LFS_CONTENT_TYPE },
    body: JSON.stringify({
      oid: input.oid,
      parts: input.etags.map((etag, index) => ({ partNumber: index + 1, etag }))
    })
  });
  await assertOk(response, "LFS completion", config);
}

/** Optional server-side check that the object landed intact. */
export async function huggingFaceVerifyLfsUpload(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig,
  input: { href: string; oid: string; size: number }
): Promise<void> {
  const response = await fetchImpl(input.href, {
    method: "POST",
    headers: jsonHeaders(config),
    body: JSON.stringify({ oid: input.oid, size: input.size })
  });
  await assertOk(response, "LFS verify", config);
}

/** Publish the uploaded blob as a commit. The body is NDJSON: a header line then one file line. */
export async function huggingFaceCommit(
  fetchImpl: typeof fetch,
  config: HuggingFaceProviderConfig,
  input: { oid: string; size: number }
): Promise<{ commitOid?: string; pullRequestUrl?: string }> {
  const url = new URL(`${huggingFaceApiBase(config)}/commit/${encodeURIComponent(huggingFaceRevision(config))}`);
  if (config.create_pr) url.searchParams.set("create_pr", "1");

  const lines = [
    {
      key: "header",
      value: {
        summary: config.commit_message ?? `Upload ${config.path} with Beam`,
        description: config.commit_description ?? ""
      }
    },
    {
      key: "lfsFile",
      value: { path: config.path, algo: "sha256", oid: input.oid, size: input.size }
    }
  ];

  const response = await fetchImpl(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${config.token}`, "Content-Type": "application/x-ndjson" },
    body: lines.map((line) => JSON.stringify(line)).join("\n")
  });
  const payload = await readJson(response, "commit", config);
  return {
    commitOid: typeof payload.commitOid === "string" ? payload.commitOid : undefined,
    pullRequestUrl: typeof payload.pullRequestUrl === "string" ? payload.pullRequestUrl : undefined
  };
}

/** Read the first `length` bytes of a URL and return them base64-encoded, for `preupload`. */
export async function readSourceSample(
  fetchImpl: typeof fetch,
  url: string,
  length = 512
): Promise<string> {
  const response = await fetchImpl(url, { headers: { Range: `bytes=0-${length - 1}` } });
  if (!response.ok) {
    throw new Error(`source sample read failed status=${response.status}`);
  }
  return base64(new Uint8Array(await response.arrayBuffer()));
}

/**
 * Stream a URL once and return the sha256 of the whole body plus, when `partSize` is given,
 * the hex MD5 of every part. The Hub will not issue upload URLs without the sha256, and the
 * per-part MD5 is the ETag the completion payload has to quote.
 */
export async function hashSourceStream(
  fetchImpl: typeof fetch,
  url: string,
  options: { partSize?: number; sha256?: boolean } = {}
): Promise<{ sha256?: string; partEtags: string[] }> {
  const { createHash } = await import("node:crypto");
  const response = await fetchImpl(url);
  if (!response.ok || !response.body) {
    throw new Error(`source hash read failed status=${response.status}`);
  }

  const whole = options.sha256 === false ? null : createHash("sha256");
  const partSize = options.partSize;
  const partEtags: string[] = [];
  let part = partSize ? createHash("md5") : null;
  let partBytes = 0;

  for await (const rawChunk of response.body as unknown as AsyncIterable<Uint8Array>) {
    let chunk = rawChunk;
    whole?.update(chunk);
    while (partSize && part && chunk.length > 0) {
      const room = partSize - partBytes;
      if (chunk.length < room) {
        part.update(chunk);
        partBytes += chunk.length;
        break;
      }
      part.update(chunk.subarray(0, room));
      partEtags.push(part.digest("hex"));
      part = createHash("md5");
      partBytes = 0;
      chunk = chunk.subarray(room);
    }
  }
  if (partSize && part && partBytes > 0) {
    partEtags.push(part.digest("hex"));
  }

  return { sha256: whole?.digest("hex"), partEtags };
}

function encodePath(path: string): string {
  return path.split("/").map(encodeURIComponent).join("/");
}

function sameHost(left: string, right: string): boolean {
  try {
    return new URL(left).host === new URL(right, left).host;
  } catch {
    return true;
  }
}

function normalizeEtag(value: string | null): string | undefined {
  if (!value) return undefined;
  return value.replace(/^W\//, "").replace(/^"|"$/g, "");
}

function jsonHeaders(config: HuggingFaceProviderConfig): Record<string, string> {
  return { Authorization: `Bearer ${config.token}`, "Content-Type": "application/json" };
}

async function readJson(
  response: Response,
  step: string,
  config: HuggingFaceProviderConfig
): Promise<Record<string, any>> {
  await assertOk(response, step, config);
  return (await response.json()) as Record<string, any>;
}

async function assertOk(
  response: Response,
  step: string,
  config: HuggingFaceProviderConfig
): Promise<void> {
  if (response.ok) return;
  const detail = (await response.text().catch(() => "")).slice(0, 512);
  throw new Error(
    `Hugging Face ${step} failed status=${response.status} for ${describe(config)}${detail ? `: ${detail}` : ""}`
  );
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return typeof btoa === "function" ? btoa(binary) : Buffer.from(bytes).toString("base64");
}

export function describe(config: HuggingFaceProviderConfig): string {
  return `${huggingFaceRepoType(config)} ${config.repo_id}@${huggingFaceRevision(config)}/${config.path}`;
}
