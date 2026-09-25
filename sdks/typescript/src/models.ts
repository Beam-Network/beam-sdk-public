export type SignedUrlFlow = "signed_url";

export interface BeamClientOptions {
  apiKey?: string;
  natsUrl?: string;
  natsWsUrl?: string;
  environment?: string;
  transferClientSubjectPrefix?: string;
  transferRuntimeShardCount?: number;
  requestTimeoutMs?: number;
  maxPayloadBytes?: number;
  routeSigningConcurrency?: number;
  multipartControlConcurrency?: number;
  fetch?: typeof fetch;
}

export interface SourceConfig {
  type: string;
  bucket?: string;
  key?: string;
  region?: string;
  project_id?: string;
  account_id?: string;
  endpoint_url?: string;
  access_key_id?: string;
  secret_access_key?: string;
  url?: string;
  headers?: Record<string, string>;
}

export interface DestConfig {
  type: string;
  bucket?: string;
  key?: string;
  region?: string;
  project_id?: string;
  account_id?: string;
  endpoint_url?: string;
  access_key_id?: string;
  secret_access_key?: string;
  url?: string;
  headers?: Record<string, string>;
}

export interface CallbackConfig {
  url: string;
  headers?: Record<string, string>;
}

export interface TransferCreateRequest {
  sources: SourceConfig[];
  destinations: DestConfig[];
  total_size: number;
  chunk_size?: number;
  name?: string;
  merkle_root?: string;
  chunk_hashes?: string[];
  callbacks?: CallbackConfig[];
  test_mode?: boolean;
  progressive_mode?: boolean;
  signed_url_flow: SignedUrlFlow;
}

export interface RawTransferCreateInput {
  sources: SourceConfig[];
  destinations: DestConfig[];
  totalSize: number;
  chunkSize?: number;
  name?: string;
  merkleRoot?: string;
  chunkHashes?: string[];
  callbacks?: CallbackConfig[];
  /**
   * Maps to BeamCore test_mode.
   */
  testMode?: boolean;
  progressiveMode?: boolean;
  signedUrlFlow?: SignedUrlFlow;
  idempotencyKey?: string;
  /** Internal recovery generation; callers normally leave this unset. */
  routeGenerationId?: string;
}

export interface TransferCreateResponse {
  success: boolean;
  transfer_id: string;
  transfer_key?: string;
  total_chunks: number;
  total_sources: number;
  total_destinations: number;
  source_urls?: Array<Record<string, string>>;
  dest_urls?: Array<Record<string, string>>;
  upload_ids?: Array<string | null>;
  error?: string;
  message?: string;
}

export interface TransferCancelResponse {
  success: boolean;
  message?: string;
}

export interface DistributeResponse {
  success: boolean;
  transfer_id: string;
  orchestrators_assigned?: number;
  message?: string;
  error?: string;
}

export interface SourceStatusInfo {
  source_index: number;
  source_type: string;
  bucket?: string;
  key?: string;
  region?: string;
}

export interface DestinationStatusInfo {
  dest_index: number;
  dest_type: string;
  status: string;
  chunks_delivered?: number;
  chunks_pending?: number;
  chunks_in_progress?: number;
  chunks_completed?: number;
  chunks_failed?: number;
  bytes_delivered?: number;
  location?: string;
}

export interface IntegrityAuditChallengeChunk {
  challenge_id: string;
  task_id: string;
  attempt_id: string | null;
  orchestrator_id: string;
  orchestrator_hotkey: string;
  worker_id: string | null;
  source_id: string;
  destination_id: string;
  route_chunk_index: number;
  delivery_index: number;
  source_offset: number;
  destination_offset: number;
  range_length: number;
  final_object_key: string;
}

export interface IntegrityAuditChallenge {
  audit_id: string;
  transfer_id: string;
  requested_at: string;
  range_bytes: number;
  chunks: IntegrityAuditChallengeChunk[];
}

export interface TransferStatusInfo {
  transfer_id: string;
  name?: string;
  status: string;
  error_message: string | null;
  started_at: string | null;
  completed_at: string | null;
  runtime?: boolean;
  source_bytes_total: number;
  delivery_bytes_total: number;
  delivery_bytes_completed: number;
  delivery_tasks_total: number;
  delivery_tasks_completed: number;
  destinations_total: number;
  destinations_completed: number;
  destination_progress: Array<{
    destination_id: string;
    delivery_bytes_total: number;
    delivery_bytes_completed: number;
    completion_verified: boolean;
  }>;
  destination_groups?: {
    totalGroups: number;
    completedGroups: number;
    pendingGroups: number;
    totalDestinations: number;
    completedDestinations: number;
    completedDestinationIds: string[];
  };
  integrity_audit_challenge?: IntegrityAuditChallenge;
}

export interface ProviderTransferResumeInput extends Omit<ProviderTransferCreateInput, "idempotencyKey" | "routeGenerationId"> {
  transferId: string;
  /** Complete durable multipart identities from onMultipartGroupReady; resume never creates replacement uploads. */
  multipartGroups: ProviderMultipartGroupIdentity[];
}

export interface TransferTerminalEvent {
  schema_version: "transfer-client-control/v6";
  producer: "transfer-runtime";
  transfer_id: string;
  status: "completed" | "failed" | "cancelled";
  occurred_at: string;
}

export interface TransferTerminalSignalWaiter {
  wait(timeoutMs: number): Promise<TransferTerminalEvent | null>;
  close(): Promise<void>;
}

export interface S3ProviderConfig {
  provider: "s3";
  id?: string;
  bucket: string;
  key: string;
  region?: string;
  access_key_id: string;
  secret_access_key: string;
  session_token?: string;
  endpoint_url?: string;
}

export interface R2ProviderConfig {
  provider: "r2";
  id?: string;
  bucket: string;
  key: string;
  access_key_id: string;
  secret_access_key: string;
  account_id?: string;
  endpoint_url?: string;
}

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

export interface HippiusProviderConfig {
  provider: "hippius";
  id?: string;
  bucket: string;
  key: string;
  api_token: string;
  base_url?: string;
}

export type HuggingFaceRepoType = "model" | "dataset" | "space" | "kernel" | "bucket";

export interface HuggingFaceProviderConfig {
  provider: "huggingface";
  id?: string;
  /** Namespace and repo or bucket name separated by a slash, for example `org/dataset`. */
  repo_id: string;
  /** Path of the file inside the repo. */
  path: string;
  /** Defaults to `model`. */
  repo_type?: HuggingFaceRepoType;
  /** Branch, tag or commit. Defaults to `main`. Buckets are unversioned and ignore it. */
  revision?: string;
  token: string;
  /** Hub endpoint. Defaults to `https://huggingface.co`. */
  endpoint?: string;
  /** Destination only. Commit summary for the upload. */
  commit_message?: string;
  /** Destination only. Commit description for the upload. */
  commit_description?: string;
  /** Destination only. Open the upload as a pull request instead of committing to `revision`. */
  create_pr?: boolean;
  /**
   * Destination only. The Hub issues LFS upload URLs only for a known sha256, so the SDK
   * must read every source byte once to hash it. Opt in explicitly.
   */
  allow_source_rehash?: boolean;
}

export type ProviderConfig =
  | S3ProviderConfig
  | R2ProviderConfig
  | S3CompatibleProviderConfig
  | HippiusProviderConfig
  | HuggingFaceProviderConfig;

export type ProviderSourceConfig = ProviderConfig;

export type ProviderDestinationConfig = ProviderConfig;

export type S3ProviderConfigInput = Omit<S3ProviderConfig, "provider">;
export type R2ProviderConfigInput = Omit<R2ProviderConfig, "provider">;
export type S3CompatibleProviderConfigInput = Omit<
  S3CompatibleProviderConfig,
  "driver"
>;
export type HippiusProviderConfigInput = Omit<HippiusProviderConfig, "provider">;
export type HuggingFaceProviderConfigInput = Omit<HuggingFaceProviderConfig, "provider">;

export const S3ProviderConfig = Object.freeze({
  create(input: S3ProviderConfigInput): S3ProviderConfig {
    requireFields("s3", input, ["bucket", "key", "access_key_id", "secret_access_key"]);
    return { provider: "s3", ...input };
  }
});

export const R2ProviderConfig = Object.freeze({
  create(input: R2ProviderConfigInput): R2ProviderConfig {
    requireFields("r2", input, ["bucket", "key", "access_key_id", "secret_access_key"]);
    if (!hasText(input.account_id) && !hasText(input.endpoint_url)) {
      throw new Error("r2 config requires account_id or endpoint_url.");
    }
    return { provider: "r2", ...input };
  }
});

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

    return { ...input, provider, driver: "s3-compatible" };
  }
});

export const HippiusProviderConfig = Object.freeze({
  create(input: HippiusProviderConfigInput): HippiusProviderConfig {
    requireFields("hippius", input, ["bucket", "key", "api_token"]);
    return { provider: "hippius", ...input };
  }
});

const HUGGINGFACE_REPO_TYPES: readonly HuggingFaceRepoType[] = [
  "model",
  "dataset",
  "space",
  "kernel",
  "bucket"
];

export const HuggingFaceProviderConfig = Object.freeze({
  create(input: HuggingFaceProviderConfigInput): HuggingFaceProviderConfig {
    requireFields("huggingface", input, ["repo_id", "path", "token"]);
    if (input.repo_type !== undefined && !HUGGINGFACE_REPO_TYPES.includes(input.repo_type)) {
      throw new Error(
        `huggingface config repo_type must be one of ${HUGGINGFACE_REPO_TYPES.join(", ")}.`
      );
    }
    if (!input.repo_id.includes("/")) {
      throw new Error("huggingface config repo_id must be `namespace/name`.");
    }
    return { provider: "huggingface", ...input };
  }
});

export interface ProviderTransferCreateInput {
  /** Ownership fence. Aborting stops signing/replay without cancelling a replacement owner. */
  signal?: AbortSignal;
  sources: ProviderSourceConfig[];
  destinations: ProviderDestinationConfig[];
  name?: string;
  /**
   * Maps to BeamCore test_mode.
   */
  testMode?: boolean;
  expiresIn?: number;
  /**
   * Defaults to true. Set to false to only prepare and stream signed routes without distribution.
   */
  distribute?: boolean;
  onBeforeTransferPrepare?: () => void | Promise<void>;
  onPrepared?: (prepared: TransferPrepareResponse) => void | Promise<void>;
  /**
   * Called after the SDK creates a multipart upload and before its signed
   * routes are streamed. The payload contains durable identity only;
   * credentials, headers, and signed URLs are never exposed.
   */
  onMultipartGroupReady?: (group: ProviderMultipartGroupIdentity) => void | Promise<void>;
  throwIfCancelled?: (transferId?: string) => void | Promise<void>;
  signedUrlFlow?: SignedUrlFlow;
  /** Stable caller identity used to derive the transfer and lifecycle request ids. */
  idempotencyKey?: string;
  /** Internal recovery generation; callers normally omit this. */
  routeGenerationId?: string;
  /** Requested plan chunk size. BeamCore may raise it; the response carries the effective value. */
  chunkSize?: number;
}

export interface ProviderMultipartGroupIdentity {
  transferId: string;
  multipartGroupId: string;
  sourceId: string;
  destinationId: string;
  objectKey: string;
  uploadId: string;
  expectedObjectSize: number;
  expectedPartCount: number;
  expiresAt: string;
}

export interface PlanningHttpSource {
  source_id: string;
  type: "http";
  provider?: string;
  url?: string;
  size: number;
  filename?: string;
  headers?: Record<string, string>;
  expires_at?: string;
  metadata?: Record<string, unknown>;
}

export interface PreparedHttpSource extends PlanningHttpSource {
  url: string;
}

export interface PreparedDestination {
  destination_id: string;
  provider: string;
  mode?: "object_chunks" | "http_chunks";
  logical_prefix?: string;
  metadata?: Record<string, unknown>;
}

export interface ChunkDestinationSigningTarget {
  destination_id: string;
  provider?: string;
  object_key?: string;
  metadata?: Record<string, unknown>;
}

export interface ChunkSigningPlanItem {
  chunk_index: number;
  source_id: string;
  source_chunk_index: number;
  source_offset: number;
  chunk_size: number;
  source_url: string;
  destinations: ChunkDestinationSigningTarget[];
}

export interface CompactTransferPlanSource extends PreparedHttpSource {
  global_chunk_start: number;
  chunk_count: number;
}

export interface CompactTransferPlanDestination extends PreparedDestination {
  destination_index: number;
  final_object_keys: Record<string, string>;
}

export interface CompactTransferPlanDescriptor {
  version: "compact-transfer-plan/v1";
  plan_nonce: string;
  chunk_size: number;
  sources: CompactTransferPlanSource[];
  destinations: CompactTransferPlanDestination[];
  logical_chunk_count: number;
  delivery_route_count: number;
  multipart_attempt_slots: 3;
  formulas: {
    source_offset: "source_chunk_index * chunk_size";
    delivery_index: "chunk_index * destination_count + destination_index";
    part_number: "source_chunk_index * 3 + attempt_slot + 1";
    route_generation_id: "initial-{chunk_index}-{destination_id}";
  };
}

export interface SignedChunkRoute {
  source_id: string;
  destination_id: string;
  chunk_index: number;
  delivery_index?: number;
  source_url: string;
  dest_url: string;
  source_offset: number;
  chunk_size: number;
  expires_at?: string;
  headers?: Record<string, string>;
  dest_headers?: Record<string, string>;
  metadata?: Record<string, unknown>;
}

export interface MultipartGroupManifest {
  multipart_group_id: string;
  source_id: string;
  destination_id: string;
  final_object_key: string;
  upload_id: string;
  expected_object_size: number;
  expected_part_count: number;
  max_part_number: number;
  complete_url: string;
  abort_url: string;
  list_page_urls: string[];
  final_head_url: string;
  final_object_metadata: Record<string, string> & {
    "beam-transfer-id": string;
  };
  urls_expires_at: string;
}

export interface TransferPlanResponse {
  success: boolean;
  chunk_size?: number;
  total_size?: number;
  total_sources?: number;
  total_destinations?: number;
  logical_chunks?: number;
  total_chunks?: number;
  plan_descriptor: CompactTransferPlanDescriptor;
  signed_url_flow: SignedUrlFlow;
  plan_fingerprint: string;
  coordinate_checksum: string;
  error?: string;
  message?: string;
}
export interface TransferPrepareResponse {
  success: boolean;
  transfer_id: string;
  transfer_key?: string;
  test_mode?: boolean;
  chunk_size?: number;
  total_size?: number;
  total_sources?: number;
  total_destinations?: number;
  logical_chunks?: number;
  total_chunks?: number;
  plan_descriptor: CompactTransferPlanDescriptor;
  signed_url_flow: SignedUrlFlow;
  plan_fingerprint: string;
  coordinate_checksum: string;
  route_generation_id: string;
  error?: string;
  message?: string;
}

export interface AttachSignedUrlsResponse {
  success: boolean;
  transfer_id: string;
  total_routes?: number;
  urls_expires_at?: string;
  error?: string;
  message?: string;
}

function requireFields(
  provider: string,
  input: Record<string, unknown>,
  fields: string[]
): void {
  for (const field of fields) {
    if (!hasText(input[field])) {
      throw new Error(`${provider} config requires ${field}.`);
    }
  }
}

function hasText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}
