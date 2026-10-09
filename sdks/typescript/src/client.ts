import { SdkPerformanceCollector, SourceSignatureHistory, currentSdkPerformance, measureSdkPhase } from "./performance.js";
import {
  signMultipartRecovery,
  abortMultipartUpload,
  createMultipartUpload,
  expiresAtIso,
  prepareProviderDestination,
  prepareProviderSource,
  releaseProviderClients,
  signAbortMultipartUpload,
  signCompleteMultipartUpload,
  signDestinationReadRange,
  signDestinationRoute,
  signFinalObjectHead,
  signListMultipartUpload,
  signSourceReadRange,
  signSourceChunk,
  boundedGrantExpiry,
  isHuggingFaceProvider
} from "./provider-signing.js";
import {
  describe as describeHuggingFace,
  hashSourceStream,
  huggingFaceCommit,
  huggingFaceCompleteLfsUpload,
  huggingFaceLfsBatch,
  huggingFacePreupload,
  huggingFaceVerifyLfsUpload,
  readSourceSample
} from "./huggingface.js";
import type {
  AttachSignedUrlsResponse,
  BeamClientOptions,
  ChunkSigningPlanItem,
  CompactTransferPlanDescriptor,
  DistributeResponse,
  HippiusProviderConfig,
  HuggingFaceProviderConfig,
  IntegrityAuditChallenge,
  MultipartGroupManifest,
  PlanningHttpSource,
  PreparedDestination,
  PreparedHttpSource,
  ProviderDestinationConfig,
  ProviderMultipartGroupIdentity,
  ProviderSourceConfig,
  ProviderTransferCreateInput,
  ProviderTransferResumeInput,
  RawTransferCreateInput,
  SignedChunkRoute,
  SignedUrlFlow,
  TransferCancelResponse,
  TransferCreateResponse,
  TransferPlanResponse,
  TransferPrepareResponse,
  TransferStatusInfo,
  TransferTerminalEvent,
  TransferTerminalSignalWaiter
} from "./models.js";
import {
  BeamTransferControl,
  BEAM_DEFAULT_NATS_URL,
  compactSignedRoutes,
  isRecoverableRouteStreamError,
  type RouteRecoverySignReplyPayload,
  type RouteRecoverySignRequestPayload,
  type TransferRecoveryLease
} from "./nats-control.js";

export class BeamRouteRecoveryPendingError extends Error {
  readonly transferId: string;
  readonly cause: unknown;

  constructor(transferId: string, cause: unknown) {
    super(`Transfer ${transferId} is prepared and route recovery is continuing in the background.`);
    this.name = "BeamRouteRecoveryPendingError";
    this.transferId = transferId;
    this.cause = cause;
  }
}
import { multipartPartNumber } from "./multipart-limits.js";

export { BEAM_DEFAULT_NATS_URL };
export const BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY = 2;

export class BeamProviderTransferError extends AggregateError {
  readonly transferId: string;
  readonly transferCancelled: boolean;
  readonly multipartCleanupComplete: boolean;
  readonly cause: unknown;

  constructor(input: {
    transferId: string;
    cause: unknown;
    cancelError?: unknown;
    cleanupError?: unknown;
  }) {
    const errors = [input.cause];
    if (input.cancelError !== undefined) errors.push(input.cancelError);
    if (input.cleanupError !== undefined) errors.push(input.cleanupError);
    const transferCancelled = input.cancelError === undefined;
    const multipartCleanupComplete = input.cleanupError === undefined;
    super(
      errors,
      `provider transfer failed for ${input.transferId} (transfer_cancelled=${transferCancelled}, multipart_cleanup_complete=${multipartCleanupComplete})`
    );
    this.name = "BeamProviderTransferError";
    this.transferId = input.transferId;
    this.transferCancelled = transferCancelled;
    this.multipartCleanupComplete = multipartCleanupComplete;
    this.cause = input.cause;
  }
}

/** Failure codes BeamCore reports when source or destination storage refuses Beam's requests. */
export type BeamStorageAccessErrorCode = "source_access_denied" | "destination_access_denied";

const STORAGE_ACCESS_ERROR_CODES: readonly BeamStorageAccessErrorCode[] = [
  "source_access_denied",
  "destination_access_denied"
];

/** A transfer that BeamCore reported as failed. */
export class BeamTransferFailedError extends Error {
  readonly transferId: string;
  /** The transfer's `error_message` from BeamCore, verbatim; null when BeamCore sent none. */
  readonly errorMessage: string | null;

  constructor(transferId: string, errorMessage: string | null) {
    super(`Transfer ${transferId} failed: ${errorMessage ?? "unknown error"}`);
    this.name = "BeamTransferFailedError";
    this.transferId = transferId;
    this.errorMessage = errorMessage;
  }
}

/**
 * The source or destination storage refused Beam's requests (`source_access_denied` or
 * `destination_access_denied`). `errorMessage` carries BeamCore's explanation verbatim.
 */
export class BeamStorageAccessError extends BeamTransferFailedError {
  readonly code: BeamStorageAccessErrorCode;

  constructor(transferId: string, errorMessage: string, code: BeamStorageAccessErrorCode) {
    super(transferId, errorMessage);
    this.name = "BeamStorageAccessError";
    this.code = code;
  }
}

/**
 * Builds the error for a failed transfer status: a {@link BeamStorageAccessError} when the
 * server message starts with a storage access code, otherwise a {@link BeamTransferFailedError}.
 */
export function transferFailedError(transferId: string, errorMessage: string | null): BeamTransferFailedError {
  const code = errorMessage === null ? undefined : storageAccessErrorCode(errorMessage);
  return code && errorMessage !== null
    ? new BeamStorageAccessError(transferId, errorMessage, code)
    : new BeamTransferFailedError(transferId, errorMessage);
}

function storageAccessErrorCode(errorMessage: string): BeamStorageAccessErrorCode | undefined {
  const separator = errorMessage.indexOf(":");
  const prefix = (separator === -1 ? errorMessage : errorMessage.slice(0, separator)).trim();
  return STORAGE_ACCESS_ERROR_CODES.find((code) => code === prefix);
}

export class BeamApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly body: string
  ) {
    super(message);
    this.name = "BeamApiError";
  }
}

type TransferPrepareInput = {
  transferId?: string;
  sources: PreparedHttpSource[];
  destinations: PreparedDestination[];
  name?: string;
  urlsExpiresAt?: string;
  signedUrlFlow?: SignedUrlFlow;
  idempotencyKey?: string;
  routeGenerationId?: string;
};

type TransferPrepareRequestOptions = {
  requestKey?: string;
  /** Part size a Hugging Face LFS destination dictates; never a caller preference. */
  providerPartSize?: number;
};

export class BeamClient {
  readonly apiKey: string;
  readonly natsUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly control: BeamTransferControl;
  private readonly routeSigningConcurrency: number;
  private readonly routeSigningConcurrencyOverridden: boolean;
  private readonly multipartControlConcurrency: number;
  private readonly onDiagnostics: BeamClientOptions["onDiagnostics"];
  private diagnosticPending = false;
  private readonly recoverySigners = new Map<string, { owner: object; stop: () => void }>();
  private readonly integrityAuditSigners = new Map<string, (challenge: IntegrityAuditChallenge) => Promise<Record<string, unknown>>>();
  private readonly integrityGrantCache = new Map<string, { transferId: string; expiresAt: number; fingerprint: string; payload: Promise<Record<string, unknown>> }>();
  private readonly integrityAuditSubmissions = new Map<string, Promise<void>>();
  private readonly huggingFaceUploads = new Map<string, HuggingFaceUploadState[]>();

  constructor(options: BeamClientOptions = {}) {
    this.onDiagnostics = options.onDiagnostics;
    if (!options.apiKey?.trim()) {
      throw new Error("apiKey is required.");
    }

    this.apiKey = options.apiKey;
    this.natsUrl = options.natsUrl ?? options.natsWsUrl ?? BEAM_DEFAULT_NATS_URL;
    this.routeSigningConcurrency = positiveInteger(options.routeSigningConcurrency ?? 64, "routeSigningConcurrency");
    this.routeSigningConcurrencyOverridden = options.routeSigningConcurrency !== undefined;
    this.multipartControlConcurrency = positiveInteger(
      options.multipartControlConcurrency ?? BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY,
      "multipartControlConcurrency"
    );
    this.control = new BeamTransferControl({
      apiKey: this.apiKey,
      natsUrl: options.natsUrl,
      natsWsUrl: options.natsWsUrl,
      environment: options.environment,
      subjectPrefix: options.transferClientSubjectPrefix,
      transferRuntimeShardCount: options.transferRuntimeShardCount,
      requestTimeoutMs: options.requestTimeoutMs,
      maxPayloadBytes: options.maxPayloadBytes
    });
    this.fetchImpl = options.fetch ?? globalThis.fetch;

    if (!this.fetchImpl) {
      throw new Error("A fetch implementation is required.");
    }
  }

  async close(): Promise<void> {
    this.stopAllRecoverySigners();
    return this.control.close();
  }

  openTransferTerminalWaiter(transferId: string): Promise<TransferTerminalSignalWaiter> {
    validateId(transferId, "transferId");
    return this.control.openTerminalSignalWaiter(transferId);
  }

  async createRawTransfer(input: RawTransferCreateInput): Promise<TransferCreateResponse> {
    const transferId = await transferIdForIdempotencyKey(input.idempotencyKey);
    const body = compact({
      transfer_id: transferId,
      sources: input.sources,
      destinations: input.destinations,
      total_size: input.totalSize,
      name: input.name,
      merkle_root: input.merkleRoot,
      chunk_hashes: input.chunkHashes,
      callbacks: input.callbacks,
      progressive_mode: input.progressiveMode || undefined,
      signed_url_flow: input.signedUrlFlow ?? "signed_url"
    });
    return this.control.request<TransferCreateResponse>("transfer.create", body, {
      transferId,
      idempotencyKey: `transfer:${transferId}:create`
    });
  }

  createTransfer(input: ProviderTransferCreateInput): Promise<TransferPrepareResponse> {
    return this.prepareProviderTransfer(input);
  }

  async resumeProviderTransfer(input: ProviderTransferResumeInput): Promise<TransferPrepareResponse> {
    validateId(input.transferId, "transferId");
    return this.executeProviderTransfer(input, input);
  }

  async transferStatus(transferId: string): Promise<TransferStatusInfo> {
    validateId(transferId, "transferId");
    const status = await this.control.request<TransferStatusInfo>("transfer.status", { transfer_id: transferId }, { transferId });
    if (status.integrity_audit_challenge) {
      try {
        await this.submitIntegrityAuditGrantsIfPresent(status);
      } catch (error) {
        status.integrity_audit_submission_error = integrityAuditErrorSummary(error);
      }
    }
    if (["completed", "failed", "cancelled"].includes(status.status)) {
      this.control.releaseRecoveryLease(transferId);
      this.stopRecoverySigner(transferId);
    }
    return status;
  }

  distributeTransfer(transferId: string): Promise<DistributeResponse> {
    validateId(transferId, "transferId");
    return this.control.request<DistributeResponse>("transfer.distribute", { transfer_id: transferId }, {
      transferId,
      idempotencyKey: `transfer:${transferId}:distribute`
    });
  }

  private requestTransferCancellation(transferId: string): Promise<TransferCancelResponse> {
    validateId(transferId, "transferId");
    return this.control.request<TransferCancelResponse>("transfer.cancel", { transfer_id: transferId }, {
      transferId,
      idempotencyKey: `transfer:${transferId}:cancel`
    });
  }

  async cancelTransfer(transferId: string): Promise<TransferCancelResponse> {
    const result = await this.requestTransferCancellation(transferId);
    this.control.releaseRecoveryLease(transferId);
    this.stopRecoverySigner(transferId);
    return result;
  }

  async planTransfer(input: {
    sources: PlanningHttpSource[];
    destinations: PreparedDestination[];
    name?: string;
    urlsExpiresAt?: string;
    signedUrlFlow?: SignedUrlFlow;
  }): Promise<TransferPlanResponse> {
    const result = await this.control.request<TransferPlanResponse>(
      "transfer.plan",
      compact({
        sources: input.sources,
        destinations: input.destinations,
        name: input.name,
        urls_expires_at: input.urlsExpiresAt,
        signed_url_flow: input.signedUrlFlow ?? "signed_url"
      })
    );
    if (result.success) validateCompactTransferPlan(result.plan_descriptor, result.signed_url_flow);
    return result;
  }

  prepareTransfer(input: TransferPrepareInput): Promise<TransferPrepareResponse> {
    return this.prepareTransferWithRequestKey(input);
  }

  private async prepareTransferWithRequestKey(
    input: TransferPrepareInput,
    options: TransferPrepareRequestOptions = {}
  ): Promise<TransferPrepareResponse> {
    const transferId = input.transferId ?? await transferIdForIdempotencyKey(input.idempotencyKey);
    validateId(transferId, "transferId");
    const prepareIdempotencyKey = options.requestKey ?? `transfer:${transferId}:prepare`;
    const routeGenerationId =
      input.routeGenerationId ?? await routeGenerationIdForPrepareIdempotencyKey(prepareIdempotencyKey);
    const result = await this.control.request<TransferPrepareResponse>(
      "transfer.prepare",
      compact({
        transfer_id: transferId,
        route_generation_id: routeGenerationId,
        sources: input.sources,
        destinations: input.destinations,
        name: input.name,
        provider_part_size: options.providerPartSize,
        urls_expires_at: input.urlsExpiresAt,
        signed_url_flow: input.signedUrlFlow ?? "signed_url"
      }),
      {
        transferId,
        idempotencyKey: prepareIdempotencyKey
      }
    );
    if (result.success) validateCompactTransferPlan(result.plan_descriptor, result.signed_url_flow);
    return result;
  }

  async attachSignedUrls(
    transferId: string,
    input: {
      chunkRoutes: SignedChunkRoute[];
      multipartGroupManifest: MultipartGroupManifest[];
      transferKey?: string;
      routeGenerationId: string;
      planFingerprint: string;
      coordinateChecksum: string;
      recoveryFactory: (routeGenerationId: string) => Promise<{
        chunkRoutes: SignedChunkRoute[];
        multipartGroupManifest: MultipartGroupManifest[];
        urlsExpiresAt?: string;
      }>;
      urlsExpiresAt?: string;
      autoDistribute?: boolean;
    }
  ): Promise<AttachSignedUrlsResponse> {
    validateId(transferId, "transferId");
    validateMultipartGroupManifest(input.multipartGroupManifest, transferId);
    validateSignedRouteManifestContract(input.chunkRoutes, input.multipartGroupManifest);
    const streamRoutes = async (
      chunkRoutes: SignedChunkRoute[],
      multipartGroupManifest: MultipartGroupManifest[],
      routeGenerationId: string,
      urlsExpiresAt?: string
    ): Promise<AttachSignedUrlsResponse> => {
      validateMultipartGroupManifest(multipartGroupManifest, transferId);
      validateSignedRouteManifestContract(chunkRoutes, multipartGroupManifest);
      const destinationCount = new Set(chunkRoutes.map((route) => route.destination_id)).size;
      const orderedRoutes = chunkRoutes.map((route) => {
        if (signedRouteDeliveryIndex(route) !== undefined) return route;
        if (destinationCount !== 1) {
          throw new Error("delivery_index is required when manually attaching routes for multiple destinations");
        }
        return { ...route, delivery_index: route.chunk_index };
      })
      .sort(compareSignedRoutesByDelivery);
      const streamId = await stableRouteStreamId({
        transferId,
        planIdentity: `${await routeCoordinateChecksum(orderedRoutes)}:${routeGenerationId}`,
        totalRoutes: orderedRoutes.length,
        totalChunks: countDistinctRouteChunks(orderedRoutes),
        signedUrlFlow: "signed_url"
      });
      const stream = new RouteStreamSender(this.control, {
        streamId,
        transferId,
        routeGenerationId,
        totalRoutes: orderedRoutes.length,
        totalChunks: countDistinctRouteChunks(orderedRoutes),
        urlsExpiresAt,
        signedUrlFlow: "signed_url",
        autoDistribute: input.autoDistribute ?? true
      });
      await stream.begin();
      if (multipartGroupManifest.length) await stream.addManifestGroups(multipartGroupManifest);
      for (const route of orderedRoutes) await stream.addRoute(route);
      return stream.complete();
    };
    const routeStreamLock = new AsyncMutex();
    const releaseInitialStream = await routeStreamLock.acquire();
    const recoveryLease: TransferRecoveryLease = {
      transferId,
      planFingerprint: input.planFingerprint,
      coordinateChecksum: input.coordinateChecksum,
      replayRoutes: async (routeGenerationId) => {
        await routeStreamLock.run(async () => {
          const fresh = await input.recoveryFactory(routeGenerationId);
          const attached = await streamRoutes(
            fresh.chunkRoutes,
            fresh.multipartGroupManifest,
            routeGenerationId,
            fresh.urlsExpiresAt
          );
          if (!attached.success) throw new Error(attached.error ?? attached.message ?? "route replay failed");
        });
      }
    };
    this.control.registerRecoveryLease(recoveryLease);
    try {
      const attached = await streamRoutes(
        input.chunkRoutes,
        input.multipartGroupManifest,
        input.routeGenerationId,
        input.urlsExpiresAt
      );
      if (!attached.success) this.control.releaseRecoveryLease(transferId, recoveryLease);
      return attached;
    } catch (error) {
      if (isRecoverableRouteStreamError(error)) {
        this.control.continueRecoveryLease(transferId, recoveryLease);
        throw new BeamRouteRecoveryPendingError(transferId, error);
      }
      this.control.releaseRecoveryLease(transferId, recoveryLease);
      throw error;
    } finally {
      releaseInitialStream();
    }
  }

  async prepareProviderTransfer(input: ProviderTransferCreateInput): Promise<TransferPrepareResponse> {
    return this.executeProviderTransfer(input);
  }

  private async executeProviderTransfer(input: ProviderTransferCreateInput, resume?: ProviderTransferResumeInput): Promise<TransferPrepareResponse> {
    input.signal?.throwIfAborted();
    const expiresIn = input.expiresIn ?? 3600;
    const requestedSignedUrlFlow: SignedUrlFlow = input.signedUrlFlow ?? "signed_url";
    const retainedRecoveryInput: ProviderTransferCreateInput = {
      ...input,
      sources: input.sources.map((source) => ({ ...source })),
      destinations: input.destinations.map((destination) => ({ ...destination })),
      onBeforeTransferPrepare: undefined,
      onPrepared: undefined,
      onMultipartGroupReady: undefined,
      throwIfCancelled: undefined
    };
    const preparedDestinations = retainedRecoveryInput.destinations.map((destination, index) =>
      prepareProviderDestination(destination, { index })
    );
    const destinationsById = new Map<string, ProviderDestinationConfig>();
    preparedDestinations.forEach((preparedDestination, index) => {
      const destination = retainedRecoveryInput.destinations[index];
      if (!destination) {
        throw new Error(`missing provider destination config for prepared destination ${preparedDestination.destination_id}`);
      }
      destinationsById.set(preparedDestination.destination_id, destination);
    });

    await input.throwIfCancelled?.();
    const discoveryTelemetry = new SdkPerformanceCollector();
    const preparedSources = await discoveryTelemetry.measure("sdk.discovery", () => Promise.all(
      retainedRecoveryInput.sources.map((source, index) =>
        prepareProviderSource(source, { index, expiresIn, fetchImpl: this.fetchImpl, signal: input.signal })
      )
    ));
    await input.throwIfCancelled?.();
    const huggingFace = await this.planHuggingFaceUploads({
      sources: retainedRecoveryInput.sources,
      preparedSources,
      destinations: retainedRecoveryInput.destinations,
      preparedDestinations
    });
    await input.throwIfCancelled?.();
    await input.onBeforeTransferPrepare?.();
    input.signal?.throwIfAborted();
    const prepareInput: TransferPrepareInput = {
      sources: preparedSources,
      destinations: preparedDestinations,
      name: input.name,
      signedUrlFlow: requestedSignedUrlFlow,
      ...(resume ? { transferId: resume.transferId } : { idempotencyKey: input.idempotencyKey, routeGenerationId: input.routeGenerationId })
    };
    const prepared = await this.prepareTransferWithRequestKey(prepareInput, {
      requestKey: resume ? `transfer:${resume.transferId}:prepare:resume:${randomUuid()}` : undefined,
      providerPartSize: huggingFace.providerPartSize
    });
    if (resume && prepared.transfer_id !== resume.transferId) throw new Error("resumed provider transfer id mismatch");
    input.signal?.throwIfAborted();
    if (!prepared.success) {
      return prepared;
    }
    if (huggingFace.states.length) {
      this.assertHuggingFacePlan(prepared, huggingFace.states);
      this.huggingFaceUploads.set(prepared.transfer_id, huggingFace.states);
    }
    const huggingFaceByDestination = new Map(
      huggingFace.states.map((state) => [state.destinationId, state])
    );

    const sourcesById = new Map<string, ProviderSourceConfig>();
    preparedSources.forEach((preparedSource, index) => {
      const source = retainedRecoveryInput.sources[index];
      if (!source) {
        throw new Error(`missing provider source config for prepared source ${preparedSource.source_id}`);
      }
      sourcesById.set(preparedSource.source_id, source);
    });

    const multipartUploads = new Map<string, MultipartUploadCleanupState>();
    if (resume) restoreProviderMultipartIdentities(prepared, destinationsById, resume.multipartGroups, multipartUploads);
    const recoveryMultipartUploads = new Map<string, MultipartUploadState>();
    const autoDistribute = input.distribute !== false;
    let initialThrowIfCancelled = input.throwIfCancelled;
    const assertOwnership = () => input.signal?.throwIfAborted();
    const routeStreamLock = new AsyncMutex();
    const releaseInitialStream = await routeStreamLock.acquire();
    const sourceSignatureHistory = new SourceSignatureHistory(!resume);
    const streamPreparedRoutes = async (routeGenerationId: string, recoveryReplay: boolean): Promise<void> => {
      const telemetry = new SdkPerformanceCollector(sourceSignatureHistory);
      telemetry.seedDiscovery(discoveryTelemetry);
      telemetry.gauge("signing_configured_limit", this.routeSigningConcurrency);
      telemetry.gauge("multipart_configured_limit", this.multipartControlConcurrency);
      telemetry.gauge("signing_limit", this.routeSigningConcurrency);
      telemetry.gauge("multipart_limit", this.multipartControlConcurrency);
      const pendingRoutes = new Set<Promise<SignedChunkRoute>>();
      let signingConcurrency = this.routeSigningConcurrency;
      let signedInWindow = 0;
      let activeSigning = 0;
      let signingWindowStartedAt = performance.now();
      let routeStream: RouteStreamSender | null = null;
      let routeStreamBeginAttempted = false;
      let multipartManifestTask: Promise<MultipartGroupManifest[]> | null = null;
      let multipartGroupWaiters = new Map<string, Deferred<MultipartUploadState>>();
      let foregroundCancelled = false;
      assertOwnership();
      const throwIfCancelled = async (transferId?: string): Promise<void> => {
        try {
          assertOwnership();
          if (!recoveryReplay) await initialThrowIfCancelled?.(transferId);
        } catch (error) {
          foregroundCancelled = true;
          throw error;
        }
      };
      try {
        const streamId = await stableRouteStreamId({
          transferId: prepared.transfer_id,
          planIdentity: `${prepared.plan_descriptor.plan_nonce}:${routeGenerationId}`,
          totalRoutes: prepared.plan_descriptor.delivery_route_count,
          totalChunks: prepared.plan_descriptor.logical_chunk_count,
          signedUrlFlow: requestedSignedUrlFlow
        });
        routeStream = new RouteStreamSender(this.control, {
          telemetry,
          onDiagnostics: (summary) => {
            if (!this.onDiagnostics || this.diagnosticPending) return;
            this.diagnosticPending = true;
            setTimeout(() => {
              void Promise.resolve().then(() => this.onDiagnostics?.(summary)).catch(() => {}).finally(() => { this.diagnosticPending = false; });
            }, 0);
          },
          streamId,
          transferId: prepared.transfer_id,
          routeGenerationId,
          totalRoutes: prepared.plan_descriptor.delivery_route_count,
          totalChunks: prepared.plan_descriptor.logical_chunk_count,
          signedUrlFlow: requestedSignedUrlFlow,
          autoDistribute,
          urlsExpiresAt: expiresAtIso(expiresIn)
        });
        routeStreamBeginAttempted = true;
        await routeStream.begin();
        multipartGroupWaiters = createMultipartGroupWaiters(prepared, destinationsById);
        multipartManifestTask = telemetry.measure("sdk.multipart_create", () => createMultipartGroupManifest({
          prepared,
          destinationsById,
          multipartUploads,
          expiresIn,
          signedUrlFlow: requestedSignedUrlFlow,
          concurrency: this.multipartControlConcurrency,
          signal: input.signal,
          onGroupReady: async (state) => {
            assertOwnership();
            recoveryMultipartUploads.set(state.manifest.multipart_group_id, state);
            validateMultipartGroupManifest([state.manifest], prepared.transfer_id);
            await telemetry.measure("sdk.multipart_callback", async () => input.onMultipartGroupReady?.(multipartGroupIdentity(prepared.transfer_id, state.manifest)));
            await routeStream!.addManifestGroups([state.manifest]);
            multipartGroupWaiters.get(state.manifest.multipart_group_id)?.resolve(state);
          },
          onGroupFailed: (groupId, error) => multipartGroupWaiters.get(groupId)?.reject(error)
        }));
        const streamNextCompletedRoute = async (): Promise<void> => {
          const settled = await telemetry.measure("sdk.producer_wait", () => Promise.race(
            [...pendingRoutes].map((pending) => pending.then((route) => ({ pending, route })))
          ));
          pendingRoutes.delete(settled.pending);
          assertOwnership();
          await routeStream!.addRoute(settled.route);
          signedInWindow += 1;
          if (signedInWindow === ROUTE_STREAM_BATCH_ROUTES) {
            const elapsedMs = performance.now() - signingWindowStartedAt;
            if (!this.routeSigningConcurrencyOverridden && elapsedMs > 4_000 && signingConcurrency < 256) {
              signingConcurrency = Math.min(256, signingConcurrency * 2);
              telemetry.increment("concurrency_changes"); telemetry.gauge("signing_limit", signingConcurrency);
            }
            signedInWindow = 0;
            signingWindowStartedAt = performance.now();
          }
        };
        for (const chunk of materializePlanChunks(prepared.plan_descriptor, prepared.transfer_id)) {
          await throwIfCancelled?.(prepared.transfer_id);
          // This promise belongs to this chunk and signing generation only. Its
          // immutable credential/range identity and actual expiry cannot be renewed.
          let sourceUses = 0;
          const sourceGrant = telemetry.measure("sdk.source_signing", () => signSourceChunk({ source: sourcesById.get(chunk.source_id), fallbackUrl: chunk.source_url, chunk, expiresIn, fetchImpl: this.fetchImpl })).then(grant => { telemetry.sourceCreated(chunk.chunk_index); return grant; });
          void sourceGrant.catch(() => {});

          for (const target of chunk.destinations) {
            const pendingRoute = (async (): Promise<SignedChunkRoute> => {
              await throwIfCancelled?.(prepared.transfer_id);
              const destination = destinationsById.get(target.destination_id);
              if (!destination) throw new Error(`BeamCore returned unknown destination_id: ${target.destination_id}`);
              const source = sourcesById.get(chunk.source_id);
              if (!source) throw new Error(`BeamCore returned unknown source_id: ${chunk.source_id}`);
              const metadata = target.metadata ?? {};
              const finalObjectKey = typeof metadata.final_object_key === "string" ? metadata.final_object_key : target.object_key;
              if (!finalObjectKey) throw new Error("destination signing target is missing object_key");
              const multipartGroupId = multipartGroupStateKey(
                prepared.transfer_id,
                target.destination_id,
                chunk.source_id,
                finalObjectKey
              );
              const upload = isDirectPutDestination(destination)
                ? undefined
                : await telemetry.measure("sdk.multipart_ready_wait", async () => multipartGroupWaiters.get(multipartGroupId)?.promise);
              if (!isDirectPutDestination(destination) && !upload) throw new Error(`multipart group manifest is missing for ${chunk.source_id}:${target.destination_id}`);
              telemetry.gauge("signing_active_peak", ++activeSigning);
              return telemetry.measure("sdk.signing", () => this.signProviderRoute({
                chunk,
                target,
                source,
                sourceGrant: telemetry.measure("sdk.source_grant_wait", () => sourceGrant).then(grant => { if (sourceUses++ > 0) telemetry.counters.source_reuses++; return grant; }),
                destination,
                expiresIn,
                ...(upload ? { upload } : {}),
                huggingFaceUpload: huggingFaceByDestination.get(target.destination_id),
                transferId: prepared.transfer_id,
                finalObjectKey,
                signedUrlFlow: requestedSignedUrlFlow,
                partNumber:
                  typeof metadata.part_number === "number"
                    ? metadata.part_number
                    : multipartPartNumber(chunk.source_chunk_index),
              })).finally(() => { activeSigning--; });
            })();
            pendingRoutes.add(pendingRoute);
            telemetry.gauge("signing_pending_peak", pendingRoutes.size);
            if (pendingRoutes.size >= signingConcurrency) await telemetry.measure("sdk.signing_queue", streamNextCompletedRoute);
          }
        }
        while (pendingRoutes.size) await streamNextCompletedRoute();
        const multipartGroupManifest = await multipartManifestTask;
        validateMultipartGroupManifest(multipartGroupManifest, prepared.transfer_id);
        assertOwnership();
        const attached = await routeStream.complete();
        if (!attached.success) {
          throw new Error(attached.error ?? attached.message ?? "route stream failed");
        }
      } catch (error) {
        if (input.signal?.aborted) {
          this.stopRecoverySigner(prepared.transfer_id, recoveryLease);
          this.control.releaseRecoveryLease(prepared.transfer_id, recoveryLease);
          await Promise.allSettled(pendingRoutes);
          if (multipartManifestTask) await Promise.allSettled([multipartManifestTask]);
          throw input.signal.reason;
        }
        if (routeStream) await routeStream.abort().catch(() => undefined);
        await Promise.allSettled(pendingRoutes);
        if (multipartManifestTask) await Promise.allSettled([multipartManifestTask]);
        if (!recoveryReplay) {
          if (foregroundCancelled) {
            this.control.continueRecoveryLease(prepared.transfer_id, recoveryLease);
          } else if (isRecoverableRouteStreamError(error)) {
            this.control.continueRecoveryLease(prepared.transfer_id, recoveryLease);
            throw new BeamRouteRecoveryPendingError(prepared.transfer_id, error);
          } else {
            try {
              await cancelAndAbortProviderFailure({
                cancelTransfer: () => this.requestTransferCancellation(prepared.transfer_id),
                transferId: prepared.transfer_id,
                cause: error,
                uploads: multipartUploads,
                abortBeforeCancel: !routeStreamBeginAttempted,
                cleanupConcurrency: this.multipartControlConcurrency
              });
            } finally {
              // The retained recovery input owns the same destination objects used by
              // multipart cleanup. Releasing it earlier scrubs their scoped credentials
              // before abortMultipartUpload can clean up already-created uploads.
              this.control.releaseRecoveryLease(prepared.transfer_id, recoveryLease);
            }
          }
        }
        throw error;
      }
    };

    // The lease object is this owner's identity: releases, recovery requests, and
    // signer shutdowns below only act while it is still the registered lease.
    const recoveryLease: TransferRecoveryLease = {
      transferId: prepared.transfer_id,
      planFingerprint: prepared.plan_fingerprint,
      coordinateChecksum: prepared.coordinate_checksum,
      replayRoutes: async (routeGenerationId) => {
        await routeStreamLock.run(() => streamPreparedRoutes(routeGenerationId, true));
      },
      disposeSecrets: () => {
        input.signal?.removeEventListener("abort", stopOwnedRecovery);
        releaseProviderClients([...retainedRecoveryInput.sources, ...retainedRecoveryInput.destinations]);
        clearRecoverySecrets(retainedRecoveryInput);
      }
    };

    await this.startProviderRouteRecoverySigner({
      owner: recoveryLease,
      prepared,
      sourcesById,
      destinationsById,
      multipartUploads: recoveryMultipartUploads,
      signal: input.signal,
      expiresIn
    });

    const stopOwnedRecovery = () => {
      this.stopRecoverySigner(prepared.transfer_id, recoveryLease);
      this.control.releaseRecoveryLease(prepared.transfer_id, recoveryLease);
    };
    input.signal?.addEventListener("abort", stopOwnedRecovery, { once: true });
    this.control.registerRecoveryLease(recoveryLease);

    try {
      try {
        assertOwnership();
        await input.onPrepared?.(prepared);
      } catch (error) {
        this.control.continueRecoveryLease(prepared.transfer_id, recoveryLease);
        throw error;
      }
      try {
        await input.throwIfCancelled?.(prepared.transfer_id);
      } catch (error) {
        this.control.continueRecoveryLease(prepared.transfer_id, recoveryLease);
        throw error;
      }
      await streamPreparedRoutes(prepared.route_generation_id, false);
      initialThrowIfCancelled = undefined;
    } finally {
      releaseInitialStream();
    }

    return prepared;
  }

  /**
   * Negotiate every Hugging Face destination before the plan exists.
   *
   * The Hub will not issue upload URLs without the object's sha256, and it chooses the part
   * size itself, so this runs first and the prepare request then carries the Hub's part size
   * as `provider_part_size`.
   */
  private async planHuggingFaceUploads(input: {
    sources: ProviderSourceConfig[];
    preparedSources: PreparedHttpSource[];
    destinations: ProviderDestinationConfig[];
    preparedDestinations: PreparedDestination[];
  }): Promise<{ states: HuggingFaceUploadState[]; providerPartSize?: number }> {
    const targets = input.destinations
      .map((destination, index) => ({ destination, index }))
      .filter((entry): entry is { destination: HuggingFaceProviderConfig; index: number } =>
        isHuggingFaceProvider(entry.destination));
    if (!targets.length) return { states: [] };

    if (input.preparedSources.length !== 1) {
      throw new Error(
        "a huggingface destination requires exactly one source: the Hub dictates the part size "
          + `and the plan carries a single chunk size, but ${input.preparedSources.length} sources were given`
      );
    }
    const preparedSource = input.preparedSources[0]!;
    const sourceConfig = input.sources[0]!;

    // For an LFS source the Hub already published the sha256 as the linked ETag.
    const publishedSha256 = isHuggingFaceProvider(sourceConfig)
      ? typeof preparedSource.metadata?.sha256 === "string" ? preparedSource.metadata.sha256 : undefined
      : undefined;

    const states: HuggingFaceUploadState[] = [];
    let providerPartSize: number | undefined;

    for (const { destination, index } of targets) {
      const preparedDestination = input.preparedDestinations[index]!;
      if ((destination.repo_type ?? "model") === "bucket") {
        throw new Error(
          `${destination.repo_id} is a Hugging Face bucket, which Beam cannot write to. Buckets `
            + "expose no LFS batch endpoint; their only upload path is the Hub's Xet CAS client, "
            + "which cannot be expressed as presigned URLs for Beam's workers. Buckets do work as "
            + "a transfer source. Use `hf sync` to write to a bucket."
        );
      }
      const path = huggingFaceTargetPath(destination, preparedSource.filename);
      const config: HuggingFaceProviderConfig = { ...destination, path };

      let oid = publishedSha256;
      if (!oid) {
        if (!destination.allow_source_rehash) {
          throw new Error(
            `uploading to ${describeHuggingFace(config)} needs the source sha256, which the Hub requires `
              + "before it issues upload URLs. The SDK must read the source once to compute it; set "
              + "allow_source_rehash: true to opt in."
          );
        }
        const hashed = await hashSourceStream(this.fetchImpl, preparedSource.url);
        oid = hashed.sha256!;
      }

      const sample = await readSourceSample(this.fetchImpl, preparedSource.url);
      const preupload = await huggingFacePreupload(this.fetchImpl, config, {
        size: preparedSource.size,
        sample
      });
      if (preupload.shouldIgnore) {
        throw new Error(`${describeHuggingFace(config)} is excluded by the repo's .gitignore`);
      }
      if (preupload.uploadMode !== "lfs") {
        throw new Error(
          `${describeHuggingFace(config)} would be committed as a regular git blob, not an LFS blob. `
            + "Beam uploads through the LFS protocol only; add the path to .gitattributes as LFS."
        );
      }

      const plan = await huggingFaceLfsBatch(this.fetchImpl, config, {
        oid,
        size: preparedSource.size
      });

      if (plan.upload?.chunkSize !== undefined) {
        if (providerPartSize !== undefined && providerPartSize !== plan.upload.chunkSize) {
          throw new Error(
            `huggingface destinations disagree on part size (${providerPartSize} vs ${plan.upload.chunkSize}); `
              + "the plan carries a single chunk size"
          );
        }
        providerPartSize = plan.upload.chunkSize;
      }

      states.push({
        destination: config,
        destinationId: preparedDestination.destination_id,
        sourceId: preparedSource.source_id,
        oid,
        size: preparedSource.size,
        chunkSize: plan.upload?.chunkSize,
        partUrls: plan.upload?.partUrls ?? [],
        uploadHref: plan.upload?.href,
        verifyHref: plan.verifyHref,
        // Runs alongside the transfer; the ETags are only needed at completion time.
        partEtags: plan.upload?.chunkSize === undefined
          ? Promise.resolve([])
          : hashSourceStream(this.fetchImpl, preparedSource.url, {
            partSize: plan.upload.chunkSize,
            sha256: false
          }).then((hashed) => hashed.partEtags)
      });
    }

    return { states, providerPartSize };
  }

  /** Fail before any byte moves if BeamCore did not adopt the Hub's part layout. */
  private assertHuggingFacePlan(prepared: TransferPrepareResponse, states: HuggingFaceUploadState[]): void {
    for (const state of states) {
      const planDestination = prepared.plan_descriptor.destinations.find(
        (candidate) => candidate.destination_id === state.destinationId
      );
      const planSource = prepared.plan_descriptor.sources.find(
        (candidate) => candidate.source_id === state.sourceId
      );
      if (!planDestination || !planSource) {
        throw new Error(`BeamCore plan is missing the huggingface coordinate ${state.sourceId}:${state.destinationId}`);
      }

      const finalObjectKey = planDestination.final_object_keys[state.sourceId];
      if (finalObjectKey !== state.destination.path) {
        throw new Error(
          `BeamCore planned ${finalObjectKey} but the Hub upload was negotiated for ${state.destination.path}`
        );
      }

      if (state.chunkSize === undefined) {
        if (planSource.chunk_count !== 1) {
          throw new Error(
            `${describeHuggingFace(state.destination)} was issued a single-part upload, but the plan has `
              + `${planSource.chunk_count} chunks`
          );
        }
        continue;
      }

      if (prepared.plan_descriptor.chunk_size !== state.chunkSize) {
        throw new Error(
          `the Hub requires ${state.chunkSize}-byte parts for ${describeHuggingFace(state.destination)}, `
            + `but BeamCore planned ${prepared.plan_descriptor.chunk_size}-byte chunks`
        );
      }
      if (planSource.chunk_count !== state.partUrls.length) {
        throw new Error(
          `the Hub issued ${state.partUrls.length} part URLs for ${describeHuggingFace(state.destination)}, `
            + `but the plan has ${planSource.chunk_count} chunks`
        );
      }
    }
  }

  /**
   * Close every Hugging Face upload for a transfer: complete the LFS multipart, verify it, and
   * commit the blob so the file appears in the repo. Runs only after the transfer is complete,
   * so it never sits in the transfer's progression path.
   */
  async finalizeHuggingFaceUploads(transferId: string): Promise<void> {
    const states = this.huggingFaceUploads.get(transferId);
    if (!states?.length) return;
    this.huggingFaceUploads.delete(transferId);

    for (const state of states) {
      if (state.uploadHref && state.chunkSize !== undefined) {
        const etags = await state.partEtags;
        if (etags.length !== state.partUrls.length) {
          throw new Error(
            `computed ${etags.length} part ETags for ${describeHuggingFace(state.destination)}, `
              + `expected ${state.partUrls.length}`
          );
        }
        await huggingFaceCompleteLfsUpload(this.fetchImpl, state.destination, {
          href: state.uploadHref,
          oid: state.oid,
          etags
        });
      }

      if (state.verifyHref) {
        await huggingFaceVerifyLfsUpload(this.fetchImpl, state.destination, {
          href: state.verifyHref,
          oid: state.oid,
          size: state.size
        });
      }

      await huggingFaceCommit(this.fetchImpl, state.destination, {
        oid: state.oid,
        size: state.size
      });
    }
  }

  private async signProviderRoute(input: {
    chunk: Parameters<typeof signDestinationRoute>[0]["chunk"];
    target: Parameters<typeof signDestinationRoute>[0]["target"];
    source: ProviderSourceConfig;
    sourceGrant?: Parameters<typeof signDestinationRoute>[0]["sourceGrant"];
    destination: ProviderDestinationConfig;
    expiresIn: number;
    upload?: MultipartUploadState;
    huggingFaceUpload?: HuggingFaceUploadState;
    transferId: string;
    finalObjectKey: string;
    signedUrlFlow: SignedUrlFlow;
    partNumber: number;
  }): Promise<SignedChunkRoute> {
    if (isHuggingFaceProvider(input.destination)) {
      const state = input.huggingFaceUpload;
      if (!state) throw new Error(`huggingface upload state is missing for ${input.target.destination_id}`);
      // The Hub presigns its own part targets; chunk N carries the URL for part N + 1.
      const destUrl = state.chunkSize === undefined
        ? state.uploadHref
        : state.partUrls[input.chunk.source_chunk_index];
      if (!destUrl) {
        throw new Error(
          `the Hub issued no upload URL for chunk ${input.chunk.source_chunk_index} of `
            + describeHuggingFace(state.destination)
        );
      }
      return signDestinationRoute({
        chunk: input.chunk,
        target: {
          ...input.target,
          object_key: input.finalObjectKey,
          metadata: directPutRouteMetadata(input.target.metadata)
        },
        source: input.source,
 sourceGrant: input.sourceGrant,
        destination: input.destination,
        expiresIn: input.expiresIn,
        destUrl,
        fetchImpl: this.fetchImpl
      });
    }

    if (isHippiusDestination(input.destination)) {
      return signDestinationRoute({
        chunk: input.chunk,
        target: { ...input.target, metadata: partRouteMetadata(input.target.metadata) },
        source: input.source,
 sourceGrant: input.sourceGrant,
        destination: input.destination,
        expiresIn: input.expiresIn,
        fetchImpl: this.fetchImpl
      });
    }

    const upload = input.upload;
    if (!upload) throw new Error(`${input.signedUrlFlow} route is missing its multipart upload`);
    validateMultipartPartNumber(input.partNumber, upload.manifest);

    return signDestinationRoute({
      chunk: input.chunk,
      target: {
        ...input.target,
        object_key: input.finalObjectKey,
        metadata: partRouteMetadata(input.target.metadata)
      },
      source: input.source,
 sourceGrant: input.sourceGrant,
      destination: input.destination,
      expiresIn: input.expiresIn,
      uploadId: upload.uploadId,
      partNumber: input.partNumber,
      finalObjectKey: input.finalObjectKey,
      expectedObjectSize: upload.manifest.expected_object_size,
      expectedPartCount: upload.manifest.expected_part_count,
      maxPartNumber: upload.manifest.max_part_number,
      multipartGroupId: upload.manifest.multipart_group_id,
      completeUrl: upload.manifest.complete_url,
      abortUrl: upload.manifest.abort_url,
      listPageUrl: upload.manifest.list_page_urls[multipartListPageIndex(input.partNumber)],
      finalHeadUrl: upload.manifest.final_head_url,
      finalObjectMetadata: upload.manifest.final_object_metadata,
      fetchImpl: this.fetchImpl
    });
  }

  private async startProviderRouteRecoverySigner(input: {
    owner: object;
    prepared: TransferPrepareResponse;
    sourcesById: Map<string, ProviderSourceConfig>;
    destinationsById: Map<string, ProviderDestinationConfig>;
    multipartUploads: Map<string, MultipartUploadState>;
    expiresIn: number;
    signal?: AbortSignal;
  }): Promise<void> {
    this.stopRecoverySigner(input.prepared.transfer_id);
    const stop = await this.control.serveRouteRecoverySigner(
      input.prepared.transfer_id,
      async (payload: RouteRecoverySignRequestPayload): Promise<RouteRecoverySignReplyPayload> => {
        input.signal?.throwIfAborted();
        if (payload.route_generation_id.length === 0) throw new Error("route recovery generation is required");
        const sourceGrants = new Map<string, ReturnType<typeof signSourceChunk>>();
        const routes = await mapOrderedWithConcurrency(
          payload.chunks,
          this.routeSigningConcurrency,
          async (requested): Promise<SignedChunkRoute> => {
            input.signal?.throwIfAborted();
            if (requested.route_generation_id !== payload.route_generation_id) {
              throw new Error("route recovery chunk generation mismatch");
            }
            const chunk = materializePlanChunk(
              input.prepared.plan_descriptor,
              input.prepared.transfer_id,
              requested.source_id,
              requested.chunk_index
            );
            if (requested.part_number !== multipartPartNumber(chunk.source_chunk_index, requested.attempt_slot)) {
              throw new Error("route recovery multipart slot mapping mismatch");
            }
            if (
              chunk.source_offset !== requested.source_offset
              || chunk.chunk_size !== requested.chunk_size
            ) {
              throw new Error("route recovery source coordinate mismatch");
            }
            const target = chunk.destinations.find((candidate) =>
              candidate.destination_id === requested.destination_id
              && Number(candidate.metadata?.delivery_index) === requested.delivery_index
            );
            if (!target) throw new Error("route recovery destination coordinate mismatch");
            const source = input.sourcesById.get(requested.source_id);
            const destination = input.destinationsById.get(requested.destination_id);
            if (!source || !destination) throw new Error("route recovery provider configuration is unavailable");
            const upload = isDirectPutDestination(destination)
              ? undefined
              : await this.recoveryMultipartUploadState({
                  prepared: input.prepared,
                  destination,
                  requested,
                  multipartUploads: input.multipartUploads,
                  expiresIn: input.expiresIn
                });
            const sourceKey = JSON.stringify([chunk.source_id, chunk.chunk_index]);
 let sourceGrant = sourceGrants.get(sourceKey);
 if (!sourceGrant) { sourceGrant = signSourceChunk({ source, chunk, fallbackUrl: chunk.source_url, expiresIn: input.expiresIn, fetchImpl: this.fetchImpl }); sourceGrants.set(sourceKey, sourceGrant); void sourceGrant.catch(() => {}); }
            const route = await this.signProviderRoute({
 sourceGrant,
              chunk,
              target: {
                ...target,
                // Direct-PUT destinations (Hippius, Hugging Face) keep the planned
                // per-chunk key exactly as initial materialization does; only
                // multipart destinations sign parts against the final object key.
                object_key: isDirectPutDestination(destination) ? target.object_key : requested.final_object_key,
                metadata: {
                  ...(target.metadata ?? {}),
                  ...(requested.destination_metadata ?? {}),
                  transfer_id: input.prepared.transfer_id,
                  final_object_key: requested.final_object_key,
                  upload_id: requested.upload_id,
                  multipart_group_id: requested.multipart_group_id,
                  delivery_index: requested.delivery_index,
                  part_number: requested.part_number,
                  logical_attempt_index: requested.logical_attempt_index,
                  attempt_slot: requested.attempt_slot,
                  route_generation_id: requested.route_generation_id
                }
              },
              source,
              destination,
              expiresIn: input.expiresIn,
              ...(upload ? { upload } : {}),
              huggingFaceUpload: this.huggingFaceUploads
                .get(input.prepared.transfer_id)
                ?.find((state) => state.destinationId === requested.destination_id),
              transferId: input.prepared.transfer_id,
              finalObjectKey: requested.final_object_key,
              signedUrlFlow: "signed_url",
              partNumber: requested.part_number
            });
            return signMultipartRecovery({ destination, transferId: input.prepared.transfer_id, requested, route, expiresIn: input.expiresIn });
          }
        );
        return {
          transfer_id: payload.transfer_id,
          route_generation_id: payload.route_generation_id,
          signed_at: new Date().toISOString(),
          chunk_routes: routes
        };
      }
    );
    if (input.signal?.aborted) { stop(); return; }
    this.stopRecoverySigner(input.prepared.transfer_id);
    this.integrityAuditSigners.set(input.prepared.transfer_id, (challenge) => this.buildProviderIntegrityAuditGrants({
      challenge, prepared: input.prepared, sourcesById: input.sourcesById, destinationsById: input.destinationsById, expiresIn: input.expiresIn
    }));
    const registration = { owner: input.owner, stop };
    this.recoverySigners.set(input.prepared.transfer_id, registration);
    try {
      const stopIntegrity = await this.control.serveIntegritySigner(input.prepared.transfer_id, challenge => {
        input.signal?.throwIfAborted();
        return this.signedIntegrityGrants(challenge);
      });
      if (input.signal?.aborted || this.recoverySigners.get(input.prepared.transfer_id) !== registration) {
        stop(); stopIntegrity(); return;
      }
      registration.stop = () => { stop(); stopIntegrity(); };
    } catch {
      // Status-driven signing remains available if the optional subscription fails.
      if (input.signal?.aborted) this.stopRecoverySigner(input.prepared.transfer_id, input.owner);
    }
  }

  private async recoveryMultipartUploadState(input: {
    prepared: TransferPrepareResponse;
    destination: ProviderDestinationConfig;
    requested: RouteRecoverySignRequestPayload["chunks"][number];
    multipartUploads: Map<string, MultipartUploadState>;
    expiresIn: number;
  }): Promise<MultipartUploadState> {
    const existing = input.multipartUploads.get(input.requested.multipart_group_id);
    if (existing) {
      if (
        existing.uploadId !== input.requested.upload_id
        || existing.objectKey !== input.requested.final_object_key
        || existing.manifest.source_id !== input.requested.source_id
        || existing.manifest.destination_id !== input.requested.destination_id
      ) {
        throw new Error("route recovery multipart identity mismatch");
      }
      return existing;
    }
    if (!input.requested.upload_id) {
      throw new Error("route recovery multipart upload id is required");
    }
    const source = input.prepared.plan_descriptor.sources.find((candidate) =>
      candidate.source_id === input.requested.source_id
    );
    if (!source) {
      throw new Error("route recovery source plan is unavailable");
    }
    const expectedGroupId = multipartGroupStateKey(
      input.prepared.transfer_id,
      input.requested.destination_id,
      input.requested.source_id,
      input.requested.final_object_key
    );
    if (input.requested.multipart_group_id !== expectedGroupId) {
      throw new Error("route recovery multipart group identity mismatch");
    }
    const finalObjectMetadata = { "beam-transfer-id": input.prepared.transfer_id };
    const maxPartNumber = multipartMaxPartNumber(source.chunk_count);
    const listPageMarkers = multipartListPageMarkers(maxPartNumber);
    const [completeUrl, abortUrl, listPageUrls, finalHeadUrl] = await Promise.all([
      signCompleteMultipartUpload(
        input.destination,
        input.requested.final_object_key,
        input.requested.upload_id,
        input.expiresIn
      ),
      signAbortMultipartUpload(
        input.destination,
        input.requested.final_object_key,
        input.requested.upload_id,
        input.expiresIn
      ),
      Promise.all(listPageMarkers.map((partNumberMarker) =>
        signListMultipartUpload(
          input.destination,
          input.requested.final_object_key,
          input.requested.upload_id,
          input.expiresIn,
          { maxParts: 1_000, ...(partNumberMarker ? { partNumberMarker } : {}) }
        )
      )),
      signFinalObjectHead(input.destination, input.requested.final_object_key, input.expiresIn)
    ]);
    const manifest: MultipartGroupManifest = {
      multipart_group_id: input.requested.multipart_group_id,
      source_id: input.requested.source_id,
      destination_id: input.requested.destination_id,
      final_object_key: input.requested.final_object_key,
      upload_id: input.requested.upload_id,
      expected_object_size: source.size,
      expected_part_count: source.chunk_count,
      max_part_number: maxPartNumber,
      complete_url: completeUrl,
      abort_url: abortUrl,
      list_page_urls: listPageUrls,
      final_head_url: finalHeadUrl,
      final_object_metadata: finalObjectMetadata,
      urls_expires_at: expiresAtIso(input.expiresIn)
    };
    validateMultipartGroupManifest([manifest], input.prepared.transfer_id);
    const state = {
      destination: input.destination,
      objectKey: input.requested.final_object_key,
      uploadId: input.requested.upload_id,
      manifest
    };
    input.multipartUploads.set(input.requested.multipart_group_id, state);
    return state;
  }

  private async submitIntegrityAuditGrantsIfPresent(status: TransferStatusInfo): Promise<void> {
    const challenge = status.integrity_audit_challenge;
    if (!challenge) return;
    const signer = this.integrityAuditSigners.get(challenge.transfer_id);
    if (!signer) throw new Error("integrity audit signer unavailable");
    const existing = this.integrityAuditSubmissions.get(challenge.audit_id);
    const cached = this.integrityGrantCache.get(challenge.audit_id);
    if (cached && cached.fingerprint !== JSON.stringify(challenge)) throw new Error("integrity audit identity changed");
    if (existing && cached && cached.expiresAt > Date.now() + 30_000) {
      await existing;
      return;
    }
    const submission = this.signedIntegrityGrants(challenge).then(async payload => {
      const result = await this.control.request<Record<string, unknown>>("transfer.integrity_audit_grants", payload, {
        transferId: challenge.transfer_id, idempotencyKey: `transfer:${challenge.transfer_id}:integrity-audit:${challenge.audit_id}:${payload.submitted_at}`
      });
      if (result.published !== true) throw new Error("integrity audit delivery unavailable");
    });
    this.integrityAuditSubmissions.set(challenge.audit_id, submission);
    try {
      await submission;
    } catch (error) {
      if (this.integrityAuditSubmissions.get(challenge.audit_id) === submission) this.integrityAuditSubmissions.delete(challenge.audit_id);
      throw error;
    }
  }

  private signedIntegrityGrants(challenge: IntegrityAuditChallenge): Promise<Record<string, unknown>> {
    const fingerprint = JSON.stringify(challenge);
    const cached = this.integrityGrantCache.get(challenge.audit_id);
    if (cached && cached.fingerprint !== fingerprint) return Promise.reject(new Error("integrity audit identity changed"));
    if (cached && cached.expiresAt > Date.now() + 30_000) return cached.payload;
    if (!cached && this.integrityGrantCache.size >= 1024) return Promise.reject(new Error("integrity signer capacity unavailable"));
    const signer = this.integrityAuditSigners.get(challenge.transfer_id);
    if (!signer) return Promise.reject(new Error("integrity audit signer unavailable"));
    this.integrityAuditSubmissions.delete(challenge.audit_id);
    const entry = { transferId: challenge.transfer_id, expiresAt: 0, fingerprint, payload: Promise.resolve({}) as Promise<Record<string, unknown>> };
    entry.payload = signer(challenge).then(payload => {
      const chunks = payload.chunks as Array<{ source: { expires_at: string }; destination: { expires_at: string } }>;
      entry.expiresAt = Math.min(...chunks.flatMap(chunk => [Date.parse(chunk.source.expires_at), Date.parse(chunk.destination.expires_at)]));
      return payload;
    }).catch(error => { if (this.integrityGrantCache.get(challenge.audit_id) === entry) this.integrityGrantCache.delete(challenge.audit_id); throw error; });
    // Pending grants are shared too. Actual expiry replaces this sentinel on completion.
    entry.expiresAt = Infinity;
    this.integrityGrantCache.set(challenge.audit_id, entry);
    return entry.payload;
  }

  private async buildProviderIntegrityAuditGrants(input: {
    challenge: IntegrityAuditChallenge;
    prepared: TransferPrepareResponse;
    sourcesById: Map<string, ProviderSourceConfig>;
    destinationsById: Map<string, ProviderDestinationConfig>;
    expiresIn: number;
  }): Promise<Record<string, unknown>> {
    if (input.challenge.transfer_id !== input.prepared.transfer_id) {
      throw new Error("integrity audit challenge transfer mismatch");
    }
    const submittedAt = new Date().toISOString();
    const chunks = await mapOrderedWithConcurrency(
      input.challenge.chunks,
      Math.min(this.routeSigningConcurrency, input.challenge.chunks.length || 1),
      async (chunk) => {
        const planChunk = materializePlanChunk(
          input.prepared.plan_descriptor,
          input.prepared.transfer_id,
          chunk.source_id,
          chunk.route_chunk_index
        );
        if (
          chunk.source_offset < planChunk.source_offset
          || chunk.source_offset + chunk.range_length > planChunk.source_offset + planChunk.chunk_size
        ) {
          throw new Error("integrity audit source range is outside the planned chunk");
        }
        const target = planChunk.destinations.find((candidate) =>
          candidate.destination_id === chunk.destination_id
          && Number(candidate.metadata?.delivery_index) === chunk.delivery_index
        );
        if (!target || target.metadata?.final_object_key !== chunk.final_object_key) {
          throw new Error("integrity audit destination coordinate mismatch");
        }
        if (chunk.destination_offset !== chunk.source_offset) {
          throw new Error("integrity audit destination range mismatch");
        }
        const source = input.sourcesById.get(chunk.source_id);
        const destination = input.destinationsById.get(chunk.destination_id);
        const plannedSource = input.prepared.plan_descriptor.sources.find((candidate) => candidate.source_id === chunk.source_id);
        if (!source || !destination) {
          throw new Error("integrity audit provider configuration is unavailable");
        }
        const sourceEtag = plannedSource?.metadata?.etag;
        const sourceVersionId = plannedSource?.metadata?.version_id;
        const grantExpiresAt = new Date(Math.floor(Date.now() / 1000) * 1000 + input.expiresIn * 1000).toISOString();
        const [sourceGrant, destinationGrant] = await Promise.all([
          signSourceReadRange({
            source,
            ...(typeof sourceEtag === "string" ? { ifMatch: sourceEtag } : {}),
            ...(typeof sourceVersionId === "string" ? { versionId: sourceVersionId } : {}),
            offset: chunk.source_offset,
            length: chunk.range_length,
            expiresIn: input.expiresIn,
            fetchImpl: this.fetchImpl
          }),
          signDestinationReadRange({
            destination,
            objectKey: chunk.final_object_key,
            ...(chunk.final_object_etag ? { ifMatch: chunk.final_object_etag } : {}),
            offset: chunk.destination_offset,
            length: chunk.range_length,
            expiresIn: input.expiresIn,
            fetchImpl: this.fetchImpl
          })
        ]);
        const {
          orchestrator_id: _orchestratorId,
          orchestrator_hotkey: _orchestratorHotkey,
          worker_id: _workerId,
          ...grantChunk
        } = chunk;
        return {
          ...grantChunk,
          source: { ...sourceGrant, expires_at: boundedGrantExpiry(sourceGrant.url, grantExpiresAt) },
          destination: { ...destinationGrant, expires_at: boundedGrantExpiry(destinationGrant.url, grantExpiresAt) }
        };
      }
    );
    return { transfer_id: input.challenge.transfer_id, audit_id: input.challenge.audit_id, submitted_at: submittedAt, chunks };
  }

  /** Stop a transfer's recovery and integrity signers; with `owner`, only if that owner installed them. */
  private stopRecoverySigner(transferId: string, owner?: object): void {
    const signer = this.recoverySigners.get(transferId);
    if (owner && signer?.owner !== owner) return;
    if (signer) {
      signer.stop();
      this.recoverySigners.delete(transferId);
    }
    this.integrityAuditSigners.delete(transferId);
    for (const [auditId, entry] of this.integrityGrantCache) if (entry.transferId === transferId) {
      this.integrityGrantCache.delete(auditId); this.integrityAuditSubmissions.delete(auditId);
    }
  }

  private stopAllRecoverySigners(): void {
    for (const transferId of [...this.recoverySigners.keys()]) this.stopRecoverySigner(transferId);
  }

  async createAndDistribute(input: RawTransferCreateInput): Promise<TransferCreateResponse> {
    const transfer = await this.createRawTransfer(input);
    if (transfer.success) {
      await this.distributeTransfer(transfer.transfer_id);
    }
    return transfer;
  }

  /**
   * Waits until the transfer completes. A failed transfer rejects with
   * {@link BeamTransferFailedError}, or its {@link BeamStorageAccessError} subclass when storage
   * refused Beam's requests.
   */
  async waitForTransfer(
    transferId: string,
    options: { timeoutMs?: number; pollIntervalMs?: number; maxPollIntervalMs?: number } = {}
  ): Promise<TransferStatusInfo> {
    const timeoutMs = options.timeoutMs ?? 300_000;
    const pollIntervalMs = options.pollIntervalMs ?? 15_000;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new Error("timeoutMs must be a positive number.");
    }
    if (!Number.isFinite(pollIntervalMs) || pollIntervalMs <= 0) {
      throw new Error("pollIntervalMs must be a positive number.");
    }
    const configuredMaxPollIntervalMs = options.maxPollIntervalMs ?? 30_000;
    if (!Number.isFinite(configuredMaxPollIntervalMs) || configuredMaxPollIntervalMs <= 0) {
      throw new Error("maxPollIntervalMs must be a positive number.");
    }
    const maxPollIntervalMs = Math.max(pollIntervalMs, configuredMaxPollIntervalMs);
    const startedAt = Date.now();
    let currentPollIntervalMs = pollIntervalMs;

    let terminalWaiter: TransferTerminalSignalWaiter | null = null;
    try {
      terminalWaiter = await this.openTransferTerminalWaiter(transferId);
    } catch {
      // Terminal delivery is advisory; authoritative status remains available.
    }
    try {
      while (true) {
        const status = await this.transferStatus(transferId);
        if (status.status === "completed") {
          // The parts have landed; publish them as a Hub commit before reporting success.
          await this.finalizeHuggingFaceUploads(transferId);
          return status;
        }
        if (status.status === "failed") {
          throw transferFailedError(transferId, status.error_message);
        }
        if (status.status === "cancelled") {
          throw new Error("Transfer cancelled");
        }
        if (Date.now() - startedAt >= timeoutMs) {
          throw new Error(`Transfer ${transferId} did not complete within ${timeoutMs}ms`);
        }
        const remainingMs = timeoutMs - (Date.now() - startedAt);
        const waitMs = Math.min(Math.max(1, remainingMs), jitteredDelay(currentPollIntervalMs));
        let terminalEvent: TransferTerminalEvent | null = null;
        if (terminalWaiter) {
          const waitStartedAt = Date.now();
          try {
            terminalEvent = await terminalWaiter.wait(waitMs);
          } catch {
            try {
              await terminalWaiter.close();
            } catch {
              // The status fallback remains authoritative even if unsubscribe fails.
            }
            terminalWaiter = null;
            const remainingWaitMs = Math.max(0, waitMs - (Date.now() - waitStartedAt));
            if (remainingWaitMs > 0) {
              await new Promise((resolve) => setTimeout(resolve, remainingWaitMs));
            }
          }
        } else {
          await new Promise((resolve) => setTimeout(resolve, waitMs));
        }
        if (terminalEvent) currentPollIntervalMs = pollIntervalMs;
        else currentPollIntervalMs = Math.min(maxPollIntervalMs, Math.ceil(currentPollIntervalMs * 1.5));
      }
    } finally {
      if (terminalWaiter) {
        try {
          await terminalWaiter.close();
        } catch {
          // Terminal delivery is advisory; closing the client remains the final cleanup boundary.
        }
      }
    }
  }
}

const ROUTE_STREAM_BATCH_ROUTES = 1_024;

class AsyncMutex {
  private tail: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    return release;
  }

  async run<T>(operation: () => Promise<T>): Promise<T> {
    const release = await this.acquire();
    try {
      return await operation();
    } finally {
      release();
    }
  }
}

class RouteStreamSender {
  private readonly started = performance.now();
  private readonly telemetry: SdkPerformanceCollector;
  private readonly streamId: string;
  private readonly checksum = new RouteKeysChecksum();
  private readonly batch: SignedChunkRoute[] = [];
  private readonly seenDeliveryIndices = new Set<number>();
  private batchIndex = 0;
  private routeCount = 0;
  private sendTail: Promise<void> = Promise.resolve();
  private sendError: unknown = null;
  private sending = false;

  constructor(
    private readonly control: BeamTransferControl,
  private readonly options: {
      streamId: string;
      transferId: string;
      totalRoutes: number;
      totalChunks: number;
      routeGenerationId: string;
      urlsExpiresAt?: string;
      signedUrlFlow: SignedUrlFlow;
      autoDistribute: boolean;
      telemetry?: SdkPerformanceCollector;
      onDiagnostics?: BeamClientOptions["onDiagnostics"];
    }
  ) {
    this.streamId = options.streamId;
    this.telemetry = options.telemetry ?? new SdkPerformanceCollector();
  }

  async begin(): Promise<void> {
    this.telemetry.startDelaySampling();
    await this.control.request("transfer.route_stream.begin", compact({
      transfer_id: this.options.transferId,
      route_generation_id: this.options.routeGenerationId,
      stream_id: this.streamId,
      total_routes: this.options.totalRoutes,
      total_chunks: this.options.totalChunks,
      route_contract_version: this.options.signedUrlFlow,
      urls_expires_at: this.options.urlsExpiresAt,
      signed_url_flow: this.options.signedUrlFlow,
      auto_distribute: this.options.autoDistribute
    }), {
      transferId: this.options.transferId,
      idempotencyKey: `transfer:${this.options.transferId}:route-stream:${this.streamId}:begin`
    });
  }

  async addManifestGroups(groups: MultipartGroupManifest[]): Promise<void> {
    const started = performance.now();
    if (!groups.length) return;
    await Promise.all(groups.map(async (group) => {
      const identity = await stableManifestBatchIdentity([group]);
      await this.control.request("transfer.route_stream.manifest", {
        transfer_id: this.options.transferId,
        route_generation_id: this.options.routeGenerationId,
        stream_id: this.streamId,
        manifest_batch_id: identity,
        groups: [group]
      }, {
        transferId: this.options.transferId,
        idempotencyKey: `transfer:${this.options.transferId}:route-stream:${this.streamId}:manifest:${identity}`
      });
    }));
    this.telemetry.observe("sdk.manifest_ack", performance.now() - started); this.telemetry.mark("first_manifest_ms");
  }

  async addRoute(route: SignedChunkRoute): Promise<void> {
    this.telemetry.mark("first_route_ms");
    const assemblyStarted = performance.now();
    const deliveryIndex = signedRouteDeliveryIndex(route);
    if (deliveryIndex === undefined) {
      throw new Error("route delivery_index is required");
    }
    if (!Number.isSafeInteger(deliveryIndex) || deliveryIndex < 0 || deliveryIndex >= this.options.totalRoutes) {
      throw new Error(`route delivery_index ${deliveryIndex} is outside the declared route stream`);
    }
    if (this.seenDeliveryIndices.has(deliveryIndex)) {
	  throw new Error(`duplicate route delivery_index ${deliveryIndex}`);
	}
    this.seenDeliveryIndices.add(deliveryIndex);
	this.routeCount += 1;
    this.batch.push({ ...route, delivery_index: deliveryIndex });
    this.telemetry.gauge("buffered_batches_peak", 1 + Number(this.sending));
    this.telemetry.observe("sdk.route_assembly", performance.now() - assemblyStarted);
    if (this.batch.length >= ROUTE_STREAM_BATCH_ROUTES) {
      await this.enqueueFlush();
	}
  }

  async complete(): Promise<AttachSignedUrlsResponse> {
    if (
      this.routeCount !== this.options.totalRoutes ||
      this.seenDeliveryIndices.size !== this.options.totalRoutes
    ) {
      throw new Error(
        `route stream has incomplete or duplicate delivery indices: received ${this.routeCount} of ${this.options.totalRoutes} routes`
      );
    }
    this.telemetry.mark("final_flush_ms");
    await this.enqueueFlush();
    await this.sendTail;
    if (this.sendError) throw this.sendError;
    this.telemetry.observe("sdk.preparation", performance.now() - this.started);
    this.telemetry.finish();
    const sdkPerformance = this.telemetry.snapshot(this.control.supportsPerformanceV2?.(this.options.transferId) ?? false);
    this.options.onDiagnostics?.(sdkPerformance);
    return this.control.request<AttachSignedUrlsResponse>("transfer.route_stream.complete", {
      sdk_performance: sdkPerformance,
      transfer_id: this.options.transferId,
      route_generation_id: this.options.routeGenerationId,
      stream_id: this.streamId,
      expected_batches: this.batchIndex,
      expected_routes: this.options.totalRoutes,
      route_keys_checksum: this.checksum.value()
    }, {
      transferId: this.options.transferId,
      idempotencyKey: `transfer:${this.options.transferId}:route-stream:${this.streamId}:complete`
    });
  }

  private async enqueueFlush(): Promise<void> {
    if (!this.batch.length) return;
	await this.telemetry.measure("sdk.buffer_wait", () => this.sendTail);
	if (this.sendError) throw this.sendError;
    const routes = this.batch.splice(0, this.batch.length);
    const encodingStarted = performance.now();
    const chunks = this.control.splitRoutesForPayload("transfer.route_stream.batch", {
      transfer_id: this.options.transferId,
      route_generation_id: this.options.routeGenerationId,
      stream_id: this.streamId,
      batch_id: `${this.streamId}:estimate`,
      batch_index: this.batchIndex
    }, routes) as SignedChunkRoute[][];
    this.telemetry.observe("sdk.batch_encode", performance.now() - encodingStarted);
    this.sending = true;
    for (const chunk of chunks) {
      const queuedAt = performance.now();
      const batchIndex = this.batchIndex;
      this.batchIndex += 1;
      this.sendTail = this.sendTail.then(async () => {
        this.telemetry.observe("sdk.batch_queue", performance.now() - queuedAt);
        if (this.sendError) return;
        try {
          const checksumStarted = performance.now();
          const chunkChecksum = new RouteKeysChecksum();
          await chunkChecksum.addMany(chunk.map(routeKeyForSignedRoute));
          this.checksum.merge(chunkChecksum);
          const routeChecksum = chunkChecksum.value();
          const coordinateChecksum = await routeCoordinateChecksum(chunk);
          const batchId = `${this.streamId}:${batchIndex}:${coordinateChecksum}`;
          this.telemetry.observe("sdk.batch_checksum", performance.now() - checksumStarted);
          this.telemetry.gauge("batch_routes_max", chunk.length);
          this.telemetry.mark("first_batch_ms");
          this.telemetry.counters.route_batches++;
          await this.telemetry.measure("sdk.batch_ack", () => this.control.request("transfer.route_stream.batch", {
            transfer_id: this.options.transferId,
            route_generation_id: this.options.routeGenerationId,
            stream_id: this.streamId,
            batch_id: batchId,
            batch_index: batchIndex,
            route_batch: compactSignedRoutes(chunk),
            route_count: chunk.length,
            route_keys_checksum: routeChecksum
          }, {
            transferId: this.options.transferId,
            idempotencyKey: `transfer:${this.options.transferId}:route-stream:${this.streamId}:batch:${batchIndex}:${coordinateChecksum}`
          }));
        } catch (error) {
          this.sendError = error;
        }
      });
    }
    this.sendTail = this.sendTail.finally(() => { this.sending = false; });
  }

  async abort(): Promise<void> {
    this.telemetry.finish();
    this.sendError ??= new Error("route_stream_aborted");
    await this.sendTail;
  }
}

class RouteKeysChecksum {
  private readonly bytes = new Uint8Array(32);
  private count = 0;

  async addMany(routeKeys: string[]): Promise<void> {
    const digests = await Promise.all(routeKeys.map(sha256Bytes));
    for (const digest of digests) {
      for (let index = 0; index < this.bytes.length; index += 1) {
        this.bytes[index] ^= digest[index] ?? 0;
      }
    }
    this.count += routeKeys.length;
  }

  merge(other: RouteKeysChecksum): void {
    for (let index = 0; index < this.bytes.length; index += 1) {
      this.bytes[index] ^= other.bytes[index] ?? 0;
    }
    this.count += other.count;
  }

  value(): string {
    return `sha256-xor-v1:${this.count}:${hex(this.bytes)}`;
  }
}

function routeKeyForSignedRoute(route: SignedChunkRoute): string {
  return `${route.source_id}:${route.destination_id}:${route.chunk_index}`;
}

function signedRouteDeliveryIndex(route: SignedChunkRoute): number | undefined {
  const metadataIndex = route.metadata?.delivery_index;
  if (typeof metadataIndex === "number") return metadataIndex;
  return route.delivery_index;
}

function compareSignedRoutesByDelivery(left: SignedChunkRoute, right: SignedChunkRoute): number {
  const leftIndex = signedRouteDeliveryIndex(left);
  const rightIndex = signedRouteDeliveryIndex(right);
  if (leftIndex !== undefined && rightIndex !== undefined && leftIndex !== rightIndex) {
    return leftIndex - rightIndex;
  }
  return routeKeyForSignedRoute(left).localeCompare(routeKeyForSignedRoute(right));
}

async function routeCoordinateChecksum(routes: SignedChunkRoute[]): Promise<string> {
  const checksum = new RouteKeysChecksum();
  await checksum.addMany(routes.map((route) =>
    `${routeKeyForSignedRoute(route)}:${signedRouteDeliveryIndex(route) ?? "missing"}`
  ));
  return checksum.value();
}

async function stableRouteStreamId(input: {
  transferId: string;
  planIdentity: string;
  totalRoutes: number;
  totalChunks: number;
  signedUrlFlow: SignedUrlFlow;
}): Promise<string> {
  const digest = await sha256Bytes(
    `beam:route-stream:${input.signedUrlFlow}:${input.transferId}:${input.planIdentity}:${input.totalRoutes}:${input.totalChunks}`
  );
  const bytes = digest.slice(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const value = hex(bytes);
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
}

async function stableManifestBatchIdentity(groups: MultipartGroupManifest[]): Promise<string> {
  const identities = groups
    .map((group) => `${group.multipart_group_id}:${group.source_id}:${group.destination_id}`)
    .sort();
  return hex((await sha256Bytes(identities.join("\n"))).slice(0, 16));
}

function countDistinctRouteChunks(routes: SignedChunkRoute[]): number {
  return new Set(routes.map((route) => `${route.source_id}:${route.chunk_index}`)).size;
}

function* materializePlanChunks(
  descriptor: CompactTransferPlanDescriptor,
  transferId: string
): Generator<ChunkSigningPlanItem> {
  for (const source of descriptor.sources) {
    for (let sourceChunkIndex = 0; sourceChunkIndex < source.chunk_count; sourceChunkIndex += 1) {
      yield materializePlanChunk(
        descriptor,
        transferId,
        source.source_id,
        source.global_chunk_start + sourceChunkIndex
      );
    }
  }
}

async function mapOrderedWithConcurrency<T, R>(
  values: readonly T[],
  concurrency: number,
  map: (value: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= values.length) return;
      results[index] = await map(values[index]!, index);
    }
  });
  await Promise.all(workers);
  return results;
}

function integrityAuditErrorSummary(error: unknown): string {
  if (!(error instanceof Error)) return "unknown_error";
  const message = error.message.trim();
  if (!message || /https?:\/\/|x-amz|password|secret|token|authorization|credential/i.test(message)) {
    return error.name || "unknown_error";
  }
  return message.slice(0, 200);
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 1) throw new Error(`${label} must be a positive integer.`);
  return value;
}

function materializePlanChunk(
  descriptor: CompactTransferPlanDescriptor,
  transferId: string,
  sourceId: string,
  chunkIndex: number
): ChunkSigningPlanItem {
  const source = descriptor.sources.find((candidate) => candidate.source_id === sourceId);
  if (!source) throw new Error(`plan source not found: ${sourceId}`);
  const sourceChunkIndex = chunkIndex - source.global_chunk_start;
  if (sourceChunkIndex < 0 || sourceChunkIndex >= source.chunk_count) {
    throw new Error(`plan chunk is outside source range: ${sourceId}:${chunkIndex}`);
  }
  const sourceOffset = sourceChunkIndex * descriptor.chunk_size;
  const chunkSize = Math.min(descriptor.chunk_size, source.size - sourceOffset);
  return {
    chunk_index: chunkIndex,
    source_id: sourceId,
    source_chunk_index: sourceChunkIndex,
    source_offset: sourceOffset,
    chunk_size: chunkSize,
    source_url: source.url,
    destinations: descriptor.destinations.map((destination) => {
      const finalObjectKey = destination.final_object_keys[sourceId];
      if (!finalObjectKey) throw new Error(`plan final object key not found: ${sourceId}:${destination.destination_id}`);
      const deliveryIndex = chunkIndex * descriptor.destinations.length + destination.destination_index;
      const partNumber = multipartPartNumber(sourceChunkIndex);
      const objectKey = destination.provider.toLowerCase() === "hippius"
        ? `${finalObjectKey}/${descriptor.plan_nonce}/chunk-${String(sourceChunkIndex).padStart(6, "0")}`
        : finalObjectKey;
      return {
        destination_id: destination.destination_id,
        delivery_index: deliveryIndex,
        provider: destination.provider,
        object_key: objectKey,
        metadata: {
          ...(destination.metadata ?? {}),
          final_object_key: finalObjectKey,
          part_number: partNumber,
          logical_attempt_index: 0,
          attempt_slot: 0,
          route_generation_id: `initial-${chunkIndex}-${destination.destination_id}`,
          delivery_index: deliveryIndex
        }
      };
    })
  };
}

async function sha256Bytes(value: string): Promise<Uint8Array> {
  const input = new TextEncoder().encode(value);
  if (globalThis.crypto?.subtle) {
    return new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", input));
  }
  const { createHash } = await import("node:crypto");
  return new Uint8Array(createHash("sha256").update(value).digest());
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function cancelTransferAfterProviderFailure(
  cancelTransfer: () => Promise<TransferCancelResponse>,
  transferId: string,
  cause: unknown
): Promise<void> {
  try {
    const result = await cancelTransfer();
    if (!result.success) {
      throw new Error(result.message ?? `Beam rejected cancellation for ${transferId}`);
    }
  } catch (cancelError) {
    throw new Error(
      `provider transfer failed (${safeErrorCode(cause)}) and transfer cancellation failed (${safeErrorCode(cancelError)})`
    );
  }
}

async function cancelAndAbortProviderFailure(input: {
  cancelTransfer: () => Promise<TransferCancelResponse>;
  transferId: string;
  cause: unknown;
  uploads: Map<string, MultipartUploadCleanupState>;
  abortBeforeCancel: boolean;
  cleanupConcurrency: number;
}): Promise<never> {
  let cleanupError: unknown;
  let cleanupFailed = false;
  let cancelError: unknown;
  if (input.abortBeforeCancel) {
    try {
      await abortCreatedUploads(input.uploads, input.cleanupConcurrency);
    } catch (error) {
      cleanupError = error;
      cleanupFailed = true;
    }
  }
  try {
    await cancelTransferAfterProviderFailure(input.cancelTransfer, input.transferId, input.cause);
  } catch (error) {
    cancelError = error;
  }
  if (!input.abortBeforeCancel || cleanupFailed) {
    try {
      await abortCreatedUploads(input.uploads, input.cleanupConcurrency);
      cleanupError = undefined;
      cleanupFailed = false;
    } catch (error) {
      cleanupError = error;
      cleanupFailed = true;
    }
  }
  throw new BeamProviderTransferError({
    transferId: input.transferId,
    cause: input.cause,
    ...(cancelError !== undefined ? { cancelError } : {}),
    ...(cleanupFailed ? { cleanupError } : {})
  });
}

function safeErrorCode(error: unknown): string {
  if (!(error instanceof Error)) return "UnknownError";
  const record = error as Error & { code?: unknown; status?: unknown; statusCode?: unknown };
  const code = typeof record.code === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(record.code)
    ? record.code
    : error.name;
  const status = [record.status, record.statusCode]
    .find((value): value is number => Number.isInteger(value) && Number(value) >= 100 && Number(value) <= 599);
  return status === undefined ? code : `${code}:status=${status}`;
}
interface MultipartUploadCleanupState {
  destination: ProviderDestinationConfig;
  objectKey: string;
  uploadId: string;
}

interface MultipartUploadState extends MultipartUploadCleanupState {
  manifest: MultipartGroupManifest;
}

function multipartGroupIdentity(
  transferId: string,
  manifest: MultipartGroupManifest
): ProviderMultipartGroupIdentity {
  return {
    transferId,
    multipartGroupId: manifest.multipart_group_id,
    sourceId: manifest.source_id,
    destinationId: manifest.destination_id,
    objectKey: manifest.final_object_key,
    uploadId: manifest.upload_id,
    expectedObjectSize: manifest.expected_object_size,
    expectedPartCount: manifest.expected_part_count,
    expiresAt: manifest.urls_expires_at
  };
}

/**
 * One Hugging Face LFS upload, held from prepare until the transfer's commit.
 *
 * The Hub issues upload URLs only for a known sha256 and dictates the part size, so the
 * plan is built around what the LFS batch hands back rather than the other way round.
 */
interface HuggingFaceUploadState {
  destination: HuggingFaceProviderConfig;
  destinationId: string;
  sourceId: string;
  oid: string;
  size: number;
  chunkSize?: number;
  partUrls: string[];
  /** Multipart completion endpoint, or the single-part PUT target. */
  uploadHref?: string;
  verifyHref?: string;
  /** Resolves alongside the transfer; awaited only at commit time. */
  partEtags: Promise<string[]>;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  // A waiter may be rejected after its route loop already stopped (for example when
  // ownership is fenced mid-stream); awaiting callers still observe the rejection.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

function createMultipartGroupWaiters(
  prepared: TransferPrepareResponse,
  destinationsById: Map<string, ProviderDestinationConfig>
): Map<string, Deferred<MultipartUploadState>> {
  const waiters = new Map<string, Deferred<MultipartUploadState>>();
  for (const source of prepared.plan_descriptor.sources) {
    for (const destination of prepared.plan_descriptor.destinations) {
      const destinationConfig = destinationsById.get(destination.destination_id);
      if (!destinationConfig || isDirectPutDestination(destinationConfig)) continue;
      const finalObjectKey = destination.final_object_keys[source.source_id];
      if (!finalObjectKey) throw new Error(`plan final object key not found: ${source.source_id}:${destination.destination_id}`);
      const groupId = multipartGroupStateKey(prepared.transfer_id, destination.destination_id, source.source_id, finalObjectKey);
      waiters.set(groupId, createDeferred<MultipartUploadState>());
    }
  }
  return waiters;
}

function restoreProviderMultipartIdentities(
  prepared: TransferPrepareResponse,
  destinations: Map<string, ProviderDestinationConfig>,
  identities: ProviderMultipartGroupIdentity[],
  uploads: Map<string, MultipartUploadCleanupState>,
): void {
  if (!Array.isArray(identities)) throw new Error("provider_multipart_recovery_identity_required");
  const expected = new Map<string, { destination: ProviderDestinationConfig; objectKey: string; source: TransferPrepareResponse["plan_descriptor"]["sources"][number]; destinationId: string }>();
  for (const source of prepared.plan_descriptor.sources) {
    for (const target of prepared.plan_descriptor.destinations) {
      const destination = destinations.get(target.destination_id);
      if (!destination) throw new Error("provider_multipart_recovery_destination_invalid");
      if (isDirectPutDestination(destination)) continue;
      const objectKey = target.final_object_keys[source.source_id];
      if (!objectKey) throw new Error("provider_multipart_recovery_object_invalid");
      const key = multipartGroupStateKey(prepared.transfer_id,target.destination_id,source.source_id,objectKey);
      expected.set(key,{destination,objectKey,source,destinationId:target.destination_id});
    }
  }
  if (expected.size !== identities.length) throw new Error("provider_multipart_recovery_incomplete");
  for (const identity of identities) {
    const group = expected.get(identity.multipartGroupId);
    if (!group || uploads.has(identity.multipartGroupId) || identity.transferId !== prepared.transfer_id ||
      identity.sourceId !== group.source.source_id || identity.destinationId !== group.destinationId ||
      identity.objectKey !== group.objectKey || identity.expectedObjectSize !== group.source.size ||
      identity.expectedPartCount !== group.source.chunk_count || !identity.uploadId?.trim()) {
      throw new Error("provider_multipart_recovery_identity_invalid");
    }
    uploads.set(identity.multipartGroupId,{destination:group.destination,objectKey:group.objectKey,uploadId:identity.uploadId});
  }
}

async function createMultipartGroupManifest(input: {
  prepared: TransferPrepareResponse;
  destinationsById: Map<string, ProviderDestinationConfig>;
  multipartUploads: Map<string, MultipartUploadCleanupState>;
  expiresIn: number;
  signedUrlFlow: SignedUrlFlow;
  concurrency: number;
  onGroupReady: (state: MultipartUploadState) => Promise<void>;
  onGroupFailed: (groupId: string, error: unknown) => void;
  signal?: AbortSignal;
}): Promise<MultipartGroupManifest[]> {
  const groups = input.prepared.plan_descriptor.sources.flatMap((source) =>
    input.prepared.plan_descriptor.destinations.map((destination) => ({ source, destination }))
  );
  const queuedAt = performance.now();
  const telemetry = currentSdkPerformance();
  let active = 0;
  const results = await mapOrderedWithConcurrency(groups, input.concurrency, async ({ source, destination }) => {
    telemetry?.observe("sdk.multipart_queue", performance.now() - queuedAt);
    telemetry?.gauge("multipart_active_peak", ++active);
    let multipartGroupId: string | null = null;
    try {
      const destinationConfig = input.destinationsById.get(destination.destination_id);
      if (!destinationConfig) {
        throw new Error(`BeamCore returned unknown destination_id: ${destination.destination_id}`);
      }
      if (isDirectPutDestination(destinationConfig)) {
        return { ok: true as const, state: null };
      }
      const finalObjectKey = destination.final_object_keys[source.source_id];
      if (!finalObjectKey) {
        throw new Error(`plan final object key not found: ${source.source_id}:${destination.destination_id}`);
      }
      multipartGroupId = multipartGroupStateKey(
        input.prepared.transfer_id,
        destination.destination_id,
        source.source_id,
        finalObjectKey
      );
      const finalObjectMetadata = { "beam-transfer-id": input.prepared.transfer_id };
      input.signal?.throwIfAborted();
      const retainedUpload = input.multipartUploads.get(multipartGroupId);
      const uploadId = retainedUpload?.uploadId ?? await measureSdkPhase("sdk.multipart_provider", () => createMultipartUpload({
          destination: destinationConfig,
          objectKey: finalObjectKey,
          metadata: finalObjectMetadata,
          signal: input.signal
        }));
      const createdUpload = !retainedUpload;
      if (createdUpload) {
        input.multipartUploads.set(multipartGroupId, {
          destination: destinationConfig,
          objectKey: finalObjectKey,
          uploadId
        });
      }
      try {
        const urlsExpiresAt = expiresAtIso(input.expiresIn);
        const maxPartNumber = multipartMaxPartNumber(source.chunk_count);
        const listPageMarkers = multipartListPageMarkers(maxPartNumber);
        const [completeUrl, abortUrl, listPageUrls, finalHeadUrl] = await Promise.all([
          signCompleteMultipartUpload(destinationConfig, finalObjectKey, uploadId, input.expiresIn),
          signAbortMultipartUpload(destinationConfig, finalObjectKey, uploadId, input.expiresIn),
          Promise.all(listPageMarkers.map((partNumberMarker) =>
            signListMultipartUpload(
              destinationConfig,
              finalObjectKey,
              uploadId,
              input.expiresIn,
              { maxParts: 1_000, ...(partNumberMarker ? { partNumberMarker } : {}) }
            )
          )),
          signFinalObjectHead(destinationConfig, finalObjectKey, input.expiresIn)
        ]);
        const manifest: MultipartGroupManifest = {
          multipart_group_id: multipartGroupId,
          source_id: source.source_id,
          destination_id: destination.destination_id,
          final_object_key: finalObjectKey,
          upload_id: uploadId,
          expected_object_size: source.size,
          expected_part_count: source.chunk_count,
          max_part_number: maxPartNumber,
          complete_url: completeUrl,
          abort_url: abortUrl,
          list_page_urls: listPageUrls,
          final_head_url: finalHeadUrl,
          final_object_metadata: finalObjectMetadata,
          urls_expires_at: urlsExpiresAt
        };
        const state: MultipartUploadState = {
          destination: destinationConfig,
          objectKey: finalObjectKey,
          uploadId,
          manifest
        };
        input.multipartUploads.set(multipartGroupId, state);
        await input.onGroupReady(state);
        return { ok: true as const, state };
      } catch (error) {
        if (createdUpload && !input.signal?.aborted) {
          try {
            await abortMultipartUpload(destinationConfig, finalObjectKey, uploadId);
            input.multipartUploads.delete(multipartGroupId);
          } catch (abortError) {
            throw new AggregateError([error, abortError], `multipart group setup and cleanup failed for ${multipartGroupId}`);
          }
        }
        throw error;
      }
    } catch (error) {
      if (multipartGroupId) input.onGroupFailed(multipartGroupId, error);
      return { ok: false as const, error };
    } finally {
      active--;
    }
  });
  const failure = results.find((result) => !result.ok);
  if (failure && !failure.ok) {
    throw failure.error;
  }
  return results.flatMap((result) => result.ok && result.state ? [result.state.manifest] : []);
}

function multipartListPageMarkers(maxPartNumber: number): number[] {
  const markers: number[] = [];
  for (let marker = 0; marker < maxPartNumber; marker += 1_000) markers.push(marker);
  return markers;
}

function multipartMaxPartNumber(chunkCount: number): number {
  return multipartPartNumber(
    positiveInteger(chunkCount, "source chunk_count") - 1
  );
}

function validateMultipartPartNumber(partNumber: number, manifest: MultipartGroupManifest): void {
  if (!Number.isInteger(partNumber) || partNumber < 1 || partNumber > manifest.max_part_number) {
    throw new Error(
      `multipart part_number ${partNumber} is outside group ${manifest.multipart_group_id} range 1-${manifest.max_part_number}`
    );
  }
}

function multipartListPageIndex(partNumber: number): number {
  return Math.floor((partNumber - 1) / 1_000);
}

function validateSignedRouteManifestContract(
  routes: SignedChunkRoute[],
  manifest: MultipartGroupManifest[]
): void {
  const groups = new Map(manifest.map((group) => [group.multipart_group_id, group]));
  for (const route of routes) {
    const uploadId = typeof route.metadata?.upload_id === "string"
      ? route.metadata.upload_id
      : undefined;
    const groupId = typeof route.metadata?.multipart_group_id === "string"
      ? route.metadata.multipart_group_id
      : undefined;
    if (uploadId && !groupId) {
      throw new Error(`signed multipart route ${route.source_id}:${route.destination_id}:${route.chunk_index} is missing multipart_group_id`);
    }
    if (!groupId) continue;
    const group = groups.get(groupId);
    if (!group) throw new Error(`signed route references unknown multipart group ${groupId}`);
    if (route.source_id !== group.source_id || route.destination_id !== group.destination_id) {
      throw new Error(`signed route identity does not match multipart group ${groupId}`);
    }
    if (uploadId !== group.upload_id || route.metadata?.final_object_key !== group.final_object_key) {
      throw new Error(`signed route upload identity does not match multipart group ${groupId}`);
    }
    const partNumber = route.metadata?.part_number;
    if (typeof partNumber !== "number") {
      throw new Error(`signed route for multipart group ${groupId} is missing part_number`);
    }
    validateMultipartPartNumber(partNumber, group);
  }
}

const COMPACT_TRANSFER_PLAN_KEYS = new Set([
  "version",
  "plan_nonce",
  "chunk_size",
  "sources",
  "destinations",
  "logical_chunk_count",
  "delivery_route_count",
  "multipart_attempt_slots",
  "formulas"
]);

const COMPACT_TRANSFER_PLAN_FORMULAS = {
  source_offset: "source_chunk_index * chunk_size",
  delivery_index: "chunk_index * destination_count + destination_index",
  part_number: "source_chunk_index + 1",
  route_generation_id: "initial-{chunk_index}-{destination_id}"
} as const;

function validateCompactTransferPlan(
  descriptor: CompactTransferPlanDescriptor,
  signedUrlFlow: SignedUrlFlow | undefined
): void {
  if (signedUrlFlow !== "signed_url") {
    throw new Error(`BeamCore returned unsupported signed_url_flow: ${String(signedUrlFlow)}`);
  }
  const keys = Object.keys(descriptor);
  if (keys.length !== COMPACT_TRANSFER_PLAN_KEYS.size || keys.some((key) => !COMPACT_TRANSFER_PLAN_KEYS.has(key))) {
    throw new Error("BeamCore returned invalid compact transfer plan fields");
  }
  if (descriptor.version !== "compact-transfer-plan/v1") {
    throw new Error(`BeamCore returned unsupported plan version: ${descriptor.version}`);
  }
  if (descriptor.multipart_attempt_slots !== 1) {
    throw new Error("BeamCore returned unsupported multipart attempt slot count");
  }
  const formulaKeys = Object.keys(descriptor.formulas);
  if (
    formulaKeys.length !== Object.keys(COMPACT_TRANSFER_PLAN_FORMULAS).length
    || Object.entries(COMPACT_TRANSFER_PLAN_FORMULAS).some(
      ([key, value]) => descriptor.formulas[key as keyof typeof COMPACT_TRANSFER_PLAN_FORMULAS] !== value
    )
  ) {
    throw new Error("BeamCore returned unsupported compact transfer plan formulas");
  }
}

const MULTIPART_GROUP_MANIFEST_KEYS = new Set([
  "multipart_group_id",
  "source_id",
  "destination_id",
  "final_object_key",
  "upload_id",
  "expected_object_size",
  "expected_part_count",
  "max_part_number",
  "complete_url",
  "abort_url",
  "list_page_urls",
  "final_head_url",
  "final_object_metadata",
  "urls_expires_at"
]);

function validateMultipartGroupManifest(manifest: MultipartGroupManifest[], transferId: string): void {
  const groupIds = new Set<string>();
  for (const group of manifest) {
    const keys = Object.keys(group);
    if (keys.length !== MULTIPART_GROUP_MANIFEST_KEYS.size || keys.some((key) => !MULTIPART_GROUP_MANIFEST_KEYS.has(key))) {
      throw new Error(`multipart group ${group.multipart_group_id || "<unknown>"} has invalid manifest fields`);
    }
    if (!group.multipart_group_id || groupIds.has(group.multipart_group_id)) {
      throw new Error(`multipart_group_id must be non-empty and unique: ${group.multipart_group_id}`);
    }
    groupIds.add(group.multipart_group_id);
    for (const [field, value] of Object.entries({
      source_id: group.source_id,
      destination_id: group.destination_id,
      final_object_key: group.final_object_key,
      upload_id: group.upload_id,
      complete_url: group.complete_url,
      abort_url: group.abort_url,
      final_head_url: group.final_head_url,
      urls_expires_at: group.urls_expires_at
    })) {
      if (typeof value !== "string" || value.length === 0) {
        throw new Error(`multipart group ${group.multipart_group_id} requires ${field}`);
      }
    }
    if (!Number.isInteger(group.expected_part_count) || group.expected_part_count < 1 || group.expected_part_count > 10_000) {
      throw new Error(`multipart group ${group.multipart_group_id} has invalid expected_part_count`);
    }
    if (!Number.isInteger(group.max_part_number) || group.max_part_number !== group.expected_part_count || group.max_part_number > 10_000) {
      throw new Error(`multipart group ${group.multipart_group_id} has invalid max_part_number`);
    }
    if (!Number.isSafeInteger(group.expected_object_size) || group.expected_object_size <= 0) {
      throw new Error(`multipart group ${group.multipart_group_id} has invalid expected_object_size`);
    }
    const expectedListPageCount = Math.ceil(group.max_part_number / 1_000);
    if (
      !Array.isArray(group.list_page_urls)
      || group.list_page_urls.length !== expectedListPageCount
      || new Set(group.list_page_urls).size !== expectedListPageCount
      || group.list_page_urls.some((url) => typeof url !== "string" || url.length === 0)
    ) {
      throw new Error(`multipart group ${group.multipart_group_id} requires ${expectedListPageCount} unique list_page_urls`);
    }
    const metadataKeys = Object.keys(group.final_object_metadata);
    if (
      metadataKeys.length !== 1
      || group.final_object_metadata["beam-transfer-id"] !== transferId
    ) {
      throw new Error(`multipart group ${group.multipart_group_id} has invalid final_object_metadata`);
    }
  }
}

function multipartGroupStateKey(
  transferId: string,
  destinationId: string,
  sourceId: string,
  finalObjectKey: string
): string {
  return `${transferId}:${destinationId}:${sourceId}:${finalObjectKey}`;
}

const PART_ROUTE_METADATA_KEYS = new Set([
  "part_number",
  "logical_attempt_index",
  "attempt_slot",
  "route_generation_id",
  "delivery_index",
  "etag_required"
]);

function partRouteMetadata(metadata: Record<string, unknown> | undefined): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(metadata ?? {}).filter(([key]) => PART_ROUTE_METADATA_KEYS.has(key))
  );
}

/**
 * Route metadata for a destination BeamCore does not treat as an S3 multipart target. It
 * rejects `part_number` there as an unexpected multipart signal, so drop it.
 */
function directPutRouteMetadata(metadata: Record<string, unknown> | undefined): Record<string, unknown> {
  const { part_number: _partNumber, ...rest } = partRouteMetadata(metadata);
  return rest;
}

function randomUuid(): string {
  const cryptoApi = globalThis.crypto;
  if (cryptoApi?.randomUUID) return cryptoApi.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

async function transferIdForIdempotencyKey(idempotencyKey: string | undefined): Promise<string> {
  if (!idempotencyKey?.trim()) return randomUuid();
  return stableUuidFromIdentity(`beam-transfer:${idempotencyKey.trim()}`);
}

async function routeGenerationIdForPrepareIdempotencyKey(idempotencyKey: string): Promise<string> {
  return stableUuidFromIdentity(`beam-route-generation:${idempotencyKey.trim()}`);
}

async function stableUuidFromIdentity(identity: string): Promise<string> {
  const input = new TextEncoder().encode(identity);
  let digest: Uint8Array;
  if (globalThis.crypto?.subtle) {
    digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", input));
  } else {
    const { createHash } = await import("node:crypto");
    digest = new Uint8Array(createHash("sha256").update(input).digest());
  }
  const bytes = digest.slice(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function jitteredDelay(baseMs: number): number {
  return Math.max(1, Math.round(baseMs * (0.8 + Math.random() * 0.4)));
}
function compact<T extends Record<string, unknown>>(input: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
}

function validateId(value: string, name: string): void {
  if (!value || value.includes("/") || value.includes("..")) {
    throw new Error(`${name} is invalid`);
  }
}

async function abortCreatedUploads(
  uploads: Map<string, MultipartUploadCleanupState>,
  concurrency: number
): Promise<void> {
  const candidates = [...uploads.entries()].filter(
    ([, upload]) => upload.uploadId && !isHippiusDestination(upload.destination)
  );
  const results = await mapOrderedWithConcurrency(
    candidates,
    Math.min(concurrency, Math.max(1, candidates.length)),
    async ([groupId, upload]): Promise<unknown | null> => {
      try {
        await abortMultipartUpload(upload.destination, upload.objectKey, upload.uploadId);
        uploads.delete(groupId);
        return null;
      } catch (error) {
        return error;
      }
    }
  );
  const failures = results.filter((result) => result !== null);
  if (failures.length) {
    throw new AggregateError(
      failures,
      `failed to abort ${failures.length} multipart upload(s)`
    );
  }
}

function isHippiusDestination(destination: ProviderDestinationConfig): destination is HippiusProviderConfig {
  return destination.provider === "hippius" && "api_token" in destination;
}

/**
 * Destinations that take a plain PUT per chunk instead of an S3 multipart upload. BeamCore
 * rejects multipart route metadata for these, so they never get a multipart group manifest.
 */
function isDirectPutDestination(destination: ProviderDestinationConfig): boolean {
  return isHippiusDestination(destination) || isHuggingFaceProvider(destination);
}

/** Resolve the path a Hugging Face upload commits to, expanding a trailing-slash prefix. */
function huggingFaceTargetPath(
  destination: HuggingFaceProviderConfig,
  sourceFilename: string | undefined
): string {
  const path = destination.path.replace(/^\/+/, "");
  if (!path.endsWith("/")) return path;
  if (!sourceFilename) {
    throw new Error(`huggingface path ${destination.path} is a folder and the source has no filename`);
  }
  return `${path}${sourceFilename}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clearRecoverySecrets(input: ProviderTransferCreateInput): void {
  const secretKeys = new Set(["access_key_id", "secret_access_key", "session_token", "api_token", "token", "password"]);
  for (const provider of [...input.sources, ...input.destinations]) {
    const record = provider as unknown as Record<string, unknown>;
    for (const key of secretKeys) {
      if (key in record) record[key] = "";
    }
  }
}
