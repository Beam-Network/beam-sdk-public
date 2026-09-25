import { encode, decode } from "@msgpack/msgpack";
import { connect, type ConnectionOptions, type NatsConnection, type Subscription } from "nats";
import type {
  MultipartGroupManifest,
  SignedChunkRoute,
  TransferTerminalEvent,
  TransferTerminalSignalWaiter
} from "./models.js";

export const TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION = "transfer-client-control/v6";
export const BEAM_DEFAULT_NATS_URL = "tls://orch-gateway.b1m.ai:4222";
export const BEAM_DEFAULT_NATS_WS_URL = "wss://orch-gateway.b1m.ai:443";
// NATS enforces max_payload per message. This guard splits signed-route control
// messages before the broker rejects them; transfer bytes never flow through NATS.
export const BEAM_DEFAULT_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;
export const BEAM_ROUTE_TARGET_PAYLOAD_BYTES = 4 * 1024 * 1024;
const ROUTE_BATCH_AUTH_TOKEN_ESTIMATE_BYTES = 64 * 1024;
const AUTH_TOKEN_REFRESH_SAFETY_SECONDS = 30;
export const ROUTE_RECOVERY_SIGN_MESSAGE_TYPE = "transfer.route_recovery.sign" as const;

export interface RouteRecoverySignChunk {
  source_id: string;
  destination_id: string;
  chunk_index: number;
  delivery_index: number;
  source_offset: number;
  chunk_size: number;
  logical_attempt_index: number;
  attempt_slot: 0 | 1 | 2;
  part_number: number;
  route_generation_id: string;
  multipart_group_id: string;
  final_object_key: string;
  upload_id: string;
  urls_expires_at?: string;
  multipart_created_at?: string;
  source_metadata?: Record<string, unknown>;
  destination_metadata?: Record<string, unknown>;
}

export interface RouteRecoverySignRequestPayload {
  transfer_id: string;
  route_generation_id: string;
  requested_at?: string;
  chunks: RouteRecoverySignChunk[];
}

export interface RouteRecoverySignReplyPayload {
  transfer_id: string;
  route_generation_id: string;
  signed_at?: string;
  chunk_routes: SignedChunkRoute[];
}

interface RouteRecoverySignEnvelope {
  message_id: string;
  schema_version: typeof TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION;
  environment: string;
  key_prefix: string;
  transfer_id: string;
  message_type: typeof ROUTE_RECOVERY_SIGN_MESSAGE_TYPE;
  request_id: string;
  occurred_at: string;
  producer: "transfer-runtime";
  payload: RouteRecoverySignRequestPayload;
}

interface RouteRecoverySignReplyEnvelope {
  message_id: string;
  schema_version: typeof TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION;
  environment: string;
  key_prefix: string;
  transfer_id: string;
  message_type: typeof ROUTE_RECOVERY_SIGN_MESSAGE_TYPE;
  request_id: string;
  occurred_at: string;
  producer: "sdk";
  ok: boolean;
  status: number;
  payload?: RouteRecoverySignReplyPayload;
  error?: { code: string; message: string; detail?: unknown };
}

export type TransferClientMessageType =
  | "runtime.hello"
  | "transfer.plan"
  | "transfer.create"
  | "transfer.prepare"
  | "transfer.route_stream.begin"
  | "transfer.route_stream.manifest"
  | "transfer.route_stream.batch"
  | "transfer.route_stream.complete"
  | "transfer.distribute"
  | "transfer.status"
  | "transfer.integrity_audit_grants"
  | "transfer.cancel"
  | "transfer.resume";

export interface CompactSignedRouteBatch {
  source_chunks: Array<{
    source_ref: number;
    source_id: string;
    chunk_index: number;
    source_url: string;
    source_offset: number;
    chunk_size: number;
    expires_at?: string;
    headers?: Record<string, string>;
  }>;
  routes: Array<{
    source_ref: number;
    destination_id: string;
    delivery_index: number;
    dest_url: string;
    expires_at?: string;
    dest_headers?: Record<string, string>;
    multipart_group_id?: string;
    metadata?: Record<string, unknown>;
  }>;
}

export interface RouteStreamBeginPayload {
  transfer_id: string;
  route_generation_id: string;
  stream_id: string;
  total_routes: number;
  total_chunks: number;
  route_contract_version: "signed_url";
  urls_expires_at?: string;
  signed_url_flow: "signed_url";
  auto_distribute: boolean;
}

export interface RouteStreamManifestPayload {
  transfer_id: string;
  route_generation_id: string;
  stream_id: string;
  manifest_batch_id: string;
  groups: MultipartGroupManifest[];
}

type NatsTlsOptions = NonNullable<ConnectionOptions["tls"]> & {
  handshakeFirst?: boolean;
  servername?: string;
};

export function buildConnectionOptions(options: {
  natsUrl: string;
  apiKey: string;
  keyPrefix: string;
}): ConnectionOptions {
  const connectionOptions: ConnectionOptions = {
    servers: options.natsUrl,
    user: options.keyPrefix,
    pass: options.apiKey,
    name: `beam-sdk-${options.keyPrefix}`,
    reconnect: true,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 1_000,
    reconnectJitter: 500,
    reconnectJitterTLS: 1_000
  };

  if (options.natsUrl.startsWith("tls://")) {
    const tlsOptions: NatsTlsOptions = { handshakeFirst: true };
    try {
      tlsOptions.servername = new URL(options.natsUrl).hostname;
    } catch {
      // URL validation stays with the NATS client; TLS-first is still required.
    }
    connectionOptions.tls = tlsOptions;
  }

  return connectionOptions;
}
export interface TransferControlOptions {
  apiKey: string;
  natsUrl?: string;
  natsWsUrl?: string;
  environment?: string;
  subjectPrefix?: string;
  transferRuntimeShardCount?: number;
  requestTimeoutMs?: number;
  maxPayloadBytes?: number;
}

interface AuthResolveResponse {
  ok: boolean;
  token?: string;
  error?: string;
}

interface SdkAuthTokenClaims {
  token_type: "beam-sdk-auth";
  environment: string;
  client_id: string;
  client_key: string;
  client_name: string | null;
  key_prefix: string;
  key_role: "client" | "admin";
  scopes: string[];
  iat: number;
  exp: number;
}

interface TransferClientReplyEnvelope<T> {
  ok: boolean;
  status: number;
  payload?: T;
  error?: { code: string; message: string; detail?: unknown };
  runtime_epoch: string;
  transport_epoch: string;
}

export interface LifecycleRequestOptions {
  transferId?: string;
  idempotencyKey?: string;
  shardId?: number;
}

export interface TransferRecoveryLease {
  transferId: string;
  planFingerprint: string;
  coordinateChecksum: string;
  replayRoutes(routeGenerationId: string): Promise<void>;
  disposeSecrets?(): void;
}

interface RuntimeHello {
  runtime_epoch: string;
  transport_epoch: string;
  shard_id: number;
  ready: boolean;
}

class LifecycleRequestError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = "LifecycleRequestError";
  }
}

export class BeamTransferControl {
  readonly environment: string;
  readonly keyPrefix: string;
  readonly shardCount: number;
  readonly maxPayloadBytes: number;
  private readonly subjectPrefix: string;
  private readonly requestTimeoutMs: number;
  private readonly natsUrl: string;
  private readonly apiKey: string;
  private nc: NatsConnection | null = null;
  private connectionPromise: Promise<NatsConnection> | null = null;
  private closed = false;
  private authToken: string | null = null;
  private authTokenExpiresAt = 0;
  private authResolvePromise: Promise<string> | null = null;
  private readonly terminalWaitCancellations = new Set<() => void>();
  private readonly routeRecoverySignerCancellations = new Set<() => void>();
  private readonly recoveryLeases = new Map<string, TransferRecoveryLease>();
  private readonly recoveryOperations = new Map<string, Promise<void>>();
  private readonly recoveryRequestedEpoch = new Map<string, string>();
  private readonly runtimeEpochs = new Map<number, { runtimeEpoch: string; transportEpoch: string }>();
  private helloTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: TransferControlOptions) {
    this.apiKey = options.apiKey;
    this.keyPrefix = options.apiKey.slice(0, 12);
    this.environment = options.environment ?? "prod";
    this.subjectPrefix = normalizeSubjectPrefix(options.subjectPrefix ?? "beam.transfer.client");
    this.shardCount = options.transferRuntimeShardCount ?? 1;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.maxPayloadBytes = options.maxPayloadBytes ?? BEAM_DEFAULT_MAX_PAYLOAD_BYTES;
    this.natsUrl = options.natsUrl ?? options.natsWsUrl ?? BEAM_DEFAULT_NATS_URL;
    if (/^https?:\/\//i.test(this.natsUrl) || /^wss?:\/\//i.test(this.natsUrl) && !options.natsWsUrl) {
      throw new Error("Beam SDK lifecycle endpoint must be nats://, tls://, or configured as natsWsUrl.");
    }
    if (!Number.isInteger(this.shardCount) || this.shardCount < 1) {
      throw new Error("transferRuntimeShardCount must be a positive integer.");
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const cancel of [...this.terminalWaitCancellations]) cancel();
    for (const cancel of [...this.routeRecoverySignerCancellations]) cancel();
    if (this.helloTimer) clearInterval(this.helloTimer);
    this.helloTimer = null;
    for (const lease of this.recoveryLeases.values()) lease.disposeSecrets?.();
    this.recoveryLeases.clear();
    this.recoveryRequestedEpoch.clear();
    if (this.connectionPromise) {
      try {
        await this.connectionPromise;
      } catch {
        // A connection that loses the close race closes itself before rejecting.
      }
    }
    const nc = this.nc;
    this.nc = null;
    if (nc && !nc.isClosed()) await nc.drain();
  }

  async serveRouteRecoverySigner(
    transferId: string,
    handler: (payload: RouteRecoverySignRequestPayload) => Promise<RouteRecoverySignReplyPayload>
  ): Promise<() => void> {
    const nc = await this.connection();
    const subject = this.routeRecoverySignSubject(transferId);
    let active = true;
    const subscription = nc.subscribe(subject, {
      callback: (_error, message) => {
        if (!active) return;
        if (_error || !isRouteRecoveryRequestMessage(message)) return;
        void (async () => {
          let request: RouteRecoverySignEnvelope | null = null;
          try {
            request = decode(message.data) as RouteRecoverySignEnvelope;
            if (
              request.schema_version !== TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION
              || request.environment !== this.environment
              || request.key_prefix !== this.keyPrefix
              || request.transfer_id !== transferId
              || request.message_type !== ROUTE_RECOVERY_SIGN_MESSAGE_TYPE
              || request.producer !== "transfer-runtime"
              || request.payload.transfer_id !== transferId
              || request.payload.route_generation_id.length === 0
              || !Array.isArray(request.payload.chunks)
              || request.payload.chunks.length === 0
            ) {
              throw new Error("route recovery request envelope mismatch");
            }
            const payload = await handler(request.payload);
            message.respond(encode(this.buildRouteRecoveryReply(request, true, 200, payload)));
          } catch (error) {
            message.respond(encode(this.buildRouteRecoveryReply(
              request ?? { message_id: "unknown", request_id: "unknown", transfer_id: transferId },
              false,
              500,
              undefined,
              {
                code: "route_recovery_sign_failed",
                message: error instanceof Error ? error.message : String(error)
              }
            )));
          }
        })();
      }
    });
    await nc.flush();
    const stop = () => {
      if (!active) return;
      active = false;
      subscription.unsubscribe();
      this.routeRecoverySignerCancellations.delete(stop);
    };
    this.routeRecoverySignerCancellations.add(stop);
    return stop;
  }

  registerRecoveryLease(lease: TransferRecoveryLease): () => void {
    if (this.closed) throw new Error("NATS lifecycle control is closed");
    const previous = this.recoveryLeases.get(lease.transferId);
    if (previous && previous !== lease) previous.disposeSecrets?.();
    this.recoveryLeases.set(lease.transferId, lease);
    this.ensureHelloMonitor();
    const shardId = transferShardId(lease.transferId, this.shardCount);
    void this.pollRuntimeHello(shardId);
    return () => {
      const current = this.recoveryLeases.get(lease.transferId);
      if (current !== lease) return;
      this.recoveryLeases.delete(lease.transferId);
      this.recoveryRequestedEpoch.delete(lease.transferId);
      lease.disposeSecrets?.();
      if (!this.recoveryLeases.size && this.helloTimer) {
        clearInterval(this.helloTimer);
        this.helloTimer = null;
      }
    };
  }

  releaseRecoveryLease(transferId: string): void {
    const lease = this.recoveryLeases.get(transferId);
    if (!lease) return;
    this.recoveryLeases.delete(transferId);
    this.recoveryRequestedEpoch.delete(transferId);
    lease.disposeSecrets?.();
    if (!this.recoveryLeases.size && this.helloTimer) {
      clearInterval(this.helloTimer);
      this.helloTimer = null;
    }
  }

  continueRecoveryLease(transferId: string): void {
    const lease = this.recoveryLeases.get(transferId);
    if (!lease || this.closed) return;
    const shardId = transferShardId(transferId, this.shardCount);
    const epoch = this.runtimeEpochs.get(shardId);
    this.recoveryRequestedEpoch.set(
      transferId,
      epoch ? `${epoch.runtimeEpoch}:${epoch.transportEpoch}` : `foreground:${randomId()}`
    );
    void this.recoverLease(lease);
  }

  async request<T>(
    messageType: TransferClientMessageType,
    payload: Record<string, unknown>,
    options: LifecycleRequestOptions = {}
  ): Promise<T> {
    const shardId = options.shardId ?? (options.transferId ? transferShardId(options.transferId, this.shardCount) : 0);
    const requestId = options.idempotencyKey
      ? await stableRequestId(`${messageType}:${options.idempotencyKey}`)
      : randomId();
    const occurredAt = new Date().toISOString();
    const subject = this.requestSubject(messageType, shardId);
    let lastError: unknown;
    for (let attempt = 0; attempt < LIFECYCLE_REQUEST_MAX_ATTEMPTS; attempt += 1) {
      try {
        const authToken = await this.resolveAuthToken();
        const envelope = {
          message_id: `${TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION}:${this.environment}:${this.keyPrefix}:${messageType}:${requestId}`,
          schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
          environment: this.environment,
          key_prefix: this.keyPrefix,
          shard_id: shardId,
          message_type: messageType,
          request_id: requestId,
          auth_token: authToken,
          occurred_at: occurredAt,
          producer: "sdk",
          payload
        };
        const bytes = encode(envelope);
        if (bytes.byteLength > this.maxPayloadBytes) {
          throw new Error(`NATS lifecycle request is ${bytes.byteLength} bytes, above maxPayloadBytes=${this.maxPayloadBytes}`);
        }
        const nc = await this.connection();
        const response = await nc.request(subject, bytes, { timeout: this.requestTimeoutMs });
        const decoded = decode(response.data) as TransferClientReplyEnvelope<T>;
        this.observeRuntimeEpoch(shardId, decoded.runtime_epoch, decoded.transport_epoch);
        if (decoded.ok) return (decoded.payload ?? {}) as T;
        const body = JSON.stringify(decoded.error ?? {});
        const error = new LifecycleRequestError(decoded.status, `Beam lifecycle request failed with ${decoded.status}: ${body}`);
        if (isExpiredAuthTokenReply(decoded) && attempt + 1 < LIFECYCLE_REQUEST_MAX_ATTEMPTS) {
          this.authToken = null;
          this.authTokenExpiresAt = 0;
          lastError = error;
          await sleepWithJitter(LIFECYCLE_REQUEST_RETRY_DELAYS_MS[attempt] ?? 1_000);
          continue;
        }
        if (!isRetryableLifecycleStatus(decoded.status) || attempt + 1 >= LIFECYCLE_REQUEST_MAX_ATTEMPTS) {
          throw error;
        }
        lastError = error;
      } catch (error) {
        if (!isRetryableLifecycleError(error) || attempt + 1 >= LIFECYCLE_REQUEST_MAX_ATTEMPTS) {
          throw error;
        }
        lastError = error;
        if (this.nc?.isClosed()) this.nc = null;
      }
      await sleepWithJitter(LIFECYCLE_REQUEST_RETRY_DELAYS_MS[attempt] ?? 1_000);
    }
    throw lastError instanceof Error ? lastError : new Error("Beam lifecycle request failed after retries");
  }

  async openTerminalSignalWaiter(transferId: string): Promise<TransferTerminalSignalWaiter> {
    const nc = await this.connection();
    const pending: TransferTerminalEvent[] = [];
    let activeWait: {
      resolve: (event: TransferTerminalEvent | null) => void;
      reject: (error: unknown) => void;
      timer: ReturnType<typeof setTimeout>;
    } | null = null;
    let closed = false;
    let terminalError: unknown = null;
    let subscription: Subscription | null = null;
    const settleActive = (event: TransferTerminalEvent | null, error?: unknown) => {
      const wait = activeWait;
      if (!wait) return;
      activeWait = null;
      clearTimeout(wait.timer);
      if (error !== undefined) wait.reject(error);
      else wait.resolve(event);
    };
    const close = async () => {
      if (closed) return;
      closed = true;
      this.terminalWaitCancellations.delete(cancel);
      subscription?.unsubscribe();
      settleActive(null);
    };
    const cancel = () => { void close(); };
    try {
      subscription = nc.subscribe(this.terminalSubject(transferId), {
        callback: (error, message) => {
          if (error) {
            terminalError = error;
            subscription?.unsubscribe();
            settleActive(null, error);
            return;
          }
          try {
            const event = parseTerminalEvent(decode(message.data), transferId);
            if (activeWait) settleActive(event);
            else if (pending.length === 0) pending.push(event);
          } catch (decodeError) {
            terminalError = decodeError;
            subscription?.unsubscribe();
            settleActive(null, decodeError);
          }
        }
      });
      await nc.flush();
      if (this.closed) {
        await close();
        throw new Error("NATS lifecycle control is closed");
      }
      this.terminalWaitCancellations.add(cancel);
      if (this.closed) {
        await close();
        throw new Error("NATS lifecycle control is closed");
      }
      return {
        wait: async (timeoutMs: number) => {
          if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
            throw new Error("timeoutMs must be a positive number.");
          }
          if (closed) return null;
          if (terminalError !== null) throw terminalError;
          const event = pending.shift();
          if (event) return event;
          if (activeWait) throw new Error("terminal signal waiter already has an active wait");
          return new Promise<TransferTerminalEvent | null>((resolve, reject) => {
            const timer = setTimeout(() => settleActive(null), timeoutMs);
            activeWait = { resolve, reject, timer };
          });
        },
        close
      };
    } catch (error) {
      subscription?.unsubscribe();
      settleActive(null);
      throw error;
    }
  }

  splitRoutesForPayload(messageType: TransferClientMessageType, basePayload: Record<string, unknown>, routes: SignedChunkRoute[]): SignedChunkRoute[][] {
	const targetPayloadBytes = Math.min(this.maxPayloadBytes, BEAM_ROUTE_TARGET_PAYLOAD_BYTES);
	const authTokenEstimateBytes = Math.min(
	  ROUTE_BATCH_AUTH_TOKEN_ESTIMATE_BYTES,
	  Math.max(512, Math.floor(this.maxPayloadBytes / 128))
	);
	const encodedBytes = (candidate: SignedChunkRoute[]): number => {
      const encoded = encode({
        schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
        environment: this.environment,
        key_prefix: this.keyPrefix,
        shard_id: 0,
        message_type: messageType,
        request_id: "00000000-0000-4000-8000-000000000000",
        // The live JWT is larger than the compact placeholder once claims and
        // signatures are encoded. Reserve enough space for token and envelope
        // growth so a batch accepted by the estimator stays below the guard.
        auth_token: "x".repeat(authTokenEstimateBytes),
        occurred_at: new Date().toISOString(),
        producer: "sdk",
        payload: { ...basePayload, route_batch: compactSignedRoutes(candidate) }
      });
	  return encoded.byteLength;
    };
	const chunks: SignedChunkRoute[][] = [];
	let offset = 0;
	while (offset < routes.length) {
	  let low = 1;
	  let high = routes.length - offset;
	  let accepted = 0;
	  while (low <= high) {
		const count = Math.floor((low + high) / 2);
		if (encodedBytes(routes.slice(offset, offset + count)) <= targetPayloadBytes) {
		  accepted = count;
		  low = count + 1;
		} else high = count - 1;
	  }
	  if (accepted === 0) {
		const bytes = encodedBytes(routes.slice(offset, offset + 1));
		if (bytes > this.maxPayloadBytes) {
		  throw new Error(`single signed route is ${bytes} bytes, above maxPayloadBytes=${this.maxPayloadBytes}`);
		}
		accepted = 1;
	  }
	  chunks.push(routes.slice(offset, offset + accepted));
	  offset += accepted;
	}
	return chunks;
  }

  private async connection(): Promise<NatsConnection> {
    if (this.closed) throw new Error("NATS lifecycle control is closed");
    if (this.nc && !this.nc.isClosed()) return this.nc;
    if (this.connectionPromise) return this.connectionPromise;
    const pending = (async () => {
      const nc = await connect(
        buildConnectionOptions({
          natsUrl: this.natsUrl,
          apiKey: this.apiKey,
          keyPrefix: this.keyPrefix
        })
      );
      if (this.closed) {
        await nc.drain();
        throw new Error("NATS lifecycle control is closed");
      }
      this.nc = nc;
      return nc;
    })();
    this.connectionPromise = pending;
    try {
      return await pending;
    } finally {
      if (this.connectionPromise === pending) this.connectionPromise = null;
    }
  }

  private ensureHelloMonitor(): void {
    if (this.helloTimer || this.closed) return;
    this.helloTimer = setInterval(() => {
      const shards = new Set([...this.recoveryLeases.keys()].map((transferId) => transferShardId(transferId, this.shardCount)));
      for (const shardId of shards) void this.pollRuntimeHello(shardId);
    }, 5_000);
    this.helloTimer.unref?.();
  }

  private async pollRuntimeHello(shardId: number): Promise<void> {
    try {
      await this.request<RuntimeHello>("runtime.hello", {}, { idempotencyKey: `runtime:hello:${shardId}`, shardId });
    } catch {
      // Infinite NATS reconnect continues in the background; the next hello reconciles epochs.
    }
  }

  private observeRuntimeEpoch(shardId: number, runtimeEpoch: string, transportEpoch: string): void {
    if (!runtimeEpoch || !transportEpoch) return;
    const previous = this.runtimeEpochs.get(shardId);
    this.runtimeEpochs.set(shardId, { runtimeEpoch, transportEpoch });
    if (!previous) return;
    if (previous.runtimeEpoch === runtimeEpoch && previous.transportEpoch === transportEpoch) return;
    if (previous.runtimeEpoch !== runtimeEpoch) {
      this.authToken = null;
      this.authTokenExpiresAt = 0;
    }
    for (const lease of this.recoveryLeases.values()) {
      if (transferShardId(lease.transferId, this.shardCount) === shardId) {
        this.recoveryRequestedEpoch.set(lease.transferId, `${runtimeEpoch}:${transportEpoch}`);
        void this.recoverLease(lease);
      }
    }
  }

  private recoverLease(lease: TransferRecoveryLease): Promise<void> {
    const existing = this.recoveryOperations.get(lease.transferId);
    if (existing) return existing;
    let operation!: Promise<void>;
    operation = (async () => {
      let attempt = 0;
      while (!this.closed && this.recoveryLeases.get(lease.transferId) === lease) {
        const requestedEpoch = this.recoveryRequestedEpoch.get(lease.transferId);
        const routeGenerationId = randomId();
        try {
          const response = await this.request<{ recovery: "terminal" | "route_replay_required" | "reconciling"; route_replay_required?: boolean }>(
            "transfer.resume",
            {
              transfer_id: lease.transferId,
              plan_fingerprint: lease.planFingerprint,
              coordinate_checksum: lease.coordinateChecksum,
              route_generation_id: routeGenerationId
            },
            { transferId: lease.transferId, idempotencyKey: `transfer:${lease.transferId}:resume:${routeGenerationId}` }
          );
          if (response.recovery === "terminal") {
            this.releaseRecoveryLease(lease.transferId);
            if (this.recoveryOperations.get(lease.transferId) === operation) {
              this.recoveryOperations.delete(lease.transferId);
            }
            return;
          }
          if (response.route_replay_required || response.recovery === "route_replay_required") {
            await lease.replayRoutes(routeGenerationId);
          }
          if (this.recoveryRequestedEpoch.get(lease.transferId) !== requestedEpoch) {
            attempt = 0;
            continue;
          }
          if (this.recoveryOperations.get(lease.transferId) === operation) {
            this.recoveryOperations.delete(lease.transferId);
          }
          return;
        } catch (error) {
          if (!isRetryableLifecycleError(error)) {
            this.releaseRecoveryLease(lease.transferId);
            return;
          }
          attempt += 1;
          await sleepWithJitter(Math.min(30_000, 500 * (2 ** Math.min(attempt, 6))));
        }
      }
    })().finally(() => {
      if (this.recoveryOperations.get(lease.transferId) === operation) this.recoveryOperations.delete(lease.transferId);
    });
    this.recoveryOperations.set(lease.transferId, operation);
    return operation;
  }

  private async resolveAuthToken(): Promise<string> {
    const now = Math.floor(Date.now() / 1000);
    if (this.authToken && this.authTokenExpiresAt - AUTH_TOKEN_REFRESH_SAFETY_SECONDS > now) return this.authToken;
    if (this.authResolvePromise) return this.authResolvePromise;
    const pending = this.resolveAuthTokenFresh();
    this.authResolvePromise = pending;
    try {
      return await pending;
    } finally {
      if (this.authResolvePromise === pending) this.authResolvePromise = null;
    }
  }

  private async resolveAuthTokenFresh(): Promise<string> {
    let lastError: unknown;
    for (let attempt = 0; attempt < LIFECYCLE_REQUEST_MAX_ATTEMPTS; attempt += 1) {
      try {
        const nc = await this.connection();
        const response = await nc.request(this.authSubject(), new TextEncoder().encode("{}"), { timeout: this.requestTimeoutMs });
        const parsed = JSON.parse(new TextDecoder().decode(response.data)) as AuthResolveResponse;
        if (!parsed.ok || !parsed.token) {
          throw new Error(`Beam SDK NATS auth resolve failed: ${parsed.error ?? "unknown_error"}`);
        }
        const claims = decodeJwtPayload<SdkAuthTokenClaims>(parsed.token);
        this.authToken = parsed.token;
        this.authTokenExpiresAt = claims.exp;
        return parsed.token;
      } catch (error) {
        if (!isRetryableLifecycleError(error) || attempt + 1 >= LIFECYCLE_REQUEST_MAX_ATTEMPTS) {
          throw error;
        }
        lastError = error;
        if (this.nc?.isClosed()) this.nc = null;
        await sleepWithJitter(LIFECYCLE_REQUEST_RETRY_DELAYS_MS[attempt] ?? 1_000);
      }
    }
    throw lastError instanceof Error ? lastError : new Error("Beam SDK NATS auth resolve failed after retries");
  }

  private authSubject(): string {
    return `${this.subjectPrefix}.${this.environment}.auth.${this.keyPrefix}.resolve`;
  }

  private routeRecoverySignSubject(transferId: string): string {
    return `${this.subjectPrefix}.${this.environment}.sdk.${this.keyPrefix}.transfer.${transferId}.route_recovery_sign`;
  }

  private buildRouteRecoveryReply(
    request: Pick<RouteRecoverySignEnvelope, "message_id" | "request_id" | "transfer_id">,
    ok: boolean,
    status: number,
    payload?: RouteRecoverySignReplyPayload,
    error?: { code: string; message: string; detail?: unknown }
  ): RouteRecoverySignReplyEnvelope {
    return {
      message_id: request.message_id,
      schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
      environment: this.environment,
      key_prefix: this.keyPrefix,
      transfer_id: request.transfer_id,
      message_type: ROUTE_RECOVERY_SIGN_MESSAGE_TYPE,
      request_id: request.request_id,
      occurred_at: new Date().toISOString(),
      producer: "sdk",
      ok,
      status,
      ...(payload ? { payload } : {}),
      ...(error ? { error } : {})
    };
  }

  private requestSubject(messageType: TransferClientMessageType, shardId: number): string {
    return `${this.subjectPrefix}.${this.environment}.sdk.${this.keyPrefix}.shard.${shardId}.${messageType.replace(/\./g, "_")}`;
  }

  private terminalSubject(transferId: string): string {
    return `${this.subjectPrefix}.${this.environment}.events.${this.keyPrefix}.${transferId}.terminal`;
  }
}

function parseTerminalEvent(value: unknown, transferId: string): TransferTerminalEvent {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid transfer terminal event");
  const event = value as Record<string, unknown>;
  if (
    event.schema_version !== TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION ||
    event.producer !== "transfer-runtime" ||
    event.transfer_id !== transferId ||
    !["completed", "failed", "cancelled"].includes(String(event.status)) ||
    typeof event.occurred_at !== "string"
  ) {
    throw new Error("invalid transfer terminal event");
  }
  return event as unknown as TransferTerminalEvent;
}

const ROUTE_ATTEMPT_METADATA_KEYS = new Set<string>([
  "part_number",
  "logical_attempt_index",
  "attempt_slot",
  "route_generation_id"
]);
const ROUTE_IDENTITY_METADATA_KEYS = [
  "source_id",
  "destination_id",
  "chunk_index",
  "route_chunk_index",
  "delivery_index"
];

export function compactSignedRoutes(routes: SignedChunkRoute[]): CompactSignedRouteBatch {
  const source_chunks: CompactSignedRouteBatch["source_chunks"] = [];
  const sourceRefs = new Map<string, number>();
  const compactRoutes: CompactSignedRouteBatch["routes"] = [];
  for (const [routeIndex, route] of routes.entries()) {
    const sourceKey = JSON.stringify([
      route.source_id,
      route.chunk_index,
      route.source_url,
      route.source_offset,
      route.chunk_size,
      route.expires_at ?? null,
      route.headers ?? null
    ]);
    let sourceRef = sourceRefs.get(sourceKey);
    if (sourceRef === undefined) {
      sourceRef = source_chunks.length;
      sourceRefs.set(sourceKey, sourceRef);
      source_chunks.push(compact({
        source_ref: sourceRef,
        source_id: route.source_id,
        chunk_index: route.chunk_index,
        source_url: route.source_url,
        source_offset: route.source_offset,
        chunk_size: route.chunk_size,
        expires_at: route.expires_at,
        headers: route.headers
      }) as CompactSignedRouteBatch["source_chunks"][number]);
    }
    const metadata = { ...(route.metadata ?? {}) };
    const deliveryIndex = route.delivery_index ?? nonNegativeInteger(metadata.delivery_index) ?? routeIndex;
    for (const key of ROUTE_IDENTITY_METADATA_KEYS) delete metadata[key];
    const multipartGroupId = typeof metadata.multipart_group_id === "string"
      ? metadata.multipart_group_id
      : undefined;
    if (multipartGroupId) {
      for (const key of Object.keys(metadata)) {
        if (!ROUTE_ATTEMPT_METADATA_KEYS.has(key)) delete metadata[key];
      }
    }
    compactRoutes.push(compact({
      source_ref: sourceRef,
      destination_id: route.destination_id,
      delivery_index: deliveryIndex,
      dest_url: route.dest_url,
      expires_at: route.expires_at,
      dest_headers: route.dest_headers,
      multipart_group_id: multipartGroupId,
      metadata: Object.keys(metadata).length ? metadata : undefined
    }) as CompactSignedRouteBatch["routes"][number]);
  }
  return { source_chunks, routes: compactRoutes };
}

function nonNegativeInteger(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

function compact<T extends Record<string, unknown>>(value: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined));
}

export function transferShardId(transferId: string, shardCount: number): number {
  let hash = 2166136261;
  for (let i = 0; i < transferId.length; i += 1) {
    hash ^= transferId.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash % shardCount;
}

function decodeJwtPayload<T>(jwt: string): T {
  const parts = jwt.split(".");
  if (parts.length < 2) throw new Error("invalid JWT");
  return JSON.parse(base64UrlDecode(parts[1]!)) as T;
}

function base64UrlDecode(value: string): string {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
  if (typeof Buffer !== "undefined") return Buffer.from(padded, "base64").toString("utf8");
  return atob(padded);
}

function normalizeSubjectPrefix(value: string): string {
  return value.replace(/^\.+|\.+$/g, "");
}

function randomId(): string {
  const cryptoApi = globalThis.crypto;
  if (cryptoApi?.randomUUID) return cryptoApi.randomUUID();
  if (!cryptoApi?.getRandomValues) throw new Error("secure UUID generation is unavailable");
  const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = [...bytes].map((value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

const LIFECYCLE_REQUEST_MAX_ATTEMPTS = 3;
const LIFECYCLE_REQUEST_RETRY_DELAYS_MS = [150, 500] as const;

function isRetryableLifecycleStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function isExpiredAuthTokenReply(reply: TransferClientReplyEnvelope<unknown>): boolean {
  return reply.status === 401 && reply.error?.code === "auth_token_expired";
}

export function isRetryableLifecycleError(error: unknown): boolean {
  if (error instanceof LifecycleRequestError) return isRetryableLifecycleStatus(error.status);
  const record = error && typeof error === "object" ? error as Record<string, unknown> : null;
  const code = typeof record?.code === "string" ? record.code : "";
  const message = error instanceof Error ? error.message : String(error);
  const combined = `${code} ${message}`.toLowerCase();
  return [
    "timeout",
    "no responders",
    "connection closed",
    "disconnected",
    "econnreset",
    "econnrefused",
    "etimedout",
    "fetch failed",
    "socket",
    "network"
  ].some((token) => combined.includes(token));
}

function isRouteRecoveryRequestMessage(message: unknown): message is { data: Uint8Array; respond: (data: Uint8Array) => boolean } {
  return Boolean(
    message
      && typeof message === "object"
      && "data" in message
      && (message as { data?: unknown }).data instanceof Uint8Array
      && typeof (message as { respond?: unknown }).respond === "function"
  );
}

export function isRecoverableRouteStreamError(error: unknown): boolean {
  if (isRetryableLifecycleError(error)) return true;
  if (error instanceof LifecycleRequestError) return error.status === 404 || error.status === 409;
  const status = error && typeof error === "object" ? Number((error as { status?: unknown }).status) : NaN;
  return status === 404 || status === 409;
}

async function sleepWithJitter(baseMs: number): Promise<void> {
  const jitter = Math.floor(baseMs * (Math.random() * 0.4 - 0.2));
  await new Promise((resolve) => setTimeout(resolve, Math.max(1, baseMs + jitter)));
}

async function stableRequestId(identity: string): Promise<string> {
  const bytes = new TextEncoder().encode(identity);
  let digest: Uint8Array;
  if (globalThis.crypto?.subtle) {
    digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
  } else {
    const { createHash } = await import("node:crypto");
    digest = new Uint8Array(createHash("sha256").update(bytes).digest());
  }
  return `idempotent-${[...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
