use crate::{error::BeamApiError, SignedChunkRoute, TransferTerminalEvent};
use base64::Engine;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use rmp_serde::{from_slice, to_vec_named};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

pub(crate) const TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION: &str = "transfer-client-control/v7";
pub(crate) const ROUTE_RECOVERY_SIGN_MESSAGE_TYPE: &str = "transfer.route_recovery.sign";
/// Signed-route batches aim for this encoded size so a single message stays well below the
/// broker limit; a single route larger than the target but within `max_payload_bytes` is sent
/// alone.
pub const BEAM_ROUTE_TARGET_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const ROUTE_BATCH_AUTH_TOKEN_ESTIMATE_BYTES: usize = 64 * 1024;
const AUTH_TOKEN_REFRESH_SAFETY_SECONDS: u64 = 30;
const LIFECYCLE_REQUEST_MAX_ATTEMPTS: usize = 3;
const RUNTIME_HELLO_INTERVAL: Duration = Duration::from_secs(5);
const SDK_MAX_RECONNECTS: Option<usize> = None;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) type RecoveryFuture = BoxFuture<'static, Result<(), BeamApiError>>;
pub(crate) type MessageStream = Pin<Box<dyn Stream<Item = InboundMessage> + Send>>;
pub(crate) type RouteRecoverySignHandler = Arc<
    dyn Fn(
            RouteRecoverySignRequestPayload,
        ) -> BoxFuture<'static, Result<RouteRecoverySignReplyPayload, BeamApiError>>
        + Send
        + Sync,
>;

pub(crate) type IntegritySignHandler = Arc<
    dyn Fn(crate::IntegrityAuditChallenge) -> BoxFuture<'static, Result<Value, BeamApiError>>
        + Send
        + Sync,
>;

/// A message delivered to a subscription.
pub(crate) struct InboundMessage {
    pub payload: Bytes,
    pub reply: Option<String>,
}

/// The broker operations the lifecycle control plane needs. Production uses NATS; unit tests
/// substitute an in-memory broker so they observe the exact encoded wire messages.
pub(crate) trait ControlTransport: Send + Sync {
    fn request(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<Bytes, String>>;
    fn subscribe(&self, subject: String) -> BoxFuture<'_, Result<MessageStream, String>>;
    fn publish(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<(), String>>;
    fn close(&self) -> BoxFuture<'_, Result<(), String>>;
}

pub(crate) struct RecoveryLease {
    pub transfer_id: String,
    pub plan_fingerprint: String,
    pub coordinate_checksum: String,
    pub replay_routes: Arc<dyn Fn(String) -> RecoveryFuture + Send + Sync>,
    pub dispose: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RouteRecoverySignChunk {
    pub source_id: String,
    pub destination_id: String,
    pub chunk_index: u64,
    pub delivery_index: u64,
    pub source_offset: u64,
    pub chunk_size: u64,
    pub logical_attempt_index: u64,
    pub attempt_slot: u64,
    pub part_number: u64,
    pub route_generation_id: String,
    pub multipart_group_id: String,
    pub final_object_key: String,
    pub upload_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urls_expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_metadata: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_metadata: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<crate::MultipartRecoveryRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RouteRecoverySignRequestPayload {
    pub transfer_id: String,
    pub route_generation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    pub chunks: Vec<RouteRecoverySignChunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RouteRecoverySignReplyPayload {
    pub transfer_id: String,
    pub route_generation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_at: Option<String>,
    pub chunk_routes: Vec<SignedChunkRoute>,
}

#[derive(Debug, Deserialize)]
struct RouteRecoverySignEnvelope {
    schema_version: String,
    environment: String,
    key_prefix: String,
    transfer_id: String,
    message_type: String,
    producer: String,
    payload: RouteRecoverySignRequestPayload,
}

#[derive(Debug, Deserialize)]
struct RouteRecoveryEnvelopeIdentity {
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct NatsControl {
    inner: Arc<NatsControlInner>,
}

struct NatsControlInner {
    key_prefix: String,
    environment: String,
    subject_prefix: String,
    shard_count: u64,
    request_timeout: Duration,
    max_payload_bytes: usize,
    closed: AtomicBool,
    transport: Arc<dyn ControlTransport>,
    auth_token: Mutex<AuthTokenState>,
    terminal_waiters: Mutex<Vec<Weak<TerminalSignalWaiterInner>>>,
    recovery: Mutex<RecoveryState>,
    recovery_signers: StdMutex<HashMap<u64, Arc<SignerStop>>>,
    next_signer_id: AtomicU64,
}

#[derive(Default)]
struct RecoveryState {
    performance_v2: HashMap<u64, std::time::Instant>,
    leases: HashMap<String, Arc<RecoveryLease>>,
    runtime_epochs: HashMap<u64, (String, String)>,
    requested_epochs: HashMap<String, (String, String)>,
    /// Leases with a recovery loop in flight, keyed by lease identity ([`lease_key`]) so a
    /// replacement owner's recovery is never coalesced into a fenced-off owner's loop.
    running: HashSet<usize>,
    hello_monitors: HashSet<u64>,
}

#[derive(Clone)]
pub(crate) struct NatsTerminalSignalWaiter {
    inner: Arc<TerminalSignalWaiterInner>,
}

struct TerminalSignalWaiterInner {
    transfer_id: String,
    subscriber: Mutex<Option<MessageStream>>,
    closed: AtomicBool,
    closed_notify: Notify,
}

struct SignerStop {
    active: AtomicBool,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SignerStop {
    fn stop(&self) {
        if !self.active.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(task) = self.task.lock().ok().and_then(|mut task| task.take()) {
            task.abort();
        }
    }
}

/// Stops a route recovery signer subscription. Stopping is idempotent.
#[derive(Clone)]
pub(crate) struct RouteRecoverySignerHandle {
    id: u64,
    stop: Arc<SignerStop>,
    control: Weak<NatsControlInner>,
}

impl RouteRecoverySignerHandle {
    pub(crate) fn stop(&self) {
        self.stop.stop();
        if let Some(control) = self.control.upgrade() {
            if let Ok(mut signers) = control.recovery_signers.lock() {
                signers.remove(&self.id);
            }
        }
    }
}

#[derive(Default)]
struct AuthTokenState {
    token: Option<String>,
    expires_at: u64,
}

#[derive(Debug, Serialize)]
struct RequestEnvelope<'a> {
    message_id: &'a str,
    schema_version: &'static str,
    environment: &'a str,
    key_prefix: &'a str,
    shard_id: u64,
    message_type: &'a str,
    request_id: &'a str,
    auth_token: &'a str,
    occurred_at: &'a str,
    producer: &'static str,
    payload: &'a Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReplyEnvelope {
    ok: bool,
    status: u16,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    error: Option<Value>,
    #[serde(default)]
    runtime_epoch: Option<String>,
    #[serde(default)]
    transport_epoch: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthResolveResponse {
    ok: bool,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    exp: u64,
}

/// Connection plan for a lifecycle URL, kept separate so it can be verified without a broker.
pub(crate) fn connect_options(
    url: &str,
    api_key: &str,
    key_prefix: &str,
) -> async_nats::ConnectOptions {
    let mut options = async_nats::ConnectOptions::new()
        .user_and_password(key_prefix.to_string(), api_key.to_string())
        .name(format!("beam-rust-sdk-{key_prefix}"))
        .max_reconnects(SDK_MAX_RECONNECTS)
        .reconnect_delay_callback(|attempt| {
            let capped = attempt.min(6) as u32;
            let base = Duration::from_millis(250 * (1_u64 << capped));
            base.min(Duration::from_secs(30))
        });
    // The production gateway only speaks TLS from the first byte (NATS `handshake_first`); it
    // never sends a plaintext INFO, so a tls:// URL must start the TLS handshake immediately.
    if url.trim().to_ascii_lowercase().starts_with("tls://") {
        options = options.tls_first();
    }
    options
}

struct NatsTransport {
    api_key: String,
    key_prefix: String,
    nats_url: String,
    closed: AtomicBool,
    client: Mutex<Option<async_nats::Client>>,
}

impl NatsTransport {
    async fn connection(&self) -> Result<async_nats::Client, String> {
        if self.closed.load(Ordering::Acquire) {
            return Err("NATS lifecycle control is closed".to_string());
        }
        let mut guard = self.client.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err("NATS lifecycle control is closed".to_string());
        }
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let client = connect_options(&self.nats_url, &self.api_key, &self.key_prefix)
            .connect(self.nats_url.clone())
            .await
            .map_err(|error| error.to_string())?;
        if self.closed.load(Ordering::Acquire) {
            let _ = client.drain().await;
            return Err("NATS lifecycle control is closed".to_string());
        }
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl ControlTransport for NatsTransport {
    fn request(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<Bytes, String>> {
        Box::pin(async move {
            let client = self.connection().await?;
            client
                .request(subject, payload)
                .await
                .map(|message| message.payload)
                .map_err(|error| error.to_string())
        })
    }

    fn subscribe(&self, subject: String) -> BoxFuture<'_, Result<MessageStream, String>> {
        Box::pin(async move {
            let client = self.connection().await?;
            let subscriber = client
                .subscribe(subject)
                .await
                .map_err(|error| error.to_string())?;
            client.flush().await.map_err(|error| error.to_string())?;
            let stream = subscriber.map(|message| InboundMessage {
                payload: message.payload,
                reply: message.reply.map(|reply| reply.to_string()),
            });
            Ok(Box::pin(stream) as MessageStream)
        })
    }

    fn publish(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let client = self.connection().await?;
            client
                .publish(subject, payload)
                .await
                .map_err(|error| error.to_string())?;
            client.flush().await.map_err(|error| error.to_string())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            let client = self.client.lock().await.take();
            match client {
                Some(client) => client.drain().await.map_err(|error| error.to_string()),
                None => Ok(()),
            }
        })
    }
}

enum AttemptOutcome {
    Done(Value),
    Retry(BeamApiError),
    Fail(BeamApiError),
}

impl NatsControl {
    pub(crate) fn new(
        api_key: String,
        nats_url: String,
        environment: String,
        subject_prefix: Option<String>,
        shard_count: u64,
        request_timeout: Duration,
        max_payload_bytes: usize,
    ) -> Self {
        let key_prefix = key_prefix(&api_key);
        let transport = Arc::new(NatsTransport {
            api_key,
            key_prefix: key_prefix.clone(),
            nats_url: nats_url.trim_end_matches('/').to_string(),
            closed: AtomicBool::new(false),
            client: Mutex::new(None),
        });
        Self::with_transport(
            key_prefix,
            environment,
            subject_prefix,
            shard_count,
            request_timeout,
            max_payload_bytes,
            transport,
        )
    }

    pub(crate) fn with_transport(
        key_prefix: String,
        environment: String,
        subject_prefix: Option<String>,
        shard_count: u64,
        request_timeout: Duration,
        max_payload_bytes: usize,
        transport: Arc<dyn ControlTransport>,
    ) -> Self {
        let subject_prefix = subject_prefix
            .as_deref()
            .unwrap_or("beam.transfer.client")
            .trim_matches('.')
            .to_string();
        Self {
            inner: Arc::new(NatsControlInner {
                key_prefix,
                environment,
                subject_prefix,
                shard_count: shard_count.max(1),
                request_timeout,
                max_payload_bytes,
                closed: AtomicBool::new(false),
                transport,
                auth_token: Mutex::new(AuthTokenState::default()),
                terminal_waiters: Mutex::new(Vec::new()),
                recovery: Mutex::new(RecoveryState::default()),
                recovery_signers: StdMutex::new(HashMap::new()),
                next_signer_id: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub(crate) async fn request_value(
        &self,
        message_type: &str,
        payload: Value,
        transfer_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<Value, BeamApiError> {
        let shard_id = transfer_id
            .map(|value| transfer_shard_id(value, self.inner.shard_count))
            .unwrap_or(0);
        self.request_value_on_shard(message_type, &payload, idempotency_key, shard_id)
            .await
    }

    /// Send one lifecycle request.
    ///
    /// The request id, message id and `occurred_at` are fixed before the first attempt so a
    /// retry is the same logical request. The envelope is rebuilt per attempt because the auth
    /// token can be refreshed between attempts (an `auth_token_expired` reply clears it).
    async fn request_value_on_shard(
        &self,
        message_type: &str,
        payload: &Value,
        idempotency_key: Option<&str>,
        shard_id: u64,
    ) -> Result<Value, BeamApiError> {
        let request_id = lifecycle_request_id(message_type, idempotency_key);
        let message_id = format!(
            "{}:{}:{}:{}:{}",
            TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            self.inner.environment,
            self.inner.key_prefix,
            message_type,
            request_id
        );
        let occurred_at = iso_now();
        let subject = self.request_subject(message_type, shard_id);
        let mut last_error = BeamApiError::Nats("lifecycle request failed".to_string());
        for attempt in 0..LIFECYCLE_REQUEST_MAX_ATTEMPTS {
            let last_attempt = attempt + 1 >= LIFECYCLE_REQUEST_MAX_ATTEMPTS;
            let outcome = self
                .request_attempt(
                    message_type,
                    payload,
                    &request_id,
                    &message_id,
                    &occurred_at,
                    &subject,
                    shard_id,
                    last_attempt,
                )
                .await;
            match outcome {
                AttemptOutcome::Done(value) => return Ok(value),
                AttemptOutcome::Fail(error) => return Err(error),
                AttemptOutcome::Retry(error) => {
                    if last_attempt {
                        return Err(error);
                    }
                    last_error = error;
                }
            }
            sleep_before_retry(attempt).await;
        }
        Err(last_error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_attempt(
        &self,
        message_type: &str,
        payload: &Value,
        request_id: &str,
        message_id: &str,
        occurred_at: &str,
        subject: &str,
        shard_id: u64,
        last_attempt: bool,
    ) -> AttemptOutcome {
        let classify = |error: BeamApiError| {
            if is_retryable_lifecycle_error(&error) {
                AttemptOutcome::Retry(error)
            } else {
                AttemptOutcome::Fail(error)
            }
        };
        let auth_token = match self.auth_token().await {
            Ok(token) => token,
            Err(error) => return classify(error),
        };
        let envelope = RequestEnvelope {
            message_id,
            schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            environment: &self.inner.environment,
            key_prefix: &self.inner.key_prefix,
            shard_id,
            message_type,
            request_id,
            auth_token: &auth_token,
            occurred_at,
            producer: "sdk",
            payload,
        };
        let encode_started = std::time::Instant::now();
        let data = match to_vec_named(&envelope) {
            Ok(data) => data,
            Err(error) => return AttemptOutcome::Fail(BeamApiError::Nats(error.to_string())),
        };
        if message_type == "transfer.route_stream.batch" {
            let _ = crate::performance::CURRENT.try_with(|metrics| {
                let mut metrics = metrics.lock().unwrap();
                metrics.observe("sdk.batch_encode", encode_started);
                metrics.gauge("batch_bytes_max", data.len() as f64);
            });
        }
        if data.len() > self.inner.max_payload_bytes {
            return AttemptOutcome::Fail(BeamApiError::Nats(format!(
                "NATS lifecycle request is {} bytes, above max_payload_bytes={}",
                data.len(),
                self.inner.max_payload_bytes
            )));
        }
        if self.is_closed() {
            return AttemptOutcome::Fail(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        let response = match tokio::time::timeout(
            self.inner.request_timeout,
            self.inner
                .transport
                .request(subject.to_string(), Bytes::from(data)),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(message)) => return classify(BeamApiError::Nats(message)),
            Err(_) => {
                return AttemptOutcome::Retry(BeamApiError::Nats(format!(
                    "NATS request timed out: {subject}"
                )))
            }
        };
        let decoded = match from_slice::<ReplyEnvelope>(&response) {
            Ok(decoded) => decoded,
            Err(error) => return AttemptOutcome::Fail(BeamApiError::Nats(error.to_string())),
        };
        self.observe_runtime_epochs(shard_id, &decoded).await;
        if decoded.ok {
            if message_type == "runtime.hello" {
                let mut state = self.inner.recovery.lock().await;
                state.performance_v2.remove(&shard_id);
                if decoded
                    .payload
                    .as_ref()
                    .and_then(|p| p.get("capabilities"))
                    .and_then(Value::as_array)
                    .is_some_and(|caps| {
                        caps.iter()
                            .any(|v| v.as_str() == Some("sdk-performance/v2"))
                    })
                {
                    state.performance_v2.insert(
                        shard_id,
                        std::time::Instant::now() + Duration::from_secs(15),
                    );
                }
            }
            return AttemptOutcome::Done(decoded.payload.unwrap_or_else(|| json!({})));
        }
        let error = lifecycle_error(decoded.status, decoded.error.as_ref());
        let expired = matches!(
            &error,
            BeamApiError::Lifecycle { status: 401, code: Some(code), .. } if code == "auth_token_expired"
        );
        if expired && !last_attempt {
            *self.inner.auth_token.lock().await = AuthTokenState::default();
            return AttemptOutcome::Retry(error);
        }
        classify(error)
    }

    pub(crate) async fn open_terminal_signal_waiter(
        &self,
        transfer_id: &str,
    ) -> Result<NatsTerminalSignalWaiter, BeamApiError> {
        if self.is_closed() {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        let subscriber = self
            .inner
            .transport
            .subscribe(self.terminal_subject(transfer_id))
            .await
            .map_err(BeamApiError::Nats)?;
        let waiter = NatsTerminalSignalWaiter {
            inner: Arc::new(TerminalSignalWaiterInner {
                transfer_id: transfer_id.to_string(),
                subscriber: Mutex::new(Some(subscriber)),
                closed: AtomicBool::new(false),
                closed_notify: Notify::new(),
            }),
        };
        if self.is_closed() {
            waiter.close().await?;
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        let mut waiters = self.inner.terminal_waiters.lock().await;
        waiters.retain(|candidate| {
            candidate
                .upgrade()
                .is_some_and(|candidate| !candidate.closed.load(Ordering::Acquire))
        });
        waiters.push(Arc::downgrade(&waiter.inner));
        drop(waiters);
        if self.is_closed() {
            waiter.close().await?;
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        Ok(waiter)
    }

    /// Answer `transfer.route_recovery.sign` requests for one transfer until stopped.
    pub(crate) async fn serve_route_recovery_signer(
        &self,
        transfer_id: &str,
        handler: RouteRecoverySignHandler,
    ) -> Result<RouteRecoverySignerHandle, BeamApiError> {
        if self.is_closed() {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        let mut stream = self
            .inner
            .transport
            .subscribe(self.route_recovery_sign_subject(transfer_id))
            .await
            .map_err(BeamApiError::Nats)?;
        let stop = Arc::new(SignerStop {
            active: AtomicBool::new(true),
            task: StdMutex::new(None),
        });
        let control = self.clone();
        let transfer_id = transfer_id.to_string();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                if !task_stop.active.load(Ordering::Acquire) {
                    return;
                }
                let Some(reply) = message.reply.clone() else {
                    continue;
                };
                let control = control.clone();
                let handler = handler.clone();
                let transfer_id = transfer_id.clone();
                tokio::spawn(async move {
                    let response = control
                        .route_recovery_response(&transfer_id, &message.payload, handler)
                        .await;
                    if let Ok(bytes) = to_vec_named(&response) {
                        let _ = control
                            .inner
                            .transport
                            .publish(reply, Bytes::from(bytes))
                            .await;
                    }
                });
            }
        });
        if let Ok(mut slot) = stop.task.lock() {
            *slot = Some(task);
        }
        let id = self.inner.next_signer_id.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut signers) = self.inner.recovery_signers.lock() {
            signers.insert(id, stop.clone());
        }
        if self.is_closed() {
            stop.stop();
        }
        Ok(RouteRecoverySignerHandle {
            id,
            stop,
            control: Arc::downgrade(&self.inner),
        })
    }

    pub(crate) async fn serve_integrity_signer(
        &self,
        transfer_id: &str,
        handler: IntegritySignHandler,
    ) -> Result<RouteRecoverySignerHandle, BeamApiError> {
        if self.is_closed() {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".into(),
            ));
        }
        let subject = format!(
            "{}.{}.sdk.{}.transfer.{}.integrity_sign",
            self.inner.subject_prefix, self.inner.environment, self.inner.key_prefix, transfer_id
        );
        let mut stream = self
            .inner
            .transport
            .subscribe(subject)
            .await
            .map_err(BeamApiError::Nats)?;
        let stop = Arc::new(SignerStop {
            active: AtomicBool::new(true),
            task: StdMutex::new(None),
        });
        let control = self.clone();
        let transfer_id = transfer_id.to_owned();
        let task_stop = stop.clone();
        // Sequential admission bounds signer work. Runtime retries unacknowledged requests.
        let task = tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                if !task_stop.active.load(Ordering::Acquire) {
                    return;
                }
                let Some(reply) = message.reply else {
                    continue;
                };
                let request: Value = match serde_json::from_slice(&message.payload) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let valid = request["schema_version"] == TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION
                    && request["capability"] == "integrity-signing/v1"
                    && request["producer"] == "transfer-runtime"
                    && request["environment"] == control.inner.environment
                    && request["key_prefix"] == control.inner.key_prefix
                    && request["transfer_id"] == transfer_id
                    && request["challenge"]["transfer_id"] == transfer_id
                    && request["request_id"]
                        .as_str()
                        .is_some_and(|id| !id.is_empty());
                let payload = if valid {
                    match serde_json::from_value(request["challenge"].clone()) {
                        Ok(challenge) => {
                            tokio::time::timeout(Duration::from_secs(30), handler(challenge))
                                .await
                                .ok()
                                .and_then(Result::ok)
                        }
                        Err(_) => None,
                    }
                } else {
                    None
                };
                let response = match payload {
                    Some(payload) => {
                        json!({ "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION, "capability": "integrity-signing/v1", "environment": control.inner.environment, "key_prefix": control.inner.key_prefix, "transfer_id": transfer_id, "request_id": request["request_id"], "payload": payload })
                    }
                    None => json!({"retry": true}),
                };
                if let Ok(bytes) = serde_json::to_vec(&response) {
                    let _ = control
                        .inner
                        .transport
                        .publish(reply, Bytes::from(bytes))
                        .await;
                }
            }
        });
        if let Ok(mut slot) = stop.task.lock() {
            *slot = Some(task);
        }
        let id = self.inner.next_signer_id.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut signers) = self.inner.recovery_signers.lock() {
            signers.insert(id, stop.clone());
        }
        if self.is_closed() {
            stop.stop();
        }
        Ok(RouteRecoverySignerHandle {
            id,
            stop,
            control: Arc::downgrade(&self.inner),
        })
    }

    async fn route_recovery_response(
        &self,
        transfer_id: &str,
        payload: &[u8],
        handler: RouteRecoverySignHandler,
    ) -> Value {
        let identity = from_slice::<RouteRecoveryEnvelopeIdentity>(payload).ok();
        let (message_id, request_id) = identity
            .map(|identity| {
                (
                    identity.message_id.unwrap_or_else(|| "unknown".to_string()),
                    identity.request_id.unwrap_or_else(|| "unknown".to_string()),
                )
            })
            .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));
        let result = async {
            let request = from_slice::<RouteRecoverySignEnvelope>(payload).map_err(|_| {
                BeamApiError::ProviderSigning("route recovery request envelope mismatch".into())
            })?;
            if request.schema_version != TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION
                || request.environment != self.inner.environment
                || request.key_prefix != self.inner.key_prefix
                || request.transfer_id != transfer_id
                || request.message_type != ROUTE_RECOVERY_SIGN_MESSAGE_TYPE
                || request.producer != "transfer-runtime"
                || request.payload.transfer_id != transfer_id
                || request.payload.route_generation_id.is_empty()
                || request.payload.chunks.is_empty()
            {
                return Err(BeamApiError::ProviderSigning(
                    "route recovery request envelope mismatch".into(),
                ));
            }
            handler(request.payload).await
        }
        .await;
        let mut reply = json!({
            "message_id": message_id,
            "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            "environment": self.inner.environment,
            "key_prefix": self.inner.key_prefix,
            "transfer_id": transfer_id,
            "message_type": ROUTE_RECOVERY_SIGN_MESSAGE_TYPE,
            "request_id": request_id,
            "occurred_at": iso_now(),
            "producer": "sdk",
        });
        match result {
            Ok(payload) => {
                reply["ok"] = json!(true);
                reply["status"] = json!(200);
                reply["payload"] = serde_json::to_value(payload).unwrap_or_else(|_| json!({}));
            }
            Err(error) => {
                reply["ok"] = json!(false);
                reply["status"] = json!(500);
                reply["error"] = json!({
                    "code": "route_recovery_sign_failed",
                    "message": crate::error::sanitized_error_summary(&error),
                });
            }
        }
        reply
    }

    /// Register `lease` as the transfer's recovery lease, replacing (and disposing) any other.
    ///
    /// The returned `Arc` is the owner's identity: pass it to [`Self::release_recovery_lease`]
    /// and [`Self::continue_recovery_lease`] so a fenced-off owner never acts on a replacement.
    pub(crate) async fn register_recovery_lease(
        &self,
        lease: impl Into<Arc<RecoveryLease>>,
    ) -> Arc<RecoveryLease> {
        let lease = lease.into();
        let shard_id = transfer_shard_id(&lease.transfer_id, self.inner.shard_count);
        let mut state = self.inner.recovery.lock().await;
        if let Some(previous) = state
            .leases
            .insert(lease.transfer_id.clone(), lease.clone())
        {
            if !Arc::ptr_eq(&previous, &lease) {
                if let Some(dispose) = &previous.dispose {
                    dispose();
                }
            }
        }
        let start_monitor = state.hello_monitors.insert(shard_id);
        drop(state);
        if start_monitor && !self.is_closed() {
            let control = self.clone();
            tokio::spawn(async move { control.monitor_runtime(shard_id).await });
        }
        lease
    }

    #[cfg(test)]
    pub(crate) async fn has_recovery_lease(&self, transfer_id: &str) -> bool {
        self.inner
            .recovery
            .lock()
            .await
            .leases
            .contains_key(transfer_id)
    }

    #[cfg(test)]
    pub(crate) async fn recovery_lease(&self, transfer_id: &str) -> Option<Arc<RecoveryLease>> {
        self.inner
            .recovery
            .lock()
            .await
            .leases
            .get(transfer_id)
            .cloned()
    }

    /// Release a transfer's recovery lease, disposing its retained secrets.
    ///
    /// With `owner`, release only while that exact lease is still registered, so a fenced-off
    /// owner can never release a replacement owner's lease.
    pub(crate) async fn release_recovery_lease(
        &self,
        transfer_id: &str,
        owner: Option<&Arc<RecoveryLease>>,
    ) {
        let lease = {
            let mut state = self.inner.recovery.lock().await;
            let Some(current) = state.leases.get(transfer_id) else {
                return;
            };
            if owner.is_some_and(|owner| !Arc::ptr_eq(owner, current)) {
                return;
            }
            state.requested_epochs.remove(transfer_id);
            state.leases.remove(transfer_id)
        };
        if let Some(dispose) = lease.and_then(|lease| lease.dispose.clone()) {
            dispose();
        }
    }

    /// Request background recovery for the registered lease; with `owner`, only while that
    /// exact lease is still registered.
    pub(crate) async fn continue_recovery_lease(
        &self,
        transfer_id: &str,
        owner: Option<&Arc<RecoveryLease>>,
    ) {
        if self.is_closed() {
            return;
        }
        let lease = {
            let mut state = self.inner.recovery.lock().await;
            let Some(lease) = state.leases.get(transfer_id).cloned() else {
                return;
            };
            if owner.is_some_and(|owner| !Arc::ptr_eq(owner, &lease)) {
                return;
            }
            let shard_id = transfer_shard_id(transfer_id, self.inner.shard_count);
            let requested_epoch = state
                .runtime_epochs
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| ("foreground".to_string(), new_transfer_id()));
            state
                .requested_epochs
                .insert(transfer_id.to_string(), requested_epoch);
            state.running.insert(lease_key(&lease)).then_some(lease)
        };
        if let Some(lease) = lease {
            self.spawn_recovery(lease);
        }
    }

    async fn observe_runtime_epochs(&self, shard_id: u64, envelope: &ReplyEnvelope) {
        let (Some(runtime_epoch), Some(transport_epoch)) = (
            envelope.runtime_epoch.as_ref(),
            envelope.transport_epoch.as_ref(),
        ) else {
            return;
        };
        if runtime_epoch.is_empty() || transport_epoch.is_empty() {
            return;
        }
        let current = (runtime_epoch.clone(), transport_epoch.clone());
        let mut state = self.inner.recovery.lock().await;
        let previous = state.runtime_epochs.insert(shard_id, current.clone());
        if previous.as_ref() != Some(&current) {
            state.performance_v2.remove(&shard_id);
        }
        let changed = previous
            .as_ref()
            .is_some_and(|previous| previous != &current);
        let mut leases = Vec::new();
        if changed {
            let eligible: Vec<_> = state
                .leases
                .values()
                .filter(|lease| {
                    transfer_shard_id(&lease.transfer_id, self.inner.shard_count) == shard_id
                })
                .cloned()
                .collect();
            for lease in eligible {
                state
                    .requested_epochs
                    .insert(lease.transfer_id.clone(), current.clone());
                if state.running.insert(lease_key(&lease)) {
                    leases.push(lease);
                }
            }
        }
        drop(state);
        if changed && previous.is_some_and(|previous| previous.0 != *runtime_epoch) {
            *self.inner.auth_token.lock().await = AuthTokenState::default();
        }
        for lease in leases {
            self.spawn_recovery(lease);
        }
    }

    fn spawn_recovery(&self, lease: Arc<RecoveryLease>) {
        let control = self.clone();
        tokio::spawn(async move { control.recover_transfer(lease).await });
    }

    pub(crate) async fn supports_performance_v2(&self, transfer_id: &str) -> bool {
        self.inner
            .recovery
            .lock()
            .await
            .performance_v2
            .get(&transfer_shard_id(transfer_id, self.inner.shard_count))
            .is_some_and(|until| *until > std::time::Instant::now())
    }
    async fn monitor_runtime(&self, shard_id: u64) {
        while !self.is_closed() {
            let has_lease = {
                let mut state = self.inner.recovery.lock().await;
                let present = state.leases.values().any(|lease| {
                    transfer_shard_id(&lease.transfer_id, self.inner.shard_count) == shard_id
                });
                if !present {
                    state.hello_monitors.remove(&shard_id);
                }
                present
            };
            if !has_lease {
                return;
            }
            let idempotency_key = format!("runtime:hello:{shard_id}");
            // Reconnect continues in the background; the next hello reconciles epochs.
            let hello = self
                .request_value_on_shard(
                    "runtime.hello",
                    &json!({"capabilities": ["integrity-signing/v1", "sdk-performance/v2"]}),
                    Some(&idempotency_key),
                    shard_id,
                )
                .await;
            if hello.is_err() {
                let legacy_key = format!("{idempotency_key}:legacy");
                let _ = self
                    .request_value_on_shard(
                        "runtime.hello",
                        &json!({}),
                        Some(&legacy_key),
                        shard_id,
                    )
                    .await;
            }
            tokio::time::sleep(RUNTIME_HELLO_INTERVAL).await;
        }
    }

    /// Drive recovery for `lease` until it succeeds, is released, or is replaced.
    ///
    /// When another owner replaced the lease while this loop was in flight, the recovery it was
    /// driving is handed to the replacement instead of being dropped.
    async fn recover_transfer(&self, lease: Arc<RecoveryLease>) {
        let outcome = self.run_recovery(&lease).await;
        if outcome == RecoveryOutcome::Settled {
            return;
        }
        let current = {
            let mut state = self.inner.recovery.lock().await;
            state.running.remove(&lease_key(&lease));
            state.leases.get(&lease.transfer_id).cloned()
        };
        if outcome != RecoveryOutcome::Replaced || self.is_closed() {
            return;
        }
        let Some(current) = current.filter(|current| !Arc::ptr_eq(current, &lease)) else {
            return;
        };
        let start = self
            .inner
            .recovery
            .lock()
            .await
            .running
            .insert(lease_key(&current));
        if start {
            self.spawn_recovery(current);
        }
    }

    /// Whether `lease` is still the registered lease of its transfer on an open control plane.
    async fn owns_lease(&self, lease: &Arc<RecoveryLease>) -> bool {
        !self.is_closed()
            && self
                .inner
                .recovery
                .lock()
                .await
                .leases
                .get(&lease.transfer_id)
                .is_some_and(|current| Arc::ptr_eq(current, lease))
    }

    /// The recovery loop. Ownership is re-checked after every await: a fenced-off owner must
    /// neither replay into nor release a replacement's lease.
    async fn run_recovery(&self, lease: &Arc<RecoveryLease>) -> RecoveryOutcome {
        let replaced = |control: &Self| {
            if control.is_closed() {
                RecoveryOutcome::Stopped
            } else {
                RecoveryOutcome::Replaced
            }
        };
        let mut attempt = 0_u32;
        loop {
            if self.is_closed() {
                return RecoveryOutcome::Stopped;
            }
            let requested_epoch = {
                let state = self.inner.recovery.lock().await;
                if !state
                    .leases
                    .get(&lease.transfer_id)
                    .is_some_and(|current| Arc::ptr_eq(current, lease))
                {
                    return replaced(self);
                }
                state.requested_epochs.get(&lease.transfer_id).cloned()
            };
            let generation_id = new_transfer_id();
            let idempotency_key =
                format!("transfer:{}:resume:{}", lease.transfer_id, generation_id);
            let resumed = self
                .request_value(
                    "transfer.resume",
                    json!({
                        "transfer_id": lease.transfer_id.clone(),
                        "plan_fingerprint": lease.plan_fingerprint.clone(),
                        "coordinate_checksum": lease.coordinate_checksum.clone(),
                        "route_generation_id": generation_id.clone(),
                    }),
                    Some(&lease.transfer_id),
                    Some(&idempotency_key),
                )
                .await;
            if !self.owns_lease(lease).await {
                return replaced(self);
            }
            let result = match resumed {
                Ok(result)
                    if result.get("recovery").and_then(Value::as_str) == Some("terminal") =>
                {
                    self.release_recovery_lease(&lease.transfer_id, Some(lease))
                        .await;
                    return RecoveryOutcome::Stopped;
                }
                Ok(result) if resume_requires_route_replay(&result) => {
                    let replayed = (lease.replay_routes)(generation_id).await;
                    if !self.owns_lease(lease).await {
                        return replaced(self);
                    }
                    replayed
                }
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => {
                    // Check for a newer request and finish under one lock, so a recovery
                    // requested meanwhile either restarts this loop or starts a new one.
                    let mut state = self.inner.recovery.lock().await;
                    if state.requested_epochs.get(&lease.transfer_id) == requested_epoch.as_ref() {
                        state.running.remove(&lease_key(lease));
                        return RecoveryOutcome::Settled;
                    }
                    drop(state);
                    attempt = 0;
                    continue;
                }
                Err(error) if !is_retryable_lifecycle_error(&error) => {
                    self.release_recovery_lease(&lease.transfer_id, Some(lease))
                        .await;
                    return RecoveryOutcome::Stopped;
                }
                Err(_) => {}
            }
            attempt = attempt.saturating_add(1);
            let delay =
                Duration::from_millis(500 * (1_u64 << attempt.min(6))).min(Duration::from_secs(30));
            tokio::time::sleep(jittered(delay)).await;
        }
    }

    /// Close the control plane: stop signers and waiters, dispose every retained lease, and
    /// drain the connection so in-flight publishes are flushed.
    pub(crate) async fn close(&self) -> Result<(), BeamApiError> {
        self.inner.closed.store(true, Ordering::Release);
        let signers = self
            .inner
            .recovery_signers
            .lock()
            .map(|mut signers| std::mem::take(&mut *signers))
            .unwrap_or_default();
        for signer in signers.into_values() {
            signer.stop();
        }
        let leases = {
            let mut recovery = self.inner.recovery.lock().await;
            recovery.requested_epochs.clear();
            std::mem::take(&mut recovery.leases)
        };
        for lease in leases.into_values() {
            if let Some(dispose) = &lease.dispose {
                dispose();
            }
        }
        let waiters = std::mem::take(&mut *self.inner.terminal_waiters.lock().await);
        let mut first_error = None;
        for waiter in waiters.into_iter().filter_map(|waiter| waiter.upgrade()) {
            if let Err(error) = (NatsTerminalSignalWaiter { inner: waiter }).close().await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Err(error) = self.inner.transport.close().await {
            if first_error.is_none() {
                first_error = Some(BeamApiError::Nats(error));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Split signed routes into route-stream batches.
    ///
    /// Batches target `min(max_payload_bytes, 4 MiB)` of encoded MessagePack including the full
    /// request envelope and a reserved auth-token allowance. A single route above the target is
    /// still sent alone when it fits `max_payload_bytes`; above that it fails before publication.
    pub(crate) fn split_routes_for_payload(
        &self,
        message_type: &str,
        base_payload: &Value,
        routes: &[Value],
    ) -> Result<Vec<Vec<Value>>, BeamApiError> {
        let max_payload_bytes = self.inner.max_payload_bytes;
        let target_payload_bytes = max_payload_bytes.min(BEAM_ROUTE_TARGET_PAYLOAD_BYTES);
        let auth_token_estimate_bytes =
            ROUTE_BATCH_AUTH_TOKEN_ESTIMATE_BYTES.min((max_payload_bytes / 128).max(512));
        let auth_token = "x".repeat(auth_token_estimate_bytes);
        let request_id = "00000000-0000-4000-8000-000000000000";
        let message_id = format!(
            "{}:{}:{}:{}:{}",
            TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            self.inner.environment,
            self.inner.key_prefix,
            message_type,
            request_id
        );
        let occurred_at = iso_now();
        let encoded_size = |candidate: &[Value]| -> Result<usize, BeamApiError> {
            let mut payload = base_payload.clone();
            payload["route_batch"] = compact_signed_route_values(candidate)?;
            let envelope = RequestEnvelope {
                message_id: &message_id,
                schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
                environment: &self.inner.environment,
                key_prefix: &self.inner.key_prefix,
                shard_id: 0,
                message_type,
                request_id,
                auth_token: &auth_token,
                occurred_at: &occurred_at,
                producer: "sdk",
                payload: &payload,
            };
            to_vec_named(&envelope)
                .map(|encoded| encoded.len())
                .map_err(|error| BeamApiError::Nats(error.to_string()))
        };
        let mut chunks = Vec::new();
        let mut offset = 0usize;
        while offset < routes.len() {
            let mut low = 1usize;
            let mut high = routes.len() - offset;
            let mut accepted = 0usize;
            while low <= high {
                let count = (low + high) / 2;
                if encoded_size(&routes[offset..offset + count])? <= target_payload_bytes {
                    accepted = count;
                    low = count + 1;
                } else {
                    high = count - 1;
                }
            }
            if accepted == 0 {
                let size = encoded_size(&routes[offset..offset + 1])?;
                if size > max_payload_bytes {
                    return Err(BeamApiError::Nats(format!(
                        "single signed route is {size} bytes, above max_payload_bytes={max_payload_bytes}"
                    )));
                }
                accepted = 1;
            }
            chunks.push(routes[offset..offset + accepted].to_vec());
            offset += accepted;
        }
        Ok(chunks)
    }

    async fn auth_token(&self) -> Result<String, BeamApiError> {
        let mut guard = self.inner.auth_token.lock().await;
        if let Some(token) = &guard.token {
            if guard
                .expires_at
                .saturating_sub(AUTH_TOKEN_REFRESH_SAFETY_SECONDS)
                > unix_now()
            {
                return Ok(token.clone());
            }
        }
        let subject = self.auth_subject();
        let mut last_error = BeamApiError::Nats("NATS auth resolve failed".to_string());
        for attempt in 0..LIFECYCLE_REQUEST_MAX_ATTEMPTS {
            let result = tokio::time::timeout(
                self.inner.request_timeout,
                self.inner
                    .transport
                    .request(subject.clone(), Bytes::from_static(b"{}")),
            )
            .await;
            let error = match result {
                Ok(Ok(reply)) => {
                    let parsed: AuthResolveResponse = serde_json::from_slice(&reply)
                        .map_err(|error| BeamApiError::Nats(error.to_string()))?;
                    let token = match (parsed.ok, parsed.token) {
                        (true, Some(token)) => token,
                        (_, _) => {
                            return Err(BeamApiError::Nats(format!(
                                "NATS auth resolve failed: {}",
                                parsed.error.unwrap_or_else(|| "unknown_error".to_string())
                            )))
                        }
                    };
                    let claims = decode_jwt_claims(&token)?;
                    guard.expires_at = claims.exp;
                    guard.token = Some(token.clone());
                    return Ok(token);
                }
                Ok(Err(message)) => BeamApiError::Nats(message),
                Err(_) => BeamApiError::Nats(format!("NATS auth resolve timed out: {subject}")),
            };
            if !is_retryable_lifecycle_error(&error) {
                return Err(error);
            }
            last_error = error;
            if attempt + 1 < LIFECYCLE_REQUEST_MAX_ATTEMPTS {
                sleep_before_retry(attempt).await;
            }
        }
        Err(last_error)
    }

    fn auth_subject(&self) -> String {
        format!(
            "{}.{}.auth.{}.resolve",
            self.inner.subject_prefix, self.inner.environment, self.inner.key_prefix
        )
    }

    fn request_subject(&self, message_type: &str, shard_id: u64) -> String {
        format!(
            "{}.{}.sdk.{}.shard.{}.{}",
            self.inner.subject_prefix,
            self.inner.environment,
            self.inner.key_prefix,
            shard_id,
            message_type.replace('.', "_")
        )
    }

    fn terminal_subject(&self, transfer_id: &str) -> String {
        format!(
            "{}.{}.events.{}.{}.terminal",
            self.inner.subject_prefix, self.inner.environment, self.inner.key_prefix, transfer_id,
        )
    }

    fn route_recovery_sign_subject(&self, transfer_id: &str) -> String {
        format!(
            "{}.{}.sdk.{}.transfer.{}.route_recovery_sign",
            self.inner.subject_prefix, self.inner.environment, self.inner.key_prefix, transfer_id,
        )
    }
}

impl NatsTerminalSignalWaiter {
    /// Wait up to `wait_timeout` for the terminal signal. Returns `Ok(None)` on timeout or once
    /// the waiter has been closed.
    pub(crate) async fn wait(
        &self,
        wait_timeout: Duration,
    ) -> Result<Option<TransferTerminalEvent>, BeamApiError> {
        if wait_timeout.is_zero() {
            return Err(BeamApiError::InvalidArgument(
                "timeout must be a positive duration".to_string(),
            ));
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let receive = async {
            let mut subscriber = self.inner.subscriber.lock().await;
            match subscriber.as_mut() {
                Some(subscriber) => subscriber.next().await.map(Some),
                None => Some(None),
            }
        };
        let message = match tokio::time::timeout(wait_timeout, async {
            tokio::select! {
                _ = self.inner.closed_notify.notified() => Some(None),
                message = receive => message,
            }
        })
        .await
        {
            Err(_) => return Ok(None),
            Ok(Some(Some(message))) => message,
            // Closed locally while waiting.
            Ok(Some(None)) => return Ok(None),
            Ok(None) => {
                if self.inner.closed.load(Ordering::Acquire) {
                    return Ok(None);
                }
                return Err(BeamApiError::Nats(
                    "transfer terminal subscription ended".to_string(),
                ));
            }
        };
        let event: TransferTerminalEvent = from_slice(&message.payload).map_err(|error| {
            BeamApiError::Nats(format!("invalid transfer terminal signal: {error}"))
        })?;
        if event.schema_version != TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION
            || event.producer != "transfer-runtime"
            || event.transfer_id != self.inner.transfer_id
            || !matches!(event.status.as_str(), "completed" | "failed" | "cancelled")
        {
            return Err(BeamApiError::Nats(
                "invalid transfer terminal signal identity".to_string(),
            ));
        }
        Ok(Some(event))
    }

    pub(crate) async fn close(&self) -> Result<(), BeamApiError> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.closed_notify.notify_waiters();
        self.inner.closed_notify.notify_one();
        // Dropping the subscription unsubscribes it.
        drop(self.inner.subscriber.lock().await.take());
        Ok(())
    }
}

fn resume_requires_route_replay(result: &Value) -> bool {
    result.get("route_replay_required").and_then(Value::as_bool) == Some(true)
        || result.get("recovery").and_then(Value::as_str) == Some("route_replay_required")
}

fn lifecycle_error(status: u16, error: Option<&Value>) -> BeamApiError {
    let body = error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "{}".to_string());
    let field = |name: &str| {
        error
            .and_then(|error| error.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    BeamApiError::Lifecycle {
        status,
        code: field("code"),
        message: field("message"),
        body,
    }
}

pub(crate) fn new_transfer_id() -> String {
    Uuid::new_v4().to_string()
}

/// How a recovery loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryOutcome {
    /// Recovery finished and the loop already left the running set.
    Settled,
    /// Terminal, non-retryable, or the control plane closed.
    Stopped,
    /// The lease is no longer registered: another owner replaced it (or it was released).
    Replaced,
}

/// Identity of a registered lease. Only compared while the lease is alive (a recovery loop
/// holds its `Arc` until it removes the key), so an address is never reused under the same key.
fn lease_key(lease: &Arc<RecoveryLease>) -> usize {
    Arc::as_ptr(lease) as usize
}

pub(crate) fn transfer_shard_id(transfer_id: &str, shard_count: u64) -> u64 {
    // FNV-1a over UTF-16 code units, matching the TypeScript SDK's `charCodeAt` hashing.
    let mut hash = 2166136261u32;
    for unit in transfer_id.encode_utf16() {
        hash ^= u32::from(unit);
        hash = hash.wrapping_mul(16777619);
    }
    u64::from(hash) % shard_count.max(1)
}

const ROUTE_ATTEMPT_METADATA_KEYS: &[&str] = &[
    "part_number",
    "logical_attempt_index",
    "attempt_slot",
    "etag_required",
    "route_generation_id",
];

const ROUTE_IDENTITY_METADATA_KEYS: &[&str] = &[
    "source_id",
    "destination_id",
    "chunk_index",
    "route_chunk_index",
    "delivery_index",
];

pub(crate) fn compact_signed_route_values(routes: &[Value]) -> Result<Value, BeamApiError> {
    let mut source_chunks = Vec::new();
    let mut source_refs = HashMap::<String, usize>::new();
    let mut compact_routes = Vec::with_capacity(routes.len());
    for (route_index, route) in routes.iter().enumerate() {
        let object = route
            .as_object()
            .ok_or_else(|| BeamApiError::Nats("signed route must be an object".to_string()))?;
        let source_key = serde_json::to_string(&json!([
            object.get("source_id"),
            object.get("chunk_index"),
            object.get("source_url"),
            object.get("source_offset"),
            object.get("chunk_size"),
            object.get("expires_at"),
            object.get("headers"),
        ]))?;
        let source_ref = if let Some(value) = source_refs.get(&source_key) {
            *value
        } else {
            let value = source_chunks.len();
            source_refs.insert(source_key, value);
            let mut source = Map::new();
            source.insert("source_ref".to_string(), json!(value));
            for key in [
                "source_id",
                "chunk_index",
                "source_url",
                "source_offset",
                "chunk_size",
                "expires_at",
                "headers",
            ] {
                if let Some(item) = object.get(key).filter(|item| !item.is_null()) {
                    source.insert(key.to_string(), item.clone());
                }
            }
            source_chunks.push(Value::Object(source));
            value
        };
        let mut metadata = object
            .get("metadata")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let delivery_index = object
            .get("delivery_index")
            .and_then(Value::as_u64)
            .or_else(|| {
                metadata
                    .get("delivery_index")
                    .and_then(non_negative_integer)
            })
            .unwrap_or(route_index as u64);
        for key in ROUTE_IDENTITY_METADATA_KEYS {
            metadata.remove(*key);
        }
        let group_id = metadata
            .get("multipart_group_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        // Only multipart routes reference their group manifest; every other route keeps its
        // metadata so Runtime still sees provider-specific route fields.
        if group_id.is_some() {
            metadata.retain(|key, _| ROUTE_ATTEMPT_METADATA_KEYS.contains(&key.as_str()));
        }
        let mut compact_route = Map::new();
        compact_route.insert("source_ref".to_string(), json!(source_ref));
        for key in ["destination_id"] {
            if let Some(item) = object.get(key) {
                compact_route.insert(key.to_string(), item.clone());
            }
        }
        compact_route.insert("delivery_index".to_string(), json!(delivery_index));
        for key in ["dest_url", "expires_at", "dest_headers"] {
            if let Some(item) = object.get(key).filter(|item| !item.is_null()) {
                compact_route.insert(key.to_string(), item.clone());
            }
        }
        if let Some(group_id) = group_id {
            compact_route.insert("multipart_group_id".to_string(), json!(group_id));
        }
        if !metadata.is_empty() {
            compact_route.insert("metadata".to_string(), Value::Object(metadata));
        }
        compact_routes.push(Value::Object(compact_route));
    }
    Ok(json!({"source_chunks": source_chunks, "routes": compact_routes}))
}

fn non_negative_integer(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64().or_else(|| {
            number
                .as_f64()
                .filter(|value| value.fract() == 0.0 && *value >= 0.0)
                .map(|value| value as u64)
        }),
        Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

pub(crate) fn lifecycle_request_id(message_type: &str, idempotency_key: Option<&str>) -> String {
    let Some(key) = idempotency_key
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return new_transfer_id();
    };
    let digest = Sha256::digest(format!("beam:{message_type}:{key}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

fn is_retryable_nats_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    [
        "timeout",
        "timed out",
        "no responders",
        "no servers",
        "connection closed",
        "disconnected",
        "connection reset",
        "connection refused",
        "socket",
        "network",
    ]
    .iter()
    .any(|token| normalized.contains(token))
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429) || status >= 500
}

pub(crate) fn is_retryable_lifecycle_error(error: &BeamApiError) -> bool {
    match error {
        BeamApiError::Lifecycle { status, .. } => is_retryable_status(*status),
        BeamApiError::HttpStatus { status } => is_retryable_status(*status),
        BeamApiError::Request(error) => error.is_timeout() || error.is_connect(),
        BeamApiError::Nats(message) => is_retryable_nats_error(message),
        _ => false,
    }
}

pub(crate) fn is_recoverable_route_stream_error(error: &BeamApiError) -> bool {
    is_retryable_lifecycle_error(error)
        || matches!(
            error,
            BeamApiError::HttpStatus { status: 404 | 409 }
                | BeamApiError::Lifecycle {
                    status: 404 | 409,
                    ..
                }
        )
}

fn jittered(base: Duration) -> Duration {
    let jitter_per_mille = 800 + (Uuid::new_v4().as_u128() % 401) as u64;
    Duration::from_millis((base.as_millis() as u64).saturating_mul(jitter_per_mille) / 1000)
        .max(Duration::from_millis(1))
}

async fn sleep_before_retry(attempt: usize) {
    let base_ms = if attempt == 0 { 150 } else { 500 };
    tokio::time::sleep(jittered(Duration::from_millis(base_ms))).await;
}

fn key_prefix(api_key: &str) -> String {
    api_key.chars().take(12).collect()
}

fn decode_jwt_claims(token: &str) -> Result<JwtClaims, BeamApiError> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| BeamApiError::Nats("invalid JWT".to_string()))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|error| BeamApiError::Nats(error.to_string()))?;
    serde_json::from_slice(&decoded).map_err(|error| BeamApiError::Nats(error.to_string()))
}

pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn iso_now() -> String {
    iso_at(std::time::SystemTime::now())
}

pub(crate) fn iso_after(duration: Duration) -> String {
    iso_at(std::time::SystemTime::now() + duration)
}

pub(crate) fn iso_at(value: std::time::SystemTime) -> String {
    let now = value
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_seconds = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let (year, month, day, hour, minute, second) = civil_time(total_seconds);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Calendar fields (UTC) for a Unix timestamp.
pub(crate) fn civil_time(total_seconds: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    (
        year,
        month,
        day,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096).div_euclid(365);
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let day = doy - (153 * mp + 2).div_euclid(5) + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

#[cfg(test)]
pub(crate) mod testing {
    //! An in-memory broker that speaks the lifecycle wire protocol, so tests observe the exact
    //! MessagePack envelopes the SDK would publish.
    use super::*;
    use tokio::sync::mpsc;

    pub(crate) type Responder =
        Arc<dyn Fn(&str, &Value) -> Result<Value, (u16, Value)> + Send + Sync>;
    /// Awaited before the responder runs, so a test can act while a request is in flight.
    pub(crate) type RequestHook = Arc<dyn Fn(&str, &Value) -> BoxFuture<'static, ()> + Send + Sync>;

    #[derive(Clone, Debug)]
    pub(crate) struct RecordedRequest {
        pub message_type: String,
        pub envelope: Value,
        pub raw: Bytes,
    }

    impl RecordedRequest {
        pub fn payload(&self) -> &Value {
            &self.envelope["payload"]
        }
    }

    pub(crate) struct FakeBroker {
        pub requests: StdMutex<Vec<RecordedRequest>>,
        pub auth_resolves: AtomicU64,
        pub published: StdMutex<Vec<(String, Bytes)>>,
        pub subscriptions: StdMutex<HashMap<String, mpsc::UnboundedSender<InboundMessage>>>,
        pub responder: StdMutex<Responder>,
        /// Transport failures returned for the next requests, in order.
        pub transport_failures: StdMutex<Vec<String>>,
        /// Transport failures for specific message types: (message type, remaining count).
        pub failing_types: StdMutex<HashMap<String, usize>>,
        pub auth_token_exp: AtomicU64,
        pub request_hook: StdMutex<Option<RequestHook>>,
    }

    impl FakeBroker {
        pub fn new(responder: Responder) -> Arc<Self> {
            Arc::new(Self {
                requests: StdMutex::new(Vec::new()),
                auth_resolves: AtomicU64::new(0),
                published: StdMutex::new(Vec::new()),
                subscriptions: StdMutex::new(HashMap::new()),
                responder: StdMutex::new(responder),
                transport_failures: StdMutex::new(Vec::new()),
                failing_types: StdMutex::new(HashMap::new()),
                auth_token_exp: AtomicU64::new(unix_now() + 3_600),
                request_hook: StdMutex::new(None),
            })
        }

        /// Fail every attempt of the next `message_type` request with "connection closed".
        pub fn fail_next(&self, message_type: &str) {
            self.failing_types
                .lock()
                .unwrap()
                .insert(message_type.to_string(), LIFECYCLE_REQUEST_MAX_ATTEMPTS);
        }

        pub fn set_responder(&self, responder: Responder) {
            *self.responder.lock().unwrap() = responder;
        }

        pub fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }

        pub fn of_type(&self, message_type: &str) -> Vec<RecordedRequest> {
            self.requests()
                .into_iter()
                .filter(|request| request.message_type == message_type)
                .collect()
        }

        pub fn message_types(&self) -> Vec<String> {
            self.requests()
                .into_iter()
                .map(|request| request.message_type)
                .filter(|message_type| message_type != "runtime.hello")
                .collect()
        }

        pub fn deliver(&self, subject: &str, payload: Bytes, reply: Option<String>) -> bool {
            let sender = self.subscriptions.lock().unwrap().get(subject).cloned();
            sender.is_some_and(|sender| sender.send(InboundMessage { payload, reply }).is_ok())
        }
    }

    pub(crate) fn unsigned_test_jwt(exp: u64) -> String {
        let encode = |value: Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
        };
        format!(
            "{}.{}.signature",
            encode(json!({"alg": "none", "typ": "JWT"})),
            encode(json!({"token_type": "beam-sdk-auth", "exp": exp}))
        )
    }

    impl ControlTransport for FakeBroker {
        fn request(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<Bytes, String>> {
            Box::pin(async move {
                if subject.contains(".auth.") {
                    self.auth_resolves.fetch_add(1, Ordering::SeqCst);
                    let exp = self.auth_token_exp.load(Ordering::SeqCst);
                    let token = unsigned_test_jwt(exp);
                    return Ok(Bytes::from(json!({"ok": true, "token": token}).to_string()));
                }
                let envelope: Value = from_slice(&payload).map_err(|error| error.to_string())?;
                let message_type = envelope["message_type"].as_str().unwrap_or("").to_string();
                self.requests.lock().unwrap().push(RecordedRequest {
                    message_type: message_type.clone(),
                    envelope: envelope.clone(),
                    raw: payload.clone(),
                });
                let failure = {
                    let mut failures = self.transport_failures.lock().unwrap();
                    (!failures.is_empty()).then(|| failures.remove(0))
                }
                .or_else(|| {
                    let mut failing = self.failing_types.lock().unwrap();
                    let remaining = failing.get_mut(&message_type)?;
                    if *remaining == 0 {
                        return None;
                    }
                    *remaining -= 1;
                    Some("connection closed".to_string())
                });
                if let Some(failure) = failure {
                    return Err(failure);
                }
                let hook = self.request_hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook(&message_type, &envelope["payload"]).await;
                }
                let responder = self.responder.lock().unwrap().clone();
                let reply = match responder(&message_type, &envelope["payload"]) {
                    Ok(payload) => json!({
                        "ok": true, "status": 200, "payload": payload,
                        "runtime_epoch": "runtime-test", "transport_epoch": "transport-test",
                    }),
                    Err((status, error)) => json!({
                        "ok": false, "status": status, "error": error,
                        "runtime_epoch": "runtime-test", "transport_epoch": "transport-test",
                    }),
                };
                Ok(Bytes::from(
                    to_vec_named(&reply).map_err(|error| error.to_string())?,
                ))
            })
        }

        fn subscribe(&self, subject: String) -> BoxFuture<'_, Result<MessageStream, String>> {
            Box::pin(async move {
                let (sender, mut receiver) = mpsc::unbounded_channel();
                self.subscriptions.lock().unwrap().insert(subject, sender);
                let stream =
                    futures_util::stream::poll_fn(move |context| receiver.poll_recv(context));
                Ok(Box::pin(stream) as MessageStream)
            })
        }

        fn publish(&self, subject: String, payload: Bytes) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async move {
                self.published.lock().unwrap().push((subject, payload));
                Ok(())
            })
        }

        fn close(&self) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }

    pub(crate) fn fake_control(broker: Arc<FakeBroker>, max_payload_bytes: usize) -> NatsControl {
        NatsControl::with_transport(
            "b1m_test".to_string(),
            "prod".to_string(),
            None,
            1,
            Duration::from_secs(5),
            max_payload_bytes,
            broker,
        )
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::testing::*;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn control() -> NatsControl {
        NatsControl::new(
            "b1m_recovery".to_string(),
            "nats://127.0.0.1:4222".to_string(),
            "dev".to_string(),
            None,
            1,
            Duration::from_secs(1),
            1024 * 1024,
        )
    }

    #[test]
    fn runtime_state_loss_keeps_route_recovery_lease() {
        assert!(is_recoverable_route_stream_error(
            &BeamApiError::HttpStatus { status: 404 }
        ));
        assert!(is_recoverable_route_stream_error(
            &BeamApiError::HttpStatus { status: 409 }
        ));
        assert!(!is_recoverable_route_stream_error(
            &BeamApiError::HttpStatus { status: 400 }
        ));
        let lifecycle = |status| BeamApiError::Lifecycle {
            status,
            code: None,
            message: None,
            body: "{}".to_string(),
        };
        assert!(is_recoverable_route_stream_error(&lifecycle(404)));
        assert!(is_recoverable_route_stream_error(&lifecycle(409)));
        assert!(is_recoverable_route_stream_error(&lifecycle(503)));
        assert!(!is_recoverable_route_stream_error(&lifecycle(400)));
    }

    #[test]
    fn tls_urls_start_the_tls_handshake_before_info() {
        let tls = format!(
            "{:?}",
            connect_options("tls://orch-gateway.b1m.ai:4222", "k", "k")
        );
        assert!(tls.contains("\"tls_first\": true"), "{tls}");
        assert!(tls.contains("\"tls_required\": true"), "{tls}");
        let upper = format!("{:?}", connect_options("TLS://gateway:4222", "k", "k"));
        assert!(upper.contains("\"tls_first\": true"));
        let plain = format!("{:?}", connect_options("nats://127.0.0.1:4222", "k", "k"));
        assert!(plain.contains("\"tls_first\": false"), "{plain}");
    }

    /// Live check against the production gateway (TLS handshake-first). Run with
    /// `cargo test -- --ignored tls_first_reaches_the_production_gateway`. An invalid key must
    /// fail at authorization, which proves TLS and INFO exchange succeeded.
    #[tokio::test]
    #[ignore = "requires network access to orch-gateway.b1m.ai"]
    async fn tls_first_reaches_the_production_gateway() {
        let url = "tls://orch-gateway.b1m.ai:4222";
        let error = connect_options(url, "b1m_invalid_key_for_tls_probe", "b1m_invalid_")
            .max_reconnects(Some(1))
            .connect(url)
            .await
            .expect_err("an invalid key must not authenticate");
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("authorization") || message.contains("auth"),
            "expected an authorization failure after a successful TLS handshake, got: {error}"
        );
    }

    #[test]
    fn shard_hash_matches_typescript_utf16_hashing() {
        assert_eq!(transfer_shard_id("abc", u64::from(u32::MAX)), 440_920_331);
        assert_eq!(transfer_shard_id("anything", 1), 0);
    }

    #[test]
    fn resume_replays_on_either_route_replay_signal() {
        assert!(resume_requires_route_replay(
            &json!({"recovery": "route_replay_required"})
        ));
        assert!(resume_requires_route_replay(
            &json!({"recovery": "reconciling", "route_replay_required": true})
        ));
        assert!(!resume_requires_route_replay(
            &json!({"recovery": "reconciling"})
        ));
    }

    #[tokio::test]
    async fn runtime_epoch_recovery_coalesces_and_clears_secrets() {
        let control = control();
        let transfer_id = "11111111-1111-4111-8111-111111111111".to_string();
        let disposed = Arc::new(AtomicUsize::new(0));
        let disposed_for_lease = disposed.clone();
        let lease = Arc::new(RecoveryLease {
            transfer_id: transfer_id.clone(),
            plan_fingerprint: "a".repeat(64),
            coordinate_checksum: format!("sha256-xor-v1:1:{}", "0".repeat(64)),
            replay_routes: Arc::new(|_| Box::pin(async { Ok(()) })),
            dispose: Some(Arc::new(move || {
                disposed_for_lease.fetch_add(1, AtomicOrdering::SeqCst);
            })),
        });
        {
            let mut state = control.inner.recovery.lock().await;
            state.running.insert(lease_key(&lease));
            state.leases.insert(transfer_id.clone(), lease);
            state
                .runtime_epochs
                .insert(0, ("runtime-a".to_string(), "transport-a".to_string()));
        }
        control
            .observe_runtime_epochs(
                0,
                &ReplyEnvelope {
                    ok: true,
                    status: 200,
                    payload: None,
                    error: None,
                    runtime_epoch: Some("runtime-b".to_string()),
                    transport_epoch: Some("transport-b".to_string()),
                },
            )
            .await;
        let requested = control
            .inner
            .recovery
            .lock()
            .await
            .requested_epochs
            .get(&transfer_id)
            .cloned();
        assert_eq!(
            requested,
            Some(("runtime-b".to_string(), "transport-b".to_string()))
        );
        assert!(SDK_MAX_RECONNECTS.is_none());
        control.release_recovery_lease(&transfer_id, None).await;
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn foreground_cancellation_requests_background_recovery_without_releasing_secrets() {
        let control = control();
        let transfer_id = "22222222-2222-4222-8222-222222222222".to_string();
        let disposed = Arc::new(AtomicUsize::new(0));
        let disposed_for_lease = disposed.clone();
        let lease = Arc::new(RecoveryLease {
            transfer_id: transfer_id.clone(),
            plan_fingerprint: "a".repeat(64),
            coordinate_checksum: format!("sha256-xor-v1:1:{}", "0".repeat(64)),
            replay_routes: Arc::new(|_| Box::pin(async { Ok(()) })),
            dispose: Some(Arc::new(move || {
                disposed_for_lease.fetch_add(1, AtomicOrdering::SeqCst);
            })),
        });
        {
            let mut state = control.inner.recovery.lock().await;
            state.running.insert(lease_key(&lease));
            state.leases.insert(transfer_id.clone(), lease);
        }
        control.continue_recovery_lease(&transfer_id, None).await;
        let state = control.inner.recovery.lock().await;
        let requested = state.requested_epochs.get(&transfer_id).cloned();
        assert_eq!(
            requested.as_ref().map(|epoch| epoch.0.as_str()),
            Some("foreground")
        );
        assert!(state.leases.contains_key(&transfer_id));
        drop(state);
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);
        control.release_recovery_lease(&transfer_id, None).await;
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn route_replay_required_recovery_replays_routes_with_fresh_generation() {
        let broker = FakeBroker::new(Arc::new(|message_type, _| {
            Ok(match message_type {
                "transfer.resume" => json!({"recovery": "route_replay_required"}),
                _ => json!({}),
            })
        }));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let replays = Arc::new(StdMutex::new(Vec::<String>::new()));
        let replays_for_lease = replays.clone();
        let transfer_id = "33333333-3333-4333-8333-333333333333";
        control
            .register_recovery_lease(RecoveryLease {
                transfer_id: transfer_id.to_string(),
                plan_fingerprint: "a".repeat(64),
                coordinate_checksum: "checksum".to_string(),
                replay_routes: Arc::new(move |generation| {
                    replays_for_lease.lock().unwrap().push(generation);
                    Box::pin(async { Ok(()) })
                }),
                dispose: None,
            })
            .await;
        control.continue_recovery_lease(transfer_id, None).await;
        for _ in 0..200 {
            if !replays.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let replays = replays.lock().unwrap().clone();
        assert_eq!(replays.len(), 1);
        let resume = broker.of_type("transfer.resume");
        assert_eq!(
            resume[0].payload()["route_generation_id"],
            json!(replays[0])
        );
        let hello = broker.of_type("runtime.hello");
        assert!(!hello.is_empty(), "registering a lease polls runtime.hello");
        assert_eq!(
            hello[0].envelope["request_id"],
            json!(lifecycle_request_id(
                "runtime.hello",
                Some("runtime:hello:0")
            ))
        );
        control.close().await.unwrap();
    }

    fn recording_lease(
        transfer_id: &str,
        owner: &str,
        events: &Arc<StdMutex<Vec<String>>>,
    ) -> Arc<RecoveryLease> {
        let (replay_events, dispose_events) = (events.clone(), events.clone());
        let (replay_owner, dispose_owner) = (owner.to_string(), owner.to_string());
        Arc::new(RecoveryLease {
            transfer_id: transfer_id.to_string(),
            plan_fingerprint: owner.to_string(),
            coordinate_checksum: format!("sha256-xor-v1:1:{}", "0".repeat(64)),
            replay_routes: Arc::new(move |_| {
                replay_events
                    .lock()
                    .unwrap()
                    .push(format!("replay:{replay_owner}"));
                Box::pin(async { Ok(()) })
            }),
            dispose: Some(Arc::new(move || {
                dispose_events
                    .lock()
                    .unwrap()
                    .push(format!("dispose:{dispose_owner}"));
            })),
        })
    }

    async fn wait_for(mut predicate: impl FnMut() -> BoxFuture<'static, bool>) {
        for _ in 0..400 {
            if predicate().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for condition");
    }

    fn no_recovery_running(control: &NatsControl) -> impl FnMut() -> BoxFuture<'static, bool> {
        let control = control.clone();
        move || {
            let control = control.clone();
            Box::pin(async move { control.inner.recovery.lock().await.running.is_empty() })
        }
    }

    fn has_event(
        events: &Arc<StdMutex<Vec<String>>>,
        event: &str,
    ) -> impl FnMut() -> BoxFuture<'static, bool> {
        let (events, event) = (events.clone(), event.to_string());
        move || {
            let found = events.lock().unwrap().contains(&event);
            Box::pin(async move { found })
        }
    }

    fn resume_hook(
        events: &Arc<StdMutex<Vec<String>>>,
        on_resume: impl Fn(String) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> RequestHook {
        let events = events.clone();
        let on_resume = Arc::new(on_resume);
        Arc::new(move |message_type, payload| {
            if message_type != "transfer.resume" {
                return Box::pin(async {});
            }
            let owner = payload["plan_fingerprint"]
                .as_str()
                .unwrap_or("")
                .to_string();
            events.lock().unwrap().push(format!("resume:{owner}"));
            on_resume(owner)
        })
    }

    async fn fenced_owner_in_flight_recovery_leaves_the_replacement_alone(
        fenced_outcome: Result<Value, (u16, Value)>,
    ) {
        let broker = FakeBroker::new(Arc::new(move |message_type, payload| {
            if message_type != "transfer.resume" {
                return Ok(json!({}));
            }
            if payload["plan_fingerprint"] == "replacement" {
                return Ok(json!({"recovery": "reconciling"}));
            }
            fenced_outcome.clone()
        }));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let transfer_id = "99999999-9999-4999-8999-999999999999";
        let events = Arc::new(StdMutex::new(Vec::new()));
        let fenced = recording_lease(transfer_id, "fenced", &events);
        let replacement = recording_lease(transfer_id, "replacement", &events);
        // The replacement owner takes over while the fenced owner awaits transfer.resume.
        let (hook_control, hook_fenced, hook_replacement) =
            (control.clone(), fenced.clone(), replacement.clone());
        *broker.request_hook.lock().unwrap() = Some(resume_hook(&events, move |_| {
            let (control, fenced, replacement) = (
                hook_control.clone(),
                hook_fenced.clone(),
                hook_replacement.clone(),
            );
            Box::pin(async move {
                let current = control.recovery_lease(&fenced.transfer_id).await;
                if current.is_some_and(|current| Arc::ptr_eq(&current, &fenced)) {
                    control.register_recovery_lease(replacement).await;
                }
            })
        }));
        control.register_recovery_lease(fenced.clone()).await;
        control
            .inner
            .recovery
            .lock()
            .await
            .running
            .insert(lease_key(&fenced));
        control.recover_transfer(fenced.clone()).await;
        wait_for(no_recovery_running(&control)).await;
        let events_now = events.lock().unwrap().clone();
        assert!(
            !events_now.contains(&"replay:fenced".to_string()),
            "a fenced-off owner must not replay routes: {events_now:?}"
        );
        assert!(
            !events_now.contains(&"dispose:replacement".to_string()),
            "a fenced-off owner must not release its replacement: {events_now:?}"
        );
        assert_eq!(events_now[..2], ["resume:fenced", "dispose:fenced"]);
        assert!(
            events_now.contains(&"resume:replacement".to_string()),
            "the fenced loop hands its recovery to the replacement: {events_now:?}"
        );
        let current = control.recovery_lease(transfer_id).await.unwrap();
        assert!(Arc::ptr_eq(&current, &replacement));
        control
            .release_recovery_lease(transfer_id, Some(&fenced))
            .await;
        let current = control.recovery_lease(transfer_id).await.unwrap();
        assert!(
            Arc::ptr_eq(&current, &replacement),
            "release is keyed by lease identity"
        );
        control
            .continue_recovery_lease(transfer_id, Some(&fenced))
            .await;
        assert!(control.inner.recovery.lock().await.running.is_empty());
        *broker.request_hook.lock().unwrap() = None;
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn fenced_owner_replay_required_recovery_neither_replays_into_nor_releases_a_replacement()
    {
        fenced_owner_in_flight_recovery_leaves_the_replacement_alone(Ok(json!({
            "recovery": "route_replay_required", "route_replay_required": true
        })))
        .await;
    }

    #[tokio::test]
    async fn fenced_owner_non_retryable_recovery_neither_replays_into_nor_releases_a_replacement() {
        fenced_owner_in_flight_recovery_leaves_the_replacement_alone(Err((
            400,
            json!({"code": "bad_request", "message": "bad request"}),
        )))
        .await;
    }

    #[tokio::test]
    async fn a_replacement_lease_recovers_while_the_fenced_owner_loop_is_in_flight() {
        let broker = FakeBroker::new(Arc::new(|_, _| {
            Ok(json!({"recovery": "route_replay_required", "route_replay_required": true}))
        }));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let transfer_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let events = Arc::new(StdMutex::new(Vec::new()));
        let fenced = recording_lease(transfer_id, "fenced", &events);
        let replacement = recording_lease(transfer_id, "replacement", &events);
        let (release_fenced, fenced_gate) = tokio::sync::watch::channel(false);
        *broker.request_hook.lock().unwrap() = Some(resume_hook(&events, move |owner| {
            let mut gate = fenced_gate.clone();
            Box::pin(async move {
                if owner == "fenced" {
                    let _ = gate.wait_for(|open| *open).await;
                }
            })
        }));
        control.register_recovery_lease(fenced.clone()).await;
        control.continue_recovery_lease(transfer_id, None).await;
        wait_for(has_event(&events, "resume:fenced")).await;
        control.register_recovery_lease(replacement.clone()).await;
        control
            .continue_recovery_lease(transfer_id, Some(&replacement))
            .await;
        control
            .continue_recovery_lease(transfer_id, Some(&fenced))
            .await;
        wait_for(has_event(&events, "replay:replacement")).await;
        release_fenced.send(true).unwrap();
        wait_for(no_recovery_running(&control)).await;
        let events_now = events.lock().unwrap().clone();
        assert!(
            !events_now.contains(&"replay:fenced".to_string()),
            "{events_now:?}"
        );
        assert_eq!(
            events_now
                .iter()
                .filter(|event| *event == "resume:fenced")
                .count(),
            1,
            "{events_now:?}"
        );
        let current = control.recovery_lease(transfer_id).await.unwrap();
        assert!(Arc::ptr_eq(&current, &replacement));
        *broker.request_hook.lock().unwrap() = None;
        control.close().await.unwrap();
    }

    /// async-nats 0.38 reports a rejected CONNECT as `ConnectErrorKind::AuthorizationViolation`
    /// with the server error as its source; the transport forwards `to_string()`. Retrying an
    /// authorization failure cannot succeed, so it must not be classified as retryable.
    #[test]
    fn nats_authorization_violations_are_not_retryable() {
        let kind_only =
            async_nats::ConnectError::from(async_nats::ConnectErrorKind::AuthorizationViolation);
        assert_eq!(kind_only.to_string(), "authorization violation");
        // What `connect()` produces: the kind plus the server's `-ERR` (ServerError) source.
        let with_server_source = "authorization violation: nats: authorization violation";
        for message in [kind_only.to_string(), with_server_source.to_string()] {
            let error = BeamApiError::Nats(message.clone());
            assert!(!is_retryable_lifecycle_error(&error), "{message}");
            assert!(!is_recoverable_route_stream_error(&error), "{message}");
        }
        // Transport failures stay retryable.
        assert!(is_retryable_lifecycle_error(&BeamApiError::Nats(
            "IO error: connection refused".to_string()
        )));
    }
}

#[cfg(test)]
mod transport_tests {
    use super::testing::*;
    use super::*;

    #[tokio::test]
    async fn lifecycle_transport_retries_reuse_the_identical_request_envelope() {
        let broker = FakeBroker::new(Arc::new(|_, _| Ok(json!({"success": true}))));
        broker
            .transport_failures
            .lock()
            .unwrap()
            .push("connection closed".to_string());
        let control = fake_control(broker.clone(), 1024 * 1024);
        control
            .request_value(
                "transfer.prepare",
                json!({"transfer_id": "transfer-one", "route_generation_id": "generation-one"}),
                Some("transfer-one"),
                Some("resume-generation-one"),
            )
            .await
            .unwrap();
        let requests = broker.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].raw, requests[1].raw);
    }

    #[tokio::test]
    async fn expired_lifecycle_auth_retries_with_fresh_token_and_stable_request_identity() {
        let calls = Arc::new(AtomicU64::new(0));
        let calls_for_responder = calls.clone();
        let broker = FakeBroker::new(Arc::new(move |_, _| {
            if calls_for_responder.fetch_add(1, Ordering::SeqCst) == 0 {
                Err((
                    401,
                    json!({"code": "auth_token_expired", "message": "auth token expired"}),
                ))
            } else {
                Ok(json!({"success": true}))
            }
        }));
        let control = fake_control(broker.clone(), 1024 * 1024);
        {
            let mut auth = control.inner.auth_token.lock().await;
            auth.token = Some("stale-token".to_string());
            auth.expires_at = unix_now() + 3_600;
        }
        control
            .request_value(
                "transfer.route_stream.batch",
                json!({"transfer_id": "transfer-one", "batch_index": 1}),
                Some("transfer-one"),
                Some("route-batch-one"),
            )
            .await
            .unwrap();
        let requests = broker.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(broker.auth_resolves.load(Ordering::SeqCst), 1);
        assert_eq!(
            requests[0].envelope["request_id"],
            requests[1].envelope["request_id"]
        );
        assert_eq!(
            requests[0].envelope["message_id"],
            requests[1].envelope["message_id"]
        );
        assert_eq!(requests[0].payload(), requests[1].payload());
        assert_eq!(requests[0].envelope["auth_token"], "stale-token");
        assert_ne!(requests[1].envelope["auth_token"], "stale-token");
    }

    #[tokio::test]
    async fn tokens_refresh_thirty_seconds_before_expiry() {
        let broker = FakeBroker::new(Arc::new(|_, _| Ok(json!({}))));
        let control = fake_control(broker.clone(), 1024 * 1024);
        {
            let mut auth = control.inner.auth_token.lock().await;
            auth.token = Some("nearly-expired".to_string());
            auth.expires_at = unix_now() + 20;
        }
        control
            .request_value("transfer.status", json!({}), None, None)
            .await
            .unwrap();
        assert_eq!(broker.auth_resolves.load(Ordering::SeqCst), 1);
        assert_ne!(
            broker.requests()[0].envelope["auth_token"],
            "nearly-expired"
        );
    }

    #[tokio::test]
    async fn lifecycle_errors_expose_the_reply_error_body() {
        let broker = FakeBroker::new(Arc::new(|_, _| {
            Err((
                422,
                json!({"code": "plan_invalid", "message": "plan rejected"}),
            ))
        }));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let error = control
            .request_value("transfer.prepare", json!({}), None, None)
            .await
            .unwrap_err();
        match error {
            BeamApiError::Lifecycle {
                status,
                code,
                message,
                body,
            } => {
                assert_eq!(status, 422);
                assert_eq!(code.as_deref(), Some("plan_invalid"));
                assert_eq!(message.as_deref(), Some("plan rejected"));
                assert!(body.contains("plan_invalid"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(broker.requests().len(), 1, "4xx replies are not retried");
    }

    fn route(index: u64, bytes: usize) -> Value {
        json!({
            "source_id": "src_0",
            "destination_id": "dest_0",
            "chunk_index": index,
            "source_url": format!("https://storage.example/{}", "x".repeat(bytes)),
            "dest_url": "https://storage.example/destination",
            "source_offset": index * 1024,
            "chunk_size": 1024,
        })
    }

    fn splitter(max_payload_bytes: usize) -> NatsControl {
        fake_control(
            FakeBroker::new(Arc::new(|_, _| Ok(json!({})))),
            max_payload_bytes,
        )
    }

    fn batch_sizes(control: &NatsControl, routes: &[Value]) -> Result<Vec<usize>, BeamApiError> {
        control
            .split_routes_for_payload("transfer.route_stream.batch", &json!({}), routes)
            .map(|batches| batches.iter().map(Vec::len).collect())
    }

    #[test]
    fn route_splitting_targets_four_mebibytes_while_honoring_guards() {
        let defaults = splitter(crate::DEFAULT_MAX_PAYLOAD_BYTES);
        let overridden = splitter(10 * 1024 * 1024);
        let routes = [route(0, 4_500_000), route(1, 4_500_000)];
        assert_eq!(batch_sizes(&defaults, &routes).unwrap(), vec![1, 1]);
        assert_eq!(batch_sizes(&overridden, &routes).unwrap(), vec![1, 1]);
        let near_limit = [route(3, 4_190_000), route(4, 4_190_000)];
        assert_eq!(batch_sizes(&defaults, &near_limit).unwrap(), vec![1, 1]);
        let error = batch_sizes(&defaults, &[route(2, 8 * 1024 * 1024)]).unwrap_err();
        assert!(error
            .to_string()
            .contains("above max_payload_bytes=8388608"));
        assert_eq!(
            batch_sizes(&defaults, &[route(5, 6 * 1024 * 1024)]).unwrap(),
            vec![1]
        );
        let lower = splitter(3 * 1024 * 1024);
        let error = batch_sizes(&lower, &[route(6, 3 * 1024 * 1024)]).unwrap_err();
        assert!(error
            .to_string()
            .contains("above max_payload_bytes=3145728"));
        let small = (0..10).map(|index| route(index, 16)).collect::<Vec<_>>();
        assert_eq!(batch_sizes(&defaults, &small).unwrap(), vec![10]);
    }

    #[tokio::test]
    async fn terminal_wait_after_close_returns_none() {
        let broker = FakeBroker::new(Arc::new(|_, _| Ok(json!({}))));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let waiter = control
            .open_terminal_signal_waiter("transfer-1")
            .await
            .unwrap();
        waiter.close().await.unwrap();
        assert!(waiter
            .wait(Duration::from_millis(10))
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            waiter.wait(Duration::ZERO).await,
            Err(BeamApiError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn terminal_signal_accepts_additional_event_fields() {
        let broker = FakeBroker::new(Arc::new(|_, _| Ok(json!({}))));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let waiter = control
            .open_terminal_signal_waiter("transfer-1")
            .await
            .unwrap();
        let event = json!({
            "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            "producer": "transfer-runtime",
            "transfer_id": "transfer-1",
            "status": "completed",
            "occurred_at": "2026-06-22T22:41:20.000Z",
            "reason": "all destinations verified",
        });
        assert!(broker.deliver(
            &control.terminal_subject("transfer-1"),
            Bytes::from(to_vec_named(&event).unwrap()),
            None
        ));
        let received = waiter.wait(Duration::from_secs(1)).await.unwrap().unwrap();
        assert_eq!(received.status, "completed");
    }

    #[tokio::test]
    async fn route_recovery_signer_answers_valid_requests_and_rejects_mismatches() {
        let broker = FakeBroker::new(Arc::new(|_, _| Ok(json!({}))));
        let control = fake_control(broker.clone(), 1024 * 1024);
        let handler: RouteRecoverySignHandler = Arc::new(|payload| {
            Box::pin(async move {
                Ok(RouteRecoverySignReplyPayload {
                    transfer_id: payload.transfer_id,
                    route_generation_id: payload.route_generation_id,
                    signed_at: None,
                    chunk_routes: Vec::new(),
                })
            })
        });
        let handle = control
            .serve_route_recovery_signer("transfer-1", handler)
            .await
            .unwrap();
        let subject = control.route_recovery_sign_subject("transfer-1");
        let request = |transfer_id: &str| {
            json!({
                "message_id": "message-1", "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
                "environment": "prod", "key_prefix": "b1m_test", "transfer_id": transfer_id,
                "message_type": ROUTE_RECOVERY_SIGN_MESSAGE_TYPE, "request_id": "request-1",
                "occurred_at": "2026-06-22T22:41:20.000Z", "producer": "transfer-runtime",
                "payload": {"transfer_id": transfer_id, "route_generation_id": "gen-1", "chunks": [{
                    "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 0,
                    "delivery_index": 0, "source_offset": 0, "chunk_size": 1,
                    "logical_attempt_index": 0, "attempt_slot": 0, "part_number": 1,
                    "route_generation_id": "gen-1", "multipart_group_id": "g",
                    "final_object_key": "k", "upload_id": "u"
                }]}
            })
        };
        // A message without a reply subject is not a request and is ignored.
        broker.deliver(&subject, Bytes::from_static(b"\x80"), None);
        broker.deliver(
            &subject,
            Bytes::from(to_vec_named(&request("transfer-1")).unwrap()),
            Some("reply.ok".to_string()),
        );
        broker.deliver(
            &subject,
            Bytes::from(to_vec_named(&request("transfer-2")).unwrap()),
            Some("reply.bad".to_string()),
        );
        for _ in 0..200 {
            if broker.published.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let published = broker.published.lock().unwrap().clone();
        assert_eq!(published.len(), 2);
        for (subject, payload) in published {
            let reply: Value = from_slice(&payload).unwrap();
            assert_eq!(reply["producer"], "sdk");
            assert_eq!(reply["message_id"], "message-1");
            if subject == "reply.ok" {
                assert_eq!(reply["ok"], true);
                assert_eq!(reply["payload"]["route_generation_id"], "gen-1");
            } else {
                assert_eq!(reply["ok"], false);
                assert_eq!(reply["error"]["code"], "route_recovery_sign_failed");
            }
        }
        handle.stop();
        handle.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::compact_signed_route_values;
    use serde_json::json;

    #[test]
    fn compact_routes_reference_multipart_group_manifest() {
        let routes = vec![
            json!({
                "source_id": "src", "destination_id": "dst", "chunk_index": 0,
                "source_url": "https://source.example/file", "dest_url": "https://dest.example/part-1",
                "source_offset": 0, "chunk_size": 512,
                "metadata": {
                    "multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin",
                    "bucket": "dest-bucket", "part_number": 1, "delivery_index": 0
                }
            }),
            json!({
                "source_id": "src", "destination_id": "dst", "chunk_index": 1, "delivery_index": 1,
                "source_url": "https://source.example/file", "dest_url": "https://dest.example/part-2",
                "source_offset": 512, "chunk_size": 512,
                "metadata": {
                    "multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin",
                    "bucket": "dest-bucket", "part_number": 4, "delivery_index": 1
                }
            }),
        ];
        let compact = compact_signed_route_values(&routes).expect("route compaction");
        assert!(compact.get("multipart_groups").is_none());
        assert_eq!(compact["routes"][0]["multipart_group_id"], "group");
        assert_eq!(compact["routes"][0]["metadata"], json!({"part_number": 1}));
        assert!(compact["routes"][0]["metadata"].get("bucket").is_none());
    }

    #[test]
    fn compact_routes_keep_metadata_for_non_multipart_routes() {
        let routes = vec![json!({
            "source_id": "src", "destination_id": "dst", "chunk_index": 0, "delivery_index": 0,
            "source_url": "https://source.example/file", "dest_url": "https://dest.example/chunk",
            "source_offset": 0, "chunk_size": 512,
            "metadata": {"part_number": 1, "bucket": "dest-bucket", "delivery_index": 0}
        })];
        let compact = compact_signed_route_values(&routes).expect("route compaction");
        assert!(compact["routes"][0].get("multipart_group_id").is_none());
        assert_eq!(
            compact["routes"][0]["metadata"],
            json!({"part_number": 1, "bucket": "dest-bucket"})
        );
    }
}
