use crate::{client::BeamApiError, TransferTerminalEvent};
use base64::Engine;
use futures_util::StreamExt;
use rmp_serde::{from_slice, to_vec_named};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

pub(crate) const TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION: &str = "transfer-client-control/v6";
const SDK_MAX_RECONNECTS: Option<usize> = None;

pub(crate) type RecoveryFuture = Pin<Box<dyn Future<Output = Result<(), BeamApiError>> + Send>>;

pub(crate) struct RecoveryLease {
    pub transfer_id: String,
    pub plan_fingerprint: String,
    pub coordinate_checksum: String,
    pub replay_routes: Arc<dyn Fn(String) -> RecoveryFuture + Send + Sync>,
    pub dispose: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Clone)]
pub(crate) struct NatsControl {
    inner: Arc<NatsControlInner>,
}

struct NatsControlInner {
    api_key: String,
    key_prefix: String,
    nats_url: String,
    environment: String,
    subject_prefix: String,
    shard_count: u64,
    request_timeout: Duration,
    max_payload_bytes: usize,
    closed: AtomicBool,
    client: Mutex<Option<async_nats::Client>>,
    auth_token: Mutex<AuthTokenState>,
    terminal_waiters: Mutex<Vec<Weak<TerminalSignalWaiterInner>>>,
    recovery: Mutex<RecoveryState>,
}

#[derive(Default)]
struct RecoveryState {
    leases: HashMap<String, Arc<RecoveryLease>>,
    runtime_epochs: HashMap<u64, (String, String)>,
    requested_epochs: HashMap<String, (String, String)>,
    running: HashSet<String>,
    hello_monitors: HashSet<u64>,
}

#[derive(Clone)]
pub(crate) struct NatsTerminalSignalWaiter {
    inner: Arc<TerminalSignalWaiterInner>,
}

struct TerminalSignalWaiterInner {
    transfer_id: String,
    subscriber: Mutex<Option<async_nats::Subscriber>>,
    closed: AtomicBool,
    closed_notify: Notify,
}

#[derive(Default)]
struct AuthTokenState {
    token: Option<String>,
    expires_at: u64,
}

#[derive(Debug, Serialize)]
struct RequestEnvelope<'a> {
    message_id: String,
    schema_version: &'static str,
    environment: &'a str,
    key_prefix: &'a str,
    shard_id: u64,
    message_type: &'a str,
    request_id: String,
    auth_token: String,
    occurred_at: String,
    producer: &'static str,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct ReplyEnvelope {
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

impl NatsControl {
    pub(crate) fn new(
        api_key: String,
        nats_url: String,
        environment: String,
        shard_count: u64,
        request_timeout: Duration,
        max_payload_bytes: usize,
    ) -> Self {
        let key_prefix = key_prefix(&api_key);
        Self {
            inner: Arc::new(NatsControlInner {
                api_key,
                key_prefix,
                nats_url: nats_url.trim_end_matches('/').to_string(),
                environment,
                subject_prefix: "beam.transfer.client".to_string(),
                shard_count: shard_count.max(1),
                request_timeout,
                max_payload_bytes,
                closed: AtomicBool::new(false),
                client: Mutex::new(None),
                auth_token: Mutex::new(AuthTokenState::default()),
                terminal_waiters: Mutex::new(Vec::new()),
                recovery: Mutex::new(RecoveryState::default()),
            }),
        }
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
        self.request_value_on_shard(
            message_type,
            payload,
            transfer_id,
            idempotency_key,
            shard_id,
        )
        .await
    }

    async fn request_value_on_shard(
        &self,
        message_type: &str,
        payload: Value,
        _transfer_id: Option<&str>,
        idempotency_key: Option<&str>,
        shard_id: u64,
    ) -> Result<Value, BeamApiError> {
        let auth_token = self.auth_token().await?;
        let request_id = lifecycle_request_id(message_type, idempotency_key);
        let envelope = RequestEnvelope {
            message_id: format!(
                "{}:{}:{}:{}:{}",
                TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
                self.inner.environment,
                self.inner.key_prefix,
                message_type,
                request_id
            ),
            schema_version: TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
            environment: &self.inner.environment,
            key_prefix: &self.inner.key_prefix,
            shard_id,
            message_type,
            request_id,
            auth_token,
            occurred_at: iso_now(),
            producer: "sdk",
            payload,
        };
        let data =
            to_vec_named(&envelope).map_err(|error| BeamApiError::Nats(error.to_string()))?;
        if data.len() > self.inner.max_payload_bytes {
            return Err(BeamApiError::Nats(format!(
                "NATS lifecycle request is {} bytes, above max_payload_bytes={}",
                data.len(),
                self.inner.max_payload_bytes
            )));
        }
        let subject = self.request_subject(message_type, shard_id);
        let mut last_error = BeamApiError::Nats("lifecycle request failed".to_string());
        for attempt in 0..3 {
            let client = match self.connection().await {
                Ok(client) => client,
                Err(error) => {
                    let message = error.to_string();
                    if !is_retryable_nats_error(&message) {
                        return Err(error);
                    }
                    last_error = error;
                    if attempt < 2 {
                        sleep_before_retry(attempt).await;
                    }
                    continue;
                }
            };
            match tokio::time::timeout(
                self.inner.request_timeout,
                client.request(subject.clone(), data.clone().into()),
            )
            .await
            {
                Ok(Ok(response)) => match from_slice::<ReplyEnvelope>(&response.payload) {
                    Ok(decoded) if decoded.ok => {
                        self.observe_runtime_epochs(shard_id, &decoded).await;
                        return Ok(decoded.payload.unwrap_or_else(|| json!({})));
                    }
                    Ok(decoded) => {
                        self.observe_runtime_epochs(shard_id, &decoded).await;
                        let status = decoded.status;
                        last_error = BeamApiError::HttpStatus { status };
                        if !matches!(status, 408 | 425 | 429) && status < 500 {
                            return Err(last_error);
                        }
                    }
                    Err(error) => return Err(BeamApiError::Nats(error.to_string())),
                },
                Ok(Err(error)) => {
                    let message = error.to_string();
                    if !is_retryable_nats_error(&message) {
                        return Err(BeamApiError::Nats(message));
                    }
                    last_error = BeamApiError::Nats(message);
                }
                Err(_) => {
                    last_error = BeamApiError::Nats(format!("NATS request timed out: {}", subject))
                }
            }
            if attempt < 2 {
                sleep_before_retry(attempt).await;
            }
        }
        Err(last_error)
    }

    pub(crate) async fn open_terminal_signal_waiter(
        &self,
        transfer_id: &str,
    ) -> Result<NatsTerminalSignalWaiter, BeamApiError> {
        let client = self.connection().await?;
        let subscriber = client
            .subscribe(self.terminal_subject(transfer_id))
            .await
            .map_err(|error| BeamApiError::Nats(error.to_string()))?;
        client
            .flush()
            .await
            .map_err(|error| BeamApiError::Nats(error.to_string()))?;
        let waiter = NatsTerminalSignalWaiter {
            inner: Arc::new(TerminalSignalWaiterInner {
                transfer_id: transfer_id.to_string(),
                subscriber: Mutex::new(Some(subscriber)),
                closed: AtomicBool::new(false),
                closed_notify: Notify::new(),
            }),
        };
        if self.inner.closed.load(Ordering::Acquire) {
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
        if self.inner.closed.load(Ordering::Acquire) {
            waiter.close().await?;
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        Ok(waiter)
    }

    pub(crate) async fn register_recovery_lease(&self, lease: RecoveryLease) {
        let lease = Arc::new(lease);
        let shard_id = transfer_shard_id(&lease.transfer_id, self.inner.shard_count);
        let mut state = self.inner.recovery.lock().await;
        if let Some(previous) = state.leases.insert(lease.transfer_id.clone(), lease) {
            if let Some(dispose) = &previous.dispose {
                dispose();
            }
        }
        let start_monitor = state.hello_monitors.insert(shard_id);
        drop(state);
        if start_monitor {
            let control = self.clone();
            tokio::spawn(async move { control.monitor_runtime(shard_id).await });
        }
    }

    pub(crate) async fn release_recovery_lease(&self, transfer_id: &str) {
        let lease = {
            let mut state = self.inner.recovery.lock().await;
            state.requested_epochs.remove(transfer_id);
            state.leases.remove(transfer_id)
        };
        if let Some(dispose) = lease.and_then(|lease| lease.dispose.clone()) {
            dispose();
        }
    }

    pub(crate) async fn continue_recovery_lease(&self, transfer_id: &str) {
        let lease = {
            let mut state = self.inner.recovery.lock().await;
            let Some(lease) = state.leases.get(transfer_id).cloned() else {
                return;
            };
            let shard_id = transfer_shard_id(transfer_id, self.inner.shard_count);
            let requested_epoch = state
                .runtime_epochs
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| ("foreground".to_string(), new_transfer_id()));
            state
                .requested_epochs
                .insert(transfer_id.to_string(), requested_epoch);
            state
                .running
                .insert(transfer_id.to_string())
                .then_some(lease)
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
        let current = (runtime_epoch.clone(), transport_epoch.clone());
        let mut state = self.inner.recovery.lock().await;
        let previous = state.runtime_epochs.insert(shard_id, current.clone());
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
                if state.running.insert(lease.transfer_id.clone()) {
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

    async fn monitor_runtime(&self, shard_id: u64) {
        while !self.inner.closed.load(Ordering::Acquire) {
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
            let idempotency_key = format!("runtime-hello:{shard_id}");
            let _ = self
                .request_value_on_shard(
                    "runtime.hello",
                    json!({}),
                    None,
                    Some(&idempotency_key),
                    shard_id,
                )
                .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn recover_transfer(&self, lease: Arc<RecoveryLease>) {
        let mut attempt = 0_u32;
        loop {
            if self.inner.closed.load(Ordering::Acquire) {
                break;
            }
            let (current, requested_epoch) = {
                let state = self.inner.recovery.lock().await;
                (
                    state.leases.get(&lease.transfer_id).cloned(),
                    state.requested_epochs.get(&lease.transfer_id).cloned(),
                )
            };
            if !current
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &lease))
            {
                break;
            }
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
            let result = match resumed {
                Ok(result)
                    if result.get("recovery").and_then(Value::as_str) == Some("terminal") =>
                {
                    self.release_recovery_lease(&lease.transfer_id).await;
                    Ok(())
                }
                Ok(result)
                    if result.get("route_replay_required").and_then(Value::as_bool)
                        == Some(true) =>
                {
                    (lease.replay_routes)(generation_id).await
                }
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            };
            if result.is_ok() {
                let mut state = self.inner.recovery.lock().await;
                let latest_epoch = state.requested_epochs.get(&lease.transfer_id).cloned();
                if latest_epoch != requested_epoch {
                    drop(state);
                    attempt = 0;
                    continue;
                }
                state.running.remove(&lease.transfer_id);
                return;
            }
            if result
                .as_ref()
                .is_err_and(|error| !is_retryable_lifecycle_error(error))
            {
                self.inner
                    .recovery
                    .lock()
                    .await
                    .running
                    .remove(&lease.transfer_id);
                self.release_recovery_lease(&lease.transfer_id).await;
                return;
            }
            attempt = attempt.saturating_add(1);
            let delay =
                Duration::from_millis(500 * (1_u64 << attempt.min(6))).min(Duration::from_secs(30));
            tokio::time::sleep(delay).await;
        }
        self.inner
            .recovery
            .lock()
            .await
            .running
            .remove(&lease.transfer_id);
    }

    pub(crate) async fn close(&self) -> Result<(), BeamApiError> {
        self.inner.closed.store(true, Ordering::Release);
        let leases = {
            let mut recovery = self.inner.recovery.lock().await;
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
        if let Some(client) = self.inner.client.lock().await.take() {
            if let Err(error) = client.flush().await {
                if first_error.is_none() {
                    first_error = Some(BeamApiError::Nats(error.to_string()));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn split_routes_for_payload(
        &self,
        message_type: &str,
        base_payload: &Value,
        routes: &[Value],
    ) -> Result<Vec<Vec<Value>>, BeamApiError> {
        fn encoded_size(
            control: &NatsControl,
            message_type: &str,
            base_payload: &Value,
            candidate: &[Value],
        ) -> Result<usize, BeamApiError> {
            let mut payload = base_payload.clone();
            payload["route_batch"] = compact_signed_route_values(candidate)?;
            let envelope = json!({
                "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
                "environment": control.inner.environment,
                "key_prefix": control.inner.key_prefix,
                "shard_id": 0,
                "message_type": message_type,
                "request_id": new_transfer_id(),
                "auth_token": "x".repeat(512),
                "occurred_at": iso_now(),
                "producer": "sdk",
                "payload": payload,
            });
            let encoded =
                to_vec_named(&envelope).map_err(|error| BeamApiError::Nats(error.to_string()))?;
            Ok(encoded.len())
        }
        let mut chunks = Vec::new();
        let mut offset = 0usize;
        while offset < routes.len() {
            let mut low = 1usize;
            let mut high = routes.len() - offset;
            let mut accepted = 0usize;
            while low <= high {
                let count = (low + high) / 2;
                let size = encoded_size(
                    self,
                    message_type,
                    base_payload,
                    &routes[offset..offset + count],
                )?;
                if size <= self.inner.max_payload_bytes {
                    accepted = count;
                    low = count + 1;
                } else {
                    high = count - 1;
                }
            }
            if accepted == 0 {
                let size = encoded_size(
                    self,
                    message_type,
                    base_payload,
                    &routes[offset..offset + 1],
                )?;
                return Err(BeamApiError::Nats(format!(
                    "single signed route is {size} bytes, above max_payload_bytes={}",
                    self.inner.max_payload_bytes
                )));
            }
            chunks.push(routes[offset..offset + accepted].to_vec());
            offset += accepted;
        }
        Ok(chunks)
    }

    async fn connection(&self) -> Result<async_nats::Client, BeamApiError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        let mut guard = self.inner.client.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let options = async_nats::ConnectOptions::new()
            .user_and_password(self.inner.key_prefix.clone(), self.inner.api_key.clone())
            .name(format!("beam-rust-sdk-{}", self.inner.key_prefix))
            .max_reconnects(SDK_MAX_RECONNECTS)
            .reconnect_delay_callback(|attempt| {
                let capped = attempt.min(6) as u32;
                let base = Duration::from_millis(250 * (1_u64 << capped));
                base.min(Duration::from_secs(30))
            });
        let client = options
            .connect(self.inner.nats_url.clone())
            .await
            .map_err(|error| BeamApiError::Nats(error.to_string()))?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(BeamApiError::Nats(
                "NATS lifecycle control is closed".to_string(),
            ));
        }
        *guard = Some(client.clone());
        Ok(client)
    }

    async fn auth_token(&self) -> Result<String, BeamApiError> {
        let now = unix_now();
        let mut guard = self.inner.auth_token.lock().await;
        if let Some(token) = &guard.token {
            if guard.expires_at.saturating_sub(5) > now {
                return Ok(token.clone());
            }
        }
        let subject = self.auth_subject();
        let mut response = None;
        let mut last_error = "NATS auth resolve failed".to_string();
        for attempt in 0..3 {
            match self.connection().await {
                Ok(client) => match tokio::time::timeout(
                    self.inner.request_timeout,
                    client.request(subject.clone(), bytes::Bytes::from_static(b"{}")),
                )
                .await
                {
                    Ok(Ok(reply)) => {
                        response = Some(reply);
                        break;
                    }
                    Ok(Err(error)) => {
                        let message = error.to_string();
                        if !is_retryable_nats_error(&message) {
                            return Err(BeamApiError::Nats(message));
                        }
                        last_error = message;
                    }
                    Err(_) => {
                        last_error = format!("NATS auth resolve timed out: {}", subject);
                    }
                },
                Err(error) => {
                    let message = error.to_string();
                    if !is_retryable_nats_error(&message) {
                        return Err(error);
                    }
                    last_error = message;
                }
            }
            if attempt < 2 {
                sleep_before_retry(attempt).await;
            }
        }
        let response = response.ok_or_else(|| BeamApiError::Nats(last_error))?;
        let parsed: AuthResolveResponse = serde_json::from_slice(&response.payload)
            .map_err(|error| BeamApiError::Nats(error.to_string()))?;
        if !parsed.ok {
            return Err(BeamApiError::Nats(format!(
                "NATS auth resolve failed: {}",
                parsed.error.unwrap_or_else(|| "unknown_error".to_string())
            )));
        }
        let token = parsed
            .token
            .ok_or_else(|| BeamApiError::Nats("NATS auth resolve omitted token".to_string()))?;
        let claims = decode_jwt_claims(&token)?;
        guard.expires_at = claims.exp;
        guard.token = Some(token.clone());
        Ok(token)
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
}

impl NatsTerminalSignalWaiter {
    pub(crate) async fn wait(
        &self,
        wait_timeout: Duration,
    ) -> Result<Option<TransferTerminalEvent>, BeamApiError> {
        if wait_timeout.is_zero() {
            return Ok(None);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(BeamApiError::Nats(
                "transfer terminal waiter is closed".to_string(),
            ));
        }
        let receive = async {
            let mut subscriber = self.inner.subscriber.lock().await;
            match subscriber.as_mut() {
                Some(subscriber) => subscriber.next().await,
                None => None,
            }
        };
        let message = match tokio::time::timeout(wait_timeout, async {
            tokio::select! {
                _ = self.inner.closed_notify.notified() => None,
                message = receive => message,
            }
        })
        .await
        {
            Err(_) => return Ok(None),
            Ok(Some(message)) => message,
            Ok(None) => {
                return Err(BeamApiError::Nats(
                    "transfer terminal waiter is closed".to_string(),
                ))
            }
        };
        let event: TransferTerminalEvent = from_slice(&message.payload).map_err(|error| {
            BeamApiError::Nats(format!("invalid transfer terminal signal: {error}"))
        })?;
        if event.schema_version != TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION
            || event.producer != "transfer-runtime"
            || event.transfer_id != self.inner.transfer_id
            || event.occurred_at.trim().is_empty()
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
        self.inner.closed_notify.notify_one();
        if let Some(mut subscriber) = self.inner.subscriber.lock().await.take() {
            subscriber
                .unsubscribe()
                .await
                .map_err(|error| BeamApiError::Nats(error.to_string()))?;
        }
        Ok(())
    }
}

pub(crate) fn new_transfer_id() -> String {
    Uuid::new_v4().to_string()
}

pub(crate) fn transfer_shard_id(transfer_id: &str, shard_count: u64) -> u64 {
    let mut hash = 2166136261u32;
    for byte in transfer_id.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16777619);
    }
    u64::from(hash) % shard_count.max(1)
}

const ROUTE_ATTEMPT_METADATA_KEYS: &[&str] = &[
    "part_number",
    "logical_attempt_index",
    "attempt_slot",
    "route_generation_id",
];

pub(crate) fn compact_signed_route_values(routes: &[Value]) -> Result<Value, BeamApiError> {
    let mut source_chunks = Vec::new();
    let mut source_refs = std::collections::HashMap::<String, usize>::new();
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
            let mut source = serde_json::Map::new();
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
                if let Some(item) = object.get(key) {
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
            .or_else(|| metadata.get("delivery_index"))
            .and_then(Value::as_u64)
            .unwrap_or(route_index as u64);
        for key in [
            "source_id",
            "destination_id",
            "chunk_index",
            "route_chunk_index",
            "delivery_index",
        ] {
            metadata.remove(key);
        }
        let group_id = metadata
            .get("multipart_group_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        metadata.retain(|key, _| ROUTE_ATTEMPT_METADATA_KEYS.contains(&key.as_str()));
        let mut compact_route = serde_json::Map::new();
        compact_route.insert("source_ref".to_string(), json!(source_ref));
        compact_route.insert("delivery_index".to_string(), json!(delivery_index));
        for key in ["destination_id", "dest_url", "expires_at", "dest_headers"] {
            if let Some(item) = object.get(key) {
                compact_route.insert(key.to_string(), item.clone());
            }
        }
        if !group_id.is_empty() {
            compact_route.insert("multipart_group_id".to_string(), json!(group_id));
        }
        if !metadata.is_empty() {
            compact_route.insert("metadata".to_string(), Value::Object(metadata));
        }
        compact_routes.push(Value::Object(compact_route));
    }
    Ok(json!({"source_chunks": source_chunks, "routes": compact_routes}))
}

fn lifecycle_request_id(message_type: &str, idempotency_key: Option<&str>) -> String {
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

pub(crate) fn is_retryable_lifecycle_error(error: &BeamApiError) -> bool {
    match error {
        BeamApiError::HttpStatus { status } => matches!(*status, 408 | 425 | 429) || *status >= 500,
        BeamApiError::Request(error) => error.is_timeout() || error.is_connect(),
        BeamApiError::Nats(message) => is_retryable_nats_error(message),
        _ => false,
    }
}

pub(crate) fn is_recoverable_route_stream_error(error: &BeamApiError) -> bool {
    is_retryable_lifecycle_error(error)
        || matches!(error, BeamApiError::HttpStatus { status: 404 | 409 })
}

async fn sleep_before_retry(attempt: usize) {
    let base_ms = if attempt == 0 { 150 } else { 500 };
    let jitter_per_mille = 800 + (Uuid::new_v4().as_u128() % 401) as u64;
    tokio::time::sleep(Duration::from_millis(base_ms * jitter_per_mille / 1000)).await;
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
        .decode(payload)
        .map_err(|error| BeamApiError::Nats(error.to_string()))?;
    serde_json::from_slice(&decoded).map_err(|error| BeamApiError::Nats(error.to_string()))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn iso_now() -> String {
    iso_at(std::time::SystemTime::now())
}

pub(crate) fn iso_after(duration: Duration) -> String {
    iso_at(std::time::SystemTime::now() + duration)
}

fn iso_at(value: std::time::SystemTime) -> String {
    let now = value
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_seconds = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
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
mod recovery_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn runtime_state_loss_keeps_route_recovery_lease() {
        assert!(is_recoverable_route_stream_error(&BeamApiError::HttpStatus { status: 404 }));
        assert!(is_recoverable_route_stream_error(&BeamApiError::HttpStatus { status: 409 }));
        assert!(!is_recoverable_route_stream_error(&BeamApiError::HttpStatus { status: 400 }));
    }

    #[tokio::test]
    async fn runtime_epoch_recovery_coalesces_and_clears_secrets() {
        let control = NatsControl::new(
            "b1m_recovery".to_string(),
            "nats://127.0.0.1:4222".to_string(),
            "dev".to_string(),
            1,
            Duration::from_secs(1),
            1024 * 1024,
        );
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
            state.leases.insert(transfer_id.clone(), lease);
            state.running.insert(transfer_id.clone());
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
        control.release_recovery_lease(&transfer_id).await;
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn foreground_cancellation_requests_background_recovery_without_releasing_secrets() {
        let control = NatsControl::new(
            "b1m_recovery".to_string(),
            "nats://127.0.0.1:4222".to_string(),
            "dev".to_string(),
            1,
            Duration::from_secs(1),
            1024 * 1024,
        );
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
            state.leases.insert(transfer_id.clone(), lease);
            state.running.insert(transfer_id.clone());
        }
        control.continue_recovery_lease(&transfer_id).await;
        let state = control.inner.recovery.lock().await;
        let requested = state.requested_epochs.get(&transfer_id).cloned();
        assert_eq!(
            requested.as_ref().map(|epoch| epoch.0.as_str()),
            Some("foreground")
        );
        assert!(state.leases.contains_key(&transfer_id));
        drop(state);
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);
        control.release_recovery_lease(&transfer_id).await;
        assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);
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
}
