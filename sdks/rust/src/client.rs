use crate::nats_control::{
    compact_signed_route_values, is_recoverable_route_stream_error, iso_after, new_transfer_id,
    NatsControl, NatsTerminalSignalWaiter, RecoveryFuture, RecoveryLease,
};
use crate::huggingface::{self, HuggingFaceConfig, HuggingFaceFileMetadata, HuggingFaceUploadPlan};
use crate::{
    AttachSignedUrlsResponse, ChunkDestinationSigningTarget, ChunkSigningPlanItem,
    CompactTransferPlanDescriptor, DestConfig, DistributeResponse, HippiusProviderDestination,
    HippiusProviderSource, HuggingFaceProviderDestination, HuggingFaceProviderSource,
    MultipartGroupManifest, PreparedDestination, PreparedHttpSource, ProviderDestinationConfig,
    ProviderSourceConfig, SignedChunkRoute, SourceConfig, TransferCancelResponse,
    TransferCreateRequest, TransferCreateResponse, TransferPrepareResponse, TransferStatusInfo,
    TransferTerminalEvent,
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use reqwest::Client as HttpClient;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle, time::sleep};
use uuid::Uuid;

pub const BEAM_DEV_URL: &str = "nats://127.0.0.1:4222";
pub const BEAM_PROD_URL: &str = "tls://orch-gateway.b1m.ai:4222";
/// Default SDK-side NATS message guard. NATS enforces max_payload at the broker,
/// so signed-route control messages are split before object-transfer metadata is rejected.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BeamClientOptions {
    pub api_key: String,
    pub nats_url: Option<String>,
    pub environment: Option<String>,
    pub http_client: Option<HttpClient>,
    pub transfer_runtime_shard_count: Option<u64>,
    pub request_timeout: Option<Duration>,
    pub max_payload_bytes: Option<usize>,
    pub route_signing_concurrency: Option<usize>,
}

#[derive(Debug, Error)]
pub enum BeamApiError {
    #[error("api_key is required")]
    MissingApiKey,
    #[error("invalid {0}")]
    InvalidId(&'static str),
    #[error("BEAM API request failed with status {status}")]
    HttpStatus { status: u16 },
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("NATS lifecycle transport failed: {0}")]
    Nats(String),
    #[error("JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("transfer failed: {0}")]
    TransferFailed(String),
    #[error("transfer cancelled")]
    TransferCancelled,
    #[error("transfer {transfer_id} did not complete within {timeout:?}")]
    Timeout {
        transfer_id: String,
        timeout: Duration,
    },
    #[error("provider signing failed: {0}")]
    ProviderSigning(String),
    #[error(
        "transfer {transfer_id} is prepared and route recovery is continuing in the background"
    )]
    RouteRecoveryPending {
        transfer_id: String,
        #[source]
        source: Box<BeamApiError>,
    },
}

#[derive(Clone)]
pub struct BeamClient {
    http: HttpClient,
    control: NatsControl,
    route_signing_concurrency: usize,
    route_signing_concurrency_overridden: bool,
    huggingface_uploads: Arc<StdMutex<HashMap<String, Vec<HuggingFaceUploadState>>>>,
}

/// One Hugging Face LFS upload, held from prepare until the transfer's commit.
///
/// The Hub issues upload URLs only for a known sha256 and dictates the part size, so the plan is
/// built around what the LFS batch hands back rather than the other way round.
#[derive(Clone)]
struct HuggingFaceUploadState {
    config: HuggingFaceConfig,
    destination_id: String,
    source_id: String,
    oid: String,
    size: u64,
    chunk_size: Option<u64>,
    part_urls: Vec<String>,
    upload_href: Option<String>,
    verify_href: Option<String>,
    /// Resolves alongside the transfer; only read at commit time.
    part_etags: Arc<AsyncMutex<Option<Result<Vec<String>, String>>>>,
}

struct ForegroundRecoveryGuard {
    control: NatsControl,
    transfer_id: String,
    armed: bool,
}

impl ForegroundRecoveryGuard {
    fn new(control: NatsControl, transfer_id: String) -> Self {
        Self {
            control,
            transfer_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ForegroundRecoveryGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let control = self.control.clone();
        let transfer_id = self.transfer_id.clone();
        tokio::spawn(async move {
            control.continue_recovery_lease(&transfer_id).await;
        });
    }
}

pub type ManualRouteRecoveryFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (
                        Vec<SignedChunkRoute>,
                        Vec<MultipartGroupManifest>,
                        Option<String>,
                    ),
                    BeamApiError,
                >,
            > + Send,
    >,
>;

#[derive(Clone)]
pub struct ManualRouteRecovery {
    pub plan_fingerprint: String,
    pub coordinate_checksum: String,
    pub regenerate: Arc<dyn Fn(String) -> ManualRouteRecoveryFuture + Send + Sync>,
}

#[derive(Clone)]
pub struct TransferTerminalSignalWaiter {
    inner: NatsTerminalSignalWaiter,
}

impl TransferTerminalSignalWaiter {
    pub async fn wait(
        &self,
        timeout: Duration,
    ) -> Result<Option<TransferTerminalEvent>, BeamApiError> {
        self.inner.wait(timeout).await
    }

    pub async fn close(&self) -> Result<(), BeamApiError> {
        self.inner.close().await
    }
}

impl BeamClient {
    pub fn new(options: BeamClientOptions) -> Result<Self, BeamApiError> {
        if options.api_key.trim().is_empty() {
            return Err(BeamApiError::MissingApiKey);
        }

        let environment = options.environment.unwrap_or_else(|| "prod".to_string());
        let nats_url = options
            .nats_url
            .unwrap_or_else(|| match environment.as_str() {
                "prod" => BEAM_PROD_URL.to_string(),
                _ => BEAM_DEV_URL.to_string(),
            })
            .trim_end_matches('/')
            .to_string();
        if nats_url.starts_with("http://")
            || nats_url.starts_with("https://")
            || nats_url.starts_with("ws://")
            || nats_url.starts_with("wss://")
        {
            return Err(BeamApiError::Nats(
                "nats_url must use nats:// or tls:// for Rust lifecycle transport".to_string(),
            ));
        }
        let control = NatsControl::new(
            options.api_key.clone(),
            nats_url.clone(),
            environment,
            options.transfer_runtime_shard_count.unwrap_or(1),
            options
                .request_timeout
                .unwrap_or_else(|| Duration::from_secs(30)),
            options
                .max_payload_bytes
                .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES),
        );
        let route_signing_concurrency_overridden = options.route_signing_concurrency.is_some();
        let route_signing_concurrency = options.route_signing_concurrency.unwrap_or(64);
        if route_signing_concurrency == 0 {
            return Err(BeamApiError::ProviderSigning(
                "route_signing_concurrency must be positive".to_string(),
            ));
        }

        Ok(Self {
            http: options.http_client.unwrap_or_default(),
            control,
            route_signing_concurrency,
            route_signing_concurrency_overridden,
            huggingface_uploads: Arc::new(StdMutex::new(HashMap::new())),
        })
    }

    pub async fn close(&self) -> Result<(), BeamApiError> {
        self.control.close().await
    }
    pub async fn create_transfer(
        &self,
        mut request: TransferCreateRequest,
    ) -> Result<TransferCreateResponse, BeamApiError> {
        if request
            .transfer_id
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            request.transfer_id = Some(transfer_id_for_idempotency_key(
                request.idempotency_key.as_deref(),
            ));
        }
        let transfer_id = request.transfer_id.clone().unwrap_or_default();
        let idempotency_key = request
            .idempotency_key
            .clone()
            .unwrap_or_else(|| format!("transfer:{transfer_id}:create"));
        let mut payload = serde_json::to_value(request)?;
        if payload
            .get("signed_url_flow")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .is_empty()
        {
            payload["signed_url_flow"] = json!("signed_url");
        }
        self.lifecycle_request(
            "transfer.create",
            payload,
            Some(&transfer_id),
            Some(&idempotency_key),
        )
        .await
    }
    pub async fn transfer_status(
        &self,
        transfer_id: &str,
    ) -> Result<TransferStatusInfo, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let result: TransferStatusInfo = self
            .lifecycle_request(
                "transfer.status",
                json!({ "transfer_id": transfer_id }),
                Some(transfer_id),
                None,
            )
            .await?;
        if matches!(result.status.as_str(), "completed" | "failed" | "cancelled") {
            self.control.release_recovery_lease(transfer_id).await;
        }
        Ok(result)
    }

    pub async fn open_transfer_terminal_waiter(
        &self,
        transfer_id: &str,
    ) -> Result<TransferTerminalSignalWaiter, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        Ok(TransferTerminalSignalWaiter {
            inner: self
                .control
                .open_terminal_signal_waiter(transfer_id)
                .await?,
        })
    }
    pub async fn distribute_transfer(
        &self,
        transfer_id: &str,
    ) -> Result<DistributeResponse, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let idempotency_key = format!("transfer:{transfer_id}:distribute");
        self.lifecycle_request(
            "transfer.distribute",
            json!({ "transfer_id": transfer_id }),
            Some(transfer_id),
            Some(&idempotency_key),
        )
        .await
    }
    pub async fn cancel_transfer(
        &self,
        transfer_id: &str,
    ) -> Result<TransferCancelResponse, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let idempotency_key = format!("transfer:{transfer_id}:cancel");
        let result: TransferCancelResponse = self
            .lifecycle_request(
                "transfer.cancel",
                json!({ "transfer_id": transfer_id }),
                Some(transfer_id),
                Some(&idempotency_key),
            )
            .await?;
        if result.success {
            self.control.release_recovery_lease(transfer_id).await;
        }
        Ok(result)
    }
    pub async fn prepare_transfer(
        &self,
        sources: Vec<PreparedHttpSource>,
        destinations: Vec<PreparedDestination>,
        name: Option<String>,
        test_mode: bool,
        urls_expires_at: Option<String>,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_transfer_with_chunk_size(
            sources,
            destinations,
            name,
            test_mode,
            urls_expires_at,
            route_generation_id,
            None,
            idempotency_key,
        )
        .await
    }

    /// Request a specific plan chunk size. BeamCore may raise it, so callers that depend on the
    /// exact value must check the response.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_transfer_with_chunk_size(
        &self,
        sources: Vec<PreparedHttpSource>,
        destinations: Vec<PreparedDestination>,
        name: Option<String>,
        test_mode: bool,
        urls_expires_at: Option<String>,
        route_generation_id: Option<String>,
        chunk_size: Option<u64>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        let transfer_id = transfer_id_for_idempotency_key(idempotency_key.as_deref());
        let prepare_idempotency_key = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("transfer:{transfer_id}:prepare"));
        let mut body = json!({
            "transfer_id": transfer_id,
            "route_generation_id": route_generation_id
                .unwrap_or_else(|| route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key)),
            "sources": sources,
            "destinations": destinations,
            "signed_url_flow": "signed_url",
        });
        if let Some(name) = name {
            body["name"] = json!(name);
        }
        if test_mode {
            body["test_mode"] = json!(true);
        }
        if let Some(expires_at) = urls_expires_at {
            body["urls_expires_at"] = json!(expires_at);
        }
        if let Some(chunk_size) = chunk_size {
            body["chunk_size"] = json!(chunk_size);
        }
        let result: TransferPrepareResponse = self
            .lifecycle_request(
                "transfer.prepare",
                body,
                Some(&transfer_id),
                Some(&prepare_idempotency_key),
            )
            .await?;
        if result.success {
            validate_compact_transfer_plan(&result.signed_url_flow, &result.plan_descriptor)?;
        }
        Ok(result)
    }
    pub async fn prepare_provider_source_config(
        &self,
        source: &ProviderSourceConfig,
        index: usize,
        expires_in: Duration,
    ) -> Result<PreparedHttpSource, BeamApiError> {
        let expires_in = if expires_in.is_zero() {
            Duration::from_secs(3600)
        } else {
            expires_in
        };

        match source {
            ProviderSourceConfig::HuggingFace(source) => {
                let config = HuggingFaceConfig::from_source(source);
                let metadata = self.huggingface_file_metadata(&config).await?;
                Ok(PreparedHttpSource {
                    source_id: source
                        .source_id
                        .clone()
                        .unwrap_or_else(|| format!("src_{}", index)),
                    source_type: "http".to_string(),
                    provider: Some("huggingface".to_string()),
                    url: metadata.url,
                    size: metadata.size,
                    filename: Some(filename(&config.path)),
                    headers: None,
                    expires_at: None,
                    metadata: huggingface::metadata(
                        &config,
                        metadata.etag.as_deref(),
                        metadata.commit_hash.as_deref(),
                    ),
                })
            }
            ProviderSourceConfig::Hippius(source) => {
                let base_url = source
                    .base_url
                    .as_deref()
                    .unwrap_or("https://api.hippius.com");
                let size = self
                    .hippius_object_size(base_url, &source.api_token, &source.bucket, &source.key)
                    .await?;
                let url = self
                    .hippius_presign(
                        base_url,
                        &source.api_token,
                        &source.bucket,
                        &source.key,
                        "get",
                        expires_in,
                    )
                    .await?;
                Ok(PreparedHttpSource {
                    source_id: source
                        .source_id
                        .clone()
                        .unwrap_or_else(|| format!("src_{}", index)),
                    source_type: "http".to_string(),
                    provider: Some("hippius".to_string()),
                    url,
                    size,
                    filename: Some(filename(&source.key)),
                    headers: None,
                    expires_at: None,
                    metadata: HashMap::from([
                        ("bucket".to_string(), json!(source.bucket)),
                        ("key".to_string(), json!(source.key)),
                        ("base_url".to_string(), json!(base_url)),
                    ]),
                })
            }
            ProviderSourceConfig::S3(_) => Err(BeamApiError::ProviderSigning(
                "S3 source signing is not implemented in the Rust SDK yet".to_string(),
            )),
            ProviderSourceConfig::R2(_) => Err(BeamApiError::ProviderSigning(
                "R2 source signing is not implemented in the Rust SDK yet".to_string(),
            )),
            ProviderSourceConfig::GCS(_) => Err(BeamApiError::ProviderSigning(
                "GCS source signing is not implemented in the Rust SDK yet".to_string(),
            )),
            ProviderSourceConfig::Azure(_) => Err(BeamApiError::ProviderSigning(
                "Azure source signing is not implemented in the Rust SDK yet".to_string(),
            )),
        }
    }

    pub async fn attach_signed_urls(
        &self,
        transfer_id: &str,
        chunk_routes: Vec<SignedChunkRoute>,
        multipart_group_manifest: Vec<MultipartGroupManifest>,
        _transfer_key: Option<String>,
        urls_expires_at: Option<String>,
        route_generation_id: String,
        recovery: ManualRouteRecovery,
    ) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        if route_generation_id.is_empty()
            || recovery.plan_fingerprint.is_empty()
            || recovery.coordinate_checksum.is_empty()
        {
            return Err(BeamApiError::ProviderSigning(
                "route generation and recovery factory are required by transfer-client-control/v6"
                    .to_string(),
            ));
        }
        let stream_lock = Arc::new(AsyncMutex::new(()));
        let initial_stream_guard = stream_lock.lock().await;
        let mut foreground_guard =
            ForegroundRecoveryGuard::new(self.control.clone(), transfer_id.to_string());
        let client = self.clone();
        let replay_transfer_id = transfer_id.to_string();
        let recovery_for_replay = recovery.clone();
        let replay_stream_lock = stream_lock.clone();
        self.control
            .register_recovery_lease(RecoveryLease {
                transfer_id: transfer_id.to_string(),
                plan_fingerprint: recovery.plan_fingerprint.clone(),
                coordinate_checksum: recovery.coordinate_checksum.clone(),
                replay_routes: Arc::new(move |generation_id| {
                    let client = client.clone();
                    let transfer_id = replay_transfer_id.clone();
                    let recovery = recovery_for_replay.clone();
                    let stream_lock = replay_stream_lock.clone();
                    Box::pin(async move {
                        let _stream_guard = stream_lock.lock().await;
                        let (routes, manifests, expires_at) =
                            (recovery.regenerate)(generation_id.clone()).await?;
                        let result = client
                            .stream_signed_routes(
                                &transfer_id,
                                routes,
                                manifests,
                                expires_at,
                                generation_id,
                            )
                            .await?;
                        if !result.success {
                            return Err(BeamApiError::ProviderSigning(format!(
                                "route stream failed: {}{}",
                                result.error.unwrap_or_default(),
                                result.message
                            )));
                        }
                        Ok(())
                    }) as RecoveryFuture
                }),
                dispose: None,
            })
            .await;
        let result = self
            .stream_signed_routes(
                transfer_id,
                chunk_routes,
                multipart_group_manifest,
                urls_expires_at,
                route_generation_id,
            )
            .await;
        drop(initial_stream_guard);
        foreground_guard.disarm();
        match result {
            Ok(result) => {
                if !result.success {
                    self.control.release_recovery_lease(transfer_id).await;
                }
                Ok(result)
            }
            Err(error) => {
                if is_recoverable_route_stream_error(&error) {
                    self.control.continue_recovery_lease(transfer_id).await;
                    return Err(BeamApiError::RouteRecoveryPending {
                        transfer_id: transfer_id.to_string(),
                        source: Box::new(error),
                    });
                } else {
                    self.control.release_recovery_lease(transfer_id).await;
                }
                Err(error)
            }
        }
    }

    async fn stream_signed_routes(
        &self,
        transfer_id: &str,
        mut chunk_routes: Vec<SignedChunkRoute>,
        multipart_group_manifest: Vec<MultipartGroupManifest>,
        urls_expires_at: Option<String>,
        route_generation_id: String,
    ) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        validate_signed_route_manifest_contract(
            transfer_id,
            &chunk_routes,
            &multipart_group_manifest,
        )?;
        let destination_count = chunk_routes
            .iter()
            .map(|route| route.destination_id.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();
        for route in &mut chunk_routes {
            if signed_route_delivery_index(route).is_none() {
                if destination_count != 1 {
                    return Err(BeamApiError::ProviderSigning(
                        "delivery_index is required when manually attaching routes for multiple destinations"
                            .to_string(),
                    ));
                }
                route.delivery_index = Some(route.chunk_index);
            }
        }
        let route_coordinate_checksum = format!(
            "{}:{}",
            route_keys_checksum_for_routes(&chunk_routes),
            route_generation_id
        );
        chunk_routes.sort_by(|left, right| {
            match (
                signed_route_delivery_index(left),
                signed_route_delivery_index(right),
            ) {
                (Some(left_index), Some(right_index)) if left_index != right_index => {
                    left_index.cmp(&right_index)
                }
                _ => route_key_for_signed_route(left).cmp(&route_key_for_signed_route(right)),
            }
        });
        let total_chunks = count_distinct_route_chunks(&chunk_routes);
        let mut sender = RouteStreamSender::new(
            self,
            transfer_id.to_string(),
            chunk_routes.len(),
            total_chunks,
            true,
            urls_expires_at,
            route_coordinate_checksum,
            "signed_url",
            route_generation_id,
        );
        sender.begin().await?;
        sender
            .add_manifest_groups(&multipart_group_manifest)
            .await?;
        for route in chunk_routes {
            sender.add_route(route).await?;
        }
        let result = sender.complete().await?;
        Ok(result)
    }
    pub async fn create_and_distribute(
        &self,
        request: TransferCreateRequest,
    ) -> Result<TransferCreateResponse, BeamApiError> {
        let transfer = self.create_transfer(request).await?;
        if transfer.success {
            self.distribute_transfer(&transfer.transfer_id).await?;
        }
        Ok(transfer)
    }

    /// Prepare, sign, and attach a Hugging Face Hub transfer.
    ///
    /// The Hub token stays local: BeamCore receives the presigned CDN URL for each source and
    /// the Hub's own presigned part URLs for each destination.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_huggingface_provider_transfer(
        &self,
        sources: Vec<HuggingFaceProviderSource>,
        destinations: Vec<HuggingFaceProviderDestination>,
        name: Option<String>,
        test_mode: bool,
        expires_in: Duration,
        distribute: bool,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        let expires_in = if expires_in.is_zero() {
            Duration::from_secs(3600)
        } else {
            expires_in
        };

        let mut prepared_sources = Vec::with_capacity(sources.len());
        for (index, source) in sources.iter().enumerate() {
            prepared_sources.push(
                self.prepare_provider_source_config(
                    &ProviderSourceConfig::HuggingFace(source.clone()),
                    index,
                    expires_in,
                )
                .await?,
            );
        }
        let prepared_destinations: Vec<PreparedDestination> = destinations
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                prepare_provider_destination_config(
                    &ProviderDestinationConfig::HuggingFace(destination.clone()),
                    index,
                )
            })
            .collect::<Result<_, _>>()?;

        let (states, chunk_size) = self
            .plan_huggingface_uploads(&prepared_sources, &destinations, &prepared_destinations)
            .await?;

        let prepared = self
            .prepare_transfer_with_chunk_size(
                prepared_sources,
                prepared_destinations.clone(),
                name,
                test_mode,
                None,
                route_generation_id,
                chunk_size,
                idempotency_key,
            )
            .await?;
        if !prepared.success {
            return Ok(prepared);
        }

        assert_huggingface_plan(&prepared, &states).map_err(BeamApiError::ProviderSigning)?;
        if let Ok(mut uploads) = self.huggingface_uploads.lock() {
            uploads.insert(prepared.transfer_id.clone(), states.clone());
        }

        let mut foreground_guard =
            ForegroundRecoveryGuard::new(self.control.clone(), prepared.transfer_id.clone());
        let streamed = self
            .stream_huggingface_provider_routes(
                &prepared,
                &states,
                expires_in,
                distribute,
                &prepared.route_generation_id,
            )
            .await;
        foreground_guard.disarm();
        if let Err(error) = streamed {
            if is_recoverable_route_stream_error(&error) {
                return Err(BeamApiError::RouteRecoveryPending {
                    transfer_id: prepared.transfer_id.clone(),
                    source: Box::new(error),
                });
            }
            return Err(self
                .cancel_after_provider_failure(&prepared.transfer_id, error)
                .await);
        }
        Ok(prepared)
    }

    /// Negotiate every Hugging Face destination before the plan exists.
    ///
    /// The Hub will not issue upload URLs without the object sha256, and it chooses the part
    /// size itself, so this runs first and the plan is then requested at that size.
    async fn plan_huggingface_uploads(
        &self,
        prepared_sources: &[PreparedHttpSource],
        destinations: &[HuggingFaceProviderDestination],
        prepared_destinations: &[PreparedDestination],
    ) -> Result<(Vec<HuggingFaceUploadState>, Option<u64>), BeamApiError> {
        if destinations.is_empty() {
            return Ok((Vec::new(), None));
        }
        if prepared_sources.len() != 1 {
            return Err(BeamApiError::ProviderSigning(format!(
                "a huggingface destination requires exactly one source: the Hub dictates the part \
                 size and the plan carries a single chunk size, but {} sources were given",
                prepared_sources.len()
            )));
        }
        let prepared_source = &prepared_sources[0];
        // For an LFS source the Hub already published the sha256 as the linked ETag.
        let published_sha256 = prepared_source
            .metadata
            .get("sha256")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut states = Vec::with_capacity(destinations.len());
        let mut chunk_size: Option<u64> = None;

        for (index, destination) in destinations.iter().enumerate() {
            if destination.repo_type.as_deref() == Some("bucket") {
                return Err(BeamApiError::ProviderSigning(format!(
                    "{} is a Hugging Face bucket, which Beam cannot write to. Buckets expose no \
                     LFS batch endpoint; their only upload path is the Hub's Xet CAS client, which \
                     cannot be expressed as presigned URLs for Beam's workers. Buckets do work as \
                     a transfer source. Use `hf sync` to write to a bucket",
                    destination.repo_id
                )));
            }
            let mut config = HuggingFaceConfig::from_destination(destination);
            config.path = huggingface::target_path(&config.path, prepared_source.filename.as_deref())
                .map_err(BeamApiError::ProviderSigning)?;

            let oid = match published_sha256.clone() {
                Some(oid) => oid,
                None => {
                    if !destination.allow_source_rehash {
                        return Err(BeamApiError::ProviderSigning(format!(
                            "uploading to {} needs the source sha256, which the Hub requires before \
                             it issues upload URLs. The SDK must read the source once to compute \
                             it; set allow_source_rehash to opt in",
                            config.describe()
                        )));
                    }
                    self.huggingface_hash_source(&prepared_source.url, None, true)
                        .await?
                        .0
                        .unwrap_or_default()
                }
            };

            let sample = self.huggingface_source_sample(&prepared_source.url).await?;
            let (upload_mode, should_ignore) = self
                .huggingface_preupload(&config, prepared_source.size, &sample)
                .await?;
            if should_ignore {
                return Err(BeamApiError::ProviderSigning(format!(
                    "{} is excluded by the repo's .gitignore",
                    config.describe()
                )));
            }
            if upload_mode != "lfs" {
                return Err(BeamApiError::ProviderSigning(format!(
                    "{} would be committed as a regular git blob, not an LFS blob. Beam uploads \
                     through the LFS protocol only; add the path to .gitattributes as LFS",
                    config.describe()
                )));
            }

            let plan = self
                .huggingface_lfs_batch(&config, &oid, prepared_source.size)
                .await?;
            if let Some(part_size) = plan.chunk_size {
                if let Some(existing) = chunk_size {
                    if existing != part_size {
                        return Err(BeamApiError::ProviderSigning(format!(
                            "huggingface destinations disagree on part size ({existing} vs \
                             {part_size}); the plan carries a single chunk size"
                        )));
                    }
                }
                chunk_size = Some(part_size);
            }

            let part_etags = Arc::new(AsyncMutex::new(None));
            if let Some(part_size) = plan.chunk_size {
                // Runs alongside the transfer; the ETags are only needed at completion time.
                let client = self.clone();
                let url = prepared_source.url.clone();
                let slot = part_etags.clone();
                tokio::spawn(async move {
                    let result = client
                        .huggingface_hash_source(&url, Some(part_size), false)
                        .await
                        .map(|(_, etags)| etags)
                        .map_err(|error| error.to_string());
                    *slot.lock().await = Some(result);
                });
            }

            states.push(HuggingFaceUploadState {
                config,
                destination_id: prepared_destinations[index].destination_id.clone(),
                source_id: prepared_source.source_id.clone(),
                oid,
                size: prepared_source.size,
                chunk_size: plan.chunk_size,
                part_urls: plan.part_urls,
                upload_href: plan.upload_href,
                verify_href: plan.verify_href,
                part_etags,
            });
        }

        Ok((states, chunk_size))
    }

    async fn stream_huggingface_provider_routes(
        &self,
        prepared: &TransferPrepareResponse,
        states: &[HuggingFaceUploadState],
        expires_in: Duration,
        distribute: bool,
        route_generation_id: &str,
    ) -> Result<(), BeamApiError> {
        let state_by_destination: HashMap<&str, &HuggingFaceUploadState> = states
            .iter()
            .map(|state| (state.destination_id.as_str(), state))
            .collect();
        let source_url_by_id: HashMap<&str, &str> = prepared
            .plan_descriptor
            .sources
            .iter()
            .map(|source| {
                (
                    source.source.source_id.as_str(),
                    source.source.url.as_str(),
                )
            })
            .collect();

        let mut sender = RouteStreamSender::new(
            self,
            prepared.transfer_id.clone(),
            prepared.plan_descriptor.delivery_route_count as usize,
            prepared.plan_descriptor.logical_chunk_count as usize,
            distribute,
            Some(iso_after(expires_in)),
            format!(
                "{}:{}",
                prepared.plan_descriptor.plan_nonce, route_generation_id
            ),
            "signed_url",
            route_generation_id.to_string(),
        );
        sender.begin().await?;
        let stream_result: Result<AttachSignedUrlsResponse, BeamApiError> = async {
            for chunk_result in
                CompactPlanChunkIter::new(&prepared.plan_descriptor, &prepared.transfer_id)
            {
                let chunk = chunk_result?;
                let source_url = source_url_by_id
                    .get(chunk.source_id.as_str())
                    .ok_or_else(|| {
                        BeamApiError::ProviderSigning(format!(
                            "BeamCore returned unknown source_id: {}",
                            chunk.source_id
                        ))
                    })?
                    .to_string();
                for target in &chunk.destinations {
                    let state = state_by_destination
                        .get(target.destination_id.as_str())
                        .ok_or_else(|| {
                            BeamApiError::ProviderSigning(format!(
                                "huggingface upload state is missing for {}",
                                target.destination_id
                            ))
                        })?;
                    // The Hub presigns its own part targets; chunk N carries the URL for part N + 1.
                    let dest_url = match state.chunk_size {
                        None => state.upload_href.clone(),
                        Some(_) => state
                            .part_urls
                            .get(chunk.source_chunk_index as usize)
                            .cloned(),
                    }
                    .ok_or_else(|| {
                        BeamApiError::ProviderSigning(format!(
                            "the Hub issued no upload URL for chunk {} of {}",
                            chunk.source_chunk_index,
                            state.config.describe()
                        ))
                    })?;

                    // BeamCore rejects part_number on a destination it does not treat as an S3
                    // multipart target.
                    let mut metadata = target.metadata.clone();
                    metadata.remove("part_number");
                    metadata.insert(
                        "transfer_id".to_string(),
                        json!(prepared.transfer_id.clone()),
                    );
                    sender
                        .add_route(SignedChunkRoute {
                            source_id: chunk.source_id.clone(),
                            destination_id: target.destination_id.clone(),
                            chunk_index: chunk.chunk_index,
                            delivery_index: target
                                .metadata
                                .get("delivery_index")
                                .and_then(Value::as_u64),
                            source_url: source_url.clone(),
                            dest_url,
                            source_offset: chunk.source_offset,
                            chunk_size: chunk.chunk_size,
                            expires_at: None,
                            headers: Some(HashMap::from([(
                                "Range".to_string(),
                                format!(
                                    "bytes={}-{}",
                                    chunk.source_offset,
                                    chunk.source_offset + chunk.chunk_size - 1
                                ),
                            )])),
                            dest_headers: None,
                            metadata,
                        })
                        .await?;
                }
            }
            sender.complete().await
        }
        .await;

        let attached = match stream_result {
            Ok(attached) => attached,
            Err(error) => {
                sender.abort().await;
                return Err(error);
            }
        };
        if !attached.success {
            let error = BeamApiError::ProviderSigning(
                attached
                    .error
                    .or(Some(attached.message))
                    .unwrap_or_else(|| "route stream failed".to_string()),
            );
            sender.abort().await;
            return Err(error);
        }
        Ok(())
    }

    /// Close every Hugging Face upload for a transfer: complete the LFS multipart, verify it, and
    /// commit the blob so the file appears in the repo.
    ///
    /// Call it once the transfer is complete. It never sits in the transfer's progression path.
    pub async fn finalize_huggingface_uploads(
        &self,
        transfer_id: &str,
    ) -> Result<(), BeamApiError> {
        let states = self
            .huggingface_uploads
            .lock()
            .ok()
            .and_then(|mut uploads| uploads.remove(transfer_id))
            .unwrap_or_default();

        for state in states {
            if let (Some(href), Some(_)) = (state.upload_href.as_deref(), state.chunk_size) {
                let etags = state
                    .part_etags
                    .lock()
                    .await
                    .clone()
                    .ok_or_else(|| {
                        BeamApiError::ProviderSigning(format!(
                            "the part ETag pass for {} has not finished",
                            state.config.describe()
                        ))
                    })?
                    .map_err(BeamApiError::ProviderSigning)?;
                if etags.len() != state.part_urls.len() {
                    return Err(BeamApiError::ProviderSigning(format!(
                        "computed {} part ETags for {}, expected {}",
                        etags.len(),
                        state.config.describe(),
                        state.part_urls.len()
                    )));
                }
                self.huggingface_complete_lfs_upload(&state.config, href, &state.oid, &etags)
                    .await?;
            }
            if let Some(href) = state.verify_href.as_deref() {
                self.huggingface_verify_lfs_upload(&state.config, href, &state.oid, state.size)
                    .await?;
            }
            self.huggingface_commit(&state.config, &state.oid, state.size)
                .await?;
        }
        Ok(())
    }

    pub async fn prepare_hippius_provider_transfer(
        &self,
        sources: Vec<HippiusProviderSource>,
        destinations: Vec<HippiusProviderDestination>,
        name: Option<String>,
        test_mode: bool,
        expires_in: Duration,
        distribute: bool,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        let expires_in = if expires_in.is_zero() {
            Duration::from_secs(3600)
        } else {
            expires_in
        };

        let mut prepared_sources = Vec::with_capacity(sources.len());
        for (index, source) in sources.iter().enumerate() {
            let base_url = source
                .base_url
                .as_deref()
                .unwrap_or("https://api.hippius.com");
            let size = self
                .hippius_object_size(base_url, &source.api_token, &source.bucket, &source.key)
                .await?;
            let url = self
                .hippius_presign(
                    base_url,
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                    "get",
                    expires_in,
                )
                .await?;
            prepared_sources.push(PreparedHttpSource {
                source_id: source
                    .source_id
                    .clone()
                    .unwrap_or_else(|| format!("src_{}", index)),
                source_type: "http".to_string(),
                provider: Some("hippius".to_string()),
                url,
                size,
                filename: Some(filename(&source.key)),
                headers: None,
                expires_at: None,
                metadata: HashMap::from([
                    ("bucket".to_string(), json!(source.bucket)),
                    ("key".to_string(), json!(source.key)),
                    ("base_url".to_string(), json!(base_url)),
                ]),
            });
        }

        let prepared_destinations: Vec<PreparedDestination> = destinations
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                let metadata = HashMap::from([
                    ("bucket".to_string(), json!(destination.bucket)),
                    ("key".to_string(), json!(destination.key)),
                    (
                        "base_url".to_string(),
                        json!(destination
                            .base_url
                            .as_deref()
                            .unwrap_or("https://api.hippius.com")),
                    ),
                ]);
                PreparedDestination {
                    destination_id: destination
                        .destination_id
                        .clone()
                        .unwrap_or_else(|| format!("dst_{}", index)),
                    provider: "hippius".to_string(),
                    mode: Some("http_chunks".to_string()),
                    logical_prefix: Some(destination.key.trim_end_matches('/').to_string()),
                    metadata,
                }
            })
            .collect();

        let prepared = self
            .prepare_transfer(
                prepared_sources,
                prepared_destinations.clone(),
                name.clone(),
                test_mode,
                None,
                route_generation_id,
                idempotency_key.clone(),
            )
            .await?;
        if !prepared.success {
            return Ok(prepared);
        }

        let retained_inputs =
            Arc::new(StdMutex::new(Some((sources.clone(), destinations.clone()))));
        let retained_for_replay = retained_inputs.clone();
        let retained_for_dispose = retained_inputs.clone();
        let stream_lock = Arc::new(AsyncMutex::new(()));
        let initial_stream_guard = stream_lock.lock().await;
        let replay_stream_lock = stream_lock.clone();
        let mut foreground_guard =
            ForegroundRecoveryGuard::new(self.control.clone(), prepared.transfer_id.clone());
        let client = self.clone();
        let recovery_prepared = prepared.clone();
        let recovery_prepared_destinations = prepared_destinations.clone();
        self.control
            .register_recovery_lease(RecoveryLease {
                transfer_id: prepared.transfer_id.clone(),
                plan_fingerprint: prepared.plan_fingerprint.clone(),
                coordinate_checksum: prepared.coordinate_checksum.clone(),
                replay_routes: Arc::new(move |generation_id| {
                    let retained = retained_for_replay
                        .lock()
                        .ok()
                        .and_then(|retained| retained.clone());
                    let client = client.clone();
                    let prepared = recovery_prepared.clone();
                    let prepared_destinations = recovery_prepared_destinations.clone();
                    let stream_lock = replay_stream_lock.clone();
                    Box::pin(async move {
                        let _stream_guard = stream_lock.lock().await;
                        let (sources, destinations) = retained.ok_or_else(|| {
                            BeamApiError::ProviderSigning(
                                "provider recovery lease was abandoned".to_string(),
                            )
                        })?;
                        client
                            .stream_hippius_provider_routes(
                                &prepared,
                                &prepared_destinations,
                                &sources,
                                &destinations,
                                expires_in,
                                distribute,
                                &generation_id,
                            )
                            .await?;
                        Ok(())
                    }) as RecoveryFuture
                }),
                dispose: Some(Arc::new(move || {
                    if let Ok(mut retained) = retained_for_dispose.lock() {
                        *retained = None;
                    }
                })),
            })
            .await;
        let streamed = self
            .stream_hippius_provider_routes(
                &prepared,
                &prepared_destinations,
                &sources,
                &destinations,
                expires_in,
                distribute,
                &prepared.route_generation_id,
            )
            .await;
        drop(initial_stream_guard);
        foreground_guard.disarm();
        if let Err(error) = streamed {
            if is_recoverable_route_stream_error(&error) {
                self.control
                    .continue_recovery_lease(&prepared.transfer_id)
                    .await;
                return Err(BeamApiError::RouteRecoveryPending {
                    transfer_id: prepared.transfer_id.clone(),
                    source: Box::new(error),
                });
            }
            let error = self
                .cancel_after_provider_failure(&prepared.transfer_id, error)
                .await;
            self.control
                .release_recovery_lease(&prepared.transfer_id)
                .await;
            return Err(error);
        }
        Ok(prepared)
    }

    async fn stream_hippius_provider_routes(
        &self,
        prepared: &TransferPrepareResponse,
        prepared_destinations: &[PreparedDestination],
        sources: &[HippiusProviderSource],
        destinations: &[HippiusProviderDestination],
        expires_in: Duration,
        distribute: bool,
        route_generation_id: &str,
    ) -> Result<(), BeamApiError> {
        let destination_by_id: HashMap<&str, &HippiusProviderDestination> = prepared_destinations
            .iter()
            .zip(destinations.iter())
            .map(|(destination, config)| (destination.destination_id.as_str(), config))
            .collect();
        let source_by_id: HashMap<String, &HippiusProviderSource> = sources
            .iter()
            .enumerate()
            .map(|(index, source)| {
                (
                    source
                        .source_id
                        .clone()
                        .unwrap_or_else(|| format!("src_{index}")),
                    source,
                )
            })
            .collect();
        let mut source_urls = HashMap::new();
        for source_plan in &prepared.plan_descriptor.sources {
            let source = source_by_id
                .get(&source_plan.source.source_id)
                .ok_or_else(|| {
                    BeamApiError::ProviderSigning(format!(
                        "BeamCore returned unknown source_id: {}",
                        source_plan.source.source_id
                    ))
                })?;
            let base_url = source
                .base_url
                .as_deref()
                .unwrap_or("https://api.hippius.com");
            source_urls.insert(
                source_plan.source.source_id.clone(),
                self.hippius_presign(
                    base_url,
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                    "get",
                    expires_in,
                )
                .await?,
            );
        }
        let mut sender = RouteStreamSender::new(
            self,
            prepared.transfer_id.clone(),
            prepared.plan_descriptor.delivery_route_count as usize,
            prepared.plan_descriptor.logical_chunk_count as usize,
            distribute,
            Some(iso_after(expires_in)),
            format!(
                "{}:{}",
                prepared.plan_descriptor.plan_nonce, route_generation_id
            ),
            "signed_url",
            route_generation_id.to_string(),
        );
        sender.begin().await?;
        let stream_result: Result<AttachSignedUrlsResponse, BeamApiError> = async {
            let mut pending_routes = FuturesUnordered::new();
            let mut signing_concurrency = self.route_signing_concurrency;
            let mut signed_in_window = 0usize;
            let mut signing_window_started_at = Instant::now();
            for chunk_result in
                CompactPlanChunkIter::new(&prepared.plan_descriptor, &prepared.transfer_id)
            {
                let chunk = chunk_result?;
                for target in &chunk.destinations {
                    let destination = (*destination_by_id
                        .get(target.destination_id.as_str())
                        .ok_or_else(|| {
                            BeamApiError::ProviderSigning(format!(
                                "BeamCore returned unknown destination_id: {}",
                                target.destination_id
                            ))
                        })?)
                    .clone();
                    let source_url =
                        source_urls.get(&chunk.source_id).cloned().ok_or_else(|| {
                            BeamApiError::ProviderSigning(format!(
                                "BeamCore returned unknown source_id: {}",
                                chunk.source_id
                            ))
                        })?;
                    let object_key = target.object_key.clone().ok_or_else(|| {
                        BeamApiError::ProviderSigning(
                            "destination signing target is missing object_key".to_string(),
                        )
                    })?;
                    let client = self.clone();
                    let chunk = chunk.clone();
                    let target = target.clone();
                    let transfer_id = prepared.transfer_id.clone();
                    pending_routes.push(async move {
                        let base_url = destination
                            .base_url
                            .as_deref()
                            .unwrap_or("https://api.hippius.com");
                        let dest_url = client
                            .hippius_presign(
                                base_url,
                                &destination.api_token,
                                &destination.bucket,
                                &object_key,
                                "put",
                                expires_in,
                            )
                            .await?;
                        let mut metadata = target.metadata.clone();
                        metadata.insert("transfer_id".to_string(), json!(transfer_id));
                        Ok::<SignedChunkRoute, BeamApiError>(SignedChunkRoute {
                            source_id: chunk.source_id.clone(),
                            destination_id: target.destination_id.clone(),
                            chunk_index: chunk.chunk_index,
                            delivery_index: target
                                .metadata
                                .get("delivery_index")
                                .and_then(Value::as_u64),
                            source_url,
                            dest_url,
                            source_offset: chunk.source_offset,
                            chunk_size: chunk.chunk_size,
                            expires_at: None,
                            headers: None,
                            dest_headers: None,
                            metadata,
                        })
                    });
                    if pending_routes.len() >= signing_concurrency {
                        let route = pending_routes.next().await.expect("pending route")?;
                        sender.add_route(route).await?;
                        signed_in_window += 1;
                        if signed_in_window == ROUTE_STREAM_BATCH_ROUTES {
                            if !self.route_signing_concurrency_overridden
                                && signing_window_started_at.elapsed() > Duration::from_secs(4)
                                && signing_concurrency < 256
                            {
                                signing_concurrency = (signing_concurrency * 2).min(256);
                            }
                            signed_in_window = 0;
                            signing_window_started_at = Instant::now();
                        }
                    }
                }
            }
            while let Some(route) = pending_routes.next().await {
                sender.add_route(route?).await?;
            }
            sender.complete().await
        }
        .await;
        let attached = match stream_result {
            Ok(attached) => attached,
            Err(error) => {
                sender.abort().await;
                return Err(error);
            }
        };
        if !attached.success {
            let error = BeamApiError::ProviderSigning(
                attached
                    .error
                    .or(Some(attached.message))
                    .unwrap_or_else(|| "route stream failed".to_string()),
            );
            sender.abort().await;
            return Err(error);
        }
        Ok(())
    }

    async fn cancel_after_provider_failure(
        &self,
        transfer_id: &str,
        cause: BeamApiError,
    ) -> BeamApiError {
        match self.cancel_transfer(transfer_id).await {
            Ok(result) if result.success => cause,
            Ok(result) => BeamApiError::ProviderSigning(format!(
                "provider transfer failed ({cause}) and transfer cancellation failed ({})",
                if result.message.is_empty() {
                    "Beam rejected cancellation"
                } else {
                    result.message.as_str()
                }
            )),
            Err(cancel_error) => BeamApiError::ProviderSigning(format!(
                "provider transfer failed ({cause}) and transfer cancellation failed ({cancel_error})"
            )),
        }
    }
    pub async fn wait_for_transfer(
        &self,
        transfer_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<TransferStatusInfo, BeamApiError> {
        let timeout = if timeout.is_zero() {
            Duration::from_secs(300)
        } else {
            timeout
        };
        let mut poll_interval = if poll_interval.is_zero() {
            Duration::from_secs(15)
        } else {
            poll_interval
        };
        let initial_poll_interval = poll_interval;
        let mut terminal_waiter = self.open_transfer_terminal_waiter(transfer_id).await.ok();
        let started = Instant::now();
        let result = async {
            loop {
                let status = self.transfer_status(transfer_id).await?;
                match status.status.as_str() {
                    "completed" => {
                        return Ok(status);
                    }
                    "failed" => {
                        return Err(BeamApiError::TransferFailed(
                            status.error_message.unwrap_or_default(),
                        ))
                    }
                    "cancelled" => {
                        return Err(BeamApiError::TransferCancelled);
                    }
                    _ => {}
                }
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    return Err(BeamApiError::Timeout {
                        transfer_id: transfer_id.to_string(),
                        timeout,
                    });
                }
                let jitter = 0.8 + (Uuid::new_v4().as_u128() % 401) as f64 / 1000.0;
                let wait_for = poll_interval.mul_f64(jitter).min(timeout - elapsed);
                let signal_received = if let Some(waiter) = terminal_waiter.as_ref() {
                    let wait_started = Instant::now();
                    match waiter.wait(wait_for).await {
                        Ok(event) => event.is_some(),
                        Err(_) => {
                            let _ = waiter.close().await;
                            terminal_waiter = None;
                            if let Some(remaining) = wait_for.checked_sub(wait_started.elapsed()) {
                                sleep(remaining).await;
                            }
                            false
                        }
                    }
                } else {
                    sleep(wait_for).await;
                    false
                };
                if signal_received {
                    poll_interval = initial_poll_interval;
                } else {
                    poll_interval = poll_interval.mul_f64(1.5).min(Duration::from_secs(30));
                }
            }
        }
        .await;
        if let Some(waiter) = terminal_waiter {
            let _ = waiter.close().await;
        }
        result
    }

    async fn lifecycle_request<T: DeserializeOwned>(
        &self,
        message_type: &str,
        payload: Value,
        transfer_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<T, BeamApiError> {
        let value = self
            .control
            .request_value(message_type, payload, transfer_id, idempotency_key)
            .await?;
        serde_json::from_value(value).map_err(|error| BeamApiError::Nats(error.to_string()))
    }
    /// HEAD the resolve URL and require the Hub to redirect to its CDN.
    ///
    /// The redirect target is presigned and carries no credential, so it is the only form of
    /// this URL that may be handed to BeamCore and the workers.
    pub async fn huggingface_file_metadata(
        &self,
        config: &HuggingFaceConfig,
    ) -> Result<HuggingFaceFileMetadata, BeamApiError> {
        let resolve_url = config.resolve_url();
        // A dedicated client so the redirect is visible instead of followed.
        let client = HttpClient::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        let response = client
            .head(&resolve_url)
            .header("Authorization", format!("Bearer {}", config.token))
            // Compression would report a transformed length instead of the real object size.
            .header("Accept-Encoding", "identity")
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        if response.status().as_u16() >= 400 {
            return Err(BeamApiError::ProviderSigning(format!(
                "Hugging Face file lookup failed status={} for {}",
                response.status(),
                config.describe()
            )));
        }

        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        huggingface::file_metadata_from_headers(
            config,
            &resolve_url,
            header("location").as_deref(),
            header("x-linked-size").as_deref(),
            header("content-length").as_deref(),
            header("x-linked-etag").as_deref(),
            header("etag").as_deref(),
            header("x-repo-commit").as_deref(),
        )
        .map_err(BeamApiError::ProviderSigning)
    }

    /// Ask the Hub whether a path is stored as an LFS blob or as a regular git blob.
    async fn huggingface_preupload(
        &self,
        config: &HuggingFaceConfig,
        size: u64,
        sample: &str,
    ) -> Result<(String, bool), BeamApiError> {
        let payload: Value = self
            .huggingface_post_json(
                &huggingface::preupload_url(config),
                config,
                "preupload",
                huggingface::preupload_body(config, size, sample),
            )
            .await?;
        huggingface::upload_mode_from_preupload(config, &payload)
            .map_err(BeamApiError::ProviderSigning)
    }

    /// Request upload instructions for one object from the LFS batch endpoint.
    async fn huggingface_lfs_batch(
        &self,
        config: &HuggingFaceConfig,
        oid: &str,
        size: u64,
    ) -> Result<HuggingFaceUploadPlan, BeamApiError> {
        let mut request = self.http.post(config.lfs_batch_url());
        for (name, value) in huggingface::lfs_headers(&config.token) {
            request = request.header(name, value);
        }
        let response = request
            .json(&huggingface::lfs_batch_body(config, oid, size))
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        let payload: Value = huggingface_response_json(response, config, "LFS batch").await?;
        huggingface::upload_plan_from_batch(config, &payload, size)
            .map_err(BeamApiError::ProviderSigning)
    }

    /// Close a multipart LFS upload with `{oid, parts:[{partNumber, etag}]}`.
    async fn huggingface_complete_lfs_upload(
        &self,
        config: &HuggingFaceConfig,
        href: &str,
        oid: &str,
        etags: &[String],
    ) -> Result<(), BeamApiError> {
        let mut request = self.http.post(href);
        for (name, value) in huggingface::lfs_headers(&config.token) {
            request = request.header(name, value);
        }
        let response = request
            .json(&huggingface::completion_body(oid, etags))
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        huggingface_response_status(response, config, "LFS completion").await
    }

    /// Run the Hub's optional server-side check that the object landed intact.
    async fn huggingface_verify_lfs_upload(
        &self,
        config: &HuggingFaceConfig,
        href: &str,
        oid: &str,
        size: u64,
    ) -> Result<(), BeamApiError> {
        let _: Value = self
            .huggingface_post_json(href, config, "LFS verify", json!({ "oid": oid, "size": size }))
            .await?;
        Ok(())
    }

    /// Publish the uploaded blob as a commit. The body is NDJSON.
    async fn huggingface_commit(
        &self,
        config: &HuggingFaceConfig,
        oid: &str,
        size: u64,
    ) -> Result<(), BeamApiError> {
        let response = self
            .http
            .post(huggingface::commit_url(config))
            .header("Authorization", format!("Bearer {}", config.token))
            .header("Content-Type", "application/x-ndjson")
            .body(huggingface::commit_ndjson(config, oid, size))
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        huggingface_response_status(response, config, "commit").await
    }

    /// Read the first bytes of a URL base64-encoded, for `preupload`.
    async fn huggingface_source_sample(&self, url: &str) -> Result<String, BeamApiError> {
        let response = self
            .http
            .get(url)
            .header("Range", "bytes=0-511")
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        if response.status().as_u16() >= 400 {
            return Err(BeamApiError::ProviderSigning(format!(
                "source sample read failed status={}",
                response.status()
            )));
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        Ok(huggingface::encode_sample(&body))
    }

    /// Stream a URL once, returning the sha256 of the whole body and the MD5 of every part.
    ///
    /// The Hub will not issue upload URLs without the sha256, and the per-part MD5 is the ETag
    /// the completion payload has to quote.
    async fn huggingface_hash_source(
        &self,
        url: &str,
        part_size: Option<u64>,
        whole_sha256: bool,
    ) -> Result<(Option<String>, Vec<String>), BeamApiError> {
        let mut response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        if response.status().as_u16() >= 400 {
            return Err(BeamApiError::ProviderSigning(format!(
                "source hash read failed status={}",
                response.status()
            )));
        }
        let mut hasher = huggingface::SourceHasher::new(part_size, whole_sha256);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?
        {
            hasher.update(&chunk);
        }
        Ok(hasher.finish())
    }

    async fn huggingface_post_json(
        &self,
        url: &str,
        config: &HuggingFaceConfig,
        step: &str,
        body: Value,
    ) -> Result<Value, BeamApiError> {
        let response = self
            .http
            .post(url)
            .header("Authorization", format!("Bearer {}", config.token))
            .json(&body)
            .send()
            .await
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        huggingface_response_json(response, config, step).await
    }

    async fn hippius_presign(
        &self,
        base_url: &str,
        token: &str,
        bucket: &str,
        key: &str,
        action: &str,
        expires_in: Duration,
    ) -> Result<String, BeamApiError> {
        let response = self
            .http
            .get(format!(
                "{}/api/objectstore/buckets/{}/presigned-url/",
                base_url.trim_end_matches('/'),
                url_escape(bucket)
            ))
            .query(&[
                ("key", key),
                ("action", action),
                ("expires_in", &expires_in.as_secs().to_string()),
            ])
            .header("Authorization", format!("Token {}", token))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(BeamApiError::HttpStatus {
                status: status.as_u16(),
            });
        }
        let text = response.text().await?;
        let payload: Value = serde_json::from_str(&text)
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        payload
            .get("url")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| BeamApiError::ProviderSigning("missing Hippius signed URL".to_string()))
    }

    async fn hippius_object_size(
        &self,
        base_url: &str,
        token: &str,
        bucket: &str,
        key: &str,
    ) -> Result<u64, BeamApiError> {
        let response = self
            .http
            .get(format!(
                "{}/api/objectstore/buckets/{}/objects/",
                base_url.trim_end_matches('/'),
                url_escape(bucket)
            ))
            .query(&[("prefix", key), ("max_keys", "1")])
            .header("Authorization", format!("Token {}", token))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(BeamApiError::HttpStatus {
                status: status.as_u16(),
            });
        }
        let text = response.text().await?;
        let payload: Value = serde_json::from_str(&text)
            .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
        payload
            .get("Contents")
            .and_then(Value::as_array)
            .and_then(|contents| contents.first())
            .and_then(|first| first.get("Size"))
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "object not found: hippius://{}/{}",
                    bucket, key
                ))
            })
    }
}

pub fn prepare_provider_destination_config(
    destination: &ProviderDestinationConfig,
    index: usize,
) -> Result<PreparedDestination, BeamApiError> {
    match destination {
        ProviderDestinationConfig::HuggingFace(destination) => {
            let config = HuggingFaceConfig::from_destination(destination);
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "huggingface".to_string(),
                mode: None,
                logical_prefix: Some(config.path.clone()),
                metadata: huggingface::metadata(&config, None, None),
            })
        }
        ProviderDestinationConfig::S3(destination) => {
            let mut metadata = HashMap::from([
                ("bucket".to_string(), json!(destination.bucket)),
                ("key".to_string(), json!(destination.key)),
                (
                    "region".to_string(),
                    json!(destination.region.as_deref().unwrap_or("us-east-1")),
                ),
            ]);
            if let Some(endpoint_url) = &destination.endpoint_url {
                metadata.insert("endpoint_url".to_string(), json!(endpoint_url));
            }
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "s3".to_string(),
                mode: None,
                logical_prefix: Some(destination.key.clone()),
                metadata,
            })
        }
        ProviderDestinationConfig::R2(destination) => {
            let endpoint_url = r2_endpoint(
                destination.account_id.as_deref(),
                destination.endpoint_url.as_deref(),
            )?;
            let metadata = HashMap::from([
                ("bucket".to_string(), json!(destination.bucket)),
                ("key".to_string(), json!(destination.key)),
                ("endpoint_url".to_string(), json!(endpoint_url)),
            ]);
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "r2".to_string(),
                mode: None,
                logical_prefix: Some(destination.key.clone()),
                metadata,
            })
        }
        ProviderDestinationConfig::GCS(destination) => {
            let metadata = HashMap::from([
                ("bucket".to_string(), json!(destination.bucket)),
                ("key".to_string(), json!(destination.key)),
                ("project_id".to_string(), json!(destination.project_id)),
            ]);
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "gcs".to_string(),
                mode: None,
                logical_prefix: Some(destination.key.clone()),
                metadata,
            })
        }
        ProviderDestinationConfig::Azure(destination) => {
            let metadata = HashMap::from([
                ("container".to_string(), json!(destination.container)),
                ("blob".to_string(), json!(destination.blob)),
                ("account_name".to_string(), json!(destination.account_name)),
            ]);
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "azure".to_string(),
                mode: None,
                logical_prefix: Some(format!(
                    "azure://{}/{}",
                    destination.container, destination.blob
                )),
                metadata,
            })
        }
        ProviderDestinationConfig::Hippius(destination) => {
            let metadata = HashMap::from([
                ("bucket".to_string(), json!(destination.bucket)),
                ("key".to_string(), json!(destination.key)),
                (
                    "base_url".to_string(),
                    json!(destination
                        .base_url
                        .as_deref()
                        .unwrap_or("https://api.hippius.com")),
                ),
            ]);
            Ok(PreparedDestination {
                destination_id: destination
                    .destination_id
                    .clone()
                    .unwrap_or_else(|| default_id("dst", index)),
                provider: "hippius".to_string(),
                mode: Some("http_chunks".to_string()),
                logical_prefix: Some(destination.key.trim_end_matches('/').to_string()),
                metadata,
            })
        }
    }
}

const ROUTE_STREAM_BATCH_ROUTES: usize = 2_048;

struct RouteStreamSender<'a> {
    client: &'a BeamClient,
    transfer_id: String,
    stream_id: String,
    total_routes: usize,
    total_chunks: usize,
    auto_distribute: bool,
    urls_expires_at: Option<String>,
    signed_url_flow: &'static str,
    route_generation_id: String,
    checksum: RouteKeysChecksum,
    batch: Vec<Value>,
    seen_delivery_indices: HashSet<u64>,
    batch_index: usize,
    route_count: usize,
    send_tail: Option<JoinHandle<Result<(), BeamApiError>>>,
}

impl<'a> RouteStreamSender<'a> {
    fn new(
        client: &'a BeamClient,
        transfer_id: String,
        total_routes: usize,
        total_chunks: usize,
        auto_distribute: bool,
        urls_expires_at: Option<String>,
        plan_identity: String,
        signed_url_flow: &'static str,
        route_generation_id: String,
    ) -> Self {
        let stream_id = stable_route_stream_id(
            &transfer_id,
            &plan_identity,
            total_routes,
            total_chunks,
            signed_url_flow,
        );
        Self {
            client,
            transfer_id,
            stream_id,
            total_routes,
            total_chunks,
            auto_distribute,
            urls_expires_at,
            signed_url_flow,
            route_generation_id,
            checksum: RouteKeysChecksum::default(),
            batch: Vec::with_capacity(ROUTE_STREAM_BATCH_ROUTES),
            seen_delivery_indices: HashSet::new(),
            batch_index: 0,
            route_count: 0,
            send_tail: None,
        }
    }

    async fn begin(&self) -> Result<(), BeamApiError> {
        let mut payload = json!({
            "transfer_id": self.transfer_id,
            "stream_id": self.stream_id,
            "route_generation_id": self.route_generation_id,
            "total_routes": self.total_routes,
            "total_chunks": self.total_chunks,
            "route_contract_version": self.signed_url_flow,
            "signed_url_flow": self.signed_url_flow,
            "auto_distribute": self.auto_distribute,
        });
        if let Some(expires_at) = &self.urls_expires_at {
            payload["urls_expires_at"] = json!(expires_at);
        }
        let idempotency_key = format!(
            "transfer:{}:route-stream:{}:begin",
            self.transfer_id, self.stream_id
        );
        let _: Value = self
            .client
            .lifecycle_request(
                "transfer.route_stream.begin",
                payload,
                Some(&self.transfer_id),
                Some(&idempotency_key),
            )
            .await?;
        Ok(())
    }

    async fn add_manifest_groups(
        &self,
        groups: &[MultipartGroupManifest],
    ) -> Result<(), BeamApiError> {
        if groups.is_empty() {
            return Ok(());
        }
        let identity = stable_manifest_batch_identity(groups);
        let idempotency_key = format!(
            "transfer:{}:route-stream:{}:manifest:{}",
            self.transfer_id, self.stream_id, identity
        );
        let _: Value = self
            .client
            .lifecycle_request(
                "transfer.route_stream.manifest",
                json!({
                    "transfer_id": self.transfer_id,
                    "stream_id": self.stream_id,
                    "route_generation_id": self.route_generation_id,
                    "manifest_batch_id": identity,
                    "groups": groups,
                }),
                Some(&self.transfer_id),
                Some(&idempotency_key),
            )
            .await?;
        Ok(())
    }

    async fn add_route(&mut self, route: SignedChunkRoute) -> Result<(), BeamApiError> {
        let delivery_index = signed_route_delivery_index(&route).ok_or_else(|| {
            BeamApiError::ProviderSigning("route delivery_index is required".to_string())
        })?;
        if delivery_index >= self.total_routes as u64 {
            return Err(BeamApiError::ProviderSigning(format!(
                "route delivery_index {delivery_index} is outside the declared route stream"
            )));
        }
        if !self.seen_delivery_indices.insert(delivery_index) {
            return Err(BeamApiError::ProviderSigning(format!(
                "duplicate route delivery_index {delivery_index}"
            )));
        }
        self.route_count += 1;
        let mut route = route;
        route.delivery_index = Some(delivery_index);
        self.checksum.add(&route_key_for_signed_route(&route));
        self.batch.push(serde_json::to_value(route)?);
        if self.batch.len() >= ROUTE_STREAM_BATCH_ROUTES {
            self.enqueue_flush().await?;
        }
        Ok(())
    }

    async fn complete(&mut self) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        if self.route_count != self.total_routes
            || self.seen_delivery_indices.len() != self.total_routes
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "route stream has incomplete or duplicate delivery indices: received {} of {} routes",
                self.route_count, self.total_routes
            )));
        }
        self.enqueue_flush().await?;
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.await.map_err(|error| {
                BeamApiError::Nats(format!("route stream sender task failed: {error}"))
            })??;
        }
        let idempotency_key = format!(
            "transfer:{}:route-stream:{}:complete",
            self.transfer_id, self.stream_id
        );
        self.client
            .lifecycle_request(
                "transfer.route_stream.complete",
                json!({
                    "transfer_id": self.transfer_id,
                    "stream_id": self.stream_id,
                    "route_generation_id": self.route_generation_id,
                    "expected_batches": self.batch_index,
                    "expected_routes": self.total_routes,
                    "route_keys_checksum": self.checksum.value(),
                }),
                Some(&self.transfer_id),
                Some(&idempotency_key),
            )
            .await
    }

    async fn abort(&mut self) {
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.abort();
            let _ = send_tail.await;
        }
    }

    async fn enqueue_flush(&mut self) -> Result<(), BeamApiError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.await.map_err(|error| {
                BeamApiError::Nats(format!("route stream sender task failed: {error}"))
            })??;
        }
        let routes = std::mem::take(&mut self.batch);
        let base_payload = json!({
            "transfer_id": self.transfer_id,
            "stream_id": self.stream_id,
            "route_generation_id": self.route_generation_id,
            "batch_id": format!("{}:estimate", self.stream_id),
            "batch_index": self.batch_index,
        });
        let chunks = self.client.control.split_routes_for_payload(
            "transfer.route_stream.batch",
            &base_payload,
            &routes,
        )?;
        let mut scheduled = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let batch_index = self.batch_index;
            self.batch_index += 1;
            let route_keys_checksum = route_keys_checksum_for_values(&chunk)?;
            scheduled.push((batch_index, chunk, route_keys_checksum));
        }
        let client = self.client.clone();
        let transfer_id = self.transfer_id.clone();
        let stream_id = self.stream_id.clone();
        let route_generation_id = self.route_generation_id.clone();
        self.send_tail = Some(tokio::spawn(async move {
            for (batch_index, chunk, route_keys_checksum) in scheduled {
                let route_count = chunk.len();
                let coordinate_checksum = route_coordinate_checksum_for_values(&chunk)?;
                let batch_id = format!("{stream_id}:{batch_index}:{coordinate_checksum}");
                let idempotency_key = format!(
                    "transfer:{transfer_id}:route-stream:{stream_id}:batch:{batch_index}:{coordinate_checksum}"
                );
                let _: Value = client
                    .lifecycle_request(
                        "transfer.route_stream.batch",
                        json!({
                            "transfer_id": transfer_id,
                            "stream_id": stream_id,
                            "route_generation_id": route_generation_id,
                            "batch_id": batch_id,
                            "batch_index": batch_index,
                            "route_batch": compact_signed_route_values(&chunk)?,
                            "route_count": route_count,
                            "route_keys_checksum": route_keys_checksum,
                        }),
                        Some(&transfer_id),
                        Some(&idempotency_key),
                    )
                    .await?;
            }
            Ok(())
        }));
        Ok(())
    }
}

impl Drop for RouteStreamSender<'_> {
    fn drop(&mut self) {
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.abort();
        }
    }
}

#[derive(Default)]
struct RouteKeysChecksum {
    bytes: [u8; 32],
    count: usize,
}

impl RouteKeysChecksum {
    fn add(&mut self, route_key: &str) {
        let digest = Sha256::digest(route_key.as_bytes());
        for (index, value) in digest.iter().enumerate() {
            self.bytes[index] ^= value;
        }
        self.count += 1;
    }

    fn value(&self) -> String {
        format!("sha256-xor-v1:{}:{}", self.count, hex_bytes(&self.bytes))
    }
}

fn route_key_for_signed_route(route: &SignedChunkRoute) -> String {
    format!(
        "{}:{}:{}",
        route.source_id, route.destination_id, route.chunk_index
    )
}

fn signed_route_delivery_index(route: &SignedChunkRoute) -> Option<u64> {
    route
        .metadata
        .get("delivery_index")
        .and_then(Value::as_u64)
        .or(route.delivery_index)
}

fn route_keys_checksum_for_routes(routes: &[SignedChunkRoute]) -> String {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        checksum.add(&format!(
            "{}:{}",
            route_key_for_signed_route(route),
            signed_route_delivery_index(route)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_string())
        ));
    }
    checksum.value()
}

fn stable_route_stream_id(
    transfer_id: &str,
    plan_identity: &str,
    total_routes: usize,
    total_chunks: usize,
    signed_url_flow: &str,
) -> String {
    let digest = Sha256::digest(
        format!(
            "beam:route-stream:{signed_url_flow}:{transfer_id}:{plan_identity}:{total_routes}:{total_chunks}"
        )
        .as_bytes(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

fn stable_manifest_batch_identity(groups: &[MultipartGroupManifest]) -> String {
    let mut identities = groups
        .iter()
        .map(|group| {
            format!(
                "{}:{}:{}",
                group.multipart_group_id, group.source_id, group.destination_id
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    hex_bytes(&Sha256::digest(identities.join("\n").as_bytes())[..16])
}

fn route_key_for_value(route: &Value) -> Result<String, BeamApiError> {
    let source_id = route
        .get("source_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing source_id".to_string()))?;
    let destination_id = route
        .get("destination_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing destination_id".to_string()))?;
    let chunk_index = route
        .get("chunk_index")
        .and_then(Value::as_u64)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing chunk_index".to_string()))?;
    Ok(format!("{}:{}:{}", source_id, destination_id, chunk_index))
}

fn route_keys_checksum_for_values(routes: &[Value]) -> Result<String, BeamApiError> {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        checksum.add(&route_key_for_value(route)?);
    }
    Ok(checksum.value())
}

fn count_distinct_route_chunks(routes: &[SignedChunkRoute]) -> usize {
    let mut seen = std::collections::HashSet::new();
    for route in routes {
        seen.insert(format!("{}:{}", route.source_id, route.chunk_index));
    }
    seen.len()
}

struct CompactPlanChunkIter<'a> {
    descriptor: &'a CompactTransferPlanDescriptor,
    transfer_id: &'a str,
    source_index: usize,
    source_chunk_index: u64,
}

impl<'a> CompactPlanChunkIter<'a> {
    fn new(descriptor: &'a CompactTransferPlanDescriptor, transfer_id: &'a str) -> Self {
        Self {
            descriptor,
            transfer_id,
            source_index: 0,
            source_chunk_index: 0,
        }
    }
}

impl Iterator for CompactPlanChunkIter<'_> {
    type Item = Result<ChunkSigningPlanItem, BeamApiError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(source) = self.descriptor.sources.get(self.source_index) {
            if self.source_chunk_index >= source.chunk_count {
                self.source_index += 1;
                self.source_chunk_index = 0;
                continue;
            }
            let chunk_index = source.global_chunk_start + self.source_chunk_index;
            self.source_chunk_index += 1;
            return Some(
                materialize_plan_chunk(
                    self.descriptor,
                    self.transfer_id,
                    &source.source.source_id,
                    chunk_index,
                )
                .ok_or_else(|| {
                    BeamApiError::ProviderSigning(format!(
                        "plan chunk not found: {}:{chunk_index}",
                        source.source.source_id
                    ))
                }),
            );
        }
        None
    }
}

fn materialize_plan_chunk(
    descriptor: &CompactTransferPlanDescriptor,
    transfer_id: &str,
    source_id: &str,
    chunk_index: u64,
) -> Option<ChunkSigningPlanItem> {
    let source = descriptor
        .sources
        .iter()
        .find(|candidate| candidate.source.source_id == source_id)?;
    let source_chunk_index = chunk_index.checked_sub(source.global_chunk_start)?;
    if source_chunk_index >= source.chunk_count {
        return None;
    }
    let source_offset = source_chunk_index * descriptor.chunk_size;
    let chunk_size = descriptor
        .chunk_size
        .min(source.source.size.checked_sub(source_offset)?);
    let mut destinations = Vec::with_capacity(descriptor.destinations.len());
    for destination in &descriptor.destinations {
        let final_object_key = destination.final_object_keys.get(source_id)?.clone();
        let delivery_index =
            chunk_index * descriptor.destinations.len() as u64 + destination.destination_index;
        let part_number = source_chunk_index * 3 + 1;
        let mut metadata = destination.destination.metadata.clone();
        metadata.insert("final_object_key".to_string(), json!(final_object_key));
        metadata.insert("part_number".to_string(), json!(part_number));
        metadata.insert("logical_attempt_index".to_string(), json!(0));
        metadata.insert("attempt_slot".to_string(), json!(0));
        metadata.insert(
            "route_generation_id".to_string(),
            json!(format!(
                "initial-{chunk_index}-{}",
                destination.destination.destination_id
            )),
        );
        metadata.insert("delivery_index".to_string(), json!(delivery_index));
        let object_key = if destination
            .destination
            .provider
            .eq_ignore_ascii_case("hippius")
        {
            format!(
                "{}/{}/chunk-{:06}",
                final_object_key, descriptor.plan_nonce, source_chunk_index
            )
        } else {
            final_object_key.clone()
        };
        destinations.push(ChunkDestinationSigningTarget {
            destination_id: destination.destination.destination_id.clone(),
            provider: Some(destination.destination.provider.clone()),
            object_key: Some(object_key),
            metadata,
        });
    }
    Some(ChunkSigningPlanItem {
        chunk_index,
        source_id: source_id.to_string(),
        source_chunk_index,
        source_offset,
        chunk_size,
        source_url: source.source.url.clone(),
        destinations,
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}

fn default_id(prefix: &str, index: usize) -> String {
    format!("{}_{}", prefix, index)
}

fn r2_endpoint(
    account_id: Option<&str>,
    endpoint_url: Option<&str>,
) -> Result<String, BeamApiError> {
    if let Some(endpoint_url) = endpoint_url {
        return Ok(endpoint_url.to_string());
    }
    if let Some(account_id) = account_id {
        return Ok(format!("https://{}.r2.cloudflarestorage.com", account_id));
    }
    Err(BeamApiError::ProviderSigning(
        "R2 signing requires account_id or endpoint_url".to_string(),
    ))
}

fn validate_signed_route_manifest_contract(
    transfer_id: &str,
    routes: &[SignedChunkRoute],
    manifest: &[MultipartGroupManifest],
) -> Result<(), BeamApiError> {
    let mut groups = HashMap::with_capacity(manifest.len());
    for group in manifest {
        if group.multipart_group_id.is_empty() {
            return Err(BeamApiError::ProviderSigning(
                "multipart_group_id is required".to_string(),
            ));
        }
        if groups
            .insert(group.multipart_group_id.as_str(), group)
            .is_some()
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "duplicate multipart_group_id: {}",
                group.multipart_group_id
            )));
        }
        if group.source_id.is_empty()
            || group.destination_id.is_empty()
            || group.final_object_key.is_empty()
            || group.upload_id.is_empty()
            || group.complete_url.is_empty()
            || group.abort_url.is_empty()
            || group.final_head_url.is_empty()
            || group.urls_expires_at.is_empty()
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {} has missing required controls",
                group.multipart_group_id
            )));
        }
        if group.expected_object_size == 0
            || group.expected_part_count == 0
            || group.expected_part_count > 10_000
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {} has invalid expected object or part count",
                group.multipart_group_id
            )));
        }
        let expected_max_part_number = ((group.expected_part_count - 1) * 3) + 3;
        if group.max_part_number != expected_max_part_number {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {} max_part_number must equal highest reserved recovery slot",
                group.multipart_group_id
            )));
        }
        let expected_list_page_count = group.max_part_number.div_ceil(1_000) as usize;
        let unique_list_page_count = group
            .list_page_urls
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        if group.list_page_urls.len() != expected_list_page_count
            || unique_list_page_count != expected_list_page_count
            || group.list_page_urls.iter().any(String::is_empty)
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {} requires {} unique list_page_urls",
                group.multipart_group_id, expected_list_page_count
            )));
        }
        if group.final_object_metadata.len() != 1
            || group
                .final_object_metadata
                .get("beam-transfer-id")
                .map(String::as_str)
                != Some(transfer_id)
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {} has invalid final_object_metadata",
                group.multipart_group_id
            )));
        }
    }
    for route in routes {
        let upload_id = route
            .metadata
            .get("upload_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let Some(group_id) = route
            .metadata
            .get("multipart_group_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            if upload_id.is_some() {
                return Err(BeamApiError::ProviderSigning(format!(
                    "signed multipart route {}:{}:{} is missing multipart_group_id",
                    route.source_id, route.destination_id, route.chunk_index
                )));
            }
            continue;
        };
        let group = groups.get(group_id).ok_or_else(|| {
            BeamApiError::ProviderSigning(format!(
                "signed route references unknown multipart group {group_id}"
            ))
        })?;
        if route.source_id != group.source_id || route.destination_id != group.destination_id {
            return Err(BeamApiError::ProviderSigning(format!(
                "signed route identity does not match multipart group {group_id}"
            )));
        }
        if upload_id != Some(group.upload_id.as_str())
            || route
                .metadata
                .get("final_object_key")
                .and_then(Value::as_str)
                != Some(group.final_object_key.as_str())
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "signed route controls do not match multipart group {group_id}"
            )));
        }
        let part_number = route
            .metadata
            .get("part_number")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if part_number < 1 || part_number > group.max_part_number {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart part_number {part_number} is outside group {group_id} range 1-{}",
                group.max_part_number
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod signed_route_manifest_tests {
    use super::*;

    #[test]
    fn multipart_upload_route_requires_group_identity() {
        let route = SignedChunkRoute {
            source_id: "src".to_string(),
            destination_id: "dst".to_string(),
            chunk_index: 0,
            delivery_index: None,
            source_url: "https://source.example/file.bin".to_string(),
            dest_url: "https://dest.example/part-1".to_string(),
            source_offset: 0,
            chunk_size: 512,
            expires_at: None,
            headers: None,
            dest_headers: None,
            metadata: HashMap::from([
                ("upload_id".to_string(), json!("upload")),
                ("final_object_key".to_string(), json!("file.bin")),
                ("part_number".to_string(), json!(1)),
            ]),
        };

        let error = validate_signed_route_manifest_contract("transfer", &[route], &[])
            .expect_err("multipart upload route without a group must fail");
        assert!(error.to_string().contains("missing multipart_group_id"));
    }
}

fn route_coordinate_checksum_for_values(routes: &[Value]) -> Result<String, BeamApiError> {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        let delivery_index = route
            .get("delivery_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("route missing delivery_index".to_string())
            })?;
        checksum.add(&format!("{}:{delivery_index}", route_key_for_value(route)?));
    }
    Ok(checksum.value())
}

fn validate_compact_transfer_plan(
    signed_url_flow: &str,
    descriptor: &CompactTransferPlanDescriptor,
) -> Result<(), BeamApiError> {
    if signed_url_flow != "signed_url" {
        return Err(BeamApiError::ProviderSigning(format!(
            "BeamCore returned unsupported signed_url_flow: {signed_url_flow}"
        )));
    }
    if descriptor.version != "compact-transfer-plan/v1" {
        return Err(BeamApiError::ProviderSigning(format!(
            "BeamCore returned unsupported plan version: {}",
            descriptor.version
        )));
    }
    if descriptor.multipart_attempt_slots != 3 {
        return Err(BeamApiError::ProviderSigning(format!(
            "BeamCore returned unsupported multipart_attempt_slots: {}",
            descriptor.multipart_attempt_slots
        )));
    }
    let formulas = &descriptor.formulas;
    if formulas.source_offset != "source_chunk_index * chunk_size"
        || formulas.delivery_index != "chunk_index * destination_count + destination_index"
        || formulas.part_number != "source_chunk_index * 3 + attempt_slot + 1"
        || formulas.route_generation_id != "initial-{chunk_index}-{destination_id}"
    {
        return Err(BeamApiError::ProviderSigning(
            "BeamCore returned unsupported compact transfer plan formulas".to_string(),
        ));
    }
    Ok(())
}

fn transfer_id_for_idempotency_key(idempotency_key: Option<&str>) -> String {
    let Some(key) = idempotency_key
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return new_transfer_id();
    };
    stable_uuid_from_identity(&format!("beam-transfer:{key}"))
}

fn route_generation_id_for_prepare_idempotency_key(idempotency_key: &str) -> String {
    stable_uuid_from_identity(&format!("beam-route-generation:{}", idempotency_key.trim()))
}

fn stable_uuid_from_identity(identity: &str) -> String {
    let digest = Sha256::digest(identity.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

fn validate_id(value: &str, name: &'static str) -> Result<(), BeamApiError> {
    if value.is_empty() || value.contains('/') || value.contains("..") {
        return Err(BeamApiError::InvalidId(name));
    }
    Ok(())
}

fn url_escape(value: &str) -> String {
    value.replace('%', "%25").replace('/', "%2F")
}

fn filename(key: &str) -> String {
    key.split('/')
        .filter(|part| !part.is_empty())
        .next_back()
        .unwrap_or(key)
        .to_string()
}

#[cfg(test)]
mod route_batch_tests {
    use super::{
        route_generation_id_for_prepare_idempotency_key, transfer_id_for_idempotency_key,
        ROUTE_STREAM_BATCH_ROUTES,
    };

    #[test]
    fn logical_route_batches_are_maximally_filled() {
        let sizes = (0..5_120)
            .collect::<Vec<_>>()
            .chunks(ROUTE_STREAM_BATCH_ROUTES)
            .map(<[usize]>::len)
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![2_048, 2_048, 1_024]);
    }

    #[test]
    fn idempotent_prepare_reuses_route_generation() {
        let transfer_id = transfer_id_for_idempotency_key(Some("studio-step-retry"));
        let prepare_idempotency_key = format!("transfer:{transfer_id}:prepare");
        assert_eq!(
            route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key),
            route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key),
        );
    }
}

#[allow(dead_code)]
fn _keep_configs_reachable(_sources: Vec<SourceConfig>, _destinations: Vec<DestConfig>) {}


async fn huggingface_response_json(
    response: reqwest::Response,
    config: &HuggingFaceConfig,
    step: &str,
) -> Result<Value, BeamApiError> {
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| BeamApiError::ProviderSigning(error.to_string()))?;
    if status.as_u16() >= 400 {
        return Err(huggingface_status_error(status, config, step, &body));
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&body).map_err(|error| BeamApiError::ProviderSigning(error.to_string()))
}

async fn huggingface_response_status(
    response: reqwest::Response,
    config: &HuggingFaceConfig,
    step: &str,
) -> Result<(), BeamApiError> {
    let status = response.status();
    if status.as_u16() < 400 {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    Err(huggingface_status_error(status, config, step, &body))
}

fn huggingface_status_error(
    status: reqwest::StatusCode,
    config: &HuggingFaceConfig,
    step: &str,
    body: &str,
) -> BeamApiError {
    let detail: String = body.chars().take(512).collect();
    if detail.is_empty() {
        BeamApiError::ProviderSigning(format!(
            "Hugging Face {step} failed status={status} for {}",
            config.describe()
        ))
    } else {
        BeamApiError::ProviderSigning(format!(
            "Hugging Face {step} failed status={status} for {}: {detail}",
            config.describe()
        ))
    }
}


/// Fail before any byte moves if BeamCore did not adopt the Hub's part layout.
fn assert_huggingface_plan(
    prepared: &TransferPrepareResponse,
    states: &[HuggingFaceUploadState],
) -> Result<(), String> {
    for state in states {
        let plan_destination = prepared
            .plan_descriptor
            .destinations
            .iter()
            .find(|candidate| candidate.destination.destination_id == state.destination_id);
        let plan_source = prepared
            .plan_descriptor
            .sources
            .iter()
            .find(|candidate| candidate.source.source_id == state.source_id);
        let (plan_destination, plan_source) = match (plan_destination, plan_source) {
            (Some(destination), Some(source)) => (destination, source),
            _ => {
                return Err(format!(
                    "BeamCore plan is missing the huggingface coordinate {}:{}",
                    state.source_id, state.destination_id
                ))
            }
        };

        match plan_destination.final_object_keys.get(&state.source_id) {
            Some(key) if *key == state.config.path => {}
            key => {
                return Err(format!(
                    "BeamCore planned {} but the Hub upload was negotiated for {}",
                    key.map(String::as_str).unwrap_or("nothing"),
                    state.config.path
                ))
            }
        }

        let Some(chunk_size) = state.chunk_size else {
            if plan_source.chunk_count != 1 {
                return Err(format!(
                    "{} was issued a single-part upload, but the plan has {} chunks",
                    state.config.describe(),
                    plan_source.chunk_count
                ));
            }
            continue;
        };
        if prepared.plan_descriptor.chunk_size != chunk_size {
            return Err(format!(
                "the Hub requires {chunk_size}-byte parts for {}, but BeamCore planned {}-byte chunks",
                state.config.describe(),
                prepared.plan_descriptor.chunk_size
            ));
        }
        if plan_source.chunk_count as usize != state.part_urls.len() {
            return Err(format!(
                "the Hub issued {} part URLs for {}, but the plan has {} chunks",
                state.part_urls.len(),
                state.config.describe(),
                plan_source.chunk_count
            ));
        }
    }
    Ok(())
}
