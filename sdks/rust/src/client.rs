use crate::error::{sanitized_error_summary, BeamApiError};
use crate::huggingface::{self, HuggingFaceConfig, HuggingFaceFileMetadata};
use crate::nats_control::{
    is_recoverable_route_stream_error, iso_after, iso_now, NatsControl, NatsTerminalSignalWaiter,
    RecoveryFuture, RecoveryLease, RouteRecoverySignerHandle,
};
use crate::performance::{Diagnostics, SdkPerformanceSummary};
use crate::provider_flow::ProviderTransferCreateInput;
use crate::provider_signing::{
    self, sign_destination_read_range, sign_source_read_range, DestinationReadRange,
    ProviderSigningOptions, SourceReadRange,
};
use crate::route_stream::{
    count_distinct_route_chunks, materialize_plan_chunk, route_coordinate_checksum_for_routes,
    route_generation_id_for_prepare_idempotency_key, signed_route_delivery_index,
    sort_routes_by_delivery, transfer_id_for_idempotency_key, validate_compact_transfer_plan,
    validate_id, validate_multipart_group_manifest, validate_signed_route_manifest_contract,
    RouteStreamSender,
};
use crate::{
    AttachSignedUrlsResponse, DistributeResponse, HippiusProviderDestination,
    HippiusProviderSource, HuggingFaceProviderDestination, HuggingFaceProviderSource,
    IntegrityAuditChallenge, IntegrityAuditChallengeChunk, MultipartGroupManifest,
    PreparedDestination, PreparedHttpSource, ProviderDestinationConfig, ProviderSourceConfig,
    SignedChunkRoute, TransferCancelResponse, TransferCreateRequest, TransferCreateResponse,
    TransferPlanInput, TransferPlanResponse, TransferPrepareInput, TransferPrepareResponse,
    TransferStatusInfo, TransferTerminalEvent, WaitForTransferOptions,
};
use futures_util::stream::StreamExt;
use reqwest::Client as HttpClient;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Mutex as AsyncMutex},
    time::sleep,
};
use uuid::Uuid;

pub const BEAM_PROD_URL: &str = "tls://orch-gateway.b1m.ai:4222";

/// A development broker is whatever the operator runs, so it is read from the
/// environment and falls back to a local one. Publishing a fixed address here
/// would ship one deployment's private infrastructure to every installation.
pub fn beam_dev_url() -> String {
    std::env::var("BEAM_DEV_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}
/// Default SDK-side NATS message guard. NATS enforces max_payload at the broker,
/// so signed-route control messages are split before object-transfer metadata is rejected.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
/// Default number of concurrent multipart control calls (create, manifest signing, abort).
pub const DEFAULT_MULTIPART_CONTROL_CONCURRENCY: usize = 2;
const DEFAULT_ROUTE_SIGNING_CONCURRENCY: usize = 64;

/// Options for [`BeamClient::new`]. Build with `..Default::default()` so new options stay
/// source compatible.
#[derive(Clone, Default)]
pub struct BeamClientOptions {
    pub api_key: String,
    pub nats_url: Option<String>,
    pub environment: Option<String>,
    pub http_client: Option<HttpClient>,
    pub transfer_runtime_shard_count: Option<u64>,
    pub request_timeout: Option<Duration>,
    pub max_payload_bytes: Option<usize>,
    pub route_signing_concurrency: Option<usize>,
    /// Concurrency for multipart control calls. Defaults to
    /// [`DEFAULT_MULTIPART_CONTROL_CONCURRENCY`].
    pub multipart_control_concurrency: Option<usize>,
    /// Subject prefix for lifecycle subjects. Defaults to `beam.transfer.client`.
    pub transfer_client_subject_prefix: Option<String>,
}

impl std::fmt::Debug for BeamClientOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeamClientOptions")
            .field("api_key", &"<redacted>")
            .field("nats_url", &self.nats_url)
            .field("environment", &self.environment)
            .field(
                "transfer_runtime_shard_count",
                &self.transfer_runtime_shard_count,
            )
            .field("request_timeout", &self.request_timeout)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("route_signing_concurrency", &self.route_signing_concurrency)
            .field(
                "multipart_control_concurrency",
                &self.multipart_control_concurrency,
            )
            .field(
                "transfer_client_subject_prefix",
                &self.transfer_client_subject_prefix,
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct BeamClient {
    pub(crate) diagnostics: Diagnostics,
    pub(crate) http: HttpClient,
    control: NatsControl,
    pub(crate) route_signing_concurrency: usize,
    pub(crate) route_signing_concurrency_overridden: bool,
    pub(crate) multipart_control_concurrency: usize,
    huggingface_uploads: Arc<StdMutex<HashMap<String, Vec<HuggingFaceUploadState>>>>,
    integrity_contexts: Arc<StdMutex<HashMap<String, IntegrityContext>>>,
    /// One entry per audit id: concurrent submissions for the same audit serialize on it, and a
    /// successful submission is never repeated.
    integrity_submissions: Arc<StdMutex<HashMap<String, IntegrityGrantEntry>>>,
    recovery_signers: Arc<StdMutex<HashMap<String, OwnedRecoverySigner>>>,
}

/// A transfer's recovery signer and the recovery lease that installed it. A fenced-off owner may
/// only stop its own signer, never a replacement owner's.
pub(crate) struct OwnedRecoverySigner {
    owner: Weak<RecoveryLease>,
    handle: RouteRecoverySignerHandle,
    integrity: Option<RouteRecoverySignerHandle>,
}

/// What an integrity audit needs to sign grants, keyed by the prepared source and destination
/// ids (never by position: BeamCore may order the plan differently from the input).
#[derive(Clone)]
pub(crate) struct IntegrityContext {
    pub prepared: TransferPrepareResponse,
    pub sources: HashMap<String, ProviderSourceConfig>,
    pub destinations: HashMap<String, ProviderDestinationConfig>,
    pub expires_in: Duration,
}

struct IntegrityGrantEntry {
    transfer_id: String,
    fingerprint: Value,
    state: Arc<AsyncMutex<IntegrityGrantState>>,
}
#[derive(Default)]
struct IntegrityGrantState {
    payload: Option<Value>,
    expires_at: Option<std::time::Instant>,
    submitted: bool,
}

pub(crate) type PartEtags = Option<Result<Vec<String>, String>>;

/// One Hugging Face LFS upload, held from prepare until the transfer's commit.
///
/// The Hub issues upload URLs only for a known sha256 and dictates the part size, so the plan is
/// built around what the LFS batch hands back rather than the other way round.
#[derive(Clone)]
pub(crate) struct HuggingFaceUploadState {
    pub config: HuggingFaceConfig,
    pub destination_id: String,
    pub source_id: String,
    pub oid: String,
    pub size: u64,
    pub chunk_size: Option<u64>,
    pub part_urls: Vec<String>,
    pub upload_href: Option<String>,
    pub verify_href: Option<String>,
    /// Resolves alongside the transfer; only awaited at commit time.
    pub part_etags: watch::Receiver<PartEtags>,
}

pub(crate) struct ForegroundRecoveryGuard {
    control: NatsControl,
    transfer_id: String,
    owner: Arc<RecoveryLease>,
    armed: bool,
}

impl ForegroundRecoveryGuard {
    /// Hand `owner`'s transfer to background recovery if the foreground run is dropped; a lease
    /// that was replaced in the meantime is left alone.
    pub(crate) fn new(
        control: NatsControl,
        transfer_id: String,
        owner: Arc<RecoveryLease>,
    ) -> Self {
        Self {
            control,
            transfer_id,
            owner,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
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
        let owner = self.owner.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                control
                    .continue_recovery_lease(&transfer_id, Some(&owner))
                    .await;
            });
        }
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

/// Input for [`BeamClient::attach_signed_urls_with_options`].
#[derive(Clone)]
pub struct AttachSignedUrlsInput {
    pub chunk_routes: Vec<SignedChunkRoute>,
    pub multipart_group_manifest: Vec<MultipartGroupManifest>,
    pub transfer_key: Option<String>,
    pub urls_expires_at: Option<String>,
    pub route_generation_id: String,
    pub recovery: ManualRouteRecovery,
    /// Distribute once the stream completes. Defaults to `true`.
    pub auto_distribute: Option<bool>,
}

#[derive(Clone)]
pub struct TransferTerminalSignalWaiter {
    inner: NatsTerminalSignalWaiter,
}

impl TransferTerminalSignalWaiter {
    /// Wait up to `timeout` (which must be positive) for the terminal signal. Returns
    /// `Ok(None)` on timeout or after [`Self::close`].
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
                _ => beam_dev_url(),
            })
            .trim_end_matches('/')
            .to_string();
        let lowered_url = nats_url.to_ascii_lowercase();
        if lowered_url.starts_with("http://")
            || lowered_url.starts_with("https://")
            || lowered_url.starts_with("ws://")
            || lowered_url.starts_with("wss://")
        {
            return Err(BeamApiError::Nats(
                "nats_url must use nats:// or tls:// for Rust lifecycle transport".to_string(),
            ));
        }
        let shard_count = options.transfer_runtime_shard_count.unwrap_or(1);
        if shard_count == 0 {
            return Err(BeamApiError::InvalidArgument(
                "transfer_runtime_shard_count must be a positive integer".to_string(),
            ));
        }
        let request_timeout = options
            .request_timeout
            .unwrap_or_else(|| Duration::from_secs(30));
        if request_timeout.is_zero() {
            return Err(BeamApiError::InvalidArgument(
                "request_timeout must be positive".to_string(),
            ));
        }
        let max_payload_bytes = options
            .max_payload_bytes
            .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES);
        if max_payload_bytes == 0 {
            return Err(BeamApiError::InvalidArgument(
                "max_payload_bytes must be a positive integer".to_string(),
            ));
        }
        let route_signing_concurrency = positive_option(
            options.route_signing_concurrency,
            DEFAULT_ROUTE_SIGNING_CONCURRENCY,
            "route_signing_concurrency",
        )?;
        let multipart_control_concurrency = positive_option(
            options.multipart_control_concurrency,
            DEFAULT_MULTIPART_CONTROL_CONCURRENCY,
            "multipart_control_concurrency",
        )?;
        let control = NatsControl::new(
            options.api_key.clone(),
            nats_url,
            environment,
            options.transfer_client_subject_prefix.clone(),
            shard_count,
            request_timeout,
            max_payload_bytes,
        );
        Ok(Self::from_parts(
            options.http_client.unwrap_or_default(),
            control,
            route_signing_concurrency,
            options.route_signing_concurrency.is_some(),
            multipart_control_concurrency,
        ))
    }

    pub(crate) fn from_parts(
        http: HttpClient,
        control: NatsControl,
        route_signing_concurrency: usize,
        route_signing_concurrency_overridden: bool,
        multipart_control_concurrency: usize,
    ) -> Self {
        Self {
            http,
            control,
            route_signing_concurrency,
            route_signing_concurrency_overridden,
            multipart_control_concurrency,
            huggingface_uploads: Arc::new(StdMutex::new(HashMap::new())),
            integrity_contexts: Arc::new(StdMutex::new(HashMap::new())),
            integrity_submissions: Arc::new(StdMutex::new(HashMap::new())),
            diagnostics: Diagnostics::default(),
            recovery_signers: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    pub(crate) fn control(&self) -> &NatsControl {
        &self.control
    }

    pub(crate) fn huggingface_uploads(
        &self,
    ) -> &Arc<StdMutex<HashMap<String, Vec<HuggingFaceUploadState>>>> {
        &self.huggingface_uploads
    }

    /// Install `owner`'s recovery signer for a transfer, stopping any previous one.
    pub(crate) fn remember_recovery_signer(
        &self,
        transfer_id: &str,
        owner: &Arc<RecoveryLease>,
        handle: RouteRecoverySignerHandle,
    ) {
        if let Ok(mut signers) = self.recovery_signers.lock() {
            let signer = OwnedRecoverySigner {
                owner: Arc::downgrade(owner),
                handle,
                integrity: None,
            };
            if let Some(previous) = signers.insert(transfer_id.to_string(), signer) {
                previous.handle.stop();
                if let Some(handle) = previous.integrity {
                    handle.stop();
                }
            }
        }
    }

    /// Receives bounded, best-effort summaries without signed storage grants.
    pub fn on_diagnostics(&self, callback: impl Fn(SdkPerformanceSummary) + Send + Sync + 'static) {
        self.diagnostics.set(Arc::new(callback));
    }

    pub(crate) fn remember_integrity_signer(
        &self,
        transfer_id: &str,
        owner: &Arc<RecoveryLease>,
        handle: RouteRecoverySignerHandle,
    ) {
        if let Ok(mut signers) = self.recovery_signers.lock() {
            if let Some(signer) = signers.get_mut(transfer_id) {
                if Weak::ptr_eq(&signer.owner, &Arc::downgrade(owner)) {
                    if let Some(previous) = signer.integrity.replace(handle) {
                        previous.stop();
                    }
                    return;
                }
            }
        }
        handle.stop();
    }

    /// Stop answering route recovery signing and integrity audits for a transfer.
    ///
    /// With `owner`, stop only when that owner installed the current signer, so a fenced-off
    /// owner never stops a replacement owner's signer or drops its integrity context.
    pub(crate) fn stop_recovery_signer(
        &self,
        transfer_id: &str,
        owner: Option<&Arc<RecoveryLease>>,
    ) {
        let signer = {
            let Ok(mut signers) = self.recovery_signers.lock() else {
                return;
            };
            let owned = match (owner, signers.get(transfer_id)) {
                (Some(owner), Some(signer)) => Weak::ptr_eq(&signer.owner, &Arc::downgrade(owner)),
                (Some(_), None) => false,
                (None, _) => true,
            };
            if !owned {
                return;
            }
            signers.remove(transfer_id)
        };
        if let Some(signer) = signer {
            signer.handle.stop();
            if let Some(handle) = signer.integrity {
                handle.stop();
            }
        }
        self.forget_integrity_context(transfer_id);
    }

    #[cfg(test)]
    pub(crate) fn has_recovery_signer(&self, transfer_id: &str) -> bool {
        self.recovery_signers
            .lock()
            .unwrap()
            .contains_key(transfer_id)
    }

    #[cfg(test)]
    pub(crate) fn has_integrity_context(&self, transfer_id: &str) -> bool {
        self.integrity_contexts
            .lock()
            .unwrap()
            .contains_key(transfer_id)
    }

    pub(crate) fn signing_options(&self, expires_in: Duration) -> ProviderSigningOptions {
        ProviderSigningOptions {
            index: 0,
            expires_in,
            http_client: Some(self.http.clone()),
            cancellation: None,
        }
    }

    /// Close the client: stop recovery, close terminal waiters, drain the NATS connection, and
    /// drop every retained provider context.
    pub async fn close(&self) -> Result<(), BeamApiError> {
        let signers = self
            .recovery_signers
            .lock()
            .map(|mut signers| std::mem::take(&mut *signers))
            .unwrap_or_default();
        for signer in signers.into_values() {
            signer.handle.stop();
            if let Some(handle) = signer.integrity {
                handle.stop();
            }
        }
        let result = self.control.close().await;
        if let Ok(mut contexts) = self.integrity_contexts.lock() {
            contexts.clear();
        }
        if let Ok(mut uploads) = self.huggingface_uploads.lock() {
            uploads.clear();
        }
        result
    }

    /// Create a transfer from raw source and destination configs (`transfer.create`).
    ///
    /// The transfer id is `request.transfer_id` when set, otherwise derived from
    /// `request.idempotency_key` (or random). The lifecycle idempotency key is always
    /// `transfer:{transfer_id}:create`.
    pub async fn create_raw_transfer(
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
        validate_id(&transfer_id, "transfer_id")?;
        let idempotency_key = format!("transfer:{transfer_id}:create");
        let mut payload = serde_json::to_value(request)?;
        if payload
            .get("signed_url_flow")
            .and_then(Value::as_str)
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

    /// Deprecated alias of [`Self::create_raw_transfer`].
    #[deprecated(
        since = "0.4.0",
        note = "use `create_raw_transfer` for raw configs, or `create_provider_transfer` for the provider-signing flow"
    )]
    pub async fn create_transfer(
        &self,
        request: TransferCreateRequest,
    ) -> Result<TransferCreateResponse, BeamApiError> {
        self.create_raw_transfer(request).await
    }

    pub async fn create_and_distribute(
        &self,
        request: TransferCreateRequest,
    ) -> Result<TransferCreateResponse, BeamApiError> {
        let transfer = self.create_raw_transfer(request).await?;
        if transfer.success {
            self.distribute_transfer(&transfer.transfer_id).await?;
        }
        Ok(transfer)
    }

    /// Read authoritative transfer status.
    ///
    /// When the status carries an integrity audit challenge the SDK answers it with signed
    /// read grants; a failure is reported in `integrity_audit_submission_error` rather than
    /// failing the status read.
    pub async fn transfer_status(
        &self,
        transfer_id: &str,
    ) -> Result<TransferStatusInfo, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let mut result: TransferStatusInfo = self
            .lifecycle_request(
                "transfer.status",
                json!({ "transfer_id": transfer_id }),
                Some(transfer_id),
                None,
            )
            .await?;
        if let Some(challenge) = result.integrity_audit_challenge.clone() {
            if let Err(error) = self.submit_integrity_audit_grants(&challenge).await {
                result.integrity_audit_submission_error = Some(sanitized_error_summary(&error));
            }
        }
        if matches!(result.status.as_str(), "completed" | "failed" | "cancelled") {
            self.control.release_recovery_lease(transfer_id, None).await;
            self.stop_recovery_signer(transfer_id, None);
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

    pub(crate) async fn request_transfer_cancellation(
        &self,
        transfer_id: &str,
    ) -> Result<TransferCancelResponse, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let idempotency_key = format!("transfer:{transfer_id}:cancel");
        self.lifecycle_request(
            "transfer.cancel",
            json!({ "transfer_id": transfer_id }),
            Some(transfer_id),
            Some(&idempotency_key),
        )
        .await
    }

    /// Cancel a transfer. Once BeamCore answers, local recovery for the transfer stops and its
    /// retained provider context is dropped, whatever the reply's `success` flag.
    pub async fn cancel_transfer(
        &self,
        transfer_id: &str,
    ) -> Result<TransferCancelResponse, BeamApiError> {
        let result = self.request_transfer_cancellation(transfer_id).await?;
        self.control.release_recovery_lease(transfer_id, None).await;
        self.stop_recovery_signer(transfer_id, None);
        Ok(result)
    }

    pub async fn prepare_transfer(
        &self,
        sources: Vec<PreparedHttpSource>,
        destinations: Vec<PreparedDestination>,
        name: Option<String>,
        urls_expires_at: Option<String>,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_transfer_with_options(TransferPrepareInput {
            transfer_id: None,
            sources,
            destinations,
            name,
            urls_expires_at,
            idempotency_key,
            route_generation_id,
        })
        .await
    }

    /// Prepare a transfer (`transfer.prepare`) and validate the returned compact plan.
    ///
    /// The lifecycle idempotency key is `transfer:{transfer_id}:prepare`, and the route
    /// generation defaults to one derived from it, so retrying the same input is idempotent.
    pub async fn prepare_transfer_with_options(
        &self,
        input: TransferPrepareInput,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_transfer_with_request_key(input, None, None).await
    }

    /// `provider_part_size` is the part size a destination provider dictates before the plan
    /// exists (a Hugging Face LFS multipart upload); the plan must use it exactly.
    pub(crate) async fn prepare_transfer_with_request_key(
        &self,
        input: TransferPrepareInput,
        request_key: Option<String>,
        provider_part_size: Option<u64>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        let transfer_id = input
            .transfer_id
            .clone()
            .unwrap_or_else(|| transfer_id_for_idempotency_key(input.idempotency_key.as_deref()));
        validate_id(&transfer_id, "transfer_id")?;
        let prepare_idempotency_key =
            request_key.unwrap_or_else(|| format!("transfer:{transfer_id}:prepare"));
        let route_generation_id = input.route_generation_id.clone().unwrap_or_else(|| {
            route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key)
        });
        let mut body = json!({
            "transfer_id": transfer_id,
            "route_generation_id": route_generation_id,
            "sources": input.sources,
            "destinations": input.destinations,
            "signed_url_flow": "signed_url",
        });
        if let Some(name) = input.name {
            body["name"] = json!(name);
        }
        if let Some(part_size) = provider_part_size {
            body["provider_part_size"] = json!(part_size);
        }
        if let Some(expires_at) = input.urls_expires_at {
            body["urls_expires_at"] = json!(expires_at);
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

    /// Ask BeamCore for the compact plan of a transfer without creating it (`transfer.plan`).
    pub async fn plan_transfer(
        &self,
        input: TransferPlanInput,
    ) -> Result<TransferPlanResponse, BeamApiError> {
        let mut body = json!({
            "sources": input.sources,
            "destinations": input.destinations,
            "signed_url_flow": "signed_url",
        });
        if let Some(name) = input.name {
            body["name"] = json!(name);
        }
        if let Some(expires_at) = input.urls_expires_at {
            body["urls_expires_at"] = json!(expires_at);
        }
        let result: TransferPlanResponse = self
            .lifecycle_request("transfer.plan", body, None, None)
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
        let mut options = self.signing_options(expires_in);
        options.index = index;
        provider_signing::prepare_provider_source(source, &options).await
    }

    /// Attach manually signed routes (auto-distributing once the stream completes).
    #[allow(clippy::too_many_arguments)]
    pub async fn attach_signed_urls(
        &self,
        transfer_id: &str,
        chunk_routes: Vec<SignedChunkRoute>,
        multipart_group_manifest: Vec<MultipartGroupManifest>,
        transfer_key: Option<String>,
        urls_expires_at: Option<String>,
        route_generation_id: String,
        recovery: ManualRouteRecovery,
    ) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        self.attach_signed_urls_with_options(
            transfer_id,
            AttachSignedUrlsInput {
                chunk_routes,
                multipart_group_manifest,
                transfer_key,
                urls_expires_at,
                route_generation_id,
                recovery,
                auto_distribute: None,
            },
        )
        .await
    }

    /// Attach manually signed routes through a route stream.
    ///
    /// A recovery lease is registered before `route_stream.begin`; a recoverable transport
    /// failure returns [`BeamApiError::RouteRecoveryPending`] and replays in the background.
    pub async fn attach_signed_urls_with_options(
        &self,
        transfer_id: &str,
        input: AttachSignedUrlsInput,
    ) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let AttachSignedUrlsInput {
            chunk_routes,
            multipart_group_manifest,
            transfer_key: _,
            urls_expires_at,
            route_generation_id,
            recovery,
            auto_distribute,
        } = input;
        let auto_distribute = auto_distribute.unwrap_or(true);
        if route_generation_id.is_empty()
            || recovery.plan_fingerprint.is_empty()
            || recovery.coordinate_checksum.is_empty()
        {
            return Err(BeamApiError::InvalidArgument(
                "route generation and recovery factory are required by transfer-client-control/v7"
                    .to_string(),
            ));
        }
        validate_multipart_group_manifest(&multipart_group_manifest, transfer_id)?;
        validate_signed_route_manifest_contract(&chunk_routes, &multipart_group_manifest)?;
        let stream_lock = Arc::new(AsyncMutex::new(()));
        let initial_stream_guard = stream_lock.clone().lock_owned().await;
        let client = self.clone();
        let replay_transfer_id = transfer_id.to_string();
        let recovery_for_replay = recovery.clone();
        let replay_stream_lock = stream_lock.clone();
        let lease = self
            .control
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
                                auto_distribute,
                            )
                            .await?;
                        if !result.success {
                            return Err(BeamApiError::ProviderSigning(
                                result
                                    .error
                                    .unwrap_or_else(|| "route replay failed".to_string()),
                            ));
                        }
                        Ok(())
                    }) as RecoveryFuture
                }),
                dispose: None,
            })
            .await;
        let mut foreground_guard = ForegroundRecoveryGuard::new(
            self.control.clone(),
            transfer_id.to_string(),
            lease.clone(),
        );
        let result = self
            .stream_signed_routes(
                transfer_id,
                chunk_routes,
                multipart_group_manifest,
                urls_expires_at,
                route_generation_id,
                auto_distribute,
            )
            .await;
        drop(initial_stream_guard);
        foreground_guard.disarm();
        match result {
            Ok(result) => {
                if !result.success {
                    self.control
                        .release_recovery_lease(transfer_id, Some(&lease))
                        .await;
                }
                Ok(result)
            }
            Err(error) if is_recoverable_route_stream_error(&error) => {
                self.control
                    .continue_recovery_lease(transfer_id, Some(&lease))
                    .await;
                Err(BeamApiError::RouteRecoveryPending {
                    transfer_id: transfer_id.to_string(),
                    source: Box::new(error),
                })
            }
            Err(error) => {
                self.control
                    .release_recovery_lease(transfer_id, Some(&lease))
                    .await;
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
        auto_distribute: bool,
    ) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        validate_multipart_group_manifest(&multipart_group_manifest, transfer_id)?;
        validate_signed_route_manifest_contract(&chunk_routes, &multipart_group_manifest)?;
        let destination_count = chunk_routes
            .iter()
            .map(|route| route.destination_id.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();
        for route in &mut chunk_routes {
            if signed_route_delivery_index(route).is_none() {
                if destination_count != 1 {
                    return Err(BeamApiError::InvalidArgument(
                        "delivery_index is required when manually attaching routes for multiple destinations"
                            .to_string(),
                    ));
                }
                route.delivery_index = Some(route.chunk_index);
            }
        }
        sort_routes_by_delivery(&mut chunk_routes);
        let plan_identity = format!(
            "{}:{}",
            route_coordinate_checksum_for_routes(&chunk_routes),
            route_generation_id
        );
        let total_chunks = count_distinct_route_chunks(&chunk_routes);
        let mut sender = RouteStreamSender::new(
            self,
            transfer_id.to_string(),
            chunk_routes.len(),
            total_chunks,
            auto_distribute,
            urls_expires_at,
            plan_identity,
            route_generation_id,
        );
        sender.begin().await?;
        sender
            .add_manifest_groups(&multipart_group_manifest)
            .await?;
        for route in chunk_routes {
            sender.add_route(route).await?;
        }
        sender.complete().await
    }

    /// Prepare, sign, and attach a Hugging Face Hub transfer.
    ///
    /// The Hub token stays local: BeamCore receives the presigned CDN URL for each source and
    /// the Hub's own presigned part URLs for each destination. A thin wrapper over
    /// [`Self::prepare_provider_transfer`].
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_huggingface_provider_transfer(
        &self,
        sources: Vec<HuggingFaceProviderSource>,
        destinations: Vec<HuggingFaceProviderDestination>,
        name: Option<String>,
        expires_in: Duration,
        distribute: bool,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_provider_transfer(ProviderTransferCreateInput {
            sources: sources
                .into_iter()
                .map(ProviderSourceConfig::HuggingFace)
                .collect(),
            destinations: destinations
                .into_iter()
                .map(ProviderDestinationConfig::HuggingFace)
                .collect(),
            name,
            expires_in: (!expires_in.is_zero()).then_some(expires_in),
            distribute: Some(distribute),
            route_generation_id,
            idempotency_key,
            ..Default::default()
        })
        .await
    }

    /// Close every Hugging Face upload for a transfer: complete the LFS multipart, verify it, and
    /// commit the blob so the file appears in the repo.
    ///
    /// [`Self::wait_for_transfer`] calls this once the transfer completes. The part ETag pass
    /// runs alongside the transfer; this waits for it if it has not finished.
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
                let etags = wait_for_part_etags(state.part_etags.clone())
                    .await
                    .map_err(BeamApiError::ProviderSigning)?;
                if etags.len() != state.part_urls.len() {
                    return Err(BeamApiError::ProviderSigning(format!(
                        "computed {} part ETags for {}, expected {}",
                        etags.len(),
                        state.config.describe(),
                        state.part_urls.len()
                    )));
                }
                huggingface::http::complete_lfs_upload(
                    &self.http,
                    &state.config,
                    href,
                    &state.oid,
                    &etags,
                )
                .await?;
            }
            if let Some(href) = state.verify_href.as_deref() {
                huggingface::http::verify_lfs_upload(
                    &self.http,
                    &state.config,
                    href,
                    &state.oid,
                    state.size,
                )
                .await?;
            }
            huggingface::http::commit(&self.http, &state.config, &state.oid, state.size).await?;
        }
        Ok(())
    }

    /// Prepare, sign, and attach a Hippius transfer. A thin wrapper over
    /// [`Self::prepare_provider_transfer`].
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_hippius_provider_transfer(
        &self,
        sources: Vec<HippiusProviderSource>,
        destinations: Vec<HippiusProviderDestination>,
        name: Option<String>,
        expires_in: Duration,
        distribute: bool,
        route_generation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_provider_transfer(ProviderTransferCreateInput {
            sources: sources
                .into_iter()
                .map(ProviderSourceConfig::Hippius)
                .collect(),
            destinations: destinations
                .into_iter()
                .map(ProviderDestinationConfig::Hippius)
                .collect(),
            name,
            expires_in: (!expires_in.is_zero()).then_some(expires_in),
            distribute: Some(distribute),
            route_generation_id,
            idempotency_key,
            ..Default::default()
        })
        .await
    }

    /// Wait until a transfer reaches a terminal status. Zero durations select the defaults
    /// (300 s timeout, 15 s initial poll interval).
    ///
    /// A failed transfer returns [`BeamApiError::StorageAccessDenied`] when storage refused
    /// Beam's requests, otherwise [`BeamApiError::TransferFailed`].
    pub async fn wait_for_transfer(
        &self,
        transfer_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<TransferStatusInfo, BeamApiError> {
        self.wait_for_transfer_with_options(
            transfer_id,
            WaitForTransferOptions {
                timeout: (!timeout.is_zero()).then_some(timeout),
                poll_interval: (!poll_interval.is_zero()).then_some(poll_interval),
                max_poll_interval: None,
            },
        )
        .await
    }

    /// Wait until a transfer reaches a terminal status.
    ///
    /// The SDK subscribes to the terminal signal and reconciles through authoritative status;
    /// without a signal the poll interval backs off by 1.5x up to `max_poll_interval`. On
    /// completion, Hugging Face uploads are committed before the status is returned.
    ///
    /// A failed transfer returns [`BeamApiError::StorageAccessDenied`] when storage refused
    /// Beam's requests, otherwise [`BeamApiError::TransferFailed`].
    pub async fn wait_for_transfer_with_options(
        &self,
        transfer_id: &str,
        options: WaitForTransferOptions,
    ) -> Result<TransferStatusInfo, BeamApiError> {
        validate_id(transfer_id, "transfer_id")?;
        let timeout = positive_duration(options.timeout, Duration::from_secs(300), "timeout")?;
        let poll_interval = positive_duration(
            options.poll_interval,
            Duration::from_secs(15),
            "poll_interval",
        )?;
        let max_poll_interval = positive_duration(
            options.max_poll_interval,
            Duration::from_secs(30),
            "max_poll_interval",
        )?
        .max(poll_interval);
        let mut current_poll_interval = poll_interval;
        let mut terminal_waiter = self.open_transfer_terminal_waiter(transfer_id).await.ok();
        let started = Instant::now();
        let result = async {
            loop {
                let status = self.transfer_status(transfer_id).await?;
                match status.status.as_str() {
                    "completed" => {
                        // The parts have landed; publish them as a Hub commit before reporting
                        // success.
                        self.finalize_huggingface_uploads(transfer_id).await?;
                        return Ok(status);
                    }
                    "failed" => return Err(BeamApiError::transfer_failed(status.error_message)),
                    "cancelled" => return Err(BeamApiError::TransferCancelled),
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
                let wait_for = current_poll_interval
                    .mul_f64(jitter)
                    .min(timeout - elapsed)
                    .max(Duration::from_millis(1));
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
                current_poll_interval = if signal_received {
                    poll_interval
                } else {
                    current_poll_interval.mul_f64(1.5).min(max_poll_interval)
                };
            }
        }
        .await;
        if let Some(waiter) = terminal_waiter {
            let _ = waiter.close().await;
        }
        result
    }

    pub(crate) async fn supports_performance_v2(&self, transfer_id: &str) -> bool {
        self.control.supports_performance_v2(transfer_id).await
    }

    pub(crate) async fn lifecycle_request<T: DeserializeOwned>(
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
        huggingface::http::file_metadata(config).await
    }

    pub(crate) fn remember_integrity_context(&self, context: IntegrityContext) {
        self.forget_integrity_context(&context.prepared.transfer_id);
        if let Ok(mut contexts) = self.integrity_contexts.lock() {
            contexts.insert(context.prepared.transfer_id.clone(), context);
        }
    }

    pub(crate) fn forget_integrity_context(&self, transfer_id: &str) {
        if let Ok(mut entries) = self.integrity_submissions.lock() {
            entries.retain(|_, entry| entry.transfer_id != transfer_id);
        }
        if let Ok(mut contexts) = self.integrity_contexts.lock() {
            contexts.remove(transfer_id);
        }
    }

    fn integrity_slot(
        &self,
        challenge: &IntegrityAuditChallenge,
    ) -> Result<Arc<AsyncMutex<IntegrityGrantState>>, BeamApiError> {
        let fingerprint = serde_json::to_value(challenge)?;
        let mut entries = self.integrity_submissions.lock().map_err(|_| {
            BeamApiError::ProviderSigning("integrity signing state unavailable".into())
        })?;
        if let Some(entry) = entries.get(&challenge.audit_id) {
            if entry.fingerprint != fingerprint {
                return Err(BeamApiError::ProviderSigning(
                    "conflicting integrity challenge".into(),
                ));
            }
            return Ok(entry.state.clone());
        }
        if entries.len() >= 1024 {
            return Err(BeamApiError::ProviderSigning(
                "integrity signer capacity unavailable".into(),
            ));
        }
        let state = Arc::new(AsyncMutex::new(IntegrityGrantState::default()));
        entries.insert(
            challenge.audit_id.clone(),
            IntegrityGrantEntry {
                transfer_id: challenge.transfer_id.clone(),
                fingerprint,
                state: state.clone(),
            },
        );
        Ok(state)
    }

    async fn fill_integrity_grants(
        &self,
        challenge: &IntegrityAuditChallenge,
        state: &mut IntegrityGrantState,
    ) -> Result<Value, BeamApiError> {
        if state
            .expires_at
            .is_some_and(|expiry| expiry > std::time::Instant::now())
        {
            if let Some(payload) = &state.payload {
                return Ok(payload.clone());
            }
        }
        let context = self
            .integrity_contexts
            .lock()
            .ok()
            .and_then(|contexts| contexts.get(&challenge.transfer_id).cloned())
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("integrity audit signer unavailable".into())
            })?;
        let mut wall_expiry = std::time::SystemTime::now() + context.expires_in;
        let payload = self
            .build_provider_integrity_audit_grants(&context, challenge)
            .await?;
        if let Some(chunks) = payload["chunks"].as_array() {
            for chunk in chunks {
                for side in ["source", "destination"] {
                    if let Some(url) = chunk[side]["url"].as_str() {
                        wall_expiry = provider_signing::bounded_grant_expiry(url, wall_expiry);
                    }
                }
            }
        }
        let remaining = wall_expiry
            .duration_since(std::time::SystemTime::now())
            .unwrap_or_default();
        state.payload = Some(payload.clone());
        state.expires_at = Some(std::time::Instant::now() + remaining);
        state.submitted = false;
        Ok(payload)
    }

    pub(crate) async fn signed_integrity_grants(
        &self,
        challenge: &IntegrityAuditChallenge,
    ) -> Result<Value, BeamApiError> {
        let slot = self.integrity_slot(challenge)?;
        let mut state = slot.lock().await;
        self.fill_integrity_grants(challenge, &mut state).await
    }

    async fn submit_integrity_audit_grants(
        &self,
        challenge: &IntegrityAuditChallenge,
    ) -> Result<(), BeamApiError> {
        let slot = self.integrity_slot(challenge)?;
        let mut state = slot.lock().await;
        let payload = self.fill_integrity_grants(challenge, &mut state).await?;
        if state.submitted {
            return Ok(());
        }
        let key = format!(
            "transfer:{}:integrity-audit:{}:{}",
            challenge.transfer_id,
            challenge.audit_id,
            payload["submitted_at"].as_str().unwrap_or_default()
        );
        let receipt: Value = self
            .lifecycle_request(
                "transfer.integrity_audit_grants",
                payload,
                Some(&challenge.transfer_id),
                Some(&key),
            )
            .await?;
        if receipt["published"] != true {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit delivery unavailable".into(),
            ));
        }
        state.submitted = true;
        Ok(())
    }

    async fn build_provider_integrity_audit_grants(
        &self,
        context: &IntegrityContext,
        challenge: &IntegrityAuditChallenge,
    ) -> Result<Value, BeamApiError> {
        if challenge.transfer_id != context.prepared.transfer_id {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit challenge transfer mismatch".to_string(),
            ));
        }
        let submitted_at = iso_now();
        let concurrency = self
            .route_signing_concurrency
            .min(challenge.chunks.len().max(1));
        let chunks = futures_util::stream::iter(challenge.chunks.iter().cloned())
            .map(|chunk| self.sign_integrity_chunk(context, chunk))
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(
            json!({ "transfer_id": challenge.transfer_id, "audit_id": challenge.audit_id, "submitted_at": submitted_at, "chunks": chunks }),
        )
    }

    async fn sign_integrity_chunk(
        &self,
        context: &IntegrityContext,
        chunk: IntegrityAuditChallengeChunk,
    ) -> Result<Value, BeamApiError> {
        let plan = materialize_plan_chunk(
            &context.prepared.plan_descriptor,
            &context.prepared.transfer_id,
            &chunk.source_id,
            chunk.route_chunk_index,
        )?;
        if chunk.source_offset < plan.source_offset
            || chunk
                .source_offset
                .checked_add(chunk.range_length)
                .is_none_or(|end| end > plan.source_offset + plan.chunk_size)
        {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit source range is outside the planned chunk".to_string(),
            ));
        }
        let target = plan.destinations.iter().find(|target| {
            target.destination_id == chunk.destination_id
                && target
                    .metadata
                    .get("delivery_index")
                    .and_then(Value::as_u64)
                    == Some(chunk.delivery_index)
        });
        if !target.is_some_and(|target| {
            target
                .metadata
                .get("final_object_key")
                .and_then(Value::as_str)
                == Some(chunk.final_object_key.as_str())
        }) {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit destination coordinate mismatch".to_string(),
            ));
        }
        if chunk.destination_offset != chunk.source_offset {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit destination range mismatch".to_string(),
            ));
        }
        let (Some(source), Some(destination)) = (
            context.sources.get(&chunk.source_id),
            context.destinations.get(&chunk.destination_id),
        ) else {
            return Err(BeamApiError::ProviderSigning(
                "integrity audit provider configuration is unavailable".to_string(),
            ));
        };
        let planned_source = context
            .prepared
            .plan_descriptor
            .sources
            .iter()
            .find(|candidate| candidate.source.source_id == chunk.source_id);
        let planned_metadata = |key: &str| {
            planned_source
                .and_then(|source| source.source.metadata.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let options = self.signing_options(context.expires_in);
        let source_range = SourceReadRange {
            offset: chunk.source_offset,
            length: chunk.range_length,
            if_match: planned_metadata("etag"),
            version_id: planned_metadata("version_id"),
        };
        let destination_range = DestinationReadRange {
            object_key: chunk.final_object_key.clone(),
            offset: chunk.destination_offset,
            length: chunk.range_length,
            if_match: chunk.final_object_etag.clone(),
        };
        let grant_expires_at = std::time::SystemTime::now() + context.expires_in;
        let (source_grant, destination_grant) = tokio::try_join!(
            sign_source_read_range(source, &source_range, &options),
            sign_destination_read_range(destination, &destination_range, &options)
        )?;
        let mut grant = serde_json::to_value(&chunk)?;
        if let Some(grant) = grant.as_object_mut() {
            for key in ["orchestrator_id", "orchestrator_hotkey", "worker_id"] {
                grant.remove(key);
            }
            grant.insert(
                "source".to_string(),
                json!({
                    "url": source_grant.url,
                    "headers": source_grant.headers,
                    "expires_at": crate::nats_control::iso_at(crate::provider_signing::bounded_grant_expiry(&source_grant.url, grant_expires_at)),
                }),
            );
            grant.insert(
                "destination".to_string(),
                json!({
                    "url": destination_grant.url,
                    "headers": destination_grant.headers,
                    "expires_at": crate::nats_control::iso_at(crate::provider_signing::bounded_grant_expiry(&destination_grant.url, grant_expires_at)),
                }),
            );
        }
        Ok(grant)
    }
}

/// Describe a destination for `transfer.prepare`. See
/// [`crate::provider_signing::prepare_provider_destination`].
pub fn prepare_provider_destination_config(
    destination: &ProviderDestinationConfig,
    index: usize,
) -> Result<PreparedDestination, BeamApiError> {
    provider_signing::prepare_provider_destination(destination, index)
}

async fn wait_for_part_etags(
    mut receiver: watch::Receiver<PartEtags>,
) -> Result<Vec<String>, String> {
    let value = receiver
        .wait_for(Option::is_some)
        .await
        .map_err(|_| "the part ETag pass ended without a result".to_string())?;
    value
        .clone()
        .unwrap_or_else(|| Err("the part ETag pass ended without a result".to_string()))
}

const PART_ROUTE_METADATA_KEYS: &[&str] = &[
    "part_number",
    "logical_attempt_index",
    "attempt_slot",
    "route_generation_id",
    "delivery_index",
    "etag_required",
];

/// Route metadata the signed route carries: only the attempt coordinates.
pub(crate) fn part_route_metadata(metadata: &HashMap<String, Value>) -> HashMap<String, Value> {
    metadata
        .iter()
        .filter(|(key, _)| PART_ROUTE_METADATA_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Route metadata for a destination BeamCore does not treat as an S3 multipart target. It
/// rejects `part_number` there as an unexpected multipart signal, so drop it.
pub(crate) fn direct_put_route_metadata(
    metadata: &HashMap<String, Value>,
) -> HashMap<String, Value> {
    let mut metadata = part_route_metadata(metadata);
    metadata.remove("part_number");
    metadata
}

fn positive_option(
    value: Option<usize>,
    default: usize,
    name: &str,
) -> Result<usize, BeamApiError> {
    match value {
        None => Ok(default),
        Some(0) => Err(BeamApiError::InvalidArgument(format!(
            "{name} must be a positive integer"
        ))),
        Some(value) => Ok(value),
    }
}

fn positive_duration(
    value: Option<Duration>,
    default: Duration,
    name: &str,
) -> Result<Duration, BeamApiError> {
    match value {
        None => Ok(default),
        Some(value) if value.is_zero() => Err(BeamApiError::InvalidArgument(format!(
            "{name} must be a positive duration"
        ))),
        Some(value) => Ok(value),
    }
}

/// Fail before any byte moves if BeamCore did not adopt the Hub's part layout.
pub(crate) fn assert_huggingface_plan(
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

#[cfg(test)]
pub(crate) mod tests;
