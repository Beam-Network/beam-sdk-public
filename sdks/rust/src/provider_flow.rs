//! The provider-signing transfer flow: prepare a transfer from provider configs, create multipart
//! uploads, sign every route locally and stream it to BeamCore, with background recovery.
//!
//! Mirrors the TypeScript SDK's `prepareProviderTransfer` / `resumeProviderTransfer`.

use crate::client::{
    assert_huggingface_plan, direct_put_route_metadata, part_route_metadata, BeamClient,
    ForegroundRecoveryGuard, HuggingFaceUploadState, IntegrityContext,
};
use crate::error::BeamApiError;
use crate::huggingface::{self, HuggingFaceConfig};
use crate::multipart_limits::{
    multipart_list_page_index, multipart_list_page_markers, multipart_max_part_number,
    multipart_part_number,
};
use crate::nats_control::{
    is_recoverable_route_stream_error, iso_after, iso_now, new_transfer_id, RecoveryFuture,
    RecoveryLease, RouteRecoverySignChunk, RouteRecoverySignHandler, RouteRecoverySignReplyPayload,
    RouteRecoverySignRequestPayload,
};
use crate::provider_signing::{
    self, abort_multipart_upload, create_multipart_upload, sign_abort_multipart_upload,
    sign_complete_multipart_upload, sign_destination_route, sign_final_object_head,
    sign_list_multipart_upload, sign_multipart_recovery, DestinationRouteInput, ListPartsPage,
    MultipartRecoverySignInput, ProviderSigningOptions,
};
use crate::provider_signing::{sign_source_chunk, SourceChunkGrant};
use crate::route_stream::{
    materialize_plan_chunk, signed_route_delivery_index, validate_id,
    validate_multipart_group_manifest, validate_multipart_part_number, CompactPlanChunkIter,
    RouteStreamHandle, RouteStreamSender, ROUTE_STREAM_BATCH_ROUTES,
};
use crate::{
    ChunkDestinationSigningTarget, ChunkSigningPlanItem, CompactTransferPlanSource,
    MultipartGroupManifest, PreparedDestination, PreparedHttpSource, ProviderDestinationConfig,
    ProviderMultipartGroupIdentity, ProviderSourceConfig, SignedChunkRoute, SignedUrlFlow,
    TransferPrepareInput, TransferPrepareResponse,
};
use futures_util::{
    future::{try_join_all, BoxFuture},
    stream::{self, FuturesUnordered, StreamExt},
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex, OnceLock, Weak,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Mutex as AsyncMutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_EXPIRES_IN: Duration = Duration::from_secs(3600);

/// Future returned by provider transfer callbacks.
pub type ProviderCallbackFuture = Pin<Box<dyn Future<Output = Result<(), BeamApiError>> + Send>>;
/// Called once sources are resolved, before `transfer.prepare`.
pub type BeforeTransferPrepareCallback = Arc<dyn Fn() -> ProviderCallbackFuture + Send + Sync>;
/// Called with the prepared transfer before any route is streamed.
pub type TransferPreparedCallback =
    Arc<dyn Fn(TransferPrepareResponse) -> ProviderCallbackFuture + Send + Sync>;
/// Called after a multipart upload is created and before its routes are streamed. The identity
/// carries no credentials, headers or signed URLs; persist it to resume the transfer. An error
/// aborts the upload and fails the transfer closed.
pub type MultipartGroupReadyCallback =
    Arc<dyn Fn(ProviderMultipartGroupIdentity) -> ProviderCallbackFuture + Send + Sync>;
/// Polled between steps of the initial run (with the transfer id once prepared); an error stops
/// the foreground run and hands the transfer to background recovery.
pub type ThrowIfCancelledCallback =
    Arc<dyn Fn(Option<String>) -> ProviderCallbackFuture + Send + Sync>;

/// Input for [`BeamClient::prepare_provider_transfer`].
#[derive(Clone, Default)]
pub struct ProviderTransferCreateInput {
    /// Ownership fence. Cancelling stops signing and replay without cancelling the transfer,
    /// so a replacement owner can resume it.
    pub cancellation: Option<CancellationToken>,
    pub sources: Vec<ProviderSourceConfig>,
    pub destinations: Vec<ProviderDestinationConfig>,
    pub name: Option<String>,
    /// Lifetime of signed URLs. Defaults to one hour.
    pub expires_in: Option<Duration>,
    /// Defaults to `true`. `false` prepares and streams signed routes without distributing.
    pub distribute: Option<bool>,
    pub on_before_transfer_prepare: Option<BeforeTransferPrepareCallback>,
    pub on_prepared: Option<TransferPreparedCallback>,
    pub on_multipart_group_ready: Option<MultipartGroupReadyCallback>,
    pub throw_if_cancelled: Option<ThrowIfCancelledCallback>,
    pub signed_url_flow: Option<SignedUrlFlow>,
    /// Stable caller identity used to derive the transfer and lifecycle request ids.
    pub idempotency_key: Option<String>,
    /// Internal recovery generation; callers normally leave this unset.
    pub route_generation_id: Option<String>,
}

/// Input for [`BeamClient::resume_provider_transfer`].
#[derive(Clone, Default)]
pub struct ProviderTransferResumeInput {
    pub transfer_id: String,
    /// Every multipart identity reported through `on_multipart_group_ready`. Resume reuses these
    /// uploads and never creates replacements.
    pub multipart_groups: Vec<ProviderMultipartGroupIdentity>,
    pub cancellation: Option<CancellationToken>,
    pub sources: Vec<ProviderSourceConfig>,
    pub destinations: Vec<ProviderDestinationConfig>,
    pub name: Option<String>,
    pub expires_in: Option<Duration>,
    pub distribute: Option<bool>,
    pub on_before_transfer_prepare: Option<BeforeTransferPrepareCallback>,
    pub on_prepared: Option<TransferPreparedCallback>,
    pub on_multipart_group_ready: Option<MultipartGroupReadyCallback>,
    pub throw_if_cancelled: Option<ThrowIfCancelledCallback>,
    pub signed_url_flow: Option<SignedUrlFlow>,
}

impl fmt::Debug for ProviderTransferCreateInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTransferCreateInput")
            .field("sources", &self.sources.len())
            .field("destinations", &self.destinations.len())
            .field("name", &self.name)
            .field("expires_in", &self.expires_in)
            .field("distribute", &self.distribute)
            .field("idempotency_key", &self.idempotency_key)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ProviderTransferResumeInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTransferResumeInput")
            .field("transfer_id", &self.transfer_id)
            .field("multipart_groups", &self.multipart_groups)
            .field("sources", &self.sources.len())
            .field("destinations", &self.destinations.len())
            .finish_non_exhaustive()
    }
}

impl From<ProviderTransferResumeInput> for ProviderTransferCreateInput {
    fn from(input: ProviderTransferResumeInput) -> Self {
        Self {
            cancellation: input.cancellation,
            sources: input.sources,
            destinations: input.destinations,
            name: input.name,
            expires_in: input.expires_in,
            distribute: input.distribute,
            on_before_transfer_prepare: input.on_before_transfer_prepare,
            on_prepared: input.on_prepared,
            on_multipart_group_ready: input.on_multipart_group_ready,
            throw_if_cancelled: input.throw_if_cancelled,
            signed_url_flow: input.signed_url_flow,
            idempotency_key: None,
            route_generation_id: None,
        }
    }
}

struct ResumeState {
    transfer_id: String,
    multipart_groups: Vec<ProviderMultipartGroupIdentity>,
}

/// Provider configs retained for recovery, keyed by prepared id. Dropped when the recovery
/// lease is released, which scrubs the credentials from memory.
struct RetainedProviders {
    sources: HashMap<String, ProviderSourceConfig>,
    destinations: HashMap<String, ProviderDestinationConfig>,
}

#[derive(Clone)]
struct MultipartUpload {
    destination: ProviderDestinationConfig,
    object_key: String,
    upload_id: String,
    manifest: Option<MultipartGroupManifest>,
}

type GroupSignal = Option<Result<Arc<MultipartUpload>, ()>>;

enum RouteFailure {
    Error(BeamApiError),
    /// The route's multipart group failed; the manifest task holds the cause.
    GroupFailed,
}

impl From<BeamApiError> for RouteFailure {
    fn from(error: BeamApiError) -> Self {
        RouteFailure::Error(error)
    }
}

struct ProviderRun {
    source_history: Arc<StdMutex<crate::performance::SourceSignatureHistory>>,
    preparation_started: Instant,
    discovery_duration: Duration,
    discovery_metrics: Arc<StdMutex<crate::performance::Collector>>,
    client: BeamClient,
    prepared: TransferPrepareResponse,
    retained: StdMutex<Option<Arc<RetainedProviders>>>,
    huggingface: HashMap<String, HuggingFaceUploadState>,
    /// Uploads this run created or restored, for reuse by replays and for cleanup.
    multipart_uploads: StdMutex<HashMap<String, MultipartUpload>>,
    /// Fully signed groups, for the route recovery signer.
    recovery_uploads: StdMutex<HashMap<String, Arc<MultipartUpload>>>,
    expires_in: Duration,
    auto_distribute: bool,
    cancellation: Option<CancellationToken>,
    initial_throw_if_cancelled: StdMutex<Option<ThrowIfCancelledCallback>>,
    on_multipart_group_ready: Option<MultipartGroupReadyCallback>,
    /// This run's recovery lease: its identity as an owner. Releases, recovery requests and
    /// signer shutdowns only act while this lease is still the registered one. Weak because the
    /// lease's replay closure holds the run.
    owner: OnceLock<Weak<RecoveryLease>>,
}

impl BeamClient {
    /// Prepare a transfer from provider configs, sign every route locally and stream the routes
    /// to BeamCore. Sources and destinations may mix providers.
    ///
    /// Provider credentials never leave this process. A recovery lease is registered before the
    /// route stream begins; a recoverable transport failure returns
    /// [`BeamApiError::RouteRecoveryPending`] while recovery continues in the background. Any
    /// other failure cancels the transfer, aborts the multipart uploads this call created and
    /// returns [`BeamApiError::ProviderTransfer`].
    pub async fn prepare_provider_transfer(
        &self,
        input: ProviderTransferCreateInput,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.execute_provider_transfer(input, None).await
    }

    /// Alias of [`Self::prepare_provider_transfer`], matching the TypeScript SDK's
    /// `createTransfer`.
    pub async fn create_provider_transfer(
        &self,
        input: ProviderTransferCreateInput,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        self.prepare_provider_transfer(input).await
    }

    /// Resume a provider transfer after the original owner stopped, reusing its multipart
    /// uploads. Every multipart group of the plan must be listed; otherwise the call fails with
    /// a `provider_multipart_recovery_*` error before any upload is touched.
    pub async fn resume_provider_transfer(
        &self,
        input: ProviderTransferResumeInput,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        validate_id(&input.transfer_id, "transfer_id")?;
        let resume = ResumeState {
            transfer_id: input.transfer_id.clone(),
            multipart_groups: input.multipart_groups.clone(),
        };
        self.execute_provider_transfer(input.into(), Some(resume))
            .await
    }

    async fn execute_provider_transfer(
        &self,
        input: ProviderTransferCreateInput,
        resume: Option<ResumeState>,
    ) -> Result<TransferPrepareResponse, BeamApiError> {
        let ownership = input.cancellation.clone();
        let assert_owned = || match &ownership {
            Some(token) if token.is_cancelled() => Err(BeamApiError::Aborted),
            _ => Ok(()),
        };
        assert_owned()?;
        let expires_in = input
            .expires_in
            .filter(|value| !value.is_zero())
            .unwrap_or(DEFAULT_EXPIRES_IN);
        let preparation_started = Instant::now();
        let discovery_metrics = Arc::new(StdMutex::new(crate::performance::Collector::new()));
        let prepared_destinations = input
            .destinations
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                provider_signing::prepare_provider_destination(destination, index)
            })
            .collect::<Result<Vec<_>, _>>()?;

        call_throw_if_cancelled(&input.throw_if_cancelled, None).await?;
        let signing = ProviderSigningOptions {
            index: 0,
            expires_in,
            http_client: Some(self.http.clone()),
            cancellation: ownership.clone(),
        };
        let prepared_sources =
            try_join_all(input.sources.iter().enumerate().map(|(index, source)| {
                let mut options = signing.clone();
                options.index = index;
                let metrics = discovery_metrics.clone();
                async move {
                    crate::performance::CURRENT
                        .scope(
                            metrics,
                            provider_signing::prepare_provider_source(source, &options),
                        )
                        .await
                }
            }))
            .await?;
        call_throw_if_cancelled(&input.throw_if_cancelled, None).await?;
        let (huggingface_states, provider_part_size) = self
            .plan_huggingface_uploads(
                &input.sources,
                &prepared_sources,
                &input.destinations,
                &prepared_destinations,
            )
            .await?;
        let discovery_duration = preparation_started.elapsed();
        call_throw_if_cancelled(&input.throw_if_cancelled, None).await?;
        if let Some(callback) = &input.on_before_transfer_prepare {
            callback().await?;
        }
        assert_owned()?;

        let prepare_input = TransferPrepareInput {
            transfer_id: resume.as_ref().map(|resume| resume.transfer_id.clone()),
            sources: prepared_sources.clone(),
            destinations: prepared_destinations.clone(),
            name: input.name.clone(),
            urls_expires_at: None,
            idempotency_key: if resume.is_some() {
                None
            } else {
                input.idempotency_key.clone()
            },
            route_generation_id: if resume.is_some() {
                None
            } else {
                input.route_generation_id.clone()
            },
        };
        let request_key = resume.as_ref().map(|resume| {
            format!(
                "transfer:{}:prepare:resume:{}",
                resume.transfer_id,
                new_transfer_id()
            )
        });
        let prepared = self
            .prepare_transfer_with_request_key(prepare_input, request_key, provider_part_size)
            .await?;
        if let Some(resume) = &resume {
            if prepared.transfer_id != resume.transfer_id {
                return Err(BeamApiError::ProviderSigning(
                    "resumed provider transfer id mismatch".to_string(),
                ));
            }
        }
        assert_owned()?;
        if !prepared.success {
            return Ok(prepared);
        }
        if !huggingface_states.is_empty() {
            assert_huggingface_plan(&prepared, &huggingface_states)
                .map_err(BeamApiError::ProviderSigning)?;
            self.remember_huggingface_uploads(&prepared.transfer_id, huggingface_states.clone());
        }

        let sources: HashMap<String, ProviderSourceConfig> = prepared_sources
            .iter()
            .zip(input.sources.iter().cloned())
            .map(|(prepared, source)| (prepared.source_id.clone(), source))
            .collect();
        let destinations: HashMap<String, ProviderDestinationConfig> = prepared_destinations
            .iter()
            .zip(input.destinations.iter().cloned())
            .map(|(prepared, destination)| (prepared.destination_id.clone(), destination))
            .collect();

        let mut multipart_uploads = HashMap::new();
        if let Some(resume) = &resume {
            restore_provider_multipart_identities(
                &prepared,
                &destinations,
                &resume.multipart_groups,
                &mut multipart_uploads,
            )?;
        }

        let run = Arc::new(ProviderRun {
            source_history: Arc::new(StdMutex::new(
                crate::performance::SourceSignatureHistory::new(resume.is_none()),
            )),
            preparation_started,
            discovery_duration,
            discovery_metrics,
            client: self.clone(),
            prepared: prepared.clone(),
            retained: StdMutex::new(Some(Arc::new(RetainedProviders {
                sources: sources.clone(),
                destinations: destinations.clone(),
            }))),
            huggingface: huggingface_states
                .into_iter()
                .map(|state| (state.destination_id.clone(), state))
                .collect(),
            multipart_uploads: StdMutex::new(multipart_uploads),
            recovery_uploads: StdMutex::new(HashMap::new()),
            expires_in,
            auto_distribute: input.distribute != Some(false),
            cancellation: ownership.clone(),
            initial_throw_if_cancelled: StdMutex::new(input.throw_if_cancelled.clone()),
            on_multipart_group_ready: input.on_multipart_group_ready.clone(),
            owner: OnceLock::new(),
        });
        let route_stream_lock = Arc::new(AsyncMutex::new(()));
        let initial_stream_guard = route_stream_lock.clone().lock_owned().await;

        // The lease is this owner's identity; it is registered once the signer is serving.
        let listener_stop = CancellationToken::new();
        let replay_run = run.clone();
        let replay_lock = route_stream_lock.clone();
        let dispose_run = run.clone();
        let dispose_listener = listener_stop.clone();
        let lease = Arc::new(RecoveryLease {
            transfer_id: prepared.transfer_id.clone(),
            plan_fingerprint: prepared.plan_fingerprint.clone(),
            coordinate_checksum: prepared.coordinate_checksum.clone(),
            replay_routes: Arc::new(move |generation_id| {
                let run = replay_run.clone();
                let lock = replay_lock.clone();
                Box::pin(async move {
                    let _guard = lock.lock().await;
                    run.stream_prepared_routes(generation_id, true).await
                }) as RecoveryFuture
            }),
            dispose: Some(Arc::new(move || {
                dispose_listener.cancel();
                if let Ok(mut retained) = dispose_run.retained.lock() {
                    *retained = None;
                }
            })),
        });
        let _ = run.owner.set(Arc::downgrade(&lease));

        self.start_provider_route_recovery_signer(&run, &lease, sources, destinations)
            .await?;

        let lease = self.control().register_recovery_lease(lease).await;

        // Ownership fence: once the token fires, stop this owner's signing and replay without
        // cancelling the transfer; a replacement owner's lease and signer are left alone.
        if let Some(token) = ownership.clone() {
            let client = self.clone();
            let transfer_id = prepared.transfer_id.clone();
            let lease = lease.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        client.stop_recovery_signer(&transfer_id, Some(&lease));
                        client
                            .control()
                            .release_recovery_lease(&transfer_id, Some(&lease))
                            .await;
                    }
                    _ = listener_stop.cancelled() => {}
                }
            });
        }

        let mut foreground_guard = ForegroundRecoveryGuard::new(
            self.control().clone(),
            prepared.transfer_id.clone(),
            lease.clone(),
        );
        let result = async {
            let before_stream = async {
                assert_owned()?;
                if let Some(callback) = &input.on_prepared {
                    callback(prepared.clone()).await?;
                }
                call_throw_if_cancelled(&input.throw_if_cancelled, Some(&prepared.transfer_id))
                    .await
            }
            .await;
            if let Err(error) = before_stream {
                if assert_owned().is_err() {
                    // Fenced off: hand nothing to background recovery.
                    self.stop_recovery_signer(&prepared.transfer_id, Some(&lease));
                    self.control()
                        .release_recovery_lease(&prepared.transfer_id, Some(&lease))
                        .await;
                } else {
                    self.control()
                        .continue_recovery_lease(&prepared.transfer_id, Some(&lease))
                        .await;
                }
                return Err(error);
            }
            run.clone()
                .stream_prepared_routes(prepared.route_generation_id.clone(), false)
                .await?;
            if let Ok(mut initial) = run.initial_throw_if_cancelled.lock() {
                *initial = None;
            }
            Ok(())
        }
        .await;
        foreground_guard.disarm();
        drop(initial_stream_guard);
        result.map(|()| prepared)
    }

    fn remember_huggingface_uploads(&self, transfer_id: &str, states: Vec<HuggingFaceUploadState>) {
        if let Ok(mut uploads) = self.huggingface_uploads().lock() {
            uploads.insert(transfer_id.to_string(), states);
        }
    }

    /// Serve route recovery signing and integrity audits for `owner`'s run.
    ///
    /// Fails with [`BeamApiError::Aborted`] (installing nothing) when the owner was fenced off
    /// while subscribing, so a replacement owner's signer is never displaced.
    async fn start_provider_route_recovery_signer(
        &self,
        run: &Arc<ProviderRun>,
        owner: &Arc<RecoveryLease>,
        sources: HashMap<String, ProviderSourceConfig>,
        destinations: HashMap<String, ProviderDestinationConfig>,
    ) -> Result<(), BeamApiError> {
        let transfer_id = run.prepared.transfer_id.clone();
        let signer_run = run.clone();
        let handler: RouteRecoverySignHandler = Arc::new(move |payload| {
            let run = signer_run.clone();
            Box::pin(async move { run.sign_recovery_routes(payload).await })
        });
        let handle = self
            .control()
            .serve_route_recovery_signer(&transfer_id, handler)
            .await?;
        if let Err(error) = run.assert_ownership() {
            handle.stop();
            return Err(error);
        }
        self.remember_recovery_signer(&transfer_id, owner, handle);
        self.remember_integrity_context(IntegrityContext {
            prepared: run.prepared.clone(),
            sources,
            destinations,
            expires_in: run.expires_in,
        });
        let client = self.clone();
        let signing_run = run.clone();
        let handler: crate::nats_control::IntegritySignHandler = Arc::new(move |challenge| {
            let client = client.clone();
            let run = signing_run.clone();
            Box::pin(async move {
                run.assert_ownership()?;
                client.signed_integrity_grants(&challenge).await
            })
        });
        if let Ok(handle) = self
            .control()
            .serve_integrity_signer(&transfer_id, handler)
            .await
        {
            self.remember_integrity_signer(&transfer_id, owner, handle);
        }
        run.assert_ownership()?;
        Ok(())
    }

    /// Negotiate every Hugging Face destination before the plan exists.
    ///
    /// The Hub will not issue upload URLs without the object sha256, and it chooses the part
    /// size itself, so this runs first. The returned part size, if any, is sent to Beam as
    /// `provider_part_size`, and the plan must match it exactly.
    async fn plan_huggingface_uploads(
        &self,
        sources: &[ProviderSourceConfig],
        prepared_sources: &[PreparedHttpSource],
        destinations: &[ProviderDestinationConfig],
        prepared_destinations: &[PreparedDestination],
    ) -> Result<(Vec<HuggingFaceUploadState>, Option<u64>), BeamApiError> {
        let targets = destinations
            .iter()
            .enumerate()
            .filter_map(|(index, destination)| match destination {
                ProviderDestinationConfig::HuggingFace(destination) => Some((index, destination)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
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
        let published_sha256 = match sources.first() {
            Some(ProviderSourceConfig::HuggingFace(_)) => prepared_source
                .metadata
                .get("sha256")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        };

        let mut states = Vec::with_capacity(targets.len());
        let mut provider_part_size: Option<u64> = None;
        for (index, destination) in targets {
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
            config.path =
                huggingface::target_path(&config.path, prepared_source.filename.as_deref())
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
                    huggingface::http::hash_source(&self.http, &prepared_source.url, None, true)
                        .await?
                        .0
                        .unwrap_or_default()
                }
            };

            let sample = huggingface::http::source_sample(&self.http, &prepared_source.url).await?;
            let (upload_mode, should_ignore) =
                huggingface::http::preupload(&self.http, &config, prepared_source.size, &sample)
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

            let plan =
                huggingface::http::lfs_batch(&self.http, &config, &oid, prepared_source.size)
                    .await?;
            if let Some(part_size) = plan.chunk_size {
                if let Some(existing) = provider_part_size {
                    if existing != part_size {
                        return Err(BeamApiError::ProviderSigning(format!(
                            "huggingface destinations disagree on part size ({existing} vs \
                             {part_size}); the plan carries a single chunk size"
                        )));
                    }
                }
                provider_part_size = Some(part_size);
            }

            let part_etags = match plan.chunk_size {
                None => watch::channel(Some(Ok(Vec::new()))).1,
                Some(part_size) => {
                    // Runs alongside the transfer; the ETags are only needed at commit time.
                    let (sender, receiver) = watch::channel(None);
                    let http = self.http.clone();
                    let url = prepared_source.url.clone();
                    tokio::spawn(async move {
                        let result =
                            huggingface::http::hash_source(&http, &url, Some(part_size), false)
                                .await
                                .map(|(_, etags)| etags)
                                .map_err(|error| error.to_string());
                        let _ = sender.send(Some(result));
                    });
                    receiver
                }
            };

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
        Ok((states, provider_part_size))
    }
}

async fn call_throw_if_cancelled(
    callback: &Option<ThrowIfCancelledCallback>,
    transfer_id: Option<&str>,
) -> Result<(), BeamApiError> {
    match callback {
        Some(callback) => callback(transfer_id.map(str::to_string)).await,
        None => Ok(()),
    }
}

impl ProviderRun {
    fn transfer_id(&self) -> &str {
        &self.prepared.transfer_id
    }

    /// This run's recovery lease, while it is alive. A dropped lease is no longer registered,
    /// so there is nothing left for this run to release, continue or stop.
    fn owner(&self) -> Option<Arc<RecoveryLease>> {
        self.owner.get().and_then(Weak::upgrade)
    }

    fn assert_ownership(&self) -> Result<(), BeamApiError> {
        match &self.cancellation {
            Some(token) if token.is_cancelled() => Err(BeamApiError::Aborted),
            _ => Ok(()),
        }
    }

    fn retained(&self) -> Result<Arc<RetainedProviders>, BeamApiError> {
        self.retained
            .lock()
            .ok()
            .and_then(|retained| retained.clone())
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("provider recovery lease was abandoned".to_string())
            })
    }

    fn signing_options(&self) -> ProviderSigningOptions {
        ProviderSigningOptions {
            index: 0,
            expires_in: self.expires_in,
            http_client: Some(self.client.http.clone()),
            cancellation: self.cancellation.clone(),
        }
    }

    async fn throw_if_cancelled(
        &self,
        recovery_replay: bool,
        foreground_cancelled: &AtomicBool,
    ) -> Result<(), BeamApiError> {
        let result = async {
            self.assert_ownership()?;
            if recovery_replay {
                return Ok(());
            }
            let callback = self
                .initial_throw_if_cancelled
                .lock()
                .ok()
                .and_then(|callback| callback.clone());
            call_throw_if_cancelled(&callback, Some(self.transfer_id())).await
        }
        .await;
        if result.is_err() {
            foreground_cancelled.store(true, Ordering::SeqCst);
        }
        result
    }

    /// Stream every planned route under `generation_id`.
    async fn stream_prepared_routes(
        self: Arc<Self>,
        generation_id: String,
        recovery_replay: bool,
    ) -> Result<(), BeamApiError> {
        let foreground_cancelled = Arc::new(AtomicBool::new(false));
        self.assert_ownership()?;
        let plan = &self.prepared.plan_descriptor;
        let mut sender = RouteStreamSender::new(
            &self.client,
            self.transfer_id().to_string(),
            plan.delivery_route_count as usize,
            plan.logical_chunk_count as usize,
            self.auto_distribute,
            Some(iso_after(self.expires_in)),
            format!("{}:{}", plan.plan_nonce, generation_id),
            generation_id.clone(),
        );
        sender.telemetry.lock().unwrap().source_history = Some(self.source_history.clone());
        if !recovery_replay {
            let mut telemetry = sender.telemetry.lock().unwrap();
            telemetry.started = self.preparation_started;
            telemetry.observe_duration("sdk.discovery", self.discovery_duration);
            telemetry.seed_discovery(&self.discovery_metrics.lock().unwrap());
        }
        let mut begin_attempted = false;
        let mut manifest_task: Option<
            JoinHandle<Result<Vec<MultipartGroupManifest>, BeamApiError>>,
        > = None;
        let mut pending: FuturesUnordered<
            BoxFuture<'static, Result<SignedChunkRoute, RouteFailure>>,
        > = FuturesUnordered::new();
        let result: Result<(), BeamApiError> = async {
            begin_attempted = true;
            sender.begin().await?;
            let (signals, waiters) = self.create_multipart_group_waiters()?;
            manifest_task = Some(tokio::spawn(
                crate::performance::CURRENT.scope(
                    sender.telemetry.clone(),
                    self.clone()
                        .create_multipart_group_manifest(sender.handle(), signals),
                ),
            ));
            let source_urls = self.stream_source_urls().await?;
            let mut signing_concurrency = self.client.route_signing_concurrency;
            sender
                .telemetry
                .lock()
                .unwrap()
                .gauge("signing_configured_limit", signing_concurrency as f64);
            sender.telemetry.lock().unwrap().gauge(
                "multipart_configured_limit",
                self.client.multipart_control_concurrency as f64,
            );
            sender
                .telemetry
                .lock()
                .unwrap()
                .gauge("signing_limit", signing_concurrency as f64);
            sender.telemetry.lock().unwrap().gauge(
                "multipart_limit",
                self.client.multipart_control_concurrency as f64,
            );
            let mut signed_in_window = 0usize;
            let mut window_started_at = Instant::now();
            for chunk in CompactPlanChunkIter::new(plan, self.transfer_id()) {
                let chunk = chunk?;
                self.throw_if_cancelled(recovery_replay, &foreground_cancelled)
                    .await?;
                let source_grant = Arc::new(tokio::sync::OnceCell::<SourceChunkGrant>::new());

                for target in chunk.destinations.clone() {
                    let source_grant = source_grant.clone();
                    let telemetry = sender.telemetry.clone();
                    let run = self.clone();
                    let chunk = chunk.clone();
                    let waiters = waiters.clone();
                    let foreground_cancelled = foreground_cancelled.clone();
                    let source_url = source_urls.get(&chunk.source_id).cloned();
                    pending.push(Box::pin(async move {
                        run.throw_if_cancelled(recovery_replay, &foreground_cancelled)
                            .await?;
                        let started = Instant::now();
                        let result = crate::performance::CURRENT
                            .scope(
                                telemetry.clone(),
                                run.sign_planned_route(
                                    chunk,
                                    target,
                                    &waiters,
                                    source_url,
                                    source_grant,
                                ),
                            )
                            .await;
                        telemetry.lock().unwrap().observe("sdk.signing", started);
                        result
                    }));
                    sender
                        .telemetry
                        .lock()
                        .unwrap()
                        .gauge("signing_pending_peak", pending.len() as f64);
                    if pending.len() >= signing_concurrency {
                        let producer_started = Instant::now();
                        let route = pending.next().await.expect("pending route");
                        sender
                            .telemetry
                            .lock()
                            .unwrap()
                            .observe("sdk.producer_wait", producer_started);
                        let route = self.settle_route(route, &mut manifest_task).await?;
                        self.assert_ownership()?;
                        sender.add_route(route).await?;
                        signed_in_window += 1;
                        if signed_in_window == ROUTE_STREAM_BATCH_ROUTES {
                            if !self.client.route_signing_concurrency_overridden
                                && window_started_at.elapsed() > Duration::from_secs(4)
                                && signing_concurrency < 256
                            {
                                signing_concurrency = (signing_concurrency * 2).min(256);
                                sender
                                    .telemetry
                                    .lock()
                                    .unwrap()
                                    .increment("concurrency_changes");
                                sender
                                    .telemetry
                                    .lock()
                                    .unwrap()
                                    .gauge("signing_limit", signing_concurrency as f64);
                            }
                            signed_in_window = 0;
                            window_started_at = Instant::now();
                        }
                    }
                }
            }
            while !pending.is_empty() {
                let producer_started = Instant::now();
                let route = pending.next().await.expect("pending route");
                sender
                    .telemetry
                    .lock()
                    .unwrap()
                    .observe("sdk.producer_wait", producer_started);
                let route = self.settle_route(route, &mut manifest_task).await?;
                self.assert_ownership()?;
                sender.add_route(route).await?;
            }
            let manifests = match manifest_task.take() {
                Some(task) => join_manifest_task(task).await?,
                None => Vec::new(),
            };
            validate_multipart_group_manifest(&manifests, self.transfer_id())?;
            self.assert_ownership()?;
            let attached = sender.complete().await?;
            if !attached.success {
                return Err(BeamApiError::ProviderSigning(
                    attached
                        .error
                        .filter(|error| !error.is_empty())
                        .or_else(|| (!attached.message.is_empty()).then_some(attached.message))
                        .unwrap_or_else(|| "route stream failed".to_string()),
                ));
            }
            Ok(())
        }
        .await;

        let Err(error) = result else {
            return Ok(());
        };
        drop(pending);
        if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            if let Some(owner) = self.owner() {
                self.client
                    .stop_recovery_signer(self.transfer_id(), Some(&owner));
                self.client
                    .control()
                    .release_recovery_lease(self.transfer_id(), Some(&owner))
                    .await;
            }
            if let Some(task) = manifest_task.take() {
                let _ = task.await;
            }
            return Err(BeamApiError::Aborted);
        }
        sender.abort().await;
        if let Some(task) = manifest_task.take() {
            let _ = task.await;
        }
        if recovery_replay {
            return Err(error);
        }
        let control = self.client.control();
        let owner = self.owner();
        if foreground_cancelled.load(Ordering::SeqCst) {
            if let Some(owner) = &owner {
                control
                    .continue_recovery_lease(self.transfer_id(), Some(owner))
                    .await;
            }
            return Err(error);
        }
        if is_recoverable_route_stream_error(&error) {
            if let Some(owner) = &owner {
                control
                    .continue_recovery_lease(self.transfer_id(), Some(owner))
                    .await;
            }
            return Err(BeamApiError::RouteRecoveryPending {
                transfer_id: self.transfer_id().to_string(),
                source: Box::new(error),
            });
        }
        let failure = self.cancel_and_abort(error, !begin_attempted).await;
        // The retained configs share credentials with multipart cleanup; release the lease only
        // once cleanup has finished.
        if let Some(owner) = &owner {
            control
                .release_recovery_lease(self.transfer_id(), Some(owner))
                .await;
        }
        Err(failure)
    }

    /// Resolve a finished route future, replacing a group-failure marker with the group's error.
    async fn settle_route(
        &self,
        route: Result<SignedChunkRoute, RouteFailure>,
        manifest_task: &mut Option<JoinHandle<Result<Vec<MultipartGroupManifest>, BeamApiError>>>,
    ) -> Result<SignedChunkRoute, BeamApiError> {
        match route {
            Ok(route) => Ok(route),
            Err(RouteFailure::Error(error)) => Err(error),
            Err(RouteFailure::GroupFailed) => {
                let error = match manifest_task.take() {
                    Some(task) => join_manifest_task(task).await.err(),
                    None => None,
                };
                Err(error.unwrap_or_else(|| {
                    BeamApiError::ProviderSigning("multipart group setup failed".to_string())
                }))
            }
        }
    }

    /// Resolve source URLs that are not bound to a range once per stream (Hippius presign
    /// calls and Hugging Face redirects); S3 reads are signed per route.
    async fn stream_source_urls(&self) -> Result<HashMap<String, String>, BeamApiError> {
        let retained = self.retained()?;
        let options = self.signing_options();
        let mut urls = HashMap::new();
        for source in &self.prepared.plan_descriptor.sources {
            let id = &source.source.source_id;
            let config = retained.sources.get(id).ok_or_else(|| {
                BeamApiError::ProviderSigning(format!("BeamCore returned unknown source_id: {id}"))
            })?;
            if !config.is_s3_compatible() {
                urls.insert(
                    id.clone(),
                    provider_signing::sign_source_url(config, &options).await?,
                );
            }
        }
        Ok(urls)
    }

    #[allow(clippy::type_complexity)]
    fn create_multipart_group_waiters(
        &self,
    ) -> Result<
        (
            HashMap<String, watch::Sender<GroupSignal>>,
            Arc<HashMap<String, watch::Receiver<GroupSignal>>>,
        ),
        BeamApiError,
    > {
        let retained = self.retained()?;
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for source in &self.prepared.plan_descriptor.sources {
            for destination in &self.prepared.plan_descriptor.destinations {
                let id = &destination.destination.destination_id;
                let Some(config) = retained.destinations.get(id) else {
                    continue;
                };
                if is_direct_put_destination(config) {
                    continue;
                }
                let final_object_key = final_object_key(destination, &source.source.source_id)?;
                let group_id = multipart_group_state_key(
                    self.transfer_id(),
                    id,
                    &source.source.source_id,
                    final_object_key,
                );
                let (sender, receiver) = watch::channel(None);
                senders.insert(group_id.clone(), sender);
                receivers.insert(group_id, receiver);
            }
        }
        Ok((senders, Arc::new(receivers)))
    }

    /// Create (or reuse) every multipart upload, sign its manifest and publish it, bounded by
    /// `multipart_control_concurrency`. Every group is attempted; the first failure is returned.
    async fn create_multipart_group_manifest(
        self: Arc<Self>,
        stream: RouteStreamHandle,
        signals: HashMap<String, watch::Sender<GroupSignal>>,
    ) -> Result<Vec<MultipartGroupManifest>, BeamApiError> {
        let signals = Arc::new(signals);
        let groups = self
            .prepared
            .plan_descriptor
            .sources
            .iter()
            .flat_map(|source| {
                self.prepared
                    .plan_descriptor
                    .destinations
                    .iter()
                    .map(move |destination| (source.clone(), destination.clone()))
            })
            .collect::<Vec<_>>();
        let queued_at = Instant::now();
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let results = stream::iter(groups)
            .map(|(source, destination)| {
                let run = self.clone();
                let stream = stream.clone();
                let signals = signals.clone();
                let active = active.clone();
                async move {
                    let count = active.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    let _ = crate::performance::CURRENT.try_with(|m| {
                        let mut m = m.lock().unwrap();
                        m.observe("sdk.multipart_queue", queued_at);
                        m.gauge("multipart_active_peak", count as f64);
                    });
                    let result = run
                        .create_multipart_group(&source, &destination, &stream, &signals)
                        .await;
                    active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    result
                }
            })
            .buffered(self.client.multipart_control_concurrency)
            .collect::<Vec<_>>()
            .await;
        let mut manifests = Vec::new();
        for result in results {
            if let Some(manifest) = result? {
                manifests.push(manifest);
            }
        }
        Ok(manifests)
    }

    async fn create_multipart_group(
        &self,
        source: &CompactTransferPlanSource,
        destination: &crate::CompactTransferPlanDestination,
        stream: &RouteStreamHandle,
        signals: &HashMap<String, watch::Sender<GroupSignal>>,
    ) -> Result<Option<MultipartGroupManifest>, BeamApiError> {
        let mut group_id: Option<String> = None;
        let result = async {
            let retained = self.retained()?;
            let destination_id = &destination.destination.destination_id;
            let config = retained.destinations.get(destination_id).ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "BeamCore returned unknown destination_id: {destination_id}"
                ))
            })?;
            if is_direct_put_destination(config) {
                return Ok(None);
            }
            let object_key = final_object_key(destination, &source.source.source_id)?.to_string();
            let id = multipart_group_state_key(
                self.transfer_id(),
                destination_id,
                &source.source.source_id,
                &object_key,
            );
            group_id = Some(id.clone());
            let final_object_metadata = HashMap::from([(
                "beam-transfer-id".to_string(),
                self.transfer_id().to_string(),
            )]);
            self.assert_ownership()?;
            let retained_upload = self
                .multipart_uploads
                .lock()
                .ok()
                .and_then(|uploads| uploads.get(&id).cloned());
            let created = retained_upload.is_none();
            let upload_id = match retained_upload {
                Some(upload) => upload.upload_id,
                None => {
                    let started = Instant::now();
                    let upload_id = create_multipart_upload(
                        config,
                        &object_key,
                        &final_object_metadata,
                        &self.signing_options(),
                    )
                    .await?;
                    stream
                        .telemetry
                        .lock()
                        .unwrap()
                        .observe("sdk.multipart_create", started);
                    if let Ok(mut uploads) = self.multipart_uploads.lock() {
                        uploads.insert(
                            id.clone(),
                            MultipartUpload {
                                destination: config.clone(),
                                object_key: object_key.clone(),
                                upload_id: upload_id.clone(),
                                manifest: None,
                            },
                        );
                    }
                    upload_id
                }
            };
            let ready = async {
                let manifest = build_multipart_manifest(
                    config,
                    &id,
                    source,
                    destination_id,
                    &object_key,
                    &upload_id,
                    final_object_metadata.clone(),
                    self.expires_in,
                )?;
                let upload = Arc::new(MultipartUpload {
                    destination: config.clone(),
                    object_key: object_key.clone(),
                    upload_id: upload_id.clone(),
                    manifest: Some(manifest.clone()),
                });
                if let Ok(mut uploads) = self.multipart_uploads.lock() {
                    uploads.insert(id.clone(), (*upload).clone());
                }
                self.on_group_ready(upload.clone(), stream).await?;
                if let Some(signal) = signals.get(&id) {
                    let _ = signal.send(Some(Ok(upload)));
                }
                Ok(manifest)
            }
            .await;
            match ready {
                Ok(manifest) => Ok(Some(manifest)),
                Err(error) => {
                    let owned = !self
                        .cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled);
                    if created && owned {
                        let options = ProviderSigningOptions {
                            cancellation: None,
                            ..self.signing_options()
                        };
                        match abort_multipart_upload(config, &object_key, &upload_id, &options)
                            .await
                        {
                            Ok(()) => {
                                if let Ok(mut uploads) = self.multipart_uploads.lock() {
                                    uploads.remove(&id);
                                }
                            }
                            Err(abort_error) => {
                                return Err(BeamApiError::Multiple {
                                    message: format!(
                                        "multipart group setup and cleanup failed for {id}"
                                    ),
                                    errors: vec![error, abort_error],
                                })
                            }
                        }
                    }
                    Err(error)
                }
            }
        }
        .await;
        if result.is_err() {
            if let Some(signal) = group_id.as_ref().and_then(|id| signals.get(id)) {
                let _ = signal.send(Some(Err(())));
            }
        }
        result
    }

    async fn on_group_ready(
        &self,
        upload: Arc<MultipartUpload>,
        stream: &RouteStreamHandle,
    ) -> Result<(), BeamApiError> {
        self.assert_ownership()?;
        let manifest = upload
            .manifest
            .clone()
            .expect("ready groups carry their manifest");
        if let Ok(mut uploads) = self.recovery_uploads.lock() {
            uploads.insert(manifest.multipart_group_id.clone(), upload.clone());
        }
        validate_multipart_group_manifest(std::slice::from_ref(&manifest), self.transfer_id())?;
        let callback_phase = crate::performance::phase("sdk.multipart_callback");
        if let Some(callback) = &self.on_multipart_group_ready {
            callback(multipart_group_identity(self.transfer_id(), &manifest)).await?;
        }
        drop(callback_phase);
        stream.add_manifest_groups(&[manifest]).await
    }

    async fn sign_planned_route(
        &self,
        chunk: ChunkSigningPlanItem,
        target: ChunkDestinationSigningTarget,
        waiters: &HashMap<String, watch::Receiver<GroupSignal>>,
        source_url: Option<String>,
        source_grant: Arc<tokio::sync::OnceCell<SourceChunkGrant>>,
    ) -> Result<SignedChunkRoute, RouteFailure> {
        let retained = self.retained()?;
        let destination = retained
            .destinations
            .get(&target.destination_id)
            .cloned()
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "BeamCore returned unknown destination_id: {}",
                    target.destination_id
                ))
            })?;
        let source = retained
            .sources
            .get(&chunk.source_id)
            .cloned()
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "BeamCore returned unknown source_id: {}",
                    chunk.source_id
                ))
            })?;
        let final_object_key = target
            .metadata
            .get("final_object_key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| target.object_key.clone())
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(
                    "destination signing target is missing object_key".to_string(),
                )
            })?;
        let upload = if is_direct_put_destination(&destination) {
            None
        } else {
            let group_id = multipart_group_state_key(
                self.transfer_id(),
                &target.destination_id,
                &chunk.source_id,
                &final_object_key,
            );
            let mut receiver = waiters.get(&group_id).cloned().ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "multipart group manifest is missing for {}:{}",
                    chunk.source_id, target.destination_id
                ))
            })?;
            let wait_phase = crate::performance::phase("sdk.multipart_ready_wait");
            let signal = receiver
                .wait_for(Option::is_some)
                .await
                .map(|signal| signal.clone())
                .map_err(|_| RouteFailure::GroupFailed)?;
            drop(wait_phase);
            match signal {
                Some(Ok(upload)) => Some(upload),
                _ => return Err(RouteFailure::GroupFailed),
            }
        };
        let part_number = match target.metadata.get("part_number").and_then(Value::as_u64) {
            Some(part_number) => part_number,
            None => multipart_part_number(chunk.source_chunk_index, 0)?,
        };
        let signing_options = self.signing_options();
        let wait_phase = crate::performance::phase("sdk.source_grant_wait");
        let mut created = false;
        let grant = source_grant
            .get_or_try_init(|| {
                created = true;
                sign_source_chunk(
                    Some(&source),
                    &chunk,
                    source_url.as_deref(),
                    &signing_options,
                )
            })
            .await?
            .clone();
        drop(wait_phase);
        let _ = crate::performance::CURRENT
            .try_with(|m| m.lock().unwrap().source_used(created, chunk.chunk_index));
        Ok(self
            .sign_provider_route(
                chunk,
                target,
                source,
                destination,
                upload.as_deref(),
                &final_object_key,
                part_number,
                source_url,
                Some(grant),
            )
            .await?)
    }

    #[allow(clippy::too_many_arguments)]
    async fn sign_provider_route(
        &self,
        chunk: ChunkSigningPlanItem,
        mut target: ChunkDestinationSigningTarget,
        source: ProviderSourceConfig,
        destination: ProviderDestinationConfig,
        upload: Option<&MultipartUpload>,
        final_object_key: &str,
        part_number: u64,
        source_url: Option<String>,
        source_grant: Option<SourceChunkGrant>,
    ) -> Result<SignedChunkRoute, BeamApiError> {
        let options = self.signing_options();
        let mut input = match &destination {
            ProviderDestinationConfig::HuggingFace(_) => {
                let state = self
                    .huggingface
                    .get(&target.destination_id)
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
                target.object_key = Some(final_object_key.to_string());
                target.metadata = direct_put_route_metadata(&target.metadata);
                let mut input = DestinationRouteInput::new(chunk, target, destination.clone());
                input.dest_url = Some(dest_url);
                input
            }
            ProviderDestinationConfig::Hippius(_) => {
                target.metadata = part_route_metadata(&target.metadata);
                DestinationRouteInput::new(chunk, target, destination.clone())
            }
            _ => {
                let upload = upload.ok_or_else(|| {
                    BeamApiError::ProviderSigning(
                        "signed_url route is missing its multipart upload".to_string(),
                    )
                })?;
                let manifest = upload.manifest.as_ref().ok_or_else(|| {
                    BeamApiError::ProviderSigning(
                        "signed_url route is missing its multipart manifest".to_string(),
                    )
                })?;
                validate_multipart_part_number(part_number, manifest)?;
                target.object_key = Some(final_object_key.to_string());
                target.metadata = part_route_metadata(&target.metadata);
                let mut input = DestinationRouteInput::new(chunk, target, destination.clone());
                input.upload_id = Some(upload.upload_id.clone());
                input.part_number = Some(part_number);
                input.final_object_key = Some(final_object_key.to_string());
                input.expected_object_size = Some(manifest.expected_object_size);
                input.expected_part_count = Some(manifest.expected_part_count);
                input.max_part_number = Some(manifest.max_part_number);
                input.multipart_group_id = Some(manifest.multipart_group_id.clone());
                input.complete_url = Some(manifest.complete_url.clone());
                input.abort_url = Some(manifest.abort_url.clone());
                input.list_page_url = manifest
                    .list_page_urls
                    .get(multipart_list_page_index(part_number))
                    .cloned();
                input.final_head_url = Some(manifest.final_head_url.clone());
                input.final_object_metadata = Some(manifest.final_object_metadata.clone());
                input
            }
        };
        input.source = Some(source);
        input.source_url = source_url;
        input.source_grant = source_grant;
        let mut route = sign_destination_route(input, &options).await?;
        route.delivery_index = signed_route_delivery_index(&route);
        Ok(route)
    }

    /// Answer a Runtime request to re-sign specific route attempts.
    async fn sign_recovery_routes(
        self: Arc<Self>,
        payload: RouteRecoverySignRequestPayload,
    ) -> Result<RouteRecoverySignReplyPayload, BeamApiError> {
        self.assert_ownership()?;
        if payload.route_generation_id.is_empty() {
            return Err(BeamApiError::ProviderSigning(
                "route recovery generation is required".to_string(),
            ));
        }
        let generation_id = payload.route_generation_id.clone();
        let source_grants = Arc::new(std::sync::Mutex::new(HashMap::<
            (String, u64),
            Arc<tokio::sync::OnceCell<SourceChunkGrant>>,
        >::new()));
        let routes = stream::iter(payload.chunks.clone())
            .map(|requested| {
                let run = self.clone();
                let generation_id = generation_id.clone();
                let source_grants = source_grants.clone();
                async move {
                    let grant = {
                        source_grants
                            .lock()
                            .unwrap()
                            .entry((requested.source_id.clone(), requested.chunk_index))
                            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                            .clone()
                    };
                    run.sign_recovery_route(requested, &generation_id, grant)
                        .await
                }
            })
            .buffered(self.client.route_signing_concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RouteRecoverySignReplyPayload {
            transfer_id: payload.transfer_id,
            route_generation_id: payload.route_generation_id,
            signed_at: Some(iso_now()),
            chunk_routes: routes,
        })
    }

    async fn sign_recovery_route(
        &self,
        requested: RouteRecoverySignChunk,
        generation_id: &str,
        source_grant: Arc<tokio::sync::OnceCell<SourceChunkGrant>>,
    ) -> Result<SignedChunkRoute, BeamApiError> {
        self.assert_ownership()?;
        if requested.route_generation_id != generation_id {
            return Err(BeamApiError::ProviderSigning(
                "route recovery chunk generation mismatch".to_string(),
            ));
        }
        let chunk = materialize_plan_chunk(
            &self.prepared.plan_descriptor,
            self.transfer_id(),
            &requested.source_id,
            requested.chunk_index,
        )?;
        if requested.part_number
            != multipart_part_number(chunk.source_chunk_index, requested.attempt_slot)?
        {
            return Err(BeamApiError::ProviderSigning(
                "route recovery multipart slot mapping mismatch".to_string(),
            ));
        }
        if chunk.source_offset != requested.source_offset
            || chunk.chunk_size != requested.chunk_size
        {
            return Err(BeamApiError::ProviderSigning(
                "route recovery source coordinate mismatch".to_string(),
            ));
        }
        let target = chunk
            .destinations
            .iter()
            .find(|candidate| {
                candidate.destination_id == requested.destination_id
                    && candidate
                        .metadata
                        .get("delivery_index")
                        .and_then(Value::as_u64)
                        == Some(requested.delivery_index)
            })
            .cloned()
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(
                    "route recovery destination coordinate mismatch".to_string(),
                )
            })?;
        let retained = self.retained()?;
        let (Some(source), Some(destination)) = (
            retained.sources.get(&requested.source_id).cloned(),
            retained
                .destinations
                .get(&requested.destination_id)
                .cloned(),
        ) else {
            return Err(BeamApiError::ProviderSigning(
                "route recovery provider configuration is unavailable".to_string(),
            ));
        };
        let upload = if is_direct_put_destination(&destination) {
            None
        } else {
            Some(self.recovery_multipart_upload(&destination, &requested)?)
        };
        let mut target = target;
        let mut metadata = target.metadata.clone();
        if let Some(extra) = &requested.destination_metadata {
            metadata.extend(
                extra
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        for (key, value) in [
            ("transfer_id", json!(self.transfer_id())),
            ("final_object_key", json!(requested.final_object_key)),
            ("upload_id", json!(requested.upload_id)),
            ("multipart_group_id", json!(requested.multipart_group_id)),
            ("delivery_index", json!(requested.delivery_index)),
            ("part_number", json!(requested.part_number)),
            (
                "logical_attempt_index",
                json!(requested.logical_attempt_index),
            ),
            ("attempt_slot", json!(requested.attempt_slot)),
            ("route_generation_id", json!(requested.route_generation_id)),
        ] {
            metadata.insert(key.to_string(), value);
        }
        // Direct-PUT destinations (Hippius, Hugging Face) keep the planned per-chunk key exactly
        // as initial materialization does; only multipart destinations sign parts against the
        // final object key. Signing a Hippius chunk against the final key would overwrite it.
        if upload.is_some() {
            target.object_key = Some(requested.final_object_key.clone());
        }
        target.metadata = metadata;
        let signing_options = self.signing_options();
        let grant = source_grant
            .get_or_try_init(|| sign_source_chunk(Some(&source), &chunk, None, &signing_options))
            .await?
            .clone();
        let route = self
            .sign_provider_route(
                chunk,
                target,
                source,
                destination.clone(),
                upload.as_deref(),
                &requested.final_object_key,
                requested.part_number,
                None,
                Some(grant),
            )
            .await?;
        // Core-only staging, listing, delete and renewal grants; the worker contract is unchanged.
        sign_multipart_recovery(
            MultipartRecoverySignInput {
                destination: &destination,
                transfer_id: self.transfer_id(),
                multipart_group_id: &requested.multipart_group_id,
                final_object_key: &requested.final_object_key,
                upload_id: &requested.upload_id,
                part_number: requested.part_number,
                recovery: requested.recovery.as_ref(),
                expires_in: self.signing_options().expires_in(),
            },
            route,
        )
    }

    fn recovery_multipart_upload(
        &self,
        destination: &ProviderDestinationConfig,
        requested: &RouteRecoverySignChunk,
    ) -> Result<Arc<MultipartUpload>, BeamApiError> {
        let existing = self
            .recovery_uploads
            .lock()
            .ok()
            .and_then(|uploads| uploads.get(&requested.multipart_group_id).cloned());
        if let Some(existing) = existing {
            let manifest = existing.manifest.as_ref();
            if existing.upload_id != requested.upload_id
                || existing.object_key != requested.final_object_key
                || manifest.is_none_or(|manifest| {
                    manifest.source_id != requested.source_id
                        || manifest.destination_id != requested.destination_id
                })
            {
                return Err(BeamApiError::ProviderSigning(
                    "route recovery multipart identity mismatch".to_string(),
                ));
            }
            return Ok(existing);
        }
        if requested.upload_id.is_empty() {
            return Err(BeamApiError::ProviderSigning(
                "route recovery multipart upload id is required".to_string(),
            ));
        }
        let source = self
            .prepared
            .plan_descriptor
            .sources
            .iter()
            .find(|candidate| candidate.source.source_id == requested.source_id)
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("route recovery source plan is unavailable".into())
            })?;
        let expected_group_id = multipart_group_state_key(
            self.transfer_id(),
            &requested.destination_id,
            &requested.source_id,
            &requested.final_object_key,
        );
        if requested.multipart_group_id != expected_group_id {
            return Err(BeamApiError::ProviderSigning(
                "route recovery multipart group identity mismatch".to_string(),
            ));
        }
        let manifest = build_multipart_manifest(
            destination,
            &requested.multipart_group_id,
            source,
            &requested.destination_id,
            &requested.final_object_key,
            &requested.upload_id,
            HashMap::from([(
                "beam-transfer-id".to_string(),
                self.transfer_id().to_string(),
            )]),
            self.expires_in,
        )?;
        validate_multipart_group_manifest(std::slice::from_ref(&manifest), self.transfer_id())?;
        let upload = Arc::new(MultipartUpload {
            destination: destination.clone(),
            object_key: requested.final_object_key.clone(),
            upload_id: requested.upload_id.clone(),
            manifest: Some(manifest),
        });
        if let Ok(mut uploads) = self.recovery_uploads.lock() {
            uploads.insert(requested.multipart_group_id.clone(), upload.clone());
        }
        Ok(upload)
    }

    /// Cancel the transfer and abort every multipart upload this run created. When the route
    /// stream never began, uploads are aborted first so no worker can race the cleanup.
    async fn cancel_and_abort(
        &self,
        cause: BeamApiError,
        abort_before_cancel: bool,
    ) -> BeamApiError {
        let cause = Arc::new(cause);
        let mut cleanup_error: Option<BeamApiError> = None;
        if abort_before_cancel {
            cleanup_error = self.abort_created_uploads().await.err();
        }
        let cancel_error = match self
            .client
            .request_transfer_cancellation(self.transfer_id())
            .await
        {
            Ok(result) if result.success => None,
            Ok(_) => Some(BeamApiError::ProviderSigning(format!(
                "provider transfer failed ({}) and transfer cancellation failed (Beam rejected cancellation for {})",
                cause.safe_code(),
                self.transfer_id()
            ))),
            Err(error) => Some(BeamApiError::ProviderSigning(format!(
                "provider transfer failed ({}) and transfer cancellation failed ({})",
                cause.safe_code(),
                error.safe_code()
            ))),
        };
        if !abort_before_cancel || cleanup_error.is_some() {
            cleanup_error = self.abort_created_uploads().await.err();
        }
        let transfer_cancelled = cancel_error.is_none();
        let multipart_cleanup_complete = cleanup_error.is_none();
        let mut errors = vec![cause.clone()];
        errors.extend(cancel_error.map(Arc::new));
        errors.extend(cleanup_error.map(Arc::new));
        BeamApiError::ProviderTransfer {
            transfer_id: self.transfer_id().to_string(),
            transfer_cancelled,
            multipart_cleanup_complete,
            cause,
            errors,
        }
    }

    async fn abort_created_uploads(&self) -> Result<(), BeamApiError> {
        let candidates = self
            .multipart_uploads
            .lock()
            .map(|uploads| {
                uploads
                    .iter()
                    .filter(|(_, upload)| {
                        !upload.upload_id.is_empty()
                            && !matches!(upload.destination, ProviderDestinationConfig::Hippius(_))
                    })
                    .map(|(group_id, upload)| (group_id.clone(), upload.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let options = ProviderSigningOptions {
            cancellation: None,
            ..self.signing_options()
        };
        let concurrency = self
            .client
            .multipart_control_concurrency
            .min(candidates.len().max(1));
        let failures = stream::iter(candidates)
            .map(|(group_id, upload)| {
                let options = options.clone();
                async move {
                    abort_multipart_upload(
                        &upload.destination,
                        &upload.object_key,
                        &upload.upload_id,
                        &options,
                    )
                    .await
                    .map(|()| group_id)
                }
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| match result {
                Ok(group_id) => {
                    if let Ok(mut uploads) = self.multipart_uploads.lock() {
                        uploads.remove(&group_id);
                    }
                    None
                }
                Err(error) => Some(error),
            })
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(BeamApiError::Multiple {
                message: format!("failed to abort {} multipart upload(s)", failures.len()),
                errors: failures,
            })
        }
    }
}

async fn join_manifest_task(
    task: JoinHandle<Result<Vec<MultipartGroupManifest>, BeamApiError>>,
) -> Result<Vec<MultipartGroupManifest>, BeamApiError> {
    task.await.map_err(|error| {
        BeamApiError::ProviderSigning(format!("multipart manifest task failed: {error}"))
    })?
}

#[allow(clippy::too_many_arguments)]
fn build_multipart_manifest(
    destination: &ProviderDestinationConfig,
    group_id: &str,
    source: &CompactTransferPlanSource,
    destination_id: &str,
    object_key: &str,
    upload_id: &str,
    final_object_metadata: HashMap<String, String>,
    expires_in: Duration,
) -> Result<MultipartGroupManifest, BeamApiError> {
    let max_part_number = multipart_max_part_number(source.chunk_count)?;
    let list_page_urls = multipart_list_page_markers(max_part_number)
        .into_iter()
        .map(|marker| {
            sign_list_multipart_upload(
                destination,
                object_key,
                upload_id,
                expires_in,
                ListPartsPage {
                    max_parts: Some(1_000),
                    part_number_marker: (marker > 0).then_some(marker),
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(MultipartGroupManifest {
        multipart_group_id: group_id.to_string(),
        source_id: source.source.source_id.clone(),
        destination_id: destination_id.to_string(),
        final_object_key: object_key.to_string(),
        upload_id: upload_id.to_string(),
        expected_object_size: source.source.size,
        expected_part_count: source.chunk_count,
        max_part_number,
        complete_url: sign_complete_multipart_upload(
            destination,
            object_key,
            upload_id,
            expires_in,
        )?,
        abort_url: sign_abort_multipart_upload(destination, object_key, upload_id, expires_in)?,
        list_page_urls,
        final_head_url: sign_final_object_head(destination, object_key, expires_in)?,
        final_object_metadata,
        urls_expires_at: iso_after(expires_in),
    })
}

fn final_object_key<'a>(
    destination: &'a crate::CompactTransferPlanDestination,
    source_id: &str,
) -> Result<&'a str, BeamApiError> {
    destination
        .final_object_keys
        .get(source_id)
        .map(String::as_str)
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!(
                "plan final object key not found: {source_id}:{}",
                destination.destination.destination_id
            ))
        })
}

fn multipart_group_state_key(
    transfer_id: &str,
    destination_id: &str,
    source_id: &str,
    final_object_key: &str,
) -> String {
    format!("{transfer_id}:{destination_id}:{source_id}:{final_object_key}")
}

fn multipart_group_identity(
    transfer_id: &str,
    manifest: &MultipartGroupManifest,
) -> ProviderMultipartGroupIdentity {
    ProviderMultipartGroupIdentity {
        transfer_id: transfer_id.to_string(),
        multipart_group_id: manifest.multipart_group_id.clone(),
        source_id: manifest.source_id.clone(),
        destination_id: manifest.destination_id.clone(),
        object_key: manifest.final_object_key.clone(),
        upload_id: manifest.upload_id.clone(),
        expected_object_size: manifest.expected_object_size,
        expected_part_count: manifest.expected_part_count,
        expires_at: manifest.urls_expires_at.clone(),
    }
}

/// Destinations that take a plain PUT per chunk instead of an S3 multipart upload. BeamCore
/// rejects multipart route metadata for these, so they never get a multipart group manifest.
fn is_direct_put_destination(destination: &ProviderDestinationConfig) -> bool {
    matches!(
        destination,
        ProviderDestinationConfig::Hippius(_) | ProviderDestinationConfig::HuggingFace(_)
    )
}

fn recovery_error(code: &str) -> BeamApiError {
    BeamApiError::InvalidArgument(code.to_string())
}

/// Check that `identities` name exactly the plan's multipart groups and seed them for reuse.
fn restore_provider_multipart_identities(
    prepared: &TransferPrepareResponse,
    destinations: &HashMap<String, ProviderDestinationConfig>,
    identities: &[ProviderMultipartGroupIdentity],
    uploads: &mut HashMap<String, MultipartUpload>,
) -> Result<(), BeamApiError> {
    struct Expected<'a> {
        destination: &'a ProviderDestinationConfig,
        object_key: &'a str,
        source: &'a CompactTransferPlanSource,
        destination_id: &'a str,
    }
    let mut expected = HashMap::new();
    for source in &prepared.plan_descriptor.sources {
        for target in &prepared.plan_descriptor.destinations {
            let destination_id = target.destination.destination_id.as_str();
            let destination = destinations
                .get(destination_id)
                .ok_or_else(|| recovery_error("provider_multipart_recovery_destination_invalid"))?;
            if is_direct_put_destination(destination) {
                continue;
            }
            let object_key = target
                .final_object_keys
                .get(&source.source.source_id)
                .ok_or_else(|| recovery_error("provider_multipart_recovery_object_invalid"))?;
            expected.insert(
                multipart_group_state_key(
                    &prepared.transfer_id,
                    destination_id,
                    &source.source.source_id,
                    object_key,
                ),
                Expected {
                    destination,
                    object_key,
                    source,
                    destination_id,
                },
            );
        }
    }
    if expected.len() != identities.len() {
        return Err(recovery_error("provider_multipart_recovery_incomplete"));
    }
    for identity in identities {
        let group = expected.get(&identity.multipart_group_id);
        let valid = group.is_some_and(|group| {
            !uploads.contains_key(&identity.multipart_group_id)
                && identity.transfer_id == prepared.transfer_id
                && identity.source_id == group.source.source.source_id
                && identity.destination_id == group.destination_id
                && identity.object_key == group.object_key
                && identity.expected_object_size == group.source.source.size
                && identity.expected_part_count == group.source.chunk_count
                && !identity.upload_id.trim().is_empty()
        });
        let Some(group) = group.filter(|_| valid) else {
            return Err(recovery_error(
                "provider_multipart_recovery_identity_invalid",
            ));
        };
        uploads.insert(
            identity.multipart_group_id.clone(),
            MultipartUpload {
                destination: group.destination.clone(),
                object_key: group.object_key.to_string(),
                upload_id: identity.upload_id.clone(),
                manifest: None,
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
