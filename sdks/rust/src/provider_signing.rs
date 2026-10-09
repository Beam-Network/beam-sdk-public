//! Provider signing helpers, mirroring the TypeScript SDK's `provider-signing.ts`.
//!
//! These functions turn provider configs into credential-free signed URLs for BeamCore and the
//! workers. Credentials never leave this process.
//!
//! Supported providers: S3, R2 and any S3-compatible store (SigV4), Hippius, and the Hugging Face
//! Hub. GCS and Azure configs can be described to BeamCore but have no signing adapter.

use crate::error::BeamApiError;
use crate::huggingface::{self, HuggingFaceConfig};
use crate::nats_control::{iso_after, iso_at};
use crate::s3::{xml_elements, xml_escape, xml_text, S3Operation, S3Settings};
use crate::{
    ChunkDestinationSigningTarget, ChunkSigningPlanItem, MultipartRecoveryMode,
    MultipartRecoveryOperation, MultipartRecoveryRequest, PlanningHttpSource, PreparedDestination,
    PreparedHttpSource, ProviderDestinationConfig, ProviderSourceConfig, SignedChunkRoute,
};
use reqwest::Client as HttpClient;
use serde_json::{json, Map, Value};
use std::{
    collections::HashMap,
    future::Future,
    sync::OnceLock,
    time::{Duration, SystemTime},
};
use tokio_util::sync::CancellationToken;

pub(crate) const HIPPIUS_DEFAULT_BASE_URL: &str = "https://api.hippius.com";
const DEFAULT_EXPIRES_IN: Duration = Duration::from_secs(3600);

/// Shared options for provider signing helpers.
#[derive(Debug, Clone)]
pub struct ProviderSigningOptions {
    /// Position used for default `src_{index}` / `dst_{index}` ids.
    pub index: usize,
    /// Lifetime of every signed URL. Defaults to one hour.
    pub expires_in: Duration,
    /// HTTP client for provider API calls. A shared default client is used when unset.
    pub http_client: Option<HttpClient>,
    /// Cancels in-flight provider HTTP calls; they then fail with [`BeamApiError::Aborted`].
    pub cancellation: Option<CancellationToken>,
}

impl Default for ProviderSigningOptions {
    fn default() -> Self {
        Self {
            index: 0,
            expires_in: DEFAULT_EXPIRES_IN,
            http_client: None,
            cancellation: None,
        }
    }
}

impl ProviderSigningOptions {
    pub(crate) fn http(&self) -> HttpClient {
        if let Some(client) = &self.http_client {
            let _ = crate::performance::CURRENT
                .try_with(|m| m.lock().unwrap().increment("provider_clients_reused"));
            client.clone()
        } else {
            default_http_client()
        }
    }

    pub(crate) fn expires_in(&self) -> Duration {
        if self.expires_in.is_zero() {
            DEFAULT_EXPIRES_IN
        } else {
            self.expires_in
        }
    }

    /// Run `future` unless the cancellation token fires first.
    pub(crate) async fn run<T>(
        &self,
        future: impl Future<Output = Result<T, BeamApiError>>,
    ) -> Result<T, BeamApiError> {
        match &self.cancellation {
            None => future.await,
            Some(token) => {
                if token.is_cancelled() {
                    return Err(BeamApiError::Aborted);
                }
                tokio::select! {
                    _ = token.cancelled() => Err(BeamApiError::Aborted),
                    result = future => result,
                }
            }
        }
    }
}

pub(crate) fn default_http_client() -> HttpClient {
    static CLIENT: OnceLock<HttpClient> = OnceLock::new();
    let mut created = false;
    let client = CLIENT
        .get_or_init(|| {
            created = true;
            let _phase = crate::performance::phase("sdk.provider_client_setup");
            HttpClient::new()
        })
        .clone();
    let _ = crate::performance::CURRENT.try_with(|m| {
        m.lock().unwrap().increment(if created {
            "provider_clients_created"
        } else {
            "provider_clients_reused"
        })
    });
    client
}

/// A signed request a worker performs as-is: the URL plus the exact headers it was signed with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRangeRequest {
    pub url: String,
    pub headers: HashMap<String, String>,
}

/// A byte range of a source object to sign for reading.
#[derive(Debug, Clone, Default)]
pub struct SourceReadRange {
    pub offset: u64,
    pub length: u64,
    /// Bind the read to this ETag (`If-Match`). S3-compatible storage only.
    pub if_match: Option<String>,
    /// Read this object version. S3-compatible storage only.
    pub version_id: Option<String>,
}

/// A byte range of a delivered destination object to sign for reading.
#[derive(Debug, Clone, Default)]
pub struct DestinationReadRange {
    pub object_key: String,
    pub offset: u64,
    pub length: u64,
    /// Bind the read to this ETag (`If-Match`). S3-compatible storage only.
    pub if_match: Option<String>,
}

/// A destination write to sign.
#[derive(Debug, Clone, Default)]
pub struct DestinationUrlInput {
    pub object_key: String,
    /// Sign an `UploadPart` when both `upload_id` and `part_number` are set, else a `PutObject`.
    pub upload_id: Option<String>,
    pub part_number: Option<u64>,
    /// Bind the upload to this base64 MD5 (`Content-MD5`). S3-compatible storage only.
    pub content_md5: Option<String>,
}

/// Page selection for a signed `ListParts` URL.
#[derive(Debug, Clone, Copy, Default)]
pub struct ListPartsPage {
    pub max_parts: Option<u64>,
    pub part_number_marker: Option<u64>,
}

/// One uploaded part reported by `ListParts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPart {
    pub part_number: u64,
    pub etag: String,
    pub size: u64,
}

/// One part to include in `CompleteMultipartUpload`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedPart {
    pub part_number: u64,
    pub etag: String,
}

/// Result of `CompleteMultipartUpload`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletedMultipartUpload {
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

/// Object metadata from a destination `HEAD`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DestinationObjectInfo {
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    /// User metadata (`x-amz-meta-*`) with the prefix removed.
    pub metadata: HashMap<String, String>,
}

/// Everything needed to sign one chunk route for one destination.
#[derive(Debug, Clone)]
pub struct DestinationRouteInput {
    pub(crate) source_grant: Option<SourceChunkGrant>,
    pub chunk: ChunkSigningPlanItem,
    pub target: ChunkDestinationSigningTarget,
    /// Source config used to sign the ranged source read. Without it the plan's source URL is
    /// used as-is.
    pub source: Option<ProviderSourceConfig>,
    pub destination: ProviderDestinationConfig,
    pub part_number: Option<u64>,
    pub upload_id: Option<String>,
    pub complete_url: Option<String>,
    pub abort_url: Option<String>,
    pub list_page_url: Option<String>,
    pub final_head_url: Option<String>,
    pub final_object_key: Option<String>,
    pub expected_object_size: Option<u64>,
    pub expected_part_count: Option<u64>,
    pub max_part_number: Option<u64>,
    pub final_object_metadata: Option<HashMap<String, String>>,
    pub multipart_group_id: Option<String>,
    /// Pre-issued destination URL, for providers that sign their own upload targets.
    pub dest_url: Option<String>,
    /// Pre-signed source URL to reuse instead of signing the source again. Ignored for
    /// S3-compatible sources, whose URLs are bound to each route's `Range`.
    pub source_url: Option<String>,
}

impl DestinationRouteInput {
    pub fn new(
        chunk: ChunkSigningPlanItem,
        target: ChunkDestinationSigningTarget,
        destination: ProviderDestinationConfig,
    ) -> Self {
        Self {
            chunk,
            target,
            source: None,
            source_grant: None,
            destination,
            part_number: None,
            upload_id: None,
            complete_url: None,
            abort_url: None,
            list_page_url: None,
            final_head_url: None,
            final_object_key: None,
            expected_object_size: None,
            expected_part_count: None,
            max_part_number: None,
            final_object_metadata: None,
            multipart_group_id: None,
            dest_url: None,
            source_url: None,
        }
    }
}

impl ProviderSourceConfig {
    /// Whether this config is served through the S3 API (S3, R2 or S3-compatible).
    pub fn is_s3_compatible(&self) -> bool {
        S3Settings::from_source(self).is_some()
    }

    /// The S3 endpoint URL (`None` for AWS S3's regional endpoint). Errors for non-S3 configs
    /// and for R2/S3-compatible configs missing their endpoint.
    pub fn s3_compatible_endpoint(&self) -> Result<Option<String>, BeamApiError> {
        s3_settings_for_source(self)?.endpoint()
    }

    /// The SigV4 signing region (`auto` for R2, `us-east-1` by default).
    pub fn s3_compatible_region(&self) -> Result<String, BeamApiError> {
        Ok(s3_settings_for_source(self)?.region())
    }

    /// `Some(true)` for path-style addressing; `None` lets AWS S3 choose.
    pub fn s3_compatible_force_path_style(&self) -> Result<Option<bool>, BeamApiError> {
        let settings = s3_settings_for_source(self)?;
        let endpoint = settings.endpoint()?;
        Ok(settings.force_path_style(endpoint.as_deref()))
    }
}

impl ProviderDestinationConfig {
    /// Whether this config is served through the S3 API (S3, R2 or S3-compatible).
    pub fn is_s3_compatible(&self) -> bool {
        S3Settings::from_destination(self).is_some()
    }

    /// See [`ProviderSourceConfig::s3_compatible_endpoint`].
    pub fn s3_compatible_endpoint(&self) -> Result<Option<String>, BeamApiError> {
        s3_settings_for_destination(self)?.endpoint()
    }

    /// See [`ProviderSourceConfig::s3_compatible_region`].
    pub fn s3_compatible_region(&self) -> Result<String, BeamApiError> {
        Ok(s3_settings_for_destination(self)?.region())
    }

    /// See [`ProviderSourceConfig::s3_compatible_force_path_style`].
    pub fn s3_compatible_force_path_style(&self) -> Result<Option<bool>, BeamApiError> {
        let settings = s3_settings_for_destination(self)?;
        let endpoint = settings.endpoint()?;
        Ok(settings.force_path_style(endpoint.as_deref()))
    }
}

fn s3_settings_for_source(source: &ProviderSourceConfig) -> Result<S3Settings, BeamApiError> {
    S3Settings::from_source(source).ok_or_else(|| {
        BeamApiError::InvalidArgument(format!(
            "{} is not an S3-compatible provider",
            source.provider_name()
        ))
    })
}

fn s3_settings_for_destination(
    destination: &ProviderDestinationConfig,
) -> Result<S3Settings, BeamApiError> {
    S3Settings::from_destination(destination).ok_or_else(|| {
        BeamApiError::InvalidArgument(format!(
            "{} is not an S3-compatible provider",
            destination.provider_name()
        ))
    })
}

struct ObjectHead {
    content_length: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    version_id: Option<String>,
    metadata: HashMap<String, String>,
}

async fn head_object(
    http: &HttpClient,
    settings: &S3Settings,
    object_key: &str,
) -> Result<ObjectHead, BeamApiError> {
    let _phase = crate::performance::phase("sdk.metadata_request");
    let response = settings
        .send(http, &S3Operation::HeadObject, object_key, Vec::new())
        .await?;
    let metadata = response
        .headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix("x-amz-meta-")?;
            Some((name.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    Ok(ObjectHead {
        content_length: response
            .header("content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        etag: response.header("etag"),
        last_modified: response
            .header("last-modified")
            .and_then(|value| httpdate::parse_http_date(&value).ok())
            .map(iso_at),
        version_id: response.header("x-amz-version-id"),
        metadata,
    })
}

fn s3_source_metadata(
    settings: &S3Settings,
    endpoint: Option<&str>,
    head: &ObjectHead,
) -> HashMap<String, Value> {
    let mut metadata = settings.metadata(endpoint);
    metadata.insert("content_length".to_string(), json!(head.content_length));
    if let Some(etag) = &head.etag {
        metadata.insert("etag".to_string(), json!(etag));
    }
    if let Some(last_modified) = &head.last_modified {
        metadata.insert("last_modified".to_string(), json!(last_modified));
    }
    if let Some(version_id) = &head.version_id {
        metadata.insert("version_id".to_string(), json!(version_id));
    }
    metadata
}

/// Resolve a source to a signed HTTP source BeamCore can plan against.
pub async fn prepare_provider_source(
    source: &ProviderSourceConfig,
    options: &ProviderSigningOptions,
) -> Result<PreparedHttpSource, BeamApiError> {
    let index = options.index;
    let expires_in = options.expires_in();
    if let Some(settings) = S3Settings::from_source(source) {
        let endpoint = settings.endpoint()?;
        let head = options
            .run(head_object(&options.http(), &settings, &settings.key))
            .await?;
        let url = settings.presign(
            &S3Operation::GetObject {
                range: None,
                if_match: None,
                version_id: None,
            },
            &settings.key,
            expires_in,
        )?;
        return Ok(PreparedHttpSource {
            source_id: source_id(source_config_id(source), index),
            source_type: "http".to_string(),
            provider: Some(source.provider_name().to_string()),
            url,
            size: head.content_length,
            filename: Some(filename(&settings.key)),
            headers: None,
            expires_at: Some(iso_after(expires_in)),
            metadata: s3_source_metadata(&settings, endpoint.as_deref(), &head),
        });
    }
    match source {
        ProviderSourceConfig::Hippius(source) => {
            let http = options.http();
            let base_url = hippius_base_url(source.base_url.as_deref());
            let size = options
                .run(hippius_object_size(
                    &http,
                    base_url,
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                ))
                .await?;
            let url = options
                .run(hippius_presign(
                    &http,
                    base_url,
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                    "get",
                    expires_in,
                ))
                .await?;
            Ok(PreparedHttpSource {
                source_id: source_id(source.source_id.as_deref(), index),
                source_type: "http".to_string(),
                provider: Some("hippius".to_string()),
                url,
                size,
                filename: Some(filename(&source.key)),
                headers: None,
                expires_at: Some(iso_after(expires_in)),
                metadata: with_storage_location(
                    hippius_metadata(&source.bucket, &source.key, base_url),
                    source.storage_location.as_deref(),
                ),
            })
        }
        ProviderSourceConfig::HuggingFace(source) => {
            let config = HuggingFaceConfig::from_source(source);
            let metadata = options
                .run(huggingface::http::file_metadata(&config))
                .await?;
            Ok(PreparedHttpSource {
                source_id: source_id(source.source_id.as_deref(), index),
                source_type: "http".to_string(),
                provider: Some("huggingface".to_string()),
                url: metadata.url,
                size: metadata.size,
                filename: Some(filename(&config.path)),
                headers: None,
                expires_at: None,
                metadata: with_storage_location(
                    huggingface::metadata(
                        &config,
                        metadata.etag.as_deref(),
                        metadata.commit_hash.as_deref(),
                    ),
                    source.storage_location.as_deref(),
                ),
            })
        }
        other => Err(unsupported_provider(other.provider_name())),
    }
}

/// Resolve a source's size and metadata for `transfer.plan` without signing a URL.
pub async fn prepare_provider_source_for_plan(
    source: &ProviderSourceConfig,
    options: &ProviderSigningOptions,
) -> Result<PlanningHttpSource, BeamApiError> {
    let index = options.index;
    if let Some(settings) = S3Settings::from_source(source) {
        let endpoint = settings.endpoint()?;
        let head = options
            .run(head_object(&options.http(), &settings, &settings.key))
            .await?;
        return Ok(PlanningHttpSource {
            source_id: source_id(source_config_id(source), index),
            source_type: "http".to_string(),
            provider: Some(source.provider_name().to_string()),
            url: None,
            size: head.content_length,
            filename: Some(filename(&settings.key)),
            headers: None,
            expires_at: None,
            metadata: s3_source_metadata(&settings, endpoint.as_deref(), &head),
        });
    }
    match source {
        ProviderSourceConfig::Hippius(source) => {
            let http = options.http();
            let base_url = hippius_base_url(source.base_url.as_deref());
            let size = options
                .run(hippius_object_size(
                    &http,
                    base_url,
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                ))
                .await?;
            Ok(PlanningHttpSource {
                source_id: source_id(source.source_id.as_deref(), index),
                source_type: "http".to_string(),
                provider: Some("hippius".to_string()),
                url: None,
                size,
                filename: Some(filename(&source.key)),
                headers: None,
                expires_at: None,
                metadata: with_storage_location(
                    hippius_metadata(&source.bucket, &source.key, base_url),
                    source.storage_location.as_deref(),
                ),
            })
        }
        ProviderSourceConfig::HuggingFace(source) => {
            let config = HuggingFaceConfig::from_source(source);
            let metadata = options
                .run(huggingface::http::file_metadata(&config))
                .await?;
            Ok(PlanningHttpSource {
                source_id: source_id(source.source_id.as_deref(), index),
                source_type: "http".to_string(),
                provider: Some("huggingface".to_string()),
                url: None,
                size: metadata.size,
                filename: Some(filename(&config.path)),
                headers: None,
                expires_at: None,
                metadata: with_storage_location(
                    huggingface::metadata(
                        &config,
                        metadata.etag.as_deref(),
                        metadata.commit_hash.as_deref(),
                    ),
                    source.storage_location.as_deref(),
                ),
            })
        }
        other => Err(unsupported_provider(other.provider_name())),
    }
}

/// Describe a destination for `transfer.prepare`. No network access.
pub fn prepare_provider_destination(
    destination: &ProviderDestinationConfig,
    index: usize,
) -> Result<PreparedDestination, BeamApiError> {
    if let Some(settings) = S3Settings::from_destination(destination) {
        let endpoint = settings.endpoint()?;
        return Ok(PreparedDestination {
            destination_id: destination_id(destination_config_id(destination), index),
            provider: destination.provider_name().to_string(),
            mode: None,
            logical_prefix: Some(settings.key.clone()),
            metadata: settings.metadata(endpoint.as_deref()),
        });
    }
    match destination {
        ProviderDestinationConfig::Hippius(destination) => Ok(PreparedDestination {
            destination_id: destination_id(destination.destination_id.as_deref(), index),
            provider: "hippius".to_string(),
            mode: None,
            logical_prefix: Some(destination.key.trim_end_matches('/').to_string()),
            metadata: with_storage_location(
                hippius_metadata(
                    &destination.bucket,
                    &destination.key,
                    hippius_base_url(destination.base_url.as_deref()),
                ),
                destination.storage_location.as_deref(),
            ),
        }),
        ProviderDestinationConfig::HuggingFace(destination) => {
            let config = HuggingFaceConfig::from_destination(destination);
            Ok(PreparedDestination {
                destination_id: destination_id(destination.destination_id.as_deref(), index),
                provider: "huggingface".to_string(),
                mode: None,
                logical_prefix: Some(destination.path.clone()),
                metadata: with_storage_location(
                    huggingface::metadata(&config, None, None),
                    destination.storage_location.as_deref(),
                ),
            })
        }
        // GCS and Azure are described for BeamCore but have no signing adapter.
        ProviderDestinationConfig::GCS(destination) => Ok(PreparedDestination {
            destination_id: destination_id(destination.destination_id.as_deref(), index),
            provider: "gcs".to_string(),
            mode: None,
            logical_prefix: Some(destination.key.clone()),
            metadata: HashMap::from([
                ("bucket".to_string(), json!(destination.bucket)),
                ("key".to_string(), json!(destination.key)),
                ("project_id".to_string(), json!(destination.project_id)),
            ]),
        }),
        ProviderDestinationConfig::Azure(destination) => Ok(PreparedDestination {
            destination_id: destination_id(destination.destination_id.as_deref(), index),
            provider: "azure".to_string(),
            mode: None,
            logical_prefix: Some(format!(
                "azure://{}/{}",
                destination.container, destination.blob
            )),
            metadata: HashMap::from([
                ("container".to_string(), json!(destination.container)),
                ("blob".to_string(), json!(destination.blob)),
                ("account_name".to_string(), json!(destination.account_name)),
            ]),
        }),
        ProviderDestinationConfig::S3(_)
        | ProviderDestinationConfig::R2(_)
        | ProviderDestinationConfig::S3Compatible(_) => unreachable!("handled above"),
    }
}

/// Create a multipart upload for `object_key` and return its upload id.
pub async fn create_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    metadata: &HashMap<String, String>,
    options: &ProviderSigningOptions,
) -> Result<String, BeamApiError> {
    let _phase = crate::performance::phase("sdk.multipart_provider");
    let settings = multipart_settings(destination, "multipart upload")?;
    let response = options
        .run(settings.send(
            &options.http(),
            &S3Operation::CreateMultipartUpload { metadata },
            object_key,
            Vec::new(),
        ))
        .await?;
    xml_text(&response.body, "UploadId")
        .filter(|upload_id| !upload_id.is_empty())
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!(
                "provider did not return UploadId for {object_key}"
            ))
        })
}

/// Abort a multipart upload. Retries throttling and transient failures.
pub async fn abort_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    options: &ProviderSigningOptions,
) -> Result<(), BeamApiError> {
    let settings = multipart_settings(destination, "multipart abort")?;
    options
        .run(settings.send(
            &options.http(),
            &S3Operation::AbortMultipartUpload { upload_id },
            object_key,
            Vec::new(),
        ))
        .await?;
    Ok(())
}

/// List every uploaded part, following pagination. Reads provider metadata only.
pub async fn list_multipart_parts(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    options: &ProviderSigningOptions,
) -> Result<Vec<MultipartPart>, BeamApiError> {
    let settings = s3_settings_for_destination(destination)?;
    let http = options.http();
    let mut parts = Vec::new();
    let mut marker: Option<u64> = None;
    loop {
        let response = options
            .run(settings.send(
                &http,
                &S3Operation::ListParts {
                    upload_id,
                    max_parts: Some(1_000),
                    part_number_marker: marker,
                },
                object_key,
                Vec::new(),
            ))
            .await?;
        for part in xml_elements(&response.body, "Part") {
            let part_number = xml_text(part, "PartNumber").and_then(|value| value.parse().ok());
            let etag = xml_text(part, "ETag").filter(|etag| !etag.is_empty());
            let size = xml_text(part, "Size").and_then(|value| value.parse().ok());
            match (part_number, etag, size) {
                (Some(part_number), Some(etag), Some(size)) if part_number > 0 => {
                    parts.push(MultipartPart {
                        part_number,
                        etag,
                        size,
                    })
                }
                _ => {
                    return Err(BeamApiError::ProviderSigning(
                        "invalid multipart part metadata".to_string(),
                    ))
                }
            }
        }
        let truncated = xml_text(&response.body, "IsTruncated").as_deref() == Some("true");
        if !truncated {
            return Ok(parts);
        }
        let next = xml_text(&response.body, "NextPartNumberMarker")
            .and_then(|value| value.parse::<u64>().ok());
        match next {
            Some(next) if Some(next) != marker => marker = Some(next),
            _ => {
                return Err(BeamApiError::ProviderSigning(
                    "invalid multipart pagination".to_string(),
                ))
            }
        }
    }
}

/// Complete a multipart upload from its parts (sorted by part number).
pub async fn complete_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    parts: &[CompletedPart],
    options: &ProviderSigningOptions,
) -> Result<CompletedMultipartUpload, BeamApiError> {
    let settings = s3_settings_for_destination(destination)?;
    let mut sorted = parts.to_vec();
    sorted.sort_by_key(|part| part.part_number);
    let body = format!(
        "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</CompleteMultipartUpload>",
        sorted
            .iter()
            .map(|part| format!(
                "<Part><ETag>{}</ETag><PartNumber>{}</PartNumber></Part>",
                xml_escape(&part.etag),
                part.part_number
            ))
            .collect::<String>()
    );
    let response = options
        .run(settings.send(
            &options.http(),
            &S3Operation::CompleteMultipartUpload { upload_id },
            object_key,
            body.into_bytes(),
        ))
        .await?;
    Ok(CompletedMultipartUpload {
        etag: xml_text(&response.body, "ETag"),
        version_id: response.header("x-amz-version-id"),
    })
}

/// `HEAD` a destination object: size, ETag, version and user metadata.
pub async fn inspect_destination_object(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    options: &ProviderSigningOptions,
) -> Result<DestinationObjectInfo, BeamApiError> {
    let settings = s3_settings_for_destination(destination)?;
    let head = options
        .run(head_object(&options.http(), &settings, object_key))
        .await?;
    Ok(DestinationObjectInfo {
        size: Some(head.content_length),
        etag: head.etag,
        version_id: head.version_id,
        metadata: head.metadata,
    })
}

/// Presign a `HEAD` of the final object, for completion verification.
pub fn sign_final_object_head(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    expires_in: Duration,
) -> Result<String, BeamApiError> {
    multipart_settings(destination, "final object HEAD signing")?.presign(
        &S3Operation::HeadObject,
        object_key,
        expires_in,
    )
}

/// Presign `CompleteMultipartUpload`.
pub fn sign_complete_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    expires_in: Duration,
) -> Result<String, BeamApiError> {
    multipart_settings(destination, "multipart completion")?.presign(
        &S3Operation::CompleteMultipartUpload { upload_id },
        object_key,
        expires_in,
    )
}

/// Presign `AbortMultipartUpload`.
pub fn sign_abort_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    expires_in: Duration,
) -> Result<String, BeamApiError> {
    multipart_settings(destination, "multipart abort")?.presign(
        &S3Operation::AbortMultipartUpload { upload_id },
        object_key,
        expires_in,
    )
}

/// Presign one `ListParts` page.
pub fn sign_list_multipart_upload(
    destination: &ProviderDestinationConfig,
    object_key: &str,
    upload_id: &str,
    expires_in: Duration,
    page: ListPartsPage,
) -> Result<String, BeamApiError> {
    multipart_settings(destination, "multipart list-parts")?.presign(
        &S3Operation::ListParts {
            upload_id,
            max_parts: page.max_parts,
            part_number_marker: page.part_number_marker,
        },
        object_key,
        expires_in,
    )
}

/// Sign a ranged read of a source object.
pub async fn sign_source_read_range(
    source: &ProviderSourceConfig,
    range: &SourceReadRange,
    options: &ProviderSigningOptions,
) -> Result<SignedRangeRequest, BeamApiError> {
    let header = range_header(range.offset, range.length);
    if let Some(settings) = S3Settings::from_source(source) {
        let url = settings.presign(
            &S3Operation::GetObject {
                range: Some(&header),
                if_match: range.if_match.as_deref(),
                version_id: range.version_id.as_deref(),
            },
            &settings.key,
            options.expires_in(),
        )?;
        return Ok(SignedRangeRequest {
            url,
            headers: range_headers(header, range.if_match.as_deref()),
        });
    }
    if range.if_match.is_some() || range.version_id.is_some() {
        return Err(BeamApiError::ProviderSigning(
            "conditional source ranges require S3-compatible storage".to_string(),
        ));
    }
    let url = sign_source_url(source, options).await?;
    Ok(SignedRangeRequest {
        url,
        headers: range_headers(header, None),
    })
}

/// Sign a ranged read of a delivered destination object.
pub async fn sign_destination_read_range(
    destination: &ProviderDestinationConfig,
    range: &DestinationReadRange,
    options: &ProviderSigningOptions,
) -> Result<SignedRangeRequest, BeamApiError> {
    let header = range_header(range.offset, range.length);
    if let Some(settings) = S3Settings::from_destination(destination) {
        let url = settings.presign(
            &S3Operation::GetObject {
                range: Some(&header),
                if_match: range.if_match.as_deref(),
                version_id: None,
            },
            &range.object_key,
            options.expires_in(),
        )?;
        return Ok(SignedRangeRequest {
            url,
            headers: range_headers(header, range.if_match.as_deref()),
        });
    }
    if range.if_match.is_some() {
        return Err(BeamApiError::ProviderSigning(
            "conditional destination ranges require S3-compatible storage".to_string(),
        ));
    }
    let url = match destination {
        ProviderDestinationConfig::Hippius(destination) => {
            options
                .run(hippius_presign(
                    &options.http(),
                    hippius_base_url(destination.base_url.as_deref()),
                    &destination.api_token,
                    &destination.bucket,
                    &range.object_key,
                    "get",
                    options.expires_in(),
                ))
                .await?
        }
        ProviderDestinationConfig::HuggingFace(destination) => {
            // Read-back resolves the committed file. Before the commit lands the bytes exist
            // only as uncommitted LFS parts, which the Hub does not expose.
            let mut config = HuggingFaceConfig::from_destination(destination);
            config.path = range.object_key.clone();
            options
                .run(huggingface::http::file_metadata(&config))
                .await?
                .url
        }
        other => return Err(unsupported_provider(other.provider_name())),
    };
    Ok(SignedRangeRequest {
        url,
        headers: range_headers(header, None),
    })
}

/// Sign a destination write (a whole-object PUT, or one multipart part).
pub async fn sign_destination_url(
    destination: &ProviderDestinationConfig,
    input: &DestinationUrlInput,
    options: &ProviderSigningOptions,
) -> Result<String, BeamApiError> {
    let _phase = crate::performance::phase("sdk.destination_signing");
    if let Some(settings) = S3Settings::from_destination(destination) {
        let operation = match (&input.upload_id, input.part_number) {
            (Some(upload_id), Some(part_number)) if part_number > 0 => S3Operation::UploadPart {
                upload_id,
                part_number,
                content_md5: input.content_md5.as_deref(),
            },
            _ => S3Operation::PutObject {
                content_md5: input.content_md5.as_deref(),
            },
        };
        return settings.presign(&operation, &input.object_key, options.expires_in());
    }
    if input.content_md5.is_some() {
        return Err(BeamApiError::ProviderSigning(
            "checksum-bound uploads require S3-compatible storage".to_string(),
        ));
    }
    match destination {
        ProviderDestinationConfig::Hippius(destination) => {
            options
                .run(hippius_presign(
                    &options.http(),
                    hippius_base_url(destination.base_url.as_deref()),
                    &destination.api_token,
                    &destination.bucket,
                    &input.object_key,
                    "put",
                    options.expires_in(),
                ))
                .await
        }
        other => Err(unsupported_provider(other.provider_name())),
    }
}

/// Sign one chunk route: the ranged source read plus the destination write.
pub async fn sign_destination_route(
    input: DestinationRouteInput,
    options: &ProviderSigningOptions,
) -> Result<SignedChunkRoute, BeamApiError> {
    let mut expires_at = SystemTime::now() + options.expires_in();
    let object_key = input.target.object_key.clone().ok_or_else(|| {
        BeamApiError::ProviderSigning("destination signing target is missing object_key".into())
    })?;
    let destination_future = async {
        match &input.dest_url {
            Some(url) => Ok(url.clone()),
            None => {
                sign_destination_url(
                    &input.destination,
                    &DestinationUrlInput {
                        object_key: object_key.clone(),
                        upload_id: input.upload_id.clone(),
                        part_number: input.part_number,
                        content_md5: None,
                    },
                    options,
                )
                .await
            }
        }
    };
    let source_future = async {
        match input.source_grant.clone() {
            Some(grant) => Ok(grant),
            None => {
                sign_source_chunk(
                    input.source.as_ref(),
                    &input.chunk,
                    input.source_url.as_deref(),
                    None,
                    None,
                    options,
                )
                .await
            }
        }
    };
    let (dest_url, source_grant) = tokio::try_join!(destination_future, source_future)?;
    expires_at = expires_at.min(source_grant.expires_at);
    if expires_at <= SystemTime::now() {
        return Err(BeamApiError::ProviderSigning(
            "source grant expired before attachment".into(),
        ));
    }
    expires_at = bounded_grant_expiry(&dest_url, expires_at);
    let source_url = source_grant.url;

    let mut metadata = input.target.metadata.clone();
    let mut insert = |key: &str, value: Value| {
        metadata.insert(key.to_string(), value);
    };
    if let Some(value) = &input.multipart_group_id {
        insert("multipart_group_id", json!(value));
    }
    if let Some(value) = &input.upload_id {
        insert("upload_id", json!(value));
    }
    if let Some(value) = &input.complete_url {
        insert("complete_url", json!(value));
    }
    if let Some(value) = &input.abort_url {
        insert("abort_url", json!(value));
    }
    if let Some(value) = &input.list_page_url {
        insert("list_page_url", json!(value));
    }
    if let Some(value) = &input.final_head_url {
        insert("final_head_url", json!(value));
    }
    if let Some(value) = &input.final_object_key {
        insert("final_object_key", json!(value));
    }
    if let Some(value) = input.expected_object_size {
        insert("expected_object_size", json!(value));
    }
    if let Some(value) = input.expected_part_count {
        insert("expected_part_count", json!(value));
    }
    if let Some(value) = input.max_part_number {
        insert("max_part_number", json!(value));
    }
    if let Some(value) = &input.final_object_metadata {
        insert("final_object_metadata", json!(value));
    }
    if let Some(value) = input.part_number.filter(|value| *value > 0) {
        insert("part_number", json!(value));
    }

    Ok(SignedChunkRoute {
        source_id: input.chunk.source_id.clone(),
        destination_id: input.target.destination_id.clone(),
        chunk_index: input.chunk.chunk_index,
        delivery_index: None,
        source_url,
        dest_url,
        source_offset: input.chunk.source_offset,
        chunk_size: input.chunk.chunk_size,
        expires_at: Some(iso_at(expires_at)),
        headers: Some(source_grant.headers),
        dest_headers: None,
        metadata,
    })
}

/// Internal immutable grant shared only by one chunk's destinations in one generation.
#[derive(Debug, Clone)]
pub(crate) struct SourceChunkGrant {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub expires_at: SystemTime,
}
pub(crate) async fn sign_source_chunk(
    source: Option<&ProviderSourceConfig>,
    chunk: &ChunkSigningPlanItem,
    source_url: Option<&str>,
    if_match: Option<&str>,
    version_id: Option<&str>,
    options: &ProviderSigningOptions,
) -> Result<SourceChunkGrant, BeamApiError> {
    let _phase = crate::performance::phase("sdk.source_signing");
    let expires_at = SystemTime::now() + options.expires_in();
    let range = range_header(chunk.source_offset, chunk.chunk_size);
    let (url, headers) = match source {
        Some(source) if source.is_s3_compatible() => {
            let settings = s3_settings_for_source(source)?;
            let url = settings.presign(
                &S3Operation::GetObject {
                    range: Some(&range),
                    if_match,
                    version_id,
                },
                &settings.key,
                options.expires_in(),
            )?;
            (url, range_headers(range, if_match))
        }
        _ => {
            let url = match (source_url, source) {
                (Some(url), _) => url.to_owned(),
                (None, Some(source)) => sign_source_url(source, options).await?,
                (None, None) => chunk.source_url.clone(),
            };
            (url, range_headers(range, None))
        }
    };
    let expires_at = bounded_grant_expiry(&url, expires_at);
    Ok(SourceChunkGrant {
        url,
        headers,
        expires_at,
    })
}

pub(crate) fn bounded_grant_expiry(url: &str, mut upper_bound: SystemTime) -> SystemTime {
    let Ok(url) = reqwest::Url::parse(url) else {
        return upper_bound;
    };
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    let parse_date = |value: &str| -> Option<SystemTime> {
        if value.len() != 16 || !value.is_ascii() {
            return None;
        }
        let year = value.get(0..4)?.parse().ok()?;
        let month = time::Month::try_from(value.get(4..6)?.parse::<u8>().ok()?).ok()?;
        let day = value.get(6..8)?.parse().ok()?;
        let hour = value.get(9..11)?.parse().ok()?;
        let minute = value.get(11..13)?.parse().ok()?;
        let second = value.get(13..15)?.parse().ok()?;
        let epoch = time::Date::from_calendar_date(year, month, day)
            .ok()?
            .with_hms(hour, minute, second)
            .ok()?
            .assume_utc()
            .unix_timestamp();
        if epoch < 0 {
            return None;
        }
        std::time::UNIX_EPOCH.checked_add(Duration::from_secs(epoch as u64))
    };
    for prefix in ["X-Amz", "X-Goog"] {
        if let (Some(date), Some(seconds)) = (
            query.get(&format!("{prefix}-Date")),
            query.get(&format!("{prefix}-Expires")),
        ) {
            if let (Some(start), Ok(seconds)) = (parse_date(date), seconds.parse::<u64>()) {
                if let Some(expiry) = start.checked_add(Duration::from_secs(seconds)) {
                    upper_bound = upper_bound.min(expiry);
                }
            }
        }
    }
    if let Some(epoch) = query
        .get("Expires")
        .and_then(|value| value.parse::<u64>().ok())
    {
        if let Some(expiry) = std::time::UNIX_EPOCH.checked_add(Duration::from_secs(epoch)) {
            upper_bound = upper_bound.min(expiry);
        }
    }
    upper_bound
}

/// Sign a whole-object read URL for a source (no range binding).
pub(crate) async fn sign_source_url(
    source: &ProviderSourceConfig,
    options: &ProviderSigningOptions,
) -> Result<String, BeamApiError> {
    if let Some(settings) = S3Settings::from_source(source) {
        return settings.presign(
            &S3Operation::GetObject {
                range: None,
                if_match: None,
                version_id: None,
            },
            &settings.key,
            options.expires_in(),
        );
    }
    match source {
        ProviderSourceConfig::Hippius(source) => {
            options
                .run(hippius_presign(
                    &options.http(),
                    hippius_base_url(source.base_url.as_deref()),
                    &source.api_token,
                    &source.bucket,
                    &source.key,
                    "get",
                    options.expires_in(),
                ))
                .await
        }
        ProviderSourceConfig::HuggingFace(source) => Ok(options
            .run(huggingface::http::file_metadata(
                &HuggingFaceConfig::from_source(source),
            ))
            .await?
            .url),
        other => Err(unsupported_provider(other.provider_name())),
    }
}

/// The multipart coordinates a recovery grant is bound to, with BeamCore's recovery request.
#[derive(Debug, Clone, Copy)]
pub struct MultipartRecoverySignInput<'a> {
    pub destination: &'a ProviderDestinationConfig,
    pub transfer_id: &'a str,
    pub multipart_group_id: &'a str,
    pub final_object_key: &'a str,
    pub upload_id: &'a str,
    pub part_number: u64,
    /// `None` (or `mode: direct` other than `renew`) returns the route unchanged.
    pub recovery: Option<&'a MultipartRecoveryRequest>,
    pub expires_in: Duration,
}

/// Add BeamCore-only multipart recovery grants to a freshly signed recovery `route`, mirroring
/// the TypeScript SDK's `signMultipartRecovery`. The worker still receives only an ordinary
/// upload URL; the grants live in route metadata that BeamCore consumes:
///
/// - `renew`: fresh `complete_url`, `abort_url`, `final_head_url`, every `ListParts` page
///   (`list_page_urls`, 1,000 parts each) and `control_urls_expires_at` for the original upload.
/// - staged `upload` / `controls`: `recovery_staging` with HEAD, `UploadPartCopy` (into the
///   original upload and part number) and delete URLs plus the signed `copy_headers`; `upload`
///   also replaces `dest_url` with a `PutObject` of the staging object.
/// - staged `list`: `recovery_listing` with one `ListObjectsV2` page of the staging prefix.
/// - staged `delete`: `recovery_delete` with a `DeleteObject` URL.
///
/// Staging objects live at
/// `{final_object_key}.beam-recovery/{transfer_id}/{encoded group id}/{part_number}/{attempt_id}`.
/// Only AWS S3 (`provider == "s3"`) binds the copy to the staged object's ETag: R2 does not
/// promise to enforce copy-source conditions, and each attempt has its own object anyway.
pub fn sign_multipart_recovery(
    input: MultipartRecoverySignInput<'_>,
    route: SignedChunkRoute,
) -> Result<SignedChunkRoute, BeamApiError> {
    sign_multipart_recovery_at(input, route, SystemTime::now())
}

pub(crate) fn sign_multipart_recovery_at(
    input: MultipartRecoverySignInput<'_>,
    mut route: SignedChunkRoute,
    now: SystemTime,
) -> Result<SignedChunkRoute, BeamApiError> {
    route.metadata.remove("recovery_staging");
    let Some(request) = input.recovery else {
        return Ok(route);
    };
    let expires_at = || iso_at(now + input.expires_in);
    if request.operation == MultipartRecoveryOperation::Renew {
        let settings = multipart_settings(input.destination, "multipart recovery renewal")?;
        let count = route
            .metadata
            .get("expected_part_count")
            .and_then(Value::as_u64)
            .filter(|count| (1..=MULTIPART_RECOVERY_MAX_PARTS).contains(count))
            .ok_or_else(|| BeamApiError::ProviderSigning("invalid multipart count".to_string()))?;
        let sign = |operation: &S3Operation<'_>, key: &str| {
            settings.presign_at(operation, key, input.expires_in, now)
        };
        let upload_id = input.upload_id;
        let complete = sign(
            &S3Operation::CompleteMultipartUpload { upload_id },
            input.final_object_key,
        )?;
        let abort = sign(
            &S3Operation::AbortMultipartUpload { upload_id },
            input.final_object_key,
        )?;
        let head = sign(&S3Operation::HeadObject, input.final_object_key)?;
        let pages = (0..count)
            .step_by(1_000)
            .map(|marker| {
                sign(
                    &S3Operation::ListParts {
                        upload_id,
                        max_parts: Some(1_000),
                        part_number_marker: Some(marker),
                    },
                    input.final_object_key,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let page = input
            .part_number
            .checked_sub(1)
            .and_then(|index| pages.get((index / 1_000) as usize))
            .cloned()
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("invalid multipart part number".to_string())
            })?;
        for (key, value) in [
            ("complete_url", json!(complete)),
            ("abort_url", json!(abort)),
            ("final_head_url", json!(head)),
            ("list_page_urls", json!(pages)),
            ("list_page_url", json!(page)),
            ("control_urls_expires_at", json!(expires_at())),
        ] {
            route.metadata.insert(key.to_string(), value);
        }
        return Ok(route);
    }
    if request.mode == MultipartRecoveryMode::Direct {
        return Ok(route);
    }
    let settings = S3Settings::from_destination(input.destination).ok_or_else(|| {
        BeamApiError::ProviderSigning(
            "multipart recovery requires an S3-compatible destination".to_string(),
        )
    })?;
    if !is_hyphenated_uuid(&request.attempt_id) {
        return Err(BeamApiError::ProviderSigning(
            "invalid staging attempt".to_string(),
        ));
    }
    let prefix = multipart_recovery_prefix(
        input.final_object_key,
        input.transfer_id,
        input.multipart_group_id,
    );
    let object_key = format!("{prefix}{}/{}", input.part_number, request.attempt_id);
    if request
        .object_key
        .as_deref()
        .is_some_and(|expected| !expected.is_empty() && expected != object_key)
    {
        return Err(BeamApiError::ProviderSigning(
            "recovery staging identity mismatch".to_string(),
        ));
    }
    let sign = |operation: &S3Operation<'_>, key: &str| {
        settings.presign_at(operation, key, input.expires_in, now)
    };
    match request.operation {
        MultipartRecoveryOperation::List => {
            let url = sign(
                &S3Operation::ListObjectsV2 {
                    prefix: &prefix,
                    continuation_token: request
                        .continuation_token
                        .as_deref()
                        .filter(|token| !token.is_empty()),
                    max_keys: Some(1_000),
                },
                "",
            )?;
            route.metadata.insert(
                "recovery_listing".to_string(),
                json!({ "prefix": prefix, "url": url }),
            );
            return Ok(route);
        }
        MultipartRecoveryOperation::Delete => {
            let url = sign(&S3Operation::DeleteObject, &object_key)?;
            route.metadata.insert(
                "recovery_delete".to_string(),
                json!({ "object_key": object_key, "url": url }),
            );
            return Ok(route);
        }
        MultipartRecoveryOperation::Upload | MultipartRecoveryOperation::Controls => {}
        MultipartRecoveryOperation::Renew => unreachable!("renewal is handled above"),
    }
    let copy_source = format!(
        "{}/{}",
        encode_uri_component(&settings.bucket),
        object_key
            .split('/')
            .map(encode_uri_component)
            .collect::<Vec<_>>()
            .join("/")
    );
    let condition = request
        .etag
        .as_deref()
        .filter(|etag| settings.provider == "s3" && !etag.is_empty())
        .map(|etag| {
            let etag = etag.strip_prefix('"').unwrap_or(etag);
            format!("\"{}\"", etag.strip_suffix('"').unwrap_or(etag))
        });
    let head = sign(&S3Operation::HeadObject, &object_key)?;
    let copy = sign(
        &S3Operation::UploadPartCopy {
            upload_id: input.upload_id,
            part_number: input.part_number,
            copy_source: &copy_source,
            copy_source_if_match: condition.as_deref(),
        },
        input.final_object_key,
    )?;
    let remove = sign(&S3Operation::DeleteObject, &object_key)?;
    let mut copy_headers = Map::new();
    copy_headers.insert("x-amz-copy-source".to_string(), json!(copy_source));
    if let Some(condition) = &condition {
        copy_headers.insert("x-amz-copy-source-if-match".to_string(), json!(condition));
    }
    route.metadata.insert(
        "recovery_staging".to_string(),
        json!({
            "object_key": object_key,
            "attempt_id": request.attempt_id,
            "head_url": head,
            "copy_url": copy,
            "delete_url": remove,
            "copy_headers": copy_headers,
            "expires_at": expires_at(),
        }),
    );
    if request.operation == MultipartRecoveryOperation::Upload {
        route.dest_url = sign(&S3Operation::PutObject { content_md5: None }, &object_key)?;
    }
    Ok(route)
}

const MULTIPART_RECOVERY_MAX_PARTS: u64 = crate::multipart_limits::MULTIPART_MAX_PART_NUMBER;

/// `{final_object_key}.beam-recovery/{transfer_id}/{group id, RFC 3986 encoded}/`.
pub(crate) fn multipart_recovery_prefix(
    final_object_key: &str,
    transfer_id: &str,
    multipart_group_id: &str,
) -> String {
    format!(
        "{final_object_key}.beam-recovery/{transfer_id}/{}/",
        crate::sigv4::uri_encode(multipart_group_id, false)
    )
}

/// JavaScript's `encodeURIComponent`: everything except `A-Z a-z 0-9 - _ . ! ~ * ' ( )`.
fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn is_hyphenated_uuid(value: &str) -> bool {
    value.len() == 36 && uuid::Uuid::try_parse(value).is_ok()
}

fn multipart_settings(
    destination: &ProviderDestinationConfig,
    operation: &str,
) -> Result<S3Settings, BeamApiError> {
    S3Settings::from_destination(destination).ok_or_else(|| {
        BeamApiError::ProviderSigning(format!(
            "{operation} is not supported for {}",
            destination.provider_name()
        ))
    })
}

fn range_headers(range: String, if_match: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::from([("Range".to_string(), range)]);
    if let Some(if_match) = if_match {
        headers.insert("If-Match".to_string(), if_match.to_string());
    }
    headers
}

pub(crate) fn range_header(offset: u64, length: u64) -> String {
    let length = length.max(1);
    format!("bytes={}-{}", offset, offset + length - 1)
}

pub(crate) fn hippius_base_url(base_url: Option<&str>) -> &str {
    base_url.unwrap_or(HIPPIUS_DEFAULT_BASE_URL)
}

fn hippius_metadata(bucket: &str, key: &str, base_url: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("bucket".to_string(), json!(bucket)),
        ("key".to_string(), json!(key)),
        ("base_url".to_string(), json!(base_url)),
    ])
}

pub(crate) async fn hippius_presign(
    http: &HttpClient,
    base_url: &str,
    token: &str,
    bucket: &str,
    key: &str,
    action: &str,
    expires_in: Duration,
) -> Result<String, BeamApiError> {
    let response = http
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
        .header("Authorization", format!("Token {token}"))
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(BeamApiError::ProviderRequest {
            provider: "hippius".to_string(),
            operation: "PresignedUrl",
            status: Some(status.as_u16()),
            code: None,
            message: None,
        });
    }
    let payload: Value = response.json().await?;
    payload
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| BeamApiError::ProviderSigning("missing Hippius signed URL".to_string()))
}

pub(crate) async fn hippius_object_size(
    http: &HttpClient,
    base_url: &str,
    token: &str,
    bucket: &str,
    key: &str,
) -> Result<u64, BeamApiError> {
    let response = http
        .get(format!(
            "{}/api/objectstore/buckets/{}/objects/",
            base_url.trim_end_matches('/'),
            url_escape(bucket)
        ))
        .query(&[("prefix", key), ("max_keys", "1")])
        .header("Authorization", format!("Token {token}"))
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(BeamApiError::ProviderRequest {
            provider: "hippius".to_string(),
            operation: "ListObjects",
            status: Some(status.as_u16()),
            code: None,
            message: None,
        });
    }
    let payload: Value = response.json().await?;
    let contents = payload
        .get("Contents")
        .or_else(|| payload.get("contents"))
        .and_then(Value::as_array)
        .and_then(|contents| contents.first())
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!("object not found: hippius://{bucket}/{key}"))
        })?;
    contents
        .get("Size")
        .or_else(|| contents.get("size"))
        .and_then(|size| {
            size.as_u64()
                .or_else(|| size.as_str().and_then(|value| value.parse().ok()))
        })
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!("object not found: hippius://{bucket}/{key}"))
        })
}

fn source_config_id(source: &ProviderSourceConfig) -> Option<&str> {
    match source {
        ProviderSourceConfig::S3(config) => config.source_id.as_deref(),
        ProviderSourceConfig::R2(config) => config.source_id.as_deref(),
        ProviderSourceConfig::S3Compatible(config) => config.source_id.as_deref(),
        ProviderSourceConfig::GCS(config) => config.source_id.as_deref(),
        ProviderSourceConfig::Azure(config) => config.source_id.as_deref(),
        ProviderSourceConfig::Hippius(config) => config.source_id.as_deref(),
        ProviderSourceConfig::HuggingFace(config) => config.source_id.as_deref(),
    }
}

fn destination_config_id(destination: &ProviderDestinationConfig) -> Option<&str> {
    match destination {
        ProviderDestinationConfig::S3(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::R2(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::S3Compatible(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::GCS(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::Azure(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::Hippius(config) => config.destination_id.as_deref(),
        ProviderDestinationConfig::HuggingFace(config) => config.destination_id.as_deref(),
    }
}

pub(crate) fn source_id(configured: Option<&str>, index: usize) -> String {
    configured
        .map(str::to_string)
        .unwrap_or_else(|| format!("src_{index}"))
}

pub(crate) fn destination_id(configured: Option<&str>, index: usize) -> String {
    configured
        .map(str::to_string)
        .unwrap_or_else(|| format!("dst_{index}"))
}

pub(crate) fn filename(key: &str) -> String {
    key.split('/')
        .rfind(|part| !part.is_empty())
        .unwrap_or(key)
        .to_string()
}

fn url_escape(value: &str) -> String {
    value.replace('%', "%25").replace('/', "%2F")
}

fn unsupported_provider(provider: &str) -> BeamApiError {
    BeamApiError::ProviderSigning(format!(
        "{provider} provider signing is not supported by the Rust SDK"
    ))
}

#[cfg(test)]
mod tests;

fn with_storage_location(
    mut metadata: HashMap<String, Value>,
    location: Option<&str>,
) -> HashMap<String, Value> {
    if let Some(location) = location.filter(|value| !value.is_empty()) {
        metadata.insert("storage_location".to_string(), json!(location));
    }
    metadata
}
