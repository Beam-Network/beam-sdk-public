use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

/// Debug view of a secret field: the value is never printed, only whether it is set.
pub(crate) struct Redacted<'a, T>(pub &'a T);

impl fmt::Debug for Redacted<'_, String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Debug for Redacted<'_, Option<String>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(_) => f.write_str("Some(<redacted>)"),
            None => f.write_str("None"),
        }
    }
}

impl fmt::Debug for Redacted<'_, Vec<String>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[<redacted>; {}]", self.0.len())
    }
}

/// Implement `Debug` printing every field, with the `secret` fields redacted. The exhaustive
/// destructuring makes a newly added field a compile error until it is classified here.
macro_rules! redacted_debug {
    ($ty:ident { $($field:ident),* $(,)? } secret { $($secret:ident),* $(,)? }) => {
        impl ::std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                let Self { $($field,)* $($secret,)* } = self;
                f.debug_struct(stringify!($ty))
                    $(.field(stringify!($field), $field))*
                    $(.field(stringify!($secret), &$crate::models::Redacted($secret)))*
                    .finish()
            }
        }
    };
}
pub(crate) use redacted_debug;

pub type SourceConfig = HashMap<String, Value>;
pub type DestConfig = HashMap<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackConfig {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransferCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    #[serde(skip)]
    pub idempotency_key: Option<String>,
    pub sources: Vec<SourceConfig>,
    pub destinations: Vec<DestConfig>,
    pub total_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merkle_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_hashes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callbacks: Option<Vec<CallbackConfig>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub progressive_mode: bool,
    /// Defaults to [`SignedUrlFlow::SignedUrl`] when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_url_flow: Option<SignedUrlFlow>,
}

/// How signed URLs reach BeamCore. `signed_url` is the only flow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignedUrlFlow {
    #[default]
    #[serde(rename = "signed_url")]
    SignedUrl,
}

impl SignedUrlFlow {
    pub fn as_str(self) -> &'static str {
        match self {
            SignedUrlFlow::SignedUrl => "signed_url",
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct S3ProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub region: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

redacted_debug!(S3ProviderSource { storage_location, source_id, bucket, key, region, access_key_id, endpoint_url } secret { secret_access_key, session_token });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct R2ProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

redacted_debug!(R2ProviderSource { storage_location, source_id, bucket, key, access_key_id, account_id, endpoint_url } secret { secret_access_key });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GCSProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub credentials_json: Option<String>,
}

redacted_debug!(GCSProviderSource { storage_location, source_id, bucket, key, project_id } secret { credentials_json });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AzureProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub container: String,
    pub blob: String,
    pub account_name: String,
    #[serde(default)]
    pub account_key: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

redacted_debug!(AzureProviderSource { storage_location, source_id, container, blob, account_name } secret { account_key, sas_token });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HippiusProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub api_token: String,
    #[serde(default)]
    pub base_url: Option<String>,
}

redacted_debug!(HippiusProviderSource { storage_location, source_id, bucket, key, base_url } secret { api_token });

/// SDK-only Hugging Face Hub source configuration.
///
/// The token stays local: the SDK resolves the file to the Hub's presigned CDN URL and sends
/// only that URL to BeamCore.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HuggingFaceProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub source_id: Option<String>,
    pub repo_id: String,
    pub path: String,
    #[serde(default)]
    pub repo_type: Option<String>,
    #[serde(default)]
    pub revision: Option<String>,
    pub token: String,
    #[serde(default)]
    pub endpoint: Option<String>,
}

redacted_debug!(HuggingFaceProviderSource { storage_location, source_id, repo_id, path, repo_type, revision, endpoint } secret { token });

/// Any S3 API-compatible store (Wasabi, MinIO, Backblaze B2, ...) addressed by
/// `endpoint_url`. Serialized with `driver: "s3-compatible"`.
///
/// `provider` names the store; `s3` and `r2` get the same endpoint and region defaults as the
/// dedicated configs. Build with [`S3CompatibleProviderSource::create`] to normalize the
/// provider name and set the driver.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct S3CompatibleProviderSource {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    pub provider: String,
    #[serde(default = "s3_compatible_driver")]
    pub driver: String,
    #[serde(default, alias = "id", skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_path_style: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

redacted_debug!(S3CompatibleProviderSource { storage_location, provider, driver, source_id, bucket, key, region, endpoint_url, access_key_id, force_path_style, account_id } secret { secret_access_key, session_token });

/// Destination counterpart of [`S3CompatibleProviderSource`].
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct S3CompatibleProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    pub provider: String,
    #[serde(default = "s3_compatible_driver")]
    pub driver: String,
    #[serde(default, alias = "id", skip_serializing_if = "Option::is_none")]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_path_style: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

redacted_debug!(S3CompatibleProviderDestination { storage_location, provider, driver, destination_id, bucket, key, region, endpoint_url, access_key_id, force_path_style, account_id } secret { secret_access_key, session_token });

pub(crate) const S3_COMPATIBLE_DRIVER: &str = "s3-compatible";

fn s3_compatible_driver() -> String {
    S3_COMPATIBLE_DRIVER.to_string()
}

/// A transfer source. Serialized with a `provider` tag; configs carrying
/// `driver: "s3-compatible"` (or any unknown provider with S3 credentials) deserialize as
/// [`ProviderSourceConfig::S3Compatible`].
///
/// GCS and Azure are described to BeamCore but have no signing adapter in this SDK.
#[derive(Debug, Clone)]
pub enum ProviderSourceConfig {
    S3(S3ProviderSource),
    R2(R2ProviderSource),
    S3Compatible(S3CompatibleProviderSource),
    GCS(GCSProviderSource),
    Azure(AzureProviderSource),
    Hippius(HippiusProviderSource),
    HuggingFace(HuggingFaceProviderSource),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct S3ProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub region: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

redacted_debug!(S3ProviderDestination { storage_location, destination_id, bucket, key, region, access_key_id, endpoint_url } secret { secret_access_key, session_token });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct R2ProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

redacted_debug!(R2ProviderDestination { storage_location, destination_id, bucket, key, access_key_id, account_id, endpoint_url } secret { secret_access_key });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GCSProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub credentials_json: Option<String>,
}

redacted_debug!(GCSProviderDestination { storage_location, destination_id, bucket, key, project_id } secret { credentials_json });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AzureProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub container: String,
    pub blob: String,
    pub account_name: String,
    #[serde(default)]
    pub account_key: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

redacted_debug!(AzureProviderDestination { storage_location, destination_id, container, blob, account_name } secret { account_key, sas_token });

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HippiusProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub api_token: String,
    #[serde(default)]
    pub base_url: Option<String>,
}

redacted_debug!(HippiusProviderDestination { storage_location, destination_id, bucket, key, base_url } secret { api_token });

/// SDK-only Hugging Face Hub destination configuration.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HuggingFaceProviderDestination {
    /// Physical location, independent of the signing region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, alias = "id")]
    pub destination_id: Option<String>,
    pub repo_id: String,
    pub path: String,
    #[serde(default)]
    pub repo_type: Option<String>,
    #[serde(default)]
    pub revision: Option<String>,
    pub token: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub commit_message: Option<String>,
    #[serde(default)]
    pub commit_description: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub create_pr: bool,
    /// The Hub issues LFS upload URLs only for a known sha256, so the SDK must read every
    /// source byte once to compute it. Opt in explicitly.
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_source_rehash: bool,
}

redacted_debug!(HuggingFaceProviderDestination { storage_location, destination_id, repo_id, path, repo_type, revision, endpoint, commit_message, commit_description, create_pr, allow_source_rehash } secret { token });

/// A transfer destination. See [`ProviderSourceConfig`] for the serialization rules.
#[derive(Debug, Clone)]
pub enum ProviderDestinationConfig {
    S3(S3ProviderDestination),
    R2(R2ProviderDestination),
    S3Compatible(S3CompatibleProviderDestination),
    GCS(GCSProviderDestination),
    Azure(AzureProviderDestination),
    Hippius(HippiusProviderDestination),
    HuggingFace(HuggingFaceProviderDestination),
}

macro_rules! provider_config_serde {
    ($enum:ident, $s3:ty, $r2:ty, $compatible:ty, $gcs:ty, $azure:ty, $hippius:ty, $huggingface:ty) => {
        impl $enum {
            /// The provider name sent to BeamCore.
            pub fn provider_name(&self) -> &str {
                match self {
                    $enum::S3(_) => "s3",
                    $enum::R2(_) => "r2",
                    $enum::S3Compatible(config) => &config.provider,
                    $enum::GCS(_) => "gcs",
                    $enum::Azure(_) => "azure",
                    $enum::Hippius(_) => "hippius",
                    $enum::HuggingFace(_) => "huggingface",
                }
            }
        }

        impl Serialize for $enum {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                fn tagged<T: Serialize, E: serde::ser::Error>(
                    provider: &str,
                    config: &T,
                ) -> Result<Value, E> {
                    let mut value = serde_json::to_value(config).map_err(E::custom)?;
                    if let Some(object) = value.as_object_mut() {
                        object.insert("provider".to_string(), Value::String(provider.to_string()));
                    }
                    Ok(value)
                }
                let value = match self {
                    $enum::S3(config) => tagged::<_, S::Error>("s3", config)?,
                    $enum::R2(config) => tagged::<_, S::Error>("r2", config)?,
                    $enum::S3Compatible(config) => {
                        serde_json::to_value(config).map_err(serde::ser::Error::custom)?
                    }
                    $enum::GCS(config) => tagged::<_, S::Error>("gcs", config)?,
                    $enum::Azure(config) => tagged::<_, S::Error>("azure", config)?,
                    $enum::Hippius(config) => tagged::<_, S::Error>("hippius", config)?,
                    $enum::HuggingFace(config) => tagged::<_, S::Error>("huggingface", config)?,
                };
                value.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $enum {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                use serde::de::Error;
                let value = Value::deserialize(deserializer)?;
                let provider = value
                    .get("provider")
                    .and_then(Value::as_str)
                    .ok_or_else(|| D::Error::missing_field("provider"))?
                    .to_string();
                let driver = value.get("driver").and_then(Value::as_str);
                let has_s3_credentials = value.get("access_key_id").is_some()
                    && value.get("secret_access_key").is_some();
                if driver == Some(S3_COMPATIBLE_DRIVER) {
                    return serde_json::from_value::<$compatible>(value)
                        .map($enum::S3Compatible)
                        .map_err(D::Error::custom);
                }
                match provider.as_str() {
                    "s3" => serde_json::from_value::<$s3>(value).map($enum::S3),
                    "r2" => serde_json::from_value::<$r2>(value).map($enum::R2),
                    "gcs" => serde_json::from_value::<$gcs>(value).map($enum::GCS),
                    "azure" => serde_json::from_value::<$azure>(value).map($enum::Azure),
                    "hippius" => serde_json::from_value::<$hippius>(value).map($enum::Hippius),
                    "huggingface" => {
                        serde_json::from_value::<$huggingface>(value).map($enum::HuggingFace)
                    }
                    _ if has_s3_credentials => {
                        serde_json::from_value::<$compatible>(value).map($enum::S3Compatible)
                    }
                    other => {
                        return Err(D::Error::custom(format!("unsupported provider: {other}")))
                    }
                }
                .map_err(D::Error::custom)
            }
        }
    };
}

provider_config_serde!(
    ProviderSourceConfig,
    S3ProviderSource,
    R2ProviderSource,
    S3CompatibleProviderSource,
    GCSProviderSource,
    AzureProviderSource,
    HippiusProviderSource,
    HuggingFaceProviderSource
);

provider_config_serde!(
    ProviderDestinationConfig,
    S3ProviderDestination,
    R2ProviderDestination,
    S3CompatibleProviderDestination,
    GCSProviderDestination,
    AzureProviderDestination,
    HippiusProviderDestination,
    HuggingFaceProviderDestination
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCreateResponse {
    pub success: bool,
    #[serde(default)]
    pub transfer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_urls: Option<Vec<HashMap<String, String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dest_urls: Option<Vec<HashMap<String, String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_ids: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub total_chunks: u64,
    #[serde(default)]
    pub total_sources: u64,
    #[serde(default)]
    pub total_destinations: u64,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributeResponse {
    pub success: bool,
    pub transfer_id: String,
    #[serde(default)]
    pub orchestrators_assigned: u64,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCancelResponse {
    pub success: bool,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatusInfo {
    pub source_index: u64,
    pub source_type: String,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationStatusInfo {
    pub dest_index: u64,
    pub dest_type: String,
    pub status: String,
    #[serde(default)]
    pub chunks_delivered: u64,
    #[serde(default)]
    pub chunks_pending: u64,
    #[serde(default)]
    pub chunks_in_progress: u64,
    #[serde(default)]
    pub chunks_completed: u64,
    #[serde(default)]
    pub chunks_failed: u64,
    #[serde(default)]
    pub bytes_delivered: u64,
    #[serde(default)]
    pub location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMeasurement {
    pub count: u64,
    pub work_ms: f64,
    pub max_ms: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceEvent {
    pub count: u64,
    pub first_ms: f64,
    pub last_ms: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceCounters {
    pub source_signatures: u64,
    pub source_reuses: u64,
    pub route_batches: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryBandwidthPerformance {
    pub excluded_tasks: u64,
    pub accepted_bytes: u64,
}
/// Work durations overlap and must not be summed into elapsed time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferPerformance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_bandwidth: Option<RecoveryBandwidthPerformance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_detail: Option<crate::performance::SdkPerformanceSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<AssignmentDeadlinePerformance>,
    pub schema_version: String,
    pub runtime_epoch: String,
    pub coverage: String,
    pub elapsed_ms: f64,
    pub measurements: std::collections::HashMap<String, PerformanceMeasurement>,
    pub events: std::collections::HashMap<String, PerformanceEvent>,
    pub sdk_counters: PerformanceCounters,
    pub detail_dropped: u64,
    #[serde(default)]
    pub unmeasured: Vec<String>,
    #[serde(default)]
    pub measurement_clocks: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub sdk_report_received: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentDeadlinePerformance {
    pub version: String,
    pub decisions: u64,
    pub selected_min_ms: f64,
    pub selected_max_ms: f64,
    pub estimated_max_ms: f64,
    pub sample_count_max: u64,
    pub data_age_max_ms: Option<f64>,
    pub reasons: std::collections::HashMap<String, u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferStatusInfo {
    #[serde(default)]
    pub performance: Option<TransferPerformance>,
    pub transfer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub status: String,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub runtime: bool,
    #[serde(default)]
    pub source_bytes_total: u64,
    #[serde(default)]
    pub delivery_bytes_total: u64,
    #[serde(default)]
    pub delivery_bytes_completed: u64,
    #[serde(default)]
    pub delivery_tasks_total: u64,
    #[serde(default)]
    pub delivery_tasks_completed: u64,
    #[serde(default)]
    pub destinations_total: u64,
    #[serde(default)]
    pub destinations_completed: u64,
    #[serde(default)]
    pub destination_progress: Vec<DestinationTransferProgress>,
    #[serde(default)]
    pub destination_groups: DestinationGroupProgress,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub integrity_audit_challenge: Option<IntegrityAuditChallenge>,
    #[serde(default)]
    pub integrity_check_warning: Option<String>,
    /// Set by the SDK when answering `integrity_audit_challenge` failed. The summary is at most
    /// 200 characters and never contains URLs or credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity_audit_submission_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityAuditChallenge {
    pub audit_id: String,
    pub transfer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_bytes: Option<u64>,
    #[serde(default)]
    pub chunks: Vec<IntegrityAuditChallengeChunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityAuditChallengeChunk {
    pub challenge_id: String,
    pub task_id: String,
    pub attempt_id: Option<String>,
    /// Runtime-internal assignment fields; never echoed back in grants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestrator_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestrator_hotkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    pub source_id: String,
    pub destination_id: String,
    pub route_chunk_index: u64,
    pub delivery_index: u64,
    pub source_offset: u64,
    pub destination_offset: u64,
    pub range_length: u64,
    pub final_object_key: String,
    pub final_object_etag: Option<String>,
    /// Fields added by newer Runtimes, echoed back unchanged in the grant.
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationTransferProgress {
    pub destination_id: String,
    pub delivery_bytes_total: u64,
    pub delivery_bytes_completed: u64,
    pub completion_verified: bool,
}

/// A source described for `transfer.plan`; the URL is optional because planning needs only the
/// size and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningHttpSource {
    pub source_id: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

impl From<PreparedHttpSource> for PlanningHttpSource {
    fn from(source: PreparedHttpSource) -> Self {
        Self {
            source_id: source.source_id,
            source_type: source.source_type,
            provider: source.provider,
            url: Some(source.url),
            size: source.size,
            filename: source.filename,
            headers: source.headers,
            expires_at: source.expires_at,
            metadata: source.metadata,
        }
    }
}

/// Input for [`crate::BeamClient::prepare_transfer_with_options`].
#[derive(Debug, Clone, Default)]
pub struct TransferPrepareInput {
    /// Explicit transfer id. Derived from `idempotency_key` (or random) when unset.
    pub transfer_id: Option<String>,
    pub sources: Vec<PreparedHttpSource>,
    pub destinations: Vec<PreparedDestination>,
    pub name: Option<String>,
    pub urls_expires_at: Option<String>,
    /// Stable caller identity used to derive the transfer id.
    pub idempotency_key: Option<String>,
    /// Internal recovery generation; callers normally leave this unset.
    pub route_generation_id: Option<String>,
}

/// Options for [`crate::BeamClient::wait_for_transfer_with_options`]. `None` selects the
/// default; an explicit zero duration is rejected.
#[derive(Debug, Clone, Copy, Default)]
pub struct WaitForTransferOptions {
    /// Overall deadline. Defaults to 300 seconds.
    pub timeout: Option<Duration>,
    /// Initial status poll interval. Defaults to 15 seconds.
    pub poll_interval: Option<Duration>,
    /// Upper bound for the backed-off poll interval. Defaults to 30 seconds and is never below
    /// `poll_interval`.
    pub max_poll_interval: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedHttpSource {
    pub source_id: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub url: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedDestination {
    pub destination_id: String,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkDestinationSigningTarget {
    pub destination_id: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub object_key: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSigningPlanItem {
    pub chunk_index: u64,
    pub source_id: String,
    pub source_chunk_index: u64,
    pub source_offset: u64,
    pub chunk_size: u64,
    pub source_url: String,
    #[serde(default)]
    pub destinations: Vec<ChunkDestinationSigningTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTransferPlanSource {
    #[serde(flatten)]
    pub source: PreparedHttpSource,
    pub global_chunk_start: u64,
    pub chunk_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTransferPlanDestination {
    #[serde(flatten)]
    pub destination: PreparedDestination,
    pub destination_index: u64,
    pub final_object_keys: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactTransferPlanDescriptor {
    pub version: String,
    pub plan_nonce: String,
    pub chunk_size: u64,
    pub sources: Vec<CompactTransferPlanSource>,
    pub destinations: Vec<CompactTransferPlanDestination>,
    pub logical_chunk_count: u64,
    pub delivery_route_count: u64,
    pub multipart_attempt_slots: u8,
    pub formulas: CompactTransferPlanFormulas,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DestinationGroupProgress {
    pub total_groups: u64,
    pub completed_groups: u64,
    pub pending_groups: u64,
    pub total_destinations: u64,
    pub completed_destinations: u64,
    pub completed_destination_ids: Vec<String>,
}

/// Terminal signal published by Runtime. Unknown fields are ignored so Runtime can extend the
/// event without breaking older SDKs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferTerminalEvent {
    pub schema_version: String,
    pub producer: String,
    pub transfer_id: String,
    pub status: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactTransferPlanFormulas {
    pub source_offset: String,
    pub delivery_index: String,
    pub part_number: String,
    pub route_generation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedChunkRoute {
    pub source_id: String,
    pub destination_id: String,
    pub chunk_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_index: Option<u64>,
    pub source_url: String,
    pub dest_url: String,
    pub source_offset: u64,
    pub chunk_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest_headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

/// What a multipart recovery signing request asks for (`transfer-client-control/v7`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MultipartRecoveryOperation {
    /// Staging `PutObject` plus its HEAD, copy and delete controls.
    Upload,
    /// HEAD, copy and delete controls for an already staged object.
    Controls,
    /// One `ListObjectsV2` page of the staging prefix.
    List,
    /// `DeleteObject` for one staged object.
    Delete,
    /// Fresh complete, abort, final HEAD and every `ListParts` page for the original upload.
    Renew,
}

/// Whether a recovered part is uploaded directly or staged and copied into the original part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MultipartRecoveryMode {
    Staged,
    Direct,
}

/// BeamCore's request for Core-only multipart recovery grants, carried by a
/// `transfer.route_recovery.sign` chunk. Workers only ever receive the ordinary upload URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartRecoveryRequest {
    pub operation: MultipartRecoveryOperation,
    pub mode: MultipartRecoveryMode,
    #[serde(default)]
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartGroupManifest {
    pub multipart_group_id: String,
    pub source_id: String,
    pub destination_id: String,
    pub final_object_key: String,
    pub upload_id: String,
    pub expected_object_size: u64,
    pub expected_part_count: u64,
    pub max_part_number: u64,
    pub complete_url: String,
    pub abort_url: String,
    pub list_page_urls: Vec<String>,
    pub final_head_url: String,
    pub final_object_metadata: HashMap<String, String>,
    pub urls_expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferPrepareResponse {
    pub success: bool,
    #[serde(default)]
    pub transfer_id: String,
    #[serde(default)]
    pub transfer_key: Option<String>,
    #[serde(default)]
    pub chunk_size: u64,
    #[serde(default)]
    pub total_size: u64,
    #[serde(default)]
    pub total_sources: u64,
    #[serde(default)]
    pub total_destinations: u64,
    #[serde(default)]
    pub logical_chunks: u64,
    #[serde(default)]
    pub total_chunks: u64,
    /// Present when `success` is true.
    #[serde(default)]
    pub plan_descriptor: CompactTransferPlanDescriptor,
    #[serde(default)]
    pub signed_url_flow: String,
    #[serde(default)]
    pub plan_fingerprint: String,
    #[serde(default)]
    pub coordinate_checksum: String,
    #[serde(default)]
    pub route_generation_id: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: String,
}

/// Reply to `transfer.plan`: the compact plan BeamCore would prepare, without creating a
/// transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferPlanResponse {
    pub success: bool,
    #[serde(default)]
    pub chunk_size: u64,
    #[serde(default)]
    pub total_size: u64,
    #[serde(default)]
    pub total_sources: u64,
    #[serde(default)]
    pub total_destinations: u64,
    #[serde(default)]
    pub logical_chunks: u64,
    #[serde(default)]
    pub total_chunks: u64,
    /// Present when `success` is true.
    #[serde(default)]
    pub plan_descriptor: CompactTransferPlanDescriptor,
    #[serde(default)]
    pub signed_url_flow: String,
    #[serde(default)]
    pub plan_fingerprint: String,
    #[serde(default)]
    pub coordinate_checksum: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: String,
}

/// Input for [`crate::BeamClient::plan_transfer`].
#[derive(Debug, Clone, Default)]
pub struct TransferPlanInput {
    pub sources: Vec<PlanningHttpSource>,
    pub destinations: Vec<PreparedDestination>,
    pub name: Option<String>,
    pub urls_expires_at: Option<String>,
}

/// Durable identity of one multipart upload created by the provider flow.
///
/// Passed to `on_multipart_group_ready` before the group's routes are streamed and required by
/// `resume_provider_transfer`. It carries no credentials, headers, or signed URLs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMultipartGroupIdentity {
    pub transfer_id: String,
    pub multipart_group_id: String,
    pub source_id: String,
    pub destination_id: String,
    pub object_key: String,
    pub upload_id: String,
    pub expected_object_size: u64,
    pub expected_part_count: u64,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachSignedUrlsResponse {
    pub success: bool,
    #[serde(default)]
    pub transfer_id: String,
    #[serde(default)]
    pub total_routes: u64,
    #[serde(default)]
    pub urls_expires_at: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: String,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;
    use crate::huggingface::{HuggingFaceConfig, HuggingFaceFileMetadata, HuggingFaceUploadPlan};

    const SECRETS: [&str; 8] = [
        "SECRET-ACCESS-KEY",
        "SESSION-TOKEN",
        "API-TOKEN",
        "HF-TOKEN",
        "CREDENTIALS-JSON",
        "ACCOUNT-KEY",
        "SAS-TOKEN",
        "PRESIGNED-SIGNATURE",
    ];

    fn assert_redacted(value: &impl fmt::Debug, visible: &str) {
        let printed = format!("{value:?}");
        for secret in SECRETS {
            assert!(!printed.contains(secret), "{secret} leaked: {printed}");
        }
        assert!(printed.contains("<redacted>"), "{printed}");
        assert!(printed.contains(visible), "{visible} missing: {printed}");
    }

    fn some(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[test]
    fn provider_config_debug_output_redacts_every_secret() {
        let (secret, session) = ("SECRET-ACCESS-KEY".to_string(), some("SESSION-TOKEN"));
        let sources = [
            ProviderSourceConfig::S3(S3ProviderSource {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                session_token: session.clone(),
                ..Default::default()
            }),
            ProviderSourceConfig::R2(R2ProviderSource {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                ..Default::default()
            }),
            ProviderSourceConfig::S3Compatible(S3CompatibleProviderSource {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                session_token: session.clone(),
                ..Default::default()
            }),
            ProviderSourceConfig::GCS(GCSProviderSource {
                bucket: "visible-bucket".into(),
                credentials_json: some("CREDENTIALS-JSON"),
                ..Default::default()
            }),
            ProviderSourceConfig::Azure(AzureProviderSource {
                container: "visible-bucket".into(),
                account_key: some("ACCOUNT-KEY"),
                sas_token: some("SAS-TOKEN"),
                ..Default::default()
            }),
            ProviderSourceConfig::Hippius(HippiusProviderSource {
                bucket: "visible-bucket".into(),
                api_token: "API-TOKEN".into(),
                ..Default::default()
            }),
            ProviderSourceConfig::HuggingFace(HuggingFaceProviderSource {
                repo_id: "visible-bucket".into(),
                token: "HF-TOKEN".into(),
                ..Default::default()
            }),
        ];
        for source in &sources {
            assert_redacted(source, "visible-bucket");
        }
        let destinations = [
            ProviderDestinationConfig::S3(S3ProviderDestination {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                session_token: session.clone(),
                ..Default::default()
            }),
            ProviderDestinationConfig::R2(R2ProviderDestination {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                ..Default::default()
            }),
            ProviderDestinationConfig::S3Compatible(S3CompatibleProviderDestination {
                bucket: "visible-bucket".into(),
                secret_access_key: secret.clone(),
                session_token: session.clone(),
                ..Default::default()
            }),
            ProviderDestinationConfig::GCS(GCSProviderDestination {
                bucket: "visible-bucket".into(),
                credentials_json: some("CREDENTIALS-JSON"),
                ..Default::default()
            }),
            ProviderDestinationConfig::Azure(AzureProviderDestination {
                container: "visible-bucket".into(),
                account_key: some("ACCOUNT-KEY"),
                sas_token: some("SAS-TOKEN"),
                ..Default::default()
            }),
            ProviderDestinationConfig::Hippius(HippiusProviderDestination {
                bucket: "visible-bucket".into(),
                api_token: "API-TOKEN".into(),
                ..Default::default()
            }),
            ProviderDestinationConfig::HuggingFace(HuggingFaceProviderDestination {
                repo_id: "visible-bucket".into(),
                token: "HF-TOKEN".into(),
                ..Default::default()
            }),
        ];
        for destination in &destinations {
            assert_redacted(destination, "visible-bucket");
        }
        let ProviderDestinationConfig::HuggingFace(huggingface) = &destinations[6] else {
            unreachable!()
        };
        assert_redacted(
            &HuggingFaceConfig::from_destination(huggingface),
            "visible-bucket",
        );
        assert_redacted(
            &HuggingFaceFileMetadata {
                url: "https://cdn.example/f?PRESIGNED-SIGNATURE".into(),
                size: 4242,
                etag: None,
                commit_hash: None,
            },
            "4242",
        );
        assert_redacted(
            &HuggingFaceUploadPlan {
                oid: "visible-oid".into(),
                upload_href: some("https://hub.example/u?PRESIGNED-SIGNATURE"),
                part_urls: vec!["https://s3.example/p1?PRESIGNED-SIGNATURE".into()],
                verify_href: some("https://hub.example/v?PRESIGNED-SIGNATURE"),
                ..Default::default()
            },
            "visible-oid",
        );
        // Unset optional secrets stay distinguishable from set ones.
        let unset = format!(
            "{:?}",
            S3ProviderSource {
                secret_access_key: secret,
                ..Default::default()
            }
        );
        assert!(unset.contains("session_token: None"), "{unset}");
    }
}
