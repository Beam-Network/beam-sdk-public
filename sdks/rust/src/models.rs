use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub type SourceConfig = HashMap<String, Value>;
pub type DestConfig = HashMap<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackConfig {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    #[serde(skip)]
    pub idempotency_key: Option<String>,
    pub sources: Vec<SourceConfig>,
    pub destinations: Vec<DestConfig>,
    pub total_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merkle_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_hashes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callbacks: Option<Vec<CallbackConfig>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub test_mode: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub progressive_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ProviderSource {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2ProviderSource {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCSProviderSource {
    #[serde(default)]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub credentials_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureProviderSource {
    #[serde(default)]
    pub source_id: Option<String>,
    pub container: String,
    pub blob: String,
    pub account_name: String,
    #[serde(default)]
    pub account_key: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HippiusProviderSource {
    #[serde(default)]
    pub source_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub api_token: String,
    #[serde(default)]
    pub base_url: Option<String>,
}

/// SDK-only Hugging Face Hub source configuration.
///
/// The token stays local: the SDK resolves the file to the Hub's presigned CDN URL and sends
/// only that URL to BeamCore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuggingFaceProviderSource {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum ProviderSourceConfig {
    S3(S3ProviderSource),
    R2(R2ProviderSource),
    #[serde(rename = "gcs")]
    GCS(GCSProviderSource),
    Azure(AzureProviderSource),
    Hippius(HippiusProviderSource),
    #[serde(rename = "huggingface")]
    HuggingFace(HuggingFaceProviderSource),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ProviderDestination {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2ProviderDestination {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCSProviderDestination {
    #[serde(default)]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub credentials_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureProviderDestination {
    #[serde(default)]
    pub destination_id: Option<String>,
    pub container: String,
    pub blob: String,
    pub account_name: String,
    #[serde(default)]
    pub account_key: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HippiusProviderDestination {
    #[serde(default)]
    pub destination_id: Option<String>,
    pub bucket: String,
    pub key: String,
    pub api_token: String,
    #[serde(default)]
    pub base_url: Option<String>,
}

/// SDK-only Hugging Face Hub destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuggingFaceProviderDestination {
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum ProviderDestinationConfig {
    S3(S3ProviderDestination),
    R2(R2ProviderDestination),
    #[serde(rename = "gcs")]
    GCS(GCSProviderDestination),
    Azure(AzureProviderDestination),
    Hippius(HippiusProviderDestination),
    #[serde(rename = "huggingface")]
    HuggingFace(HuggingFaceProviderDestination),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCreateResponse {
    pub success: bool,
    #[serde(default)]
    pub transfer_id: String,
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
pub struct TransferStatusInfo {
    pub transfer_id: String,
    pub status: String,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub runtime: bool,
    #[serde(default)]
    pub source_bytes_total: u64,
    pub delivery_bytes_total: u64,
    pub delivery_bytes_completed: u64,
    pub delivery_tasks_total: u64,
    pub delivery_tasks_completed: u64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationTransferProgress {
    pub destination_id: String,
    pub delivery_bytes_total: u64,
    pub delivery_bytes_completed: u64,
    pub completion_verified: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferTerminalEvent {
    pub schema_version: String,
    pub producer: String,
    pub transfer_id: String,
    pub status: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub test_mode: bool,
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
    pub plan_descriptor: CompactTransferPlanDescriptor,
    pub signed_url_flow: String,
    pub plan_fingerprint: String,
    pub coordinate_checksum: String,
    pub route_generation_id: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: String,
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
