"""Transfer models for the BEAM Python SDK."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

SignedUrlFlow = Literal["signed_url"]


class SourceConfig(BaseModel):
    """Provider-agnostic source configuration accepted by BeamCore."""

    type: str = Field(..., description="Source type: http, s3, r2, gcs, or webhook")
    bucket: str | None = None
    key: str | None = None
    region: str = "us-east-1"
    project_id: str | None = None
    account_id: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    url: str | None = None
    headers: dict[str, str] | None = None


class DestConfig(BaseModel):
    """Provider-agnostic destination configuration accepted by BeamCore."""

    type: str = Field(..., description="Destination type: http, s3, r2, gcs, or webhook")
    bucket: str | None = None
    key: str | None = None
    region: str = "us-east-1"
    project_id: str | None = None
    account_id: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    url: str | None = None
    headers: dict[str, str] | None = None


class S3ProviderSource(BaseModel):
    """SDK-only S3 source configuration.

    Credentials stay local. The SDK signs a short-lived read URL and sends only
    the prepared HTTP source to BeamCore.
    """

    provider: Literal["s3"] = "s3"
    source_id: str | None = None
    bucket: str
    key: str
    region: str = "us-east-1"
    access_key_id: str
    secret_access_key: SecretStr
    session_token: SecretStr | None = None
    endpoint_url: str | None = None


class R2ProviderSource(BaseModel):
    """SDK-only Cloudflare R2 source configuration."""

    provider: Literal["r2"] = "r2"
    source_id: str | None = None
    bucket: str
    key: str
    access_key_id: str
    secret_access_key: SecretStr
    account_id: str | None = None
    endpoint_url: str | None = None


class GCSProviderSource(BaseModel):
    """SDK-only GCS source configuration placeholder."""

    provider: Literal["gcs"] = "gcs"
    source_id: str | None = None
    bucket: str
    key: str
    project_id: str | None = None
    credentials_path: str | None = None
    service_account_json: dict[str, Any] | None = None


class AzureProviderSource(BaseModel):
    """SDK-only Azure Blob source configuration placeholder."""

    provider: Literal["azure"] = "azure"
    source_id: str | None = None
    container: str
    blob: str
    account_name: str
    account_key: SecretStr | None = None
    sas_token: SecretStr | None = None


class HippiusProviderSource(BaseModel):
    """SDK-only Hippius object-store source configuration."""

    provider: Literal["hippius"] = "hippius"
    source_id: str | None = None
    bucket: str
    key: str
    api_token: SecretStr
    base_url: str = "https://api.hippius.com"


HuggingFaceRepoType = Literal["model", "dataset", "space", "kernel", "bucket"]


class HuggingFaceProviderSource(BaseModel):
    """SDK-only Hugging Face Hub source configuration.

    The token stays local: the SDK resolves the file to the Hub's presigned CDN URL and
    sends only that URL to BeamCore.
    """

    provider: Literal["huggingface"] = "huggingface"
    source_id: str | None = None
    repo_id: str
    path: str
    repo_type: HuggingFaceRepoType = "model"
    revision: str = "main"
    token: SecretStr
    endpoint: str = "https://huggingface.co"


class S3ProviderDestination(BaseModel):
    """SDK-only S3 destination configuration."""

    provider: Literal["s3"] = "s3"
    destination_id: str | None = None
    bucket: str
    key: str
    region: str = "us-east-1"
    access_key_id: str
    secret_access_key: SecretStr
    session_token: SecretStr | None = None
    endpoint_url: str | None = None


class R2ProviderDestination(BaseModel):
    """SDK-only Cloudflare R2 destination configuration."""

    provider: Literal["r2"] = "r2"
    destination_id: str | None = None
    bucket: str
    key: str
    access_key_id: str
    secret_access_key: SecretStr
    account_id: str | None = None
    endpoint_url: str | None = None


class GCSProviderDestination(BaseModel):
    """SDK-only GCS destination configuration placeholder."""

    provider: Literal["gcs"] = "gcs"
    destination_id: str | None = None
    bucket: str
    key: str
    project_id: str | None = None
    credentials_path: str | None = None
    service_account_json: dict[str, Any] | None = None


class AzureProviderDestination(BaseModel):
    """SDK-only Azure Blob destination configuration placeholder."""

    provider: Literal["azure"] = "azure"
    destination_id: str | None = None
    container: str
    blob: str
    account_name: str
    account_key: SecretStr | None = None
    sas_token: SecretStr | None = None


class HippiusProviderDestination(BaseModel):
    """SDK-only Hippius object-store destination configuration."""

    provider: Literal["hippius"] = "hippius"
    destination_id: str | None = None
    bucket: str
    key: str
    api_token: SecretStr
    base_url: str = "https://api.hippius.com"


class HuggingFaceProviderDestination(BaseModel):
    """SDK-only Hugging Face Hub destination configuration."""

    provider: Literal["huggingface"] = "huggingface"
    destination_id: str | None = None
    repo_id: str
    path: str
    repo_type: HuggingFaceRepoType = "model"
    revision: str = "main"
    token: SecretStr
    endpoint: str = "https://huggingface.co"
    commit_message: str | None = None
    commit_description: str | None = None
    create_pr: bool = False
    #: The Hub issues LFS upload URLs only for a known sha256, so the SDK must read every
    #: source byte once to compute it. Opt in explicitly.
    allow_source_rehash: bool = False


ProviderSourceConfig = (
    S3ProviderSource
    | R2ProviderSource
    | GCSProviderSource
    | AzureProviderSource
    | HippiusProviderSource
    | HuggingFaceProviderSource
)
ProviderDestinationConfig = (
    S3ProviderDestination
    | R2ProviderDestination
    | GCSProviderDestination
    | AzureProviderDestination
    | HippiusProviderDestination
    | HuggingFaceProviderDestination
)


class PreparedHttpSource(BaseModel):
    """Provider-agnostic source descriptor sent from the SDK to BeamCore."""

    source_id: str
    type: Literal["http"] = "http"
    provider: str | None = None
    url: str
    size: int = Field(..., gt=0)
    filename: str | None = None
    headers: dict[str, str] | None = None
    expires_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PreparedDestination(BaseModel):
    """Non-sensitive destination descriptor sent from the SDK to BeamCore."""

    destination_id: str
    provider: str
    mode: Literal["object_chunks", "http_chunks"] = "object_chunks"
    logical_prefix: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChunkDestinationSigningTarget(BaseModel):
    """Destination target returned by BeamCore for SDK-side URL signing."""

    destination_id: str
    provider: str | None = None
    object_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChunkSigningPlanItem(BaseModel):
    """One source chunk and its destination targets in BeamCore's signing plan."""

    chunk_index: int = Field(..., ge=0)
    source_id: str
    source_chunk_index: int = Field(..., ge=0)
    source_offset: int = Field(..., ge=0)
    chunk_size: int = Field(..., gt=0)
    source_url: str
    destinations: list[ChunkDestinationSigningTarget] = Field(default_factory=list)


class CompactTransferPlanSource(PreparedHttpSource):
    global_chunk_start: int = Field(..., ge=0)
    chunk_count: int = Field(..., gt=0)


class CompactTransferPlanDestination(PreparedDestination):
    destination_index: int = Field(..., ge=0)
    final_object_keys: dict[str, str] = Field(default_factory=dict)


class CompactTransferPlanFormulas(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_offset: Literal["source_chunk_index * chunk_size"]
    delivery_index: Literal["chunk_index * destination_count + destination_index"]
    part_number: Literal["source_chunk_index * 3 + attempt_slot + 1"]
    route_generation_id: Literal["initial-{chunk_index}-{destination_id}"]


class CompactTransferPlanDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal["compact-transfer-plan/v1"]
    plan_nonce: str
    chunk_size: int = Field(..., gt=0)
    sources: list[CompactTransferPlanSource] = Field(..., min_length=1)
    destinations: list[CompactTransferPlanDestination] = Field(..., min_length=1)
    logical_chunk_count: int = Field(..., gt=0)
    delivery_route_count: int = Field(..., gt=0)
    multipart_attempt_slots: Literal[3]
    formulas: CompactTransferPlanFormulas


class SignedChunkRoute(BaseModel):
    """HTTP route attached by the SDK after destination URL signing."""

    source_id: str
    destination_id: str
    chunk_index: int = Field(..., ge=0)
    delivery_index: int | None = Field(default=None, ge=0)
    source_url: str
    dest_url: str
    source_offset: int = Field(..., ge=0)
    chunk_size: int = Field(..., gt=0)
    expires_at: str | None = None
    headers: dict[str, str] | None = None
    dest_headers: dict[str, str] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MultipartGroupManifest(BaseModel):
    """Deduplicated signed-url lifecycle contract for one multipart object."""

    model_config = ConfigDict(extra="forbid")

    multipart_group_id: str = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    destination_id: str = Field(..., min_length=1)
    final_object_key: str = Field(..., min_length=1)
    upload_id: str = Field(..., min_length=1)
    expected_object_size: int = Field(..., gt=0)
    expected_part_count: int = Field(..., gt=0)
    max_part_number: int = Field(..., ge=1, le=10_000)
    complete_url: str = Field(..., min_length=1)
    abort_url: str = Field(..., min_length=1)
    list_page_urls: list[str] = Field(..., min_length=1)
    final_head_url: str = Field(..., min_length=1)
    final_object_metadata: dict[str, str]
    urls_expires_at: str = Field(..., min_length=1)

    @field_validator("final_object_metadata")
    @classmethod
    def validate_final_object_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        if "beam-transfer-id" not in value:
            raise ValueError("final_object_metadata must contain beam-transfer-id")
        return value

    @model_validator(mode="after")
    def validate_source_local_part_range(self) -> MultipartGroupManifest:
        expected_max_part_number = ((self.expected_part_count - 1) * 3) + 3
        if self.max_part_number != expected_max_part_number:
            raise ValueError("max_part_number must equal the highest reserved recovery slot")
        if (
            "beam-multipart-group-id" in self.final_object_metadata
            and self.final_object_metadata["beam-multipart-group-id"] != self.multipart_group_id
        ):
            raise ValueError("final_object_metadata multipart group identity does not match")
        expected_list_pages = (self.max_part_number + 999) // 1_000
        if (
            len(self.list_page_urls) != expected_list_pages
            or len(set(self.list_page_urls)) != expected_list_pages
            or any(not url for url in self.list_page_urls)
        ):
            raise ValueError(
                f"list_page_urls must contain exactly {expected_list_pages} unique URL(s)"
            )
        return self


class CallbackConfig(BaseModel):
    """Webhook callback configuration for transfer notifications."""

    url: str = Field(..., description="URL to call when the transfer finishes")
    headers: dict[str, str] | None = None


class TransferCreateRequest(BaseModel):
    """Request to create a transfer."""

    sources: list[SourceConfig] = Field(..., min_length=1)
    destinations: list[DestConfig] = Field(..., min_length=1)
    total_size: int = Field(..., gt=0)
    chunk_size: int | None = Field(default=None, gt=0)
    name: str | None = None
    merkle_root: str | None = None
    chunk_hashes: list[str] | None = None
    callbacks: list[CallbackConfig] | None = None
    signed_url_flow: SignedUrlFlow = "signed_url"


class TransferCreateResponse(BaseModel):
    """Response from creating a transfer."""

    success: bool
    transfer_id: str = ""
    total_chunks: int = 0
    total_sources: int = 0
    total_destinations: int = 0
    source_urls: list[dict[str, str]] = Field(default_factory=list)
    dest_urls: list[dict[str, str]] = Field(default_factory=list)
    upload_ids: list[str | None] = Field(default_factory=list)
    error: str | None = None
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.success


class TransferPrepareResponse(BaseModel):
    """Response from preparing a provider-aware signed-URL transfer."""

    success: bool
    transfer_id: str = ""
    transfer_key: str | None = None
    test_mode: bool = False
    chunk_size: int = 0
    total_size: int = 0
    total_sources: int = 0
    total_destinations: int = 0
    logical_chunks: int = 0
    total_chunks: int = 0
    plan_descriptor: CompactTransferPlanDescriptor
    signed_url_flow: SignedUrlFlow
    plan_fingerprint: str
    coordinate_checksum: str
    route_generation_id: str
    error: str | None = None
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.success


class TransferPlanResponse(BaseModel):
    """Response from non-mutating signed-URL transfer planning."""

    success: bool
    chunk_size: int = 0
    total_size: int = 0
    total_sources: int = 0
    total_destinations: int = 0
    logical_chunks: int = 0
    total_chunks: int = 0
    plan_descriptor: CompactTransferPlanDescriptor
    signed_url_flow: SignedUrlFlow
    plan_fingerprint: str
    coordinate_checksum: str
    error: str | None = None
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.success


class TransferCancelResponse(BaseModel):
    """Response from cancelling a transfer."""

    success: bool
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.success


class AttachSignedUrlsResponse(BaseModel):
    """Response from attaching signed destination URLs."""

    success: bool
    transfer_id: str = ""
    total_routes: int = 0
    urls_expires_at: str | None = None
    error: str | None = None
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.success


class DestinationStatusInfo(BaseModel):
    """Status of one destination in a transfer."""

    dest_index: int
    dest_type: str
    status: str
    chunks_delivered: int = 0
    chunks_pending: int = 0
    chunks_in_progress: int = 0
    chunks_completed: int = 0
    total_destinations: int = 0
    destinations_completed: int = 0
    chunks_failed: int = 0
    bytes_delivered: int = 0
    location: str | None = None


class SourceStatusInfo(BaseModel):
    """Status of one source in a transfer."""

    source_index: int
    source_type: str
    bucket: str | None = None
    key: str | None = None
    region: str | None = None


class TransferStatusInfo(BaseModel):
    """Status healthcheck for a transfer."""

    transfer_id: str
    status: str
    error_message: str | None = None
    runtime: bool = False
    source_bytes_total: int
    delivery_bytes_total: int
    delivery_bytes_completed: int
    delivery_tasks_total: int
    delivery_tasks_completed: int
    destinations_total: int
    destinations_completed: int = 0
    destination_progress: list[dict[str, int | str | bool]]
    destination_groups: dict[str, int | list[str]]
    started_at: str | None = None
    completed_at: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.status == "completed"


class TransferTerminalEvent(BaseModel):
    """Owned terminal notification emitted by BeamCore for one transfer."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["transfer-client-control/v6"]
    producer: Literal["transfer-runtime"]
    transfer_id: str
    status: Literal["completed", "failed", "cancelled"]
    occurred_at: str


class DistributeResponse(BaseModel):
    """Response from distributing a transfer."""

    success: bool
    transfer_id: str
    orchestrators_assigned: int = 0
    message: str = ""
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.success
