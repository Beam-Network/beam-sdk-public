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

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

    provider: Literal["gcs"] = "gcs"
    source_id: str | None = None
    bucket: str
    key: str
    project_id: str | None = None
    credentials_path: str | None = None
    service_account_json: dict[str, Any] | None = None


class AzureProviderSource(BaseModel):
    """SDK-only Azure Blob source configuration placeholder."""

    storage_location: str | None = Field(default=None, max_length=64)

    provider: Literal["azure"] = "azure"
    source_id: str | None = None
    container: str
    blob: str
    account_name: str
    account_key: SecretStr | None = None
    sas_token: SecretStr | None = None


class HippiusProviderSource(BaseModel):
    """SDK-only Hippius object-store source configuration."""

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

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

    storage_location: str | None = Field(default=None, max_length=64)

    provider: Literal["gcs"] = "gcs"
    destination_id: str | None = None
    bucket: str
    key: str
    project_id: str | None = None
    credentials_path: str | None = None
    service_account_json: dict[str, Any] | None = None


class AzureProviderDestination(BaseModel):
    """SDK-only Azure Blob destination configuration placeholder."""

    storage_location: str | None = Field(default=None, max_length=64)

    provider: Literal["azure"] = "azure"
    destination_id: str | None = None
    container: str
    blob: str
    account_name: str
    account_key: SecretStr | None = None
    sas_token: SecretStr | None = None


class HippiusProviderDestination(BaseModel):
    """SDK-only Hippius object-store destination configuration."""

    storage_location: str | None = Field(default=None, max_length=64)

    provider: Literal["hippius"] = "hippius"
    destination_id: str | None = None
    bucket: str
    key: str
    api_token: SecretStr
    base_url: str = "https://api.hippius.com"


class HuggingFaceProviderDestination(BaseModel):
    """SDK-only Hugging Face Hub destination configuration."""

    storage_location: str | None = Field(default=None, max_length=64)

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


class _S3CompatibleProviderConfig(BaseModel):
    """Any storage that speaks the S3 API (MinIO, Wasabi, Backblaze B2, R2, ...).

    ``provider`` is a free-form, lower-cased name reported to BeamCore. ``endpoint_url``
    is required except for ``s3`` (AWS regional default) and ``r2`` (derived from
    ``account_id``). ``region`` defaults to ``auto`` for R2 and ``us-east-1``
    otherwise; path-style addressing defaults on for custom endpoints.
    """

    storage_location: str | None = Field(default=None, max_length=64)

    provider: str
    driver: Literal["s3-compatible"] = "s3-compatible"
    bucket: str
    key: str
    region: str | None = None
    endpoint_url: str | None = None
    access_key_id: str
    secret_access_key: SecretStr
    session_token: SecretStr | None = None
    force_path_style: bool | None = None
    account_id: str | None = None

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip().lower()
            if not value:
                raise ValueError("s3-compatible config requires provider")
        return value

    @model_validator(mode="after")
    def validate_endpoint(self) -> _S3CompatibleProviderConfig:
        for field_name in ("bucket", "key", "access_key_id"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{self.provider} config requires {field_name}.")
        if not self.secret_access_key.get_secret_value().strip():
            raise ValueError(f"{self.provider} config requires secret_access_key.")
        has_endpoint = bool(self.endpoint_url and self.endpoint_url.strip())
        if (
            self.provider == "r2"
            and not has_endpoint
            and not (self.account_id and self.account_id.strip())
        ):
            raise ValueError("r2 config requires account_id or endpoint_url.")
        if self.provider not in {"s3", "r2"} and not has_endpoint:
            raise ValueError(f"{self.provider} config requires endpoint_url.")
        return self


class S3CompatibleProviderSource(_S3CompatibleProviderConfig):
    """SDK-only source on any S3-compatible object store."""

    source_id: str | None = None


class S3CompatibleProviderDestination(_S3CompatibleProviderConfig):
    """SDK-only destination on any S3-compatible object store."""

    destination_id: str | None = None


ProviderSourceConfig = (
    S3ProviderSource
    | R2ProviderSource
    | GCSProviderSource
    | AzureProviderSource
    | HippiusProviderSource
    | HuggingFaceProviderSource
    | S3CompatibleProviderSource
)
ProviderDestinationConfig = (
    S3ProviderDestination
    | R2ProviderDestination
    | GCSProviderDestination
    | AzureProviderDestination
    | HippiusProviderDestination
    | HuggingFaceProviderDestination
    | S3CompatibleProviderDestination
)


class PlanningHttpSource(BaseModel):
    """Source descriptor for non-mutating planning; carries no signed URL."""

    source_id: str
    type: Literal["http"] = "http"
    provider: str | None = None
    url: str | None = None
    size: int = Field(..., gt=0)
    filename: str | None = None
    headers: dict[str, str] | None = None
    expires_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


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
    part_number: Literal["source_chunk_index + 1"]
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
    multipart_attempt_slots: Literal[1]
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
    expected_part_count: int = Field(..., gt=0, le=10_000)
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
        if set(value) != {"beam-transfer-id"} or not value["beam-transfer-id"]:
            raise ValueError("final_object_metadata must contain exactly beam-transfer-id")
        return value

    @model_validator(mode="after")
    def validate_source_local_part_range(self) -> MultipartGroupManifest:
        expected_max_part_number = self.expected_part_count
        if self.max_part_number != expected_max_part_number:
            raise ValueError("max_part_number must equal the consecutive part count")
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


class ProviderMultipartGroupIdentity(BaseModel):
    """Durable identity of one multipart upload created for a provider transfer.

    Passed to ``on_multipart_group_ready`` and accepted by
    ``resume_provider_transfer``. It never carries credentials, headers, or signed URLs.
    """

    model_config = ConfigDict(frozen=True)

    transfer_id: str
    multipart_group_id: str
    source_id: str
    destination_id: str
    object_key: str
    upload_id: str
    expected_object_size: int
    expected_part_count: int
    expires_at: str


class MultipartPart(BaseModel):
    """One uploaded part as reported by the provider's ListParts."""

    part_number: int = Field(..., ge=1)
    etag: str
    #: Reported by ListParts; not needed to complete an upload.
    size: int | None = Field(default=None, ge=0)


class CompletedMultipartUpload(BaseModel):
    """Result of completing a multipart upload."""

    etag: str | None = None
    version_id: str | None = None


class DestinationObjectInfo(BaseModel):
    """Metadata of a destination object from a HEAD request; no payload is read."""

    size: int | None = None
    etag: str | None = None
    version_id: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class CallbackConfig(BaseModel):
    """Webhook callback configuration for transfer notifications."""

    url: str = Field(..., description="URL to call when the transfer finishes")
    headers: dict[str, str] | None = None


class TransferCreateRequest(BaseModel):
    """Request to create a transfer."""

    sources: list[SourceConfig] = Field(..., min_length=1)
    destinations: list[DestConfig] = Field(..., min_length=1)
    total_size: int = Field(..., gt=0)
    name: str | None = None
    merkle_root: str | None = None
    chunk_hashes: list[str] | None = None
    callbacks: list[CallbackConfig] | None = None
    signed_url_flow: SignedUrlFlow = "signed_url"


class TransferCreateResponse(BaseModel):
    """Response from creating a transfer."""

    success: bool
    transfer_id: str = ""
    transfer_key: str | None = None
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
    chunk_size: int = 0
    total_size: int = 0
    total_sources: int = 0
    total_destinations: int = 0
    logical_chunks: int = 0
    total_chunks: int = 0
    #: Present on success; a failed prepare may omit the plan fields.
    plan_descriptor: CompactTransferPlanDescriptor | None = None
    signed_url_flow: SignedUrlFlow = "signed_url"
    plan_fingerprint: str = ""
    coordinate_checksum: str = ""
    route_generation_id: str = ""
    error: str | None = None
    message: str = ""

    @model_validator(mode="after")
    def validate_successful_plan(self) -> TransferPrepareResponse:
        if self.success:
            _require_plan_fields(
                self,
                ("plan_descriptor", "signed_url_flow", "plan_fingerprint", "coordinate_checksum"),
            )
            if not self.route_generation_id:
                raise ValueError("a successful prepare requires route_generation_id")
        return self

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
    #: Present on success; a failed plan may omit the plan fields.
    plan_descriptor: CompactTransferPlanDescriptor | None = None
    signed_url_flow: SignedUrlFlow = "signed_url"
    plan_fingerprint: str = ""
    coordinate_checksum: str = ""
    error: str | None = None
    message: str = ""

    @model_validator(mode="after")
    def validate_successful_plan(self) -> TransferPlanResponse:
        if self.success:
            _require_plan_fields(
                self,
                ("plan_descriptor", "signed_url_flow", "plan_fingerprint", "coordinate_checksum"),
            )
        return self

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


class PerformanceMeasurement(BaseModel):
    count: int
    work_ms: float
    max_ms: float


class PerformanceEvent(BaseModel):
    count: int
    first_ms: float
    last_ms: float


class PerformanceCounters(BaseModel):
    source_signatures: int
    source_reuses: int
    route_batches: int
    source_renewals: int | None = None


class AssignmentDeadlinePerformance(BaseModel):
    version: Literal["assignment-deadline/v1"]
    decisions: int
    selected_min_ms: float
    selected_max_ms: float
    estimated_max_ms: float
    sample_count_max: int
    data_age_max_ms: float | None
    reasons: dict[str, int]


class SdkPerformanceMeasurement(PerformanceMeasurement):
    name: str
    histogram: list[int] | None = None


class SdkPerformanceSummary(BaseModel):
    schema_version: Literal["sdk-performance/v1", "sdk-performance/v2"]
    measurements: list[SdkPerformanceMeasurement]
    counters: PerformanceCounters
    gauges: dict[str, float] | None = None
    milestones: dict[str, float] | None = None
    unmeasured: list[str] | None = None
    detail_dropped: int | None = None


class RecoveryBandwidthPerformance(BaseModel):
    excluded_tasks: int
    accepted_bytes: int


class TransferPerformance(BaseModel):
    """Local elapsed time and overlapping work aggregates; do not sum work_ms."""

    recovery_bandwidth: RecoveryBandwidthPerformance | None = None
    sdk_detail: SdkPerformanceSummary | None = None
    deadline: AssignmentDeadlinePerformance | None = None

    schema_version: Literal["transfer-performance/v1"]
    runtime_epoch: str
    coverage: Literal["complete", "restarted"]
    elapsed_ms: float
    measurements: dict[str, PerformanceMeasurement]
    events: dict[str, PerformanceEvent]
    sdk_counters: PerformanceCounters
    detail_dropped: int
    unmeasured: list[str] | None = None
    measurement_clocks: dict[str, Literal["runtime", "sdk", "fraud-service"]] | None = None
    sdk_report_received: bool | None = None


class IntegrityAuditChallengeChunk(BaseModel):
    """One range BeamCore asks the SDK to sign read grants for."""

    model_config = ConfigDict(extra="allow")

    challenge_id: str | None = None
    task_id: str | None = None
    attempt_id: str | None = None
    orchestrator_id: str | None = None
    orchestrator_hotkey: str | None = None
    worker_id: str | None = None
    source_id: str
    destination_id: str
    route_chunk_index: int
    delivery_index: int
    source_offset: int
    destination_offset: int
    range_length: int
    final_object_key: str
    final_object_etag: str | None = None


class IntegrityAuditChallenge(BaseModel):
    """Pre-completion integrity challenge carried by a transfer status."""

    model_config = ConfigDict(extra="allow")

    audit_id: str
    transfer_id: str
    requested_at: str | None = None
    range_bytes: int | None = None
    chunks: list[IntegrityAuditChallengeChunk] = Field(default_factory=list)

    # Mapping-style access keeps code written against the former dict field working.
    def __getitem__(self, key: str) -> Any:
        try:
            value = getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc
        if key == "chunks":
            return [chunk.model_dump(exclude_unset=True) for chunk in self.chunks]
        return value

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


class TransferStatusInfo(BaseModel):
    """Status healthcheck for a transfer."""

    transfer_id: str
    name: str | None = None
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
    destination_groups: dict[str, int | list[str]] | None = None
    started_at: str | None = None
    completed_at: str | None = None
    phase: str | None = None
    integrity_audit_challenge: IntegrityAuditChallenge | None = None
    #: Sanitized reason the SDK could not submit grants for the challenge; the next
    #: status poll retries. Never contains signed URLs or credentials.
    integrity_audit_submission_error: str | None = None
    integrity_check_warning: str | None = None
    performance: TransferPerformance | None = None

    @property
    def is_complete(self) -> bool:
        return self.status == "completed"


class TransferTerminalEvent(BaseModel):
    """Owned terminal notification emitted by BeamCore for one transfer."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["transfer-client-control/v7"]
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


def _require_plan_fields(model: BaseModel, fields: tuple[str, ...]) -> None:
    missing = [name for name in fields if name not in model.model_fields_set]
    if missing:
        raise ValueError(f"a successful plan response requires {', '.join(missing)}")
