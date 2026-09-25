package beamnetworksdk

import (
	"bytes"
	"context"
	"encoding/json"
)

type SignedURLFlow string

const (
	SignedURLFlowCanonical SignedURLFlow = "signed_url"
)

type SourceConfig map[string]any

type DestConfig map[string]any

type CallbackConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

type TransferCreateRequest struct {
	TransferID      string           `json:"transfer_id,omitempty"`
	Sources         []SourceConfig   `json:"sources"`
	Destinations    []DestConfig     `json:"destinations"`
	TotalSize       int64            `json:"total_size"`
	ChunkSize       int64            `json:"chunk_size,omitempty"`
	Name            string           `json:"name,omitempty"`
	MerkleRoot      string           `json:"merkle_root,omitempty"`
	ChunkHashes     []string         `json:"chunk_hashes,omitempty"`
	Callbacks       []CallbackConfig `json:"callbacks,omitempty"`
	TestMode        bool             `json:"test_mode,omitempty"`
	ProgressiveMode bool             `json:"progressive_mode,omitempty"`
	SignedURLFlow   SignedURLFlow    `json:"signed_url_flow,omitempty"`
	IdempotencyKey  string           `json:"-"`
}

type TransferCreateResponse struct {
	Success           bool                `json:"success"`
	TransferID        string              `json:"transfer_id"`
	TotalChunks       int                 `json:"total_chunks"`
	TotalSources      int                 `json:"total_sources"`
	TotalDestinations int                 `json:"total_destinations"`
	SourceURLs        []map[string]string `json:"source_urls,omitempty"`
	DestURLs          []map[string]string `json:"dest_urls,omitempty"`
	UploadIDs         []*string           `json:"upload_ids,omitempty"`
	Error             string              `json:"error,omitempty"`
	Message           string              `json:"message,omitempty"`
}

type DistributeResponse struct {
	Success               bool   `json:"success"`
	TransferID            string `json:"transfer_id"`
	OrchestratorsAssigned int    `json:"orchestrators_assigned,omitempty"`
	Message               string `json:"message,omitempty"`
	Error                 string `json:"error,omitempty"`
}

type TransferCancelResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type SourceStatusInfo struct {
	SourceIndex int    `json:"source_index"`
	SourceType  string `json:"source_type"`
	Bucket      string `json:"bucket,omitempty"`
	Key         string `json:"key,omitempty"`
	Region      string `json:"region,omitempty"`
}

type DestinationStatusInfo struct {
	DestIndex        int    `json:"dest_index"`
	DestType         string `json:"dest_type"`
	Status           string `json:"status"`
	ChunksDelivered  int    `json:"chunks_delivered,omitempty"`
	ChunksPending    int    `json:"chunks_pending,omitempty"`
	ChunksInProgress int    `json:"chunks_in_progress,omitempty"`
	ChunksCompleted  int    `json:"chunks_completed,omitempty"`
	ChunksFailed     int    `json:"chunks_failed,omitempty"`
	BytesDelivered   int64  `json:"bytes_delivered,omitempty"`
	Location         string `json:"location,omitempty"`
}

type DestinationGroupProgress struct {
	TotalGroups             int      `json:"totalGroups"`
	CompletedGroups         int      `json:"completedGroups"`
	PendingGroups           int      `json:"pendingGroups"`
	TotalDestinations       int      `json:"totalDestinations"`
	CompletedDestinations   int      `json:"completedDestinations"`
	CompletedDestinationIDs []string `json:"completedDestinationIds"`
}

type DestinationTransferProgress struct {
	DestinationID          string `json:"destination_id"`
	DeliveryBytesTotal     int64  `json:"delivery_bytes_total"`
	DeliveryBytesCompleted int64  `json:"delivery_bytes_completed"`
	CompletionVerified     bool   `json:"completion_verified"`
}

type TransferStatusInfo struct {
	TransferID             string                        `json:"transfer_id"`
	Status                 string                        `json:"status"`
	ErrorMessage           *string                       `json:"error_message"`
	Runtime                bool                          `json:"runtime,omitempty"`
	SourceBytesTotal       int64                         `json:"source_bytes_total"`
	DeliveryBytesTotal     int64                         `json:"delivery_bytes_total"`
	DeliveryBytesCompleted int64                         `json:"delivery_bytes_completed"`
	DeliveryTasksTotal     int                           `json:"delivery_tasks_total"`
	DeliveryTasksCompleted int                           `json:"delivery_tasks_completed"`
	DestinationsTotal      int                           `json:"destinations_total"`
	DestinationsCompleted  int                           `json:"destinations_completed,omitempty"`
	DestinationProgress    []DestinationTransferProgress `json:"destination_progress"`
	DestinationGroups      DestinationGroupProgress      `json:"destination_groups"`
	StartedAt              *string                       `json:"started_at"`
	CompletedAt            *string                       `json:"completed_at"`
}

// TransferTerminalEvent is the owned, transfer-scoped completion signal emitted
// by BeamCore. Clients still reconcile TransferStatus after receiving it.
type TransferTerminalEvent struct {
	SchemaVersion string `json:"schema_version" msgpack:"schema_version"`
	Producer      string `json:"producer" msgpack:"producer"`
	TransferID    string `json:"transfer_id" msgpack:"transfer_id"`
	Status        string `json:"status" msgpack:"status"`
	OccurredAt    string `json:"occurred_at" msgpack:"occurred_at"`
}

type PreparedHTTPSource struct {
	SourceID  string            `json:"source_id"`
	Type      string            `json:"type"`
	Provider  string            `json:"provider,omitempty"`
	URL       string            `json:"url"`
	Size      int64             `json:"size"`
	Filename  string            `json:"filename,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt string            `json:"expires_at,omitempty"`
	Metadata  map[string]any    `json:"metadata,omitempty"`
}

type PreparedDestination struct {
	DestinationID string         `json:"destination_id"`
	Provider      string         `json:"provider"`
	Mode          string         `json:"mode,omitempty"`
	LogicalPrefix string         `json:"logical_prefix,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type ChunkDestinationSigningTarget struct {
	DestinationID string         `json:"destination_id"`
	Provider      string         `json:"provider,omitempty"`
	ObjectKey     string         `json:"object_key,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type ChunkSigningPlanItem struct {
	ChunkIndex       int                             `json:"chunk_index"`
	SourceID         string                          `json:"source_id"`
	SourceChunkIndex int                             `json:"source_chunk_index"`
	SourceOffset     int64                           `json:"source_offset"`
	ChunkSize        int64                           `json:"chunk_size"`
	SourceURL        string                          `json:"source_url"`
	Destinations     []ChunkDestinationSigningTarget `json:"destinations"`
}

type CompactTransferPlanSource struct {
	PreparedHTTPSource
	GlobalChunkStart int `json:"global_chunk_start"`
	ChunkCount       int `json:"chunk_count"`
}

type CompactTransferPlanDestination struct {
	PreparedDestination
	DestinationIndex int               `json:"destination_index"`
	FinalObjectKeys  map[string]string `json:"final_object_keys"`
}

type CompactTransferPlanDescriptor struct {
	Version            string                           `json:"version"`
	PlanNonce          string                           `json:"plan_nonce"`
	ChunkSize          int64                            `json:"chunk_size"`
	Sources            []CompactTransferPlanSource      `json:"sources"`
	Destinations       []CompactTransferPlanDestination `json:"destinations"`
	LogicalChunkCount  int                              `json:"logical_chunk_count"`
	DeliveryRouteCount int                              `json:"delivery_route_count"`
	MultipartAttemptSlots int                           `json:"multipart_attempt_slots"`
	Formulas           CompactTransferPlanFormulas      `json:"formulas"`
}

func (descriptor *CompactTransferPlanDescriptor) UnmarshalJSON(data []byte) error {
	type descriptorAlias CompactTransferPlanDescriptor
	var decoded descriptorAlias
	if err := unmarshalStrictJSON(data, &decoded); err != nil {
		return err
	}
	*descriptor = CompactTransferPlanDescriptor(decoded)
	return nil
}

type CompactTransferPlanFormulas struct {
	SourceOffset      string `json:"source_offset"`
	DeliveryIndex     string `json:"delivery_index"`
	PartNumber        string `json:"part_number"`
	RouteGenerationID string `json:"route_generation_id"`
}

func (formulas *CompactTransferPlanFormulas) UnmarshalJSON(data []byte) error {
	type formulasAlias CompactTransferPlanFormulas
	var decoded formulasAlias
	if err := unmarshalStrictJSON(data, &decoded); err != nil {
		return err
	}
	*formulas = CompactTransferPlanFormulas(decoded)
	return nil
}

type SignedChunkRoute struct {
	SourceID      string            `json:"source_id"`
	DestinationID string            `json:"destination_id"`
	ChunkIndex    int               `json:"chunk_index"`
	DeliveryIndex int               `json:"delivery_index,omitempty"`
	SourceURL     string            `json:"source_url"`
	DestURL       string            `json:"dest_url"`
	SourceOffset  int64             `json:"source_offset"`
	ChunkSize     int64             `json:"chunk_size"`
	ExpiresAt     string            `json:"expires_at,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	DestHeaders   map[string]string `json:"dest_headers,omitempty"`
	Metadata      map[string]any    `json:"metadata,omitempty"`
}

type MultipartGroupManifest struct {
	MultipartGroupID    string            `json:"multipart_group_id"`
	SourceID            string            `json:"source_id"`
	DestinationID       string            `json:"destination_id"`
	FinalObjectKey      string            `json:"final_object_key"`
	UploadID            string            `json:"upload_id"`
	ExpectedObjectSize  int64             `json:"expected_object_size"`
	ExpectedPartCount   int               `json:"expected_part_count"`
	MaxPartNumber       int               `json:"max_part_number"`
	CompleteURL         string            `json:"complete_url"`
	AbortURL            string            `json:"abort_url"`
	ListPageURLs        []string          `json:"list_page_urls"`
	FinalHeadURL        string            `json:"final_head_url"`
	FinalObjectMetadata map[string]string `json:"final_object_metadata"`
	URLsExpiresAt       string            `json:"urls_expires_at"`
}

func (manifest *MultipartGroupManifest) UnmarshalJSON(data []byte) error {
	type manifestAlias MultipartGroupManifest
	var decoded manifestAlias
	if err := unmarshalStrictJSON(data, &decoded); err != nil {
		return err
	}
	*manifest = MultipartGroupManifest(decoded)
	return nil
}

func unmarshalStrictJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}

type TransferPrepareResponse struct {
	Success            bool                          `json:"success"`
	TransferID         string                        `json:"transfer_id"`
	TransferKey        string                        `json:"transfer_key,omitempty"`
	TestMode           bool                          `json:"test_mode,omitempty"`
	ChunkSize          int64                         `json:"chunk_size,omitempty"`
	TotalSize          int64                         `json:"total_size,omitempty"`
	TotalSources       int                           `json:"total_sources,omitempty"`
	TotalDestinations  int                           `json:"total_destinations,omitempty"`
	LogicalChunks      int                           `json:"logical_chunks,omitempty"`
	TotalChunks        int                           `json:"total_chunks,omitempty"`
	PlanDescriptor     CompactTransferPlanDescriptor `json:"plan_descriptor"`
	SignedURLFlow      SignedURLFlow                 `json:"signed_url_flow"`
	PlanFingerprint    string                        `json:"plan_fingerprint"`
	CoordinateChecksum string                        `json:"coordinate_checksum"`
	RouteGenerationID  string                        `json:"route_generation_id"`
	Error              string                        `json:"error,omitempty"`
	Message            string                        `json:"message,omitempty"`
}

// ManualRouteRecovery regenerates short-lived routes in memory after a Runtime
// epoch change. Implementations must not persist credentials or signed routes.
type ManualRouteRecovery struct {
	PlanFingerprint    string
	CoordinateChecksum string
	Regenerate         func(ctx context.Context, routeGenerationID string) ([]SignedChunkRoute, []MultipartGroupManifest, string, error)
}

type AttachSignedURLsResponse struct {
	Success       bool   `json:"success"`
	TransferID    string `json:"transfer_id"`
	TotalRoutes   int    `json:"total_routes,omitempty"`
	URLsExpiresAt string `json:"urls_expires_at,omitempty"`
	Error         string `json:"error,omitempty"`
	Message       string `json:"message,omitempty"`
}

type S3ProviderSource struct {
	Provider        string `json:"provider"`
	SourceID        string `json:"source_id,omitempty"`
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	EndpointURL     string `json:"endpoint_url,omitempty"`
}

type R2ProviderSource struct {
	Provider        string `json:"provider"`
	SourceID        string `json:"source_id,omitempty"`
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	AccountID       string `json:"account_id,omitempty"`
	EndpointURL     string `json:"endpoint_url,omitempty"`
}

type HippiusProviderSource struct {
	Provider string `json:"provider"`
	SourceID string `json:"source_id,omitempty"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	APIToken string `json:"api_token"`
	BaseURL  string `json:"base_url,omitempty"`
}

// HuggingFaceProviderSource is an SDK-only Hugging Face Hub source configuration.
//
// The token stays local: the SDK resolves the file to the Hub's presigned CDN URL and sends
// only that URL to BeamCore.
type HuggingFaceProviderSource struct {
	Provider string `json:"provider"`
	SourceID string `json:"source_id,omitempty"`
	RepoID   string `json:"repo_id"`
	Path     string `json:"path"`
	RepoType string `json:"repo_type,omitempty"`
	Revision string `json:"revision,omitempty"`
	Token    string `json:"token"`
	Endpoint string `json:"endpoint,omitempty"`
}

type GCSProviderSource struct {
	Provider  string `json:"provider"`
	SourceID  string `json:"source_id,omitempty"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	ProjectID string `json:"project_id,omitempty"`
}

type AzureProviderSource struct {
	Provider    string `json:"provider"`
	SourceID    string `json:"source_id,omitempty"`
	Container   string `json:"container"`
	Blob        string `json:"blob"`
	AccountName string `json:"account_name"`
	AccountKey  string `json:"account_key,omitempty"`
	SASToken    string `json:"sas_token,omitempty"`
}

type S3ProviderDestination struct {
	Provider        string `json:"provider"`
	DestinationID   string `json:"destination_id,omitempty"`
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	EndpointURL     string `json:"endpoint_url,omitempty"`
}

type R2ProviderDestination struct {
	Provider        string `json:"provider"`
	DestinationID   string `json:"destination_id,omitempty"`
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	AccountID       string `json:"account_id,omitempty"`
	EndpointURL     string `json:"endpoint_url,omitempty"`
}

type HippiusProviderDestination struct {
	Provider      string `json:"provider"`
	DestinationID string `json:"destination_id,omitempty"`
	Bucket        string `json:"bucket"`
	Key           string `json:"key"`
	APIToken      string `json:"api_token"`
	BaseURL       string `json:"base_url,omitempty"`
}

// HuggingFaceProviderDestination is an SDK-only Hugging Face Hub destination configuration.
type HuggingFaceProviderDestination struct {
	Provider          string `json:"provider"`
	DestinationID     string `json:"destination_id,omitempty"`
	RepoID            string `json:"repo_id"`
	Path              string `json:"path"`
	RepoType          string `json:"repo_type,omitempty"`
	Revision          string `json:"revision,omitempty"`
	Token             string `json:"token"`
	Endpoint          string `json:"endpoint,omitempty"`
	CommitMessage     string `json:"commit_message,omitempty"`
	CommitDescription string `json:"commit_description,omitempty"`
	CreatePR          bool   `json:"create_pr,omitempty"`
	// AllowSourceRehash opts in to reading every source byte once. The Hub issues LFS upload
	// URLs only for a known sha256, so an upload from a non-Hub source needs it.
	AllowSourceRehash bool `json:"allow_source_rehash,omitempty"`
}

type GCSProviderDestination struct {
	Provider      string `json:"provider"`
	DestinationID string `json:"destination_id,omitempty"`
	Bucket        string `json:"bucket"`
	Key           string `json:"key"`
	ProjectID     string `json:"project_id,omitempty"`
}

type AzureProviderDestination struct {
	Provider      string `json:"provider"`
	DestinationID string `json:"destination_id,omitempty"`
	Container     string `json:"container"`
	Blob          string `json:"blob"`
	AccountName   string `json:"account_name"`
	AccountKey    string `json:"account_key,omitempty"`
	SASToken      string `json:"sas_token,omitempty"`
}
