package beamnetworksdk

// Hybrid signing helpers let credential adapters sign individual provider
// operations without running a whole provider transfer. They reuse the same
// provider configs, cached S3 clients, and addressing rules as the transfer
// flow. Only short-lived range and upload routes should reach workers;
// multipart creation, verification, completion, and abort stay in the
// caller's control path. Every helper honours ctx cancellation, which stops
// waiting and network activity but cannot undo a provider operation that was
// already accepted.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ProviderSourceOptions configures PrepareProviderSource and PrepareProviderSourceForPlan.
type ProviderSourceOptions struct {
	// Index derives the default source id src_<Index>.
	Index int
	// ExpiresIn is the lifetime of the signed read URL. Defaults to one hour.
	ExpiresIn time.Duration
	// HTTPClient is used for Hippius and Hugging Face; nil uses http.DefaultClient.
	HTTPClient *http.Client
}

// PrepareProviderSource resolves a provider source to a signed HTTP source.
func PrepareProviderSource(ctx context.Context, source ProviderSource, options ProviderSourceOptions) (PreparedHTTPSource, error) {
	return prepareProviderSource(ctx, httpClientOrDefault(options.HTTPClient), source, options.Index, options.ExpiresIn)
}

// PrepareProviderSourceForPlan describes a provider source for PlanTransfer
// using metadata requests only; it signs no URL.
func PrepareProviderSourceForPlan(ctx context.Context, source ProviderSource, options ProviderSourceOptions) (PlanningHTTPSource, error) {
	httpClient := httpClientOrDefault(options.HTTPClient)
	settings, s3Compatible, err := s3SettingsFor(source)
	if err != nil {
		return PlanningHTTPSource{}, err
	}
	if s3Compatible {
		head, err := s3ClientForContext(ctx, settings).HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(settings.key)})
		if err != nil {
			return PlanningHTTPSource{}, err
		}
		return PlanningHTTPSource{
			SourceID: defaultID(settings.id, "src", options.Index),
			Type:     "http",
			Provider: settings.provider,
			Size:     aws.ToInt64(head.ContentLength),
			Filename: path.Base(settings.key),
			Metadata: s3SourceMetadata(settings, head),
		}, nil
	}
	switch source := source.(type) {
	case HippiusProviderSource:
		baseURL := defaultString(source.BaseURL, "https://api.hippius.com")
		size, err := hippiusObjectSize(ctx, httpClient, baseURL, source.APIToken, source.Bucket, source.Key)
		if err != nil {
			return PlanningHTTPSource{}, err
		}
		return PlanningHTTPSource{
			SourceID: defaultID(source.SourceID, "src", options.Index),
			Type:     "http",
			Provider: "hippius",
			Size:     size,
			Filename: path.Base(source.Key),
			Metadata: map[string]any{"bucket": source.Bucket, "key": source.Key, "base_url": baseURL},
		}, nil
	case HuggingFaceProviderSource:
		config := huggingFaceSourceConfig(source)
		metadata, err := huggingFaceFileMetadataFor(ctx, httpClient, config)
		if err != nil {
			return PlanningHTTPSource{}, err
		}
		return PlanningHTTPSource{
			SourceID: defaultID(source.SourceID, "src", options.Index),
			Type:     "http",
			Provider: "huggingface",
			Size:     metadata.Size,
			Filename: path.Base(config.Path),
			Metadata: huggingFaceMetadata(config, metadata.ETag, metadata.CommitHash),
		}, nil
	default:
		return PlanningHTTPSource{}, fmt.Errorf("provider source planning is not implemented for %T", source)
	}
}

// PrepareProviderDestination describes a provider destination for BeamCore.
func PrepareProviderDestination(destination ProviderDestination, index int) (PreparedDestination, error) {
	return prepareProviderDestination(destination, index)
}

// CreateMultipartUpload creates a multipart upload with the given object metadata.
func CreateMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, metadata map[string]string) (string, error) {
	return createMultipartUpload(ctx, destination, objectKey, metadata)
}

// AbortMultipartUpload aborts a multipart upload.
func AbortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string) error {
	return abortMultipartUpload(ctx, destination, objectKey, uploadID)
}

// SignFinalObjectHead signs a HEAD of the final object.
func SignFinalObjectHead(ctx context.Context, destination ProviderDestination, objectKey string, expiresIn time.Duration) (string, error) {
	return signFinalObjectHead(ctx, destination, objectKey, expiresIn)
}

// SignCompleteMultipartUpload signs a CompleteMultipartUpload request.
func SignCompleteMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	return signCompleteMultipartUpload(ctx, destination, objectKey, uploadID, expiresIn)
}

// SignAbortMultipartUpload signs an AbortMultipartUpload request.
func SignAbortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	return signAbortMultipartUpload(ctx, destination, objectKey, uploadID, expiresIn)
}

// ListPartsSignOptions pages a signed ListParts request; zero omits a field.
type ListPartsSignOptions struct {
	MaxParts         int
	PartNumberMarker int
}

// SignListMultipartUpload signs one ListParts page.
func SignListMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration, options ListPartsSignOptions) (string, error) {
	return signListMultipartUpload(ctx, destination, objectKey, uploadID, expiresIn, options.MaxParts, options.PartNumberMarker)
}

// SignDestinationRoute signs one worker route: the destination upload URL
// (UploadPart when UploadID and PartNumber are set, otherwise PutObject, or
// DestURL when the provider issued one) and the source range read.
func SignDestinationRoute(ctx context.Context, input DestinationRouteInput) (SignedChunkRoute, error) {
	return signDestinationRoute(ctx, httpClientOrDefault(input.HTTPClient), input)
}

// DestinationURLInput is the input for SignDestinationURL.
type DestinationURLInput struct {
	Destination ProviderDestination
	ObjectKey   string
	// UploadID and PartNumber select UploadPart; otherwise PutObject is signed.
	UploadID   string
	PartNumber int
	ExpiresIn  time.Duration
	// ContentMD5 (base64) binds the worker-computed body checksum into the
	// signature. Only S3-compatible storage supports it.
	ContentMD5 string
	HTTPClient *http.Client
}

// SignDestinationURL signs a destination upload URL without a source
// descriptor; it reads no source data.
func SignDestinationURL(ctx context.Context, input DestinationURLInput) (string, error) {
	return signDestinationURL(ctx, httpClientOrDefault(input.HTTPClient), input.Destination, input.ObjectKey, input.UploadID, input.PartNumber, input.ExpiresIn, input.ContentMD5)
}

// SourceReadRangeInput is the input for SignSourceReadRange.
type SourceReadRangeInput struct {
	Source ProviderSource
	// IfMatch and VersionID freeze an S3-compatible source: the ETag condition
	// is signed and the version is part of the signed request. Other providers
	// reject them instead of dropping them.
	IfMatch    string
	VersionID  string
	Offset     int64
	Length     int64
	ExpiresIn  time.Duration
	HTTPClient *http.Client
}

// SignSourceReadRange signs a read-only source range. Replay the returned
// headers unchanged.
func SignSourceReadRange(ctx context.Context, input SourceReadRangeInput) (SignedReadRange, error) {
	return signSourceReadRange(ctx, httpClientOrDefault(input.HTTPClient), input.Source, input.IfMatch, input.VersionID, input.Offset, input.Length, input.ExpiresIn)
}

// DestinationReadRangeInput is the input for SignDestinationReadRange.
type DestinationReadRangeInput struct {
	Destination ProviderDestination
	ObjectKey   string
	// IfMatch pins the final object on S3-compatible storage.
	IfMatch    string
	Offset     int64
	Length     int64
	ExpiresIn  time.Duration
	HTTPClient *http.Client
}

// SignDestinationReadRange signs a read-only range of a final destination object.
func SignDestinationReadRange(ctx context.Context, input DestinationReadRangeInput) (SignedReadRange, error) {
	return signDestinationReadRange(ctx, httpClientOrDefault(input.HTTPClient), input.Destination, input.ObjectKey, input.IfMatch, input.Offset, input.Length, input.ExpiresIn)
}

// MultipartPart is one uploaded part reported by the provider.
type MultipartPart struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

// ListMultipartParts walks every ListParts page of an upload. It returns
// provider metadata only and never reads object bytes.
func ListMultipartParts(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string) ([]MultipartPart, error) {
	settings, err := destinationS3Settings(destination, "multipart list-parts")
	if err != nil {
		return nil, err
	}
	client := s3ClientForContext(ctx, settings)
	parts := make([]MultipartPart, 0)
	var marker *string
	for {
		page, err := client.ListParts(ctx, &s3.ListPartsInput{
			Bucket: aws.String(settings.bucket), Key: aws.String(objectKey), UploadId: aws.String(uploadID),
			MaxParts: aws.Int32(1000), PartNumberMarker: marker,
		})
		if err != nil {
			return nil, err
		}
		for _, part := range page.Parts {
			if aws.ToInt32(part.PartNumber) < 1 || aws.ToString(part.ETag) == "" || part.Size == nil {
				return nil, errors.New("invalid multipart part metadata")
			}
			parts = append(parts, MultipartPart{PartNumber: int(aws.ToInt32(part.PartNumber)), ETag: aws.ToString(part.ETag), Size: aws.ToInt64(part.Size)})
		}
		if !aws.ToBool(page.IsTruncated) {
			return parts, nil
		}
		next := aws.ToString(page.NextPartNumberMarker)
		if next == "" || (marker != nil && next == *marker) {
			return nil, errors.New("invalid multipart pagination")
		}
		marker = aws.String(next)
	}
}

// CompletedMultipartUpload is the provider's completion result.
type CompletedMultipartUpload struct {
	ETag      string
	VersionID string
}

// CompleteMultipartUpload completes an upload from provider-verified parts,
// submitted in part-number order.
func CompleteMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, parts []MultipartPart) (*CompletedMultipartUpload, error) {
	settings, err := destinationS3Settings(destination, "multipart completion")
	if err != nil {
		return nil, err
	}
	sorted := append([]MultipartPart(nil), parts...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left].PartNumber < sorted[right].PartNumber })
	completed := make([]types.CompletedPart, 0, len(sorted))
	for _, part := range sorted {
		if part.PartNumber < 1 || part.PartNumber > MultipartMaxPartNumber {
			return nil, fmt.Errorf("multipart part_number %d is outside 1-%d", part.PartNumber, MultipartMaxPartNumber)
		}
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(int32(part.PartNumber)), ETag: aws.String(part.ETag)})
	}
	result, err := s3ClientForContext(ctx, settings).CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(settings.bucket), Key: aws.String(objectKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		return nil, err
	}
	return &CompletedMultipartUpload{ETag: aws.ToString(result.ETag), VersionID: aws.ToString(result.VersionId)}, nil
}

// DestinationObjectInfo is the provider's HEAD metadata for a final object.
type DestinationObjectInfo struct {
	Size      int64
	ETag      string
	VersionID string
	Metadata  map[string]string
}

// InspectDestinationObject HEADs a final destination object.
func InspectDestinationObject(ctx context.Context, destination ProviderDestination, objectKey string) (*DestinationObjectInfo, error) {
	settings, err := destinationS3Settings(destination, "object inspection")
	if err != nil {
		return nil, err
	}
	result, err := s3ClientForContext(ctx, settings).HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(objectKey)})
	if err != nil {
		return nil, err
	}
	metadata := result.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	return &DestinationObjectInfo{Size: aws.ToInt64(result.ContentLength), ETag: aws.ToString(result.ETag), VersionID: aws.ToString(result.VersionId), Metadata: metadata}, nil
}
