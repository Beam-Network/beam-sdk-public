package beamnetworksdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ProviderSource interface{}
type ProviderDestination interface{}

// PrepareProviderTransfer signs provider sources and destinations, prepares the
// transfer, and streams its signed routes.
func (client *Client) PrepareProviderTransfer(
	ctx context.Context,
	sources []ProviderSource,
	destinations []ProviderDestination,
	name string,
	testMode bool,
	expiresIn time.Duration,
	distribute bool,
	routeGenerationID string,
	idempotencyKey ...string,
) (*TransferPrepareResponse, error) {
	key := ""
	if len(idempotencyKey) > 0 {
		key = idempotencyKey[0]
	}
	return client.executeProviderTransfer(ctx, providerTransferInput{
		sources:           sources,
		destinations:      destinations,
		name:              name,
		testMode:          testMode,
		expiresIn:         expiresIn,
		distribute:        distribute,
		idempotencyKey:    key,
		routeGenerationID: routeGenerationID,
	}, nil)
}

// providerRouteInput is one planned route to sign for a provider destination.
type providerRouteInput struct {
	sourceGrant       *sourceChunkGrant
	chunk             ChunkSigningPlanItem
	target            ChunkDestinationSigningTarget
	source            ProviderSource
	destination       ProviderDestination
	expiresIn         time.Duration
	upload            *multipartUploadState
	huggingFaceUpload *huggingFaceUploadState
	finalObjectKey    string
	partNumber        int
}

// partRouteMetadataKeys are the per-attempt route metadata keys BeamCore accepts.
var partRouteMetadataKeys = map[string]struct{}{
	"part_number": {}, "logical_attempt_index": {}, "attempt_slot": {}, "route_generation_id": {}, "delivery_index": {}, "etag_required": {},
}

func partRouteMetadata(metadata map[string]any) map[string]any {
	out := make(map[string]any, len(partRouteMetadataKeys))
	for key, value := range metadata {
		if _, keep := partRouteMetadataKeys[key]; keep {
			out[key] = value
		}
	}
	return out
}

// directPutRouteMetadata is the route metadata for a destination BeamCore does
// not treat as an S3 multipart target. It rejects part_number there as an
// unexpected multipart signal.
func directPutRouteMetadata(metadata map[string]any) map[string]any {
	out := partRouteMetadata(metadata)
	delete(out, "part_number")
	return out
}

func (client *Client) signProviderRoute(ctx context.Context, input providerRouteInput) (SignedChunkRoute, error) {
	defer performancePhase(ctx, "sdk.destination_signing")()
	if _, huggingFace := input.destination.(HuggingFaceProviderDestination); huggingFace {
		state := input.huggingFaceUpload
		if state == nil {
			return SignedChunkRoute{}, fmt.Errorf("huggingface upload state is missing for %s", input.target.DestinationID)
		}
		// The Hub presigns its own part targets; chunk N carries the URL for part N + 1.
		destURL := state.UploadHref
		if state.ChunkSize > 0 {
			destURL = ""
			if input.chunk.SourceChunkIndex < len(state.PartURLs) {
				destURL = state.PartURLs[input.chunk.SourceChunkIndex]
			}
		}
		if destURL == "" {
			return SignedChunkRoute{}, fmt.Errorf(
				"the Hub issued no upload URL for chunk %d of %s",
				input.chunk.SourceChunkIndex, state.Config.describe(),
			)
		}
		target := input.target
		target.ObjectKey = input.finalObjectKey
		target.Metadata = directPutRouteMetadata(input.target.Metadata)
		return signDestinationRoute(ctx, client.httpClient, DestinationRouteInput{
			Chunk: input.chunk, Target: target, Source: input.source, Destination: input.destination,
			ExpiresIn: input.expiresIn, sourceGrant: input.sourceGrant, DestURL: destURL,
		})
	}
	if _, hippius := input.destination.(HippiusProviderDestination); hippius {
		target := input.target
		target.Metadata = partRouteMetadata(input.target.Metadata)
		return signDestinationRoute(ctx, client.httpClient, DestinationRouteInput{
			Chunk: input.chunk, Target: target, Source: input.source, Destination: input.destination,
			ExpiresIn: input.expiresIn, sourceGrant: input.sourceGrant,
		})
	}
	upload := input.upload
	if upload == nil || upload.UploadID == "" {
		return SignedChunkRoute{}, fmt.Errorf("%s route is missing its multipart upload", SignedURLFlowCanonical)
	}
	if err := validateMultipartPartNumber(input.partNumber, upload.Manifest); err != nil {
		return SignedChunkRoute{}, err
	}
	listPageIndex := (input.partNumber - 1) / 1000
	if listPageIndex >= len(upload.Manifest.ListPageURLs) {
		return SignedChunkRoute{}, fmt.Errorf("multipart list_page_url missing for part_number %d", input.partNumber)
	}
	target := input.target
	target.ObjectKey = input.finalObjectKey
	target.Metadata = partRouteMetadata(input.target.Metadata)
	return signDestinationRoute(ctx, client.httpClient, DestinationRouteInput{
		Chunk:               input.chunk,
		Target:              target,
		Source:              input.source,
		Destination:         input.destination,
		ExpiresIn:           input.expiresIn,
		sourceGrant:         input.sourceGrant,
		MultipartGroupID:    upload.Manifest.MultipartGroupID,
		UploadID:            upload.UploadID,
		PartNumber:          input.partNumber,
		FinalObjectKey:      input.finalObjectKey,
		CompleteURL:         upload.Manifest.CompleteURL,
		AbortURL:            upload.Manifest.AbortURL,
		ListPageURL:         upload.Manifest.ListPageURLs[listPageIndex],
		FinalHeadURL:        upload.Manifest.FinalHeadURL,
		ExpectedObjectSize:  upload.Manifest.ExpectedObjectSize,
		ExpectedPartCount:   upload.Manifest.ExpectedPartCount,
		MaxPartNumber:       upload.Manifest.MaxPartNumber,
		FinalObjectMetadata: upload.Manifest.FinalObjectMetadata,
	})
}

func validateMultipartPartNumber(partNumber int, manifest MultipartGroupManifest) error {
	if partNumber < 1 || partNumber > manifest.MaxPartNumber {
		return fmt.Errorf("multipart part_number %d is outside group %s range 1-%d", partNumber, manifest.MultipartGroupID, manifest.MaxPartNumber)
	}
	return nil
}

type planChunkIterator struct {
	descriptor       CompactTransferPlanDescriptor
	transferID       string
	sourceIndex      int
	sourceChunkIndex int
}

func newPlanChunkIterator(descriptor CompactTransferPlanDescriptor, transferID string) *planChunkIterator {
	return &planChunkIterator{descriptor: descriptor, transferID: transferID}
}

func (iterator *planChunkIterator) next() (ChunkSigningPlanItem, bool, error) {
	for iterator.sourceIndex < len(iterator.descriptor.Sources) {
		source := iterator.descriptor.Sources[iterator.sourceIndex]
		if iterator.sourceChunkIndex >= source.ChunkCount {
			iterator.sourceIndex++
			iterator.sourceChunkIndex = 0
			continue
		}
		chunkIndex := source.GlobalChunkStart + iterator.sourceChunkIndex
		chunk, ok := materializePlanChunk(iterator.descriptor, iterator.transferID, source.SourceID, chunkIndex)
		iterator.sourceChunkIndex++
		if !ok {
			return ChunkSigningPlanItem{}, false, fmt.Errorf("plan chunk not found: %s:%d", source.SourceID, chunkIndex)
		}
		return chunk, true, nil
	}
	return ChunkSigningPlanItem{}, false, nil
}

func materializePlanChunk(descriptor CompactTransferPlanDescriptor, transferID string, sourceID string, chunkIndex int) (ChunkSigningPlanItem, bool) {
	var source *CompactTransferPlanSource
	for index := range descriptor.Sources {
		if descriptor.Sources[index].SourceID == sourceID {
			source = &descriptor.Sources[index]
			break
		}
	}
	if source == nil {
		return ChunkSigningPlanItem{}, false
	}
	sourceChunkIndex := chunkIndex - source.GlobalChunkStart
	if sourceChunkIndex < 0 || sourceChunkIndex >= source.ChunkCount {
		return ChunkSigningPlanItem{}, false
	}
	sourceOffset := int64(sourceChunkIndex) * descriptor.ChunkSize
	chunkSize := descriptor.ChunkSize
	if remaining := source.Size - sourceOffset; remaining < chunkSize {
		chunkSize = remaining
	}
	partNumber, err := MultipartPartNumber(sourceChunkIndex, 0)
	if err != nil {
		return ChunkSigningPlanItem{}, false
	}
	destinations := make([]ChunkDestinationSigningTarget, 0, len(descriptor.Destinations))
	for _, destination := range descriptor.Destinations {
		finalObjectKey := destination.FinalObjectKeys[sourceID]
		if finalObjectKey == "" {
			return ChunkSigningPlanItem{}, false
		}
		deliveryIndex := chunkIndex*len(descriptor.Destinations) + destination.DestinationIndex
		metadata := map[string]any{}
		for key, value := range destination.Metadata {
			metadata[key] = value
		}
		metadata["final_object_key"] = finalObjectKey
		metadata["part_number"] = partNumber
		metadata["logical_attempt_index"] = 0
		metadata["attempt_slot"] = 0
		metadata["route_generation_id"] = fmt.Sprintf("initial-%d-%s", chunkIndex, destination.DestinationID)
		metadata["delivery_index"] = deliveryIndex
		objectKey := finalObjectKey
		if strings.EqualFold(destination.Provider, "hippius") {
			objectKey = fmt.Sprintf("%s/%s/chunk-%06d", finalObjectKey, descriptor.PlanNonce, sourceChunkIndex)
		}
		destinations = append(destinations, ChunkDestinationSigningTarget{
			DestinationID: destination.DestinationID,
			Provider:      destination.Provider,
			ObjectKey:     objectKey,
			Metadata:      metadata,
		})
	}
	return ChunkSigningPlanItem{
		ChunkIndex: chunkIndex, SourceID: sourceID, SourceChunkIndex: sourceChunkIndex,
		SourceOffset: sourceOffset, ChunkSize: chunkSize, SourceURL: source.URL,
		Destinations: destinations,
	}, true
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case uint8:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

// multipartUploadState is one multipart group. Cleanup-only entries (created
// or restored uploads not yet signed) carry an empty Manifest.
type multipartUploadState struct {
	Destination ProviderDestination
	ObjectKey   string
	UploadID    string
	Manifest    MultipartGroupManifest
}

func prepareProviderSource(
	ctx context.Context,
	httpClient *http.Client,
	source ProviderSource,
	index int,
	expiresIn time.Duration,
) (PreparedHTTPSource, error) {
	if expiresIn <= 0 {
		expiresIn = time.Hour
	}
	settings, s3Compatible, err := s3SettingsFor(source)
	if err != nil {
		return PreparedHTTPSource{}, err
	}
	if s3Compatible {
		client := s3ClientForContext(ctx, settings)
		metadataStarted := time.Now()
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(settings.key)})
		if metrics := performanceFromContext(ctx); metrics != nil {
			metrics.observe("sdk.metadata_request", metadataStarted)
		}
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		getURL, err := s3.NewPresignClient(client).PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(settings.key)}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		return PreparedHTTPSource{
			SourceID:  defaultID(settings.id, "src", index),
			Type:      "http",
			Provider:  settings.provider,
			URL:       getURL.URL,
			Size:      aws.ToInt64(head.ContentLength),
			Filename:  path.Base(settings.key),
			ExpiresAt: time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano),
			Metadata:  s3SourceMetadata(settings, head),
		}, nil
	}
	switch source := source.(type) {
	case HuggingFaceProviderSource:
		config := huggingFaceSourceConfig(source)
		metadata, err := huggingFaceFileMetadataFor(ctx, httpClient, config)
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		return PreparedHTTPSource{
			SourceID: defaultID(source.SourceID, "src", index),
			Type:     "http",
			Provider: "huggingface",
			URL:      metadata.URL,
			Size:     metadata.Size,
			Filename: path.Base(config.Path),
			Metadata: withStorageLocation(huggingFaceMetadata(config, metadata.ETag, metadata.CommitHash), source.StorageLocation),
		}, nil
	case HippiusProviderSource:
		baseURL := defaultString(source.BaseURL, "https://api.hippius.com")
		size, err := hippiusObjectSize(ctx, httpClient, baseURL, source.APIToken, source.Bucket, source.Key)
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		getURL, err := hippiusPresign(ctx, httpClient, baseURL, source.APIToken, source.Bucket, source.Key, "get", expiresIn)
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		return PreparedHTTPSource{
			SourceID:  defaultID(source.SourceID, "src", index),
			Type:      "http",
			Provider:  "hippius",
			URL:       getURL,
			Size:      size,
			Filename:  path.Base(source.Key),
			ExpiresAt: time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano),
			Metadata:  withStorageLocation(map[string]any{"bucket": source.Bucket, "key": source.Key, "base_url": baseURL}, source.StorageLocation),
		}, nil
	default:
		return PreparedHTTPSource{}, fmt.Errorf("provider source signing is not implemented for %T", source)
	}
}

// s3CompatibleMetadata identifies an S3-compatible object for BeamCore.
func s3CompatibleMetadata(settings s3Settings) map[string]any {
	metadata := map[string]any{
		"driver": "s3-compatible",
		"bucket": settings.bucket,
		"key":    settings.key,
		"region": settings.region,
	}
	if settings.storageLocation != "" {
		metadata["storage_location"] = settings.storageLocation
	}
	if settings.endpoint != "" {
		metadata["endpoint_url"] = settings.endpoint
	}
	if settings.accountID != "" {
		metadata["account_id"] = settings.accountID
	}
	return metadata
}

// s3SourceMetadata adds the HEAD identity (length, ETag, modification time,
// version) that later pins integrity grants with If-Match and VersionId.
func s3SourceMetadata(settings s3Settings, head *s3.HeadObjectOutput) map[string]any {
	metadata := s3CompatibleMetadata(settings)
	metadata["content_length"] = aws.ToInt64(head.ContentLength)
	if etag := aws.ToString(head.ETag); etag != "" {
		metadata["etag"] = etag
	}
	if head.LastModified != nil {
		metadata["last_modified"] = head.LastModified.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	if versionID := aws.ToString(head.VersionId); versionID != "" {
		metadata["version_id"] = versionID
	}
	return metadata
}

func prepareProviderDestination(destination ProviderDestination, index int) (PreparedDestination, error) {
	settings, s3Compatible, err := s3SettingsFor(destination)
	if err != nil {
		return PreparedDestination{}, err
	}
	if s3Compatible {
		return PreparedDestination{
			DestinationID: defaultID(settings.id, "dst", index),
			Provider:      settings.provider,
			LogicalPrefix: settings.key,
			Metadata:      s3CompatibleMetadata(settings),
		}, nil
	}
	switch destination := destination.(type) {
	case GCSProviderDestination:
		metadata := map[string]any{"bucket": destination.Bucket, "key": destination.Key, "project_id": destination.ProjectID}
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "gcs",
			LogicalPrefix: destination.Key,
			Metadata:      metadata,
		}, nil
	case AzureProviderDestination:
		metadata := map[string]any{"container": destination.Container, "blob": destination.Blob, "account_name": destination.AccountName}
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "azure",
			LogicalPrefix: fmt.Sprintf("azure://%s/%s", destination.Container, destination.Blob),
			Metadata:      metadata,
		}, nil
	case HuggingFaceProviderDestination:
		config := huggingFaceDestinationConfig(destination)
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "huggingface",
			LogicalPrefix: config.Path,
			Metadata:      withStorageLocation(huggingFaceMetadata(config, "", ""), destination.StorageLocation),
		}, nil
	case HippiusProviderDestination:
		metadata := withStorageLocation(map[string]any{"bucket": destination.Bucket, "key": destination.Key, "base_url": defaultString(destination.BaseURL, "https://api.hippius.com")}, destination.StorageLocation)
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "hippius",
			LogicalPrefix: strings.TrimRight(destination.Key, "/"),
			Metadata:      metadata,
		}, nil
	default:
		return PreparedDestination{}, fmt.Errorf("unsupported provider destination: %T", destination)
	}
}

func destinationS3Settings(destination ProviderDestination, operation string) (s3Settings, error) {
	settings, s3Compatible, err := s3SettingsFor(destination)
	if err != nil {
		return s3Settings{}, err
	}
	if !s3Compatible {
		return s3Settings{}, fmt.Errorf("%s is not supported for %s", operation, providerName(destination))
	}
	return settings, nil
}

func createMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, metadata map[string]string) (string, error) {
	settings, err := destinationS3Settings(destination, "multipart upload")
	if err != nil {
		return "", err
	}
	response, err := s3ClientForContext(ctx, settings).CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(settings.bucket), Key: aws.String(objectKey), Metadata: metadata})
	if err != nil {
		return "", err
	}
	if response.UploadId == nil || *response.UploadId == "" {
		return "", fmt.Errorf("provider did not return UploadId for %s", objectKey)
	}
	return *response.UploadId, nil
}

func signFinalObjectHead(ctx context.Context, destination ProviderDestination, objectKey string, expiresIn time.Duration) (string, error) {
	settings, err := destinationS3Settings(destination, "final object HEAD signing")
	if err != nil {
		return "", err
	}
	result, err := s3.NewPresignClient(s3ClientForContext(ctx, settings)).PresignHeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(objectKey)}, func(options *s3.PresignOptions) {
		options.Expires = expiresIn
	})
	if err != nil {
		return "", err
	}
	return result.URL, nil
}

func signCompleteMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	settings, err := destinationS3Settings(destination, "multipart completion")
	if err != nil {
		return "", err
	}
	return presignS3Request(ctx, settings, http.MethodPost, objectKey, "CompleteMultipartUpload", expiresIn, map[string]string{"uploadId": uploadID}, nil)
}

func signAbortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	settings, err := destinationS3Settings(destination, "multipart abort")
	if err != nil {
		return "", err
	}
	return presignS3Request(ctx, settings, http.MethodDelete, objectKey, "AbortMultipartUpload", expiresIn, map[string]string{"uploadId": uploadID}, nil)
}

// signListMultipartUpload signs one ListParts page. The optional list options
// are max-parts then part-number-marker; zero omits either.
func signListMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration, listOptions ...int) (string, error) {
	settings, err := destinationS3Settings(destination, "multipart list-parts")
	if err != nil {
		return "", err
	}
	query := map[string]string{"uploadId": uploadID}
	if len(listOptions) > 0 && listOptions[0] > 0 {
		query["max-parts"] = fmt.Sprintf("%d", listOptions[0])
	}
	if len(listOptions) > 1 && listOptions[1] > 0 {
		query["part-number-marker"] = fmt.Sprintf("%d", listOptions[1])
	}
	return presignS3Request(ctx, settings, http.MethodGet, objectKey, "ListParts", expiresIn, query, nil)
}

func abortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string) error {
	settings, err := destinationS3Settings(destination, "multipart abort")
	if err != nil {
		return err
	}
	_, err = s3ClientForContext(ctx, settings).AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(settings.bucket),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	return err
}

// DestinationRouteInput is the input for SignDestinationRoute. Source is
// optional: without it the route reads Chunk.SourceURL. The multipart fields
// are copied into the route metadata when set.
type DestinationRouteInput struct {
	sourceGrant         *sourceChunkGrant
	Chunk               ChunkSigningPlanItem
	Target              ChunkDestinationSigningTarget
	Source              ProviderSource
	Destination         ProviderDestination
	ExpiresIn           time.Duration
	MultipartGroupID    string
	UploadID            string
	PartNumber          int
	FinalObjectKey      string
	CompleteURL         string
	AbortURL            string
	ListPageURL         string
	FinalHeadURL        string
	ExpectedObjectSize  int64
	ExpectedPartCount   int
	MaxPartNumber       int
	FinalObjectMetadata map[string]string
	// DestURL is a pre-issued upload target, used by providers that presign their own.
	DestURL string
	// HTTPClient is used for Hippius and Hugging Face calls; nil uses http.DefaultClient.
	HTTPClient *http.Client
}

func rangeHeaderForRange(offset int64, length int64) string {
	start := max(offset, 0)
	size := max(length, 1)
	return fmt.Sprintf("bytes=%d-%d", start, start+size-1)
}

func signSourceRoute(ctx context.Context, httpClient *http.Client, source ProviderSource, chunk ChunkSigningPlanItem, expiresIn time.Duration) (string, map[string]string, error) {
	rangeHeader := rangeHeaderForRange(chunk.SourceOffset, chunk.ChunkSize)
	headers := map[string]string{"Range": rangeHeader}
	if source == nil {
		return chunk.SourceURL, headers, nil
	}
	settings, s3Compatible, err := s3SettingsFor(source)
	if err != nil {
		return "", nil, err
	}
	if s3Compatible {
		result, err := s3.NewPresignClient(s3ClientForContext(ctx, settings)).PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(settings.bucket),
			Key:    aws.String(settings.key),
			Range:  aws.String(rangeHeader),
		}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return "", nil, err
		}
		return result.URL, headers, nil
	}
	switch source := source.(type) {
	case HippiusProviderSource:
		getURL, err := hippiusPresign(ctx, httpClient, defaultString(source.BaseURL, "https://api.hippius.com"), source.APIToken, source.Bucket, source.Key, "get", expiresIn)
		if err != nil {
			return "", nil, err
		}
		return getURL, headers, nil
	case HuggingFaceProviderSource:
		// Re-resolving mints a fresh presigned CDN URL; the token stays here.
		metadata, err := huggingFaceFileMetadataFor(ctx, httpClient, huggingFaceSourceConfig(source))
		if err != nil {
			return "", nil, err
		}
		return metadata.URL, headers, nil
	default:
		return "", nil, fmt.Errorf("source signing is not implemented for %T", source)
	}
}

func signDestinationRoute(ctx context.Context, httpClient *http.Client, input DestinationRouteInput) (SignedChunkRoute, error) {
	expiresAt := time.Now().UTC().Add(input.ExpiresIn)
	objectKey := input.Target.ObjectKey
	if objectKey == "" {
		return SignedChunkRoute{}, fmt.Errorf("destination signing target is missing object_key")
	}
	destURL := input.DestURL
	if destURL == "" {
		if _, huggingFace := input.Destination.(HuggingFaceProviderDestination); huggingFace {
			return SignedChunkRoute{}, fmt.Errorf("huggingface destination routes must carry the Hub's presigned part URL as DestURL")
		}
		var err error
		destURL, err = signDestinationURL(ctx, httpClient, input.Destination, objectKey, input.UploadID, input.PartNumber, input.ExpiresIn, "")
		if err != nil {
			return SignedChunkRoute{}, err
		}
	}
	grant := input.sourceGrant
	if grant == nil {
		var err error
		grant, err = signSourceChunk(ctx, httpClient, input.Source, input.Chunk, input.ExpiresIn)
		if err != nil {
			return SignedChunkRoute{}, err
		}
	}
	if grant.expiresAt.Before(expiresAt) {
		expiresAt = grant.expiresAt
	}
	if !expiresAt.After(time.Now()) {
		return SignedChunkRoute{}, fmt.Errorf("source grant expired before attachment")
	}
	sourceURL, sourceHeaders := grant.url, grant.headers

	metadata := map[string]any{}
	for key, value := range input.Target.Metadata {
		metadata[key] = value
	}
	if input.MultipartGroupID != "" {
		metadata["multipart_group_id"] = input.MultipartGroupID
	}
	if input.UploadID != "" {
		metadata["upload_id"] = input.UploadID
	}
	if input.CompleteURL != "" {
		metadata["complete_url"] = input.CompleteURL
	}
	if input.AbortURL != "" {
		metadata["abort_url"] = input.AbortURL
	}
	if input.ListPageURL != "" {
		metadata["list_page_url"] = input.ListPageURL
	}
	if input.FinalHeadURL != "" {
		metadata["final_head_url"] = input.FinalHeadURL
	}
	if input.FinalObjectKey != "" {
		metadata["final_object_key"] = input.FinalObjectKey
	}
	if input.ExpectedObjectSize > 0 {
		metadata["expected_object_size"] = input.ExpectedObjectSize
	}
	if input.ExpectedPartCount > 0 {
		metadata["expected_part_count"] = input.ExpectedPartCount
	}
	if input.MaxPartNumber > 0 {
		metadata["max_part_number"] = input.MaxPartNumber
	}
	if input.FinalObjectMetadata != nil {
		metadata["final_object_metadata"] = input.FinalObjectMetadata
	}
	if input.PartNumber > 0 {
		metadata["part_number"] = input.PartNumber
	}

	return SignedChunkRoute{
		SourceID:      input.Chunk.SourceID,
		DestinationID: input.Target.DestinationID,
		ChunkIndex:    input.Chunk.ChunkIndex,
		SourceURL:     sourceURL,
		DestURL:       destURL,
		SourceOffset:  input.Chunk.SourceOffset,
		ChunkSize:     input.Chunk.ChunkSize,
		ExpiresAt:     boundedGrantExpiry(destURL, expiresAt).Format(time.RFC3339Nano),
		Headers:       sourceHeaders,
		Metadata:      metadata,
	}, nil
}

// signDestinationURL signs a destination upload URL: UploadPart when an upload
// id and part number are given, otherwise PutObject. A contentMD5 binds the
// upload body checksum into the signature.
func signDestinationURL(ctx context.Context, httpClient *http.Client, destination ProviderDestination, objectKey string, uploadID string, partNumber int, expiresIn time.Duration, contentMD5 string) (string, error) {
	settings, s3Compatible, err := s3SettingsFor(destination)
	if err != nil {
		return "", err
	}
	if s3Compatible {
		presigner := s3.NewPresignClient(s3ClientForContext(ctx, settings))
		expires := func(options *s3.PresignOptions) { options.Expires = expiresIn }
		if uploadID != "" && partNumber > 0 {
			input := &s3.UploadPartInput{
				Bucket:     aws.String(settings.bucket),
				Key:        aws.String(objectKey),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(int32(partNumber)),
			}
			if contentMD5 != "" {
				input.ContentMD5 = aws.String(contentMD5)
			}
			result, err := presigner.PresignUploadPart(ctx, input, expires)
			if err != nil {
				return "", err
			}
			return result.URL, nil
		}
		input := &s3.PutObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(objectKey)}
		if contentMD5 != "" {
			input.ContentMD5 = aws.String(contentMD5)
		}
		result, err := presigner.PresignPutObject(ctx, input, expires)
		if err != nil {
			return "", err
		}
		return result.URL, nil
	}
	if contentMD5 != "" {
		return "", fmt.Errorf("checksum-bound uploads require S3-compatible storage")
	}
	switch destination := destination.(type) {
	case HippiusProviderDestination:
		return hippiusPresign(ctx, httpClient, defaultString(destination.BaseURL, "https://api.hippius.com"), destination.APIToken, destination.Bucket, objectKey, "put", expiresIn)
	default:
		return "", fmt.Errorf("destination signing is not implemented for %T", destination)
	}
}

func hippiusPresign(ctx context.Context, httpClient *http.Client, baseURL string, token string, bucket string, key string, action string, expiresIn time.Duration) (string, error) {
	endpoint, err := url.Parse(fmt.Sprintf("%s/api/objectstore/buckets/%s/presigned-url/", strings.TrimRight(baseURL, "/"), bucket))
	if err != nil {
		return "", err
	}
	query := endpoint.Query()
	query.Set("key", key)
	query.Set("action", action)
	query.Set("expires_in", fmt.Sprintf("%.0f", expiresIn.Seconds()))
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Token "+token)
	response, err := httpClientOrDefault(httpClient).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("hippius presigned URL failed with status %d", response.StatusCode)
	}
	var payload struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", err
	}
	return payload.URL, nil
}

func hippiusObjectSize(ctx context.Context, httpClient *http.Client, baseURL string, token string, bucket string, key string) (int64, error) {
	endpoint, err := url.Parse(fmt.Sprintf("%s/api/objectstore/buckets/%s/objects/", strings.TrimRight(baseURL, "/"), bucket))
	if err != nil {
		return 0, err
	}
	query := endpoint.Query()
	query.Set("prefix", key)
	query.Set("max_keys", "1")
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Token "+token)
	response, err := httpClientOrDefault(httpClient).Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, fmt.Errorf("hippius object lookup failed with status %d", response.StatusCode)
	}
	var payload struct {
		Contents []struct {
			Size int64 `json:"Size"`
		} `json:"Contents"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return 0, err
	}
	if len(payload.Contents) == 0 {
		return 0, fmt.Errorf("object not found: hippius://%s/%s", bucket, key)
	}
	return payload.Contents[0].Size, nil
}

func httpClientOrDefault(httpClient *http.Client) *http.Client {
	if httpClient == nil {
		return http.DefaultClient
	}
	return httpClient
}

func defaultID(value string, prefix string, index int) string {
	if value != "" {
		return value
	}
	return fmt.Sprintf("%s_%d", prefix, index)
}

func defaultString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func defaultInt(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func providerName(config any) string {
	switch typed := config.(type) {
	case S3ProviderSource, S3ProviderDestination:
		return "s3"
	case R2ProviderSource, R2ProviderDestination:
		return "r2"
	case S3CompatibleProviderSource:
		return s3CompatibleProviderName(typed.Provider)
	case S3CompatibleProviderDestination:
		return s3CompatibleProviderName(typed.Provider)
	case HippiusProviderSource, HippiusProviderDestination:
		return "hippius"
	case HuggingFaceProviderSource, HuggingFaceProviderDestination:
		return "huggingface"
	case GCSProviderSource, GCSProviderDestination:
		return "gcs"
	case AzureProviderSource, AzureProviderDestination:
		return "azure"
	default:
		return fmt.Sprintf("%T", config)
	}
}

// isDirectPutDestination reports whether a destination takes a plain PUT per chunk instead of an
// S3 multipart upload. BeamCore rejects multipart route metadata for these, so they never get a
// multipart group manifest.
func isDirectPutDestination(destination ProviderDestination) bool {
	switch destination.(type) {
	case HippiusProviderDestination, HuggingFaceProviderDestination:
		return true
	default:
		return false
	}
}

func withStorageLocation(metadata map[string]any, location string) map[string]any {
	if location != "" {
		metadata["storage_location"] = location
	}
	return metadata
}
