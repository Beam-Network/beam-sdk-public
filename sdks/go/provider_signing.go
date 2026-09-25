package beamnetworksdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ProviderSource interface{}
type ProviderDestination interface{}

const providerFailureCancellationTimeout = 2 * time.Minute
const providerUploadAbortTimeout = 30 * time.Second
const providerUploadAbortConcurrency = 32

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
	return client.prepareProviderTransfer(ctx, sources, destinations, name, testMode, expiresIn, distribute, routeGenerationID, idempotencyKey, nil)
}

type providerRecoveryContext struct {
	uploads  map[string]multipartUploadState
	prepared *TransferPrepareResponse
	mu       sync.RWMutex
	streamMu sync.Mutex
}

func (client *Client) prepareProviderTransfer(
	ctx context.Context,
	sources []ProviderSource,
	destinations []ProviderDestination,
	name string,
	testMode bool,
	expiresIn time.Duration,
	distribute bool,
	routeGenerationID string,
	idempotencyKey []string,
	recovery *providerRecoveryContext,
) (*TransferPrepareResponse, error) {
	if expiresIn == 0 {
		expiresIn = time.Hour
	}

	type sourcePrepareResult struct {
		index    int
		prepared PreparedHTTPSource
		err      error
	}
	preparedSources := make([]PreparedHTTPSource, len(sources))
	completedSources := make(chan sourcePrepareResult, client.routeSigningConcurrency)
	pendingSources := 0
	var sourcePrepareErr error
	collectPreparedSource := func() {
		result := <-completedSources
		pendingSources--
		if result.err != nil {
			if sourcePrepareErr == nil {
				sourcePrepareErr = result.err
			}
			return
		}
		preparedSources[result.index] = result.prepared
	}
	for index, source := range sources {
		go func(index int, source ProviderSource) {
			prepared, err := prepareProviderSource(ctx, client.httpClient, source, index, expiresIn)
			completedSources <- sourcePrepareResult{index: index, prepared: prepared, err: err}
		}(index, source)
		pendingSources++
		if pendingSources >= client.routeSigningConcurrency {
			collectPreparedSource()
		}
	}
	for pendingSources > 0 {
		collectPreparedSource()
	}
	if sourcePrepareErr != nil {
		return nil, sourcePrepareErr
	}

	preparedDestinations := make([]PreparedDestination, 0, len(destinations))
	for index, destination := range destinations {
		prepared, err := prepareProviderDestination(destination, index)
		if err != nil {
			return nil, err
		}
		preparedDestinations = append(preparedDestinations, prepared)
	}

	huggingFaceStates, huggingFaceChunkSize, err := client.planHuggingFaceUploads(
		ctx, sources, preparedSources, destinations, preparedDestinations,
	)
	if err != nil {
		return nil, err
	}

	var prepared *TransferPrepareResponse
	if recovery != nil {
		preparedCopy := *recovery.prepared
		preparedCopy.RouteGenerationID = routeGenerationID
		prepared = &preparedCopy
	} else {
		prepared, err = client.prepareTransferWithChunkSize(ctx, preparedSources, preparedDestinations, name, testMode, "", routeGenerationID, huggingFaceChunkSize, idempotencyKey...)
	}
	if err != nil || !prepared.Success {
		return prepared, err
	}

	huggingFaceByDestination := map[string]*huggingFaceUploadState{}
	if len(huggingFaceStates) > 0 {
		if err := assertHuggingFacePlan(prepared, huggingFaceStates); err != nil {
			return nil, err
		}
		client.huggingFaceMu.Lock()
		if client.huggingFaceUploads == nil {
			client.huggingFaceUploads = map[string][]*huggingFaceUploadState{}
		}
		client.huggingFaceUploads[prepared.TransferID] = huggingFaceStates
		client.huggingFaceMu.Unlock()
		for _, state := range huggingFaceStates {
			huggingFaceByDestination[state.DestinationID] = state
		}
	}

	destinationByID := map[string]ProviderDestination{}
	for index, preparedDestination := range preparedDestinations {
		destinationByID[preparedDestination.DestinationID] = destinations[index]
	}
	sourceByID := map[string]ProviderSource{}
	for index, preparedSource := range preparedSources {
		sourceByID[preparedSource.SourceID] = sources[index]
	}

	uploads := map[string]multipartUploadState{}
	var uploadsMu sync.RWMutex
	if recovery != nil {
		uploads = recovery.uploads
	}
	uploadsLock := &uploadsMu
	if recovery != nil {
		uploadsLock = &recovery.mu
	}
	var initialRecovery *providerRecoveryContext
	if recovery == nil {
		recoverySources := append([]ProviderSource(nil), sources...)
		recoveryDestinations := append([]ProviderDestination(nil), destinations...)
		recoveryKey := ""
		if len(idempotencyKey) > 0 {
			recoveryKey = idempotencyKey[0]
		}
		preparedSnapshot := *prepared
		initialRecovery = &providerRecoveryContext{uploads: uploads, prepared: &preparedSnapshot}
		uploadsLock = &initialRecovery.mu
		initialRecovery.streamMu.Lock()
		defer initialRecovery.streamMu.Unlock()
		client.control.registerRecoveryLease(&recoveryLease{
			transferID:         prepared.TransferID,
			planFingerprint:    prepared.PlanFingerprint,
			coordinateChecksum: prepared.CoordinateChecksum,
			replayRoutes: func(background context.Context, generationID string) error {
				initialRecovery.streamMu.Lock()
				defer initialRecovery.streamMu.Unlock()
				keys := []string{}
				if recoveryKey != "" {
					keys = append(keys, recoveryKey)
				}
				_, replayErr := client.prepareProviderTransfer(background, recoverySources, recoveryDestinations, name, testMode, expiresIn, distribute, generationID, keys, initialRecovery)
				return replayErr
			},
			dispose: func() {
				initialRecovery.mu.Lock()
				clear(initialRecovery.uploads)
				initialRecovery.mu.Unlock()
				for index := range recoverySources {
					recoverySources[index] = nil
				}
				for index := range recoveryDestinations {
					recoveryDestinations[index] = nil
				}
				recoverySources = nil
				recoveryDestinations = nil
			},
		})
	}
	handleStreamingFailure := func(cause error) error {
		if recovery != nil {
			return cause
		}
		if ctx.Err() != nil {
			client.control.continueRecoveryLease(prepared.TransferID)
			return cause
		}
		if isRecoverableRouteStreamError(cause) {
			client.control.continueRecoveryLease(prepared.TransferID)
			return &RouteRecoveryPendingError{TransferID: prepared.TransferID, Cause: cause}
		}
		cleanupErr := client.cancelAndAbortProviderTransferAfterFailure(prepared.TransferID, cause, uploads)
		client.control.releaseRecoveryLease(prepared.TransferID)
		return cleanupErr
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	signedURLFlow := SignedURLFlowCanonical
	urlsExpiresAt := time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano)
	stream := newRouteStreamSender(client.control, prepared.TransferID, prepared.PlanDescriptor.DeliveryRouteCount, prepared.PlanDescriptor.LogicalChunkCount, distribute, urlsExpiresAt, prepared.PlanDescriptor.PlanNonce+":"+prepared.RouteGenerationID, signedURLFlow, prepared.RouteGenerationID)
	if err := stream.begin(streamCtx); err != nil {
		cancelStream()
		return nil, handleStreamingFailure(err)
	}
	multipartWaiters, multipartDone, err := client.startMultipartGroupManifest(streamCtx, prepared, destinationByID, uploads, uploadsLock, expiresIn, stream, signedURLFlow)
	if err != nil {
		cancelStream()
		return nil, handleStreamingFailure(err)
	}
	type routeSignResult struct {
		route SignedChunkRoute
		err   error
	}
	completedRoutes := make(chan routeSignResult, client.routeSigningConcurrency)
	pendingRouteCount := 0
	signingConcurrency := client.routeSigningConcurrency
	signedInWindow := 0
	signingWindowStartedAt := time.Now()
	failStreaming := func(cause error) error {
		cancelStream()
		for pendingRouteCount > 0 {
			<-completedRoutes
			pendingRouteCount--
		}
		if sendErr := stream.waitForPendingSend(); sendErr != nil {
			cause = errors.Join(cause, fmt.Errorf("route stream shutdown failed: %w", sendErr))
		}
		<-multipartDone.done
		if multipartDone.err != nil {
			cause = errors.Join(cause, fmt.Errorf("multipart manifest streaming failed: %w", multipartDone.err))
		}
		return handleStreamingFailure(cause)
	}
	flushCompletedRoute := func() error {
		result := <-completedRoutes
		pendingRouteCount--
		if result.err != nil {
			return result.err
		}
		if err := stream.addRoute(streamCtx, result.route); err != nil {
			return err
		}
		signedInWindow++
		if signedInWindow == routeStreamBatchRoutes {
			if !client.routeSigningConcurrencyOverridden && time.Since(signingWindowStartedAt) > 4*time.Second && signingConcurrency < 256 {
				signingConcurrency *= 2
				if signingConcurrency > 256 {
					signingConcurrency = 256
				}
			}
			signedInWindow = 0
			signingWindowStartedAt = time.Now()
		}
		return nil
	}
	planIterator := newPlanChunkIterator(prepared.PlanDescriptor, prepared.TransferID)
	for {
		chunk, hasChunk, planErr := planIterator.next()
		if planErr != nil {
			return nil, failStreaming(planErr)
		}
		if !hasChunk {
			break
		}
		for _, target := range chunk.Destinations {
			destination, ok := destinationByID[target.DestinationID]
			if !ok {
				return nil, failStreaming(fmt.Errorf("BeamCore returned unknown destination_id: %s", target.DestinationID))
			}
			source, ok := sourceByID[chunk.SourceID]
			if !ok {
				return nil, failStreaming(fmt.Errorf("BeamCore returned unknown source_id: %s", chunk.SourceID))
			}
			finalObjectKey := target.ObjectKey
			if value, ok := target.Metadata["final_object_key"].(string); ok && value != "" {
				finalObjectKey = value
			}
			if finalObjectKey == "" {
				return nil, failStreaming(fmt.Errorf("destination signing target is missing object_key"))
			}

			multipartKey := fmt.Sprintf("%s:%s:%s:%s", prepared.TransferID, target.DestinationID, chunk.SourceID, finalObjectKey)
			var upload multipartUploadState
			if !isDirectPutDestination(destination) {
				waiter, ok := multipartWaiters[multipartKey]
				if !ok {
					return nil, failStreaming(fmt.Errorf("multipart group manifest missing for %s", multipartKey))
				}
				<-waiter.done
				if waiter.err != nil {
					return nil, failStreaming(waiter.err)
				}
				upload = waiter.state
			}

			partNumber, err := multipartPartNumber(chunk.SourceChunkIndex)
			if err != nil {
				return nil, failStreaming(err)
			}
			go func() {
				route, signErr := client.signProviderRouteForAttempt(streamCtx, chunk, target, source, destination, upload, finalObjectKey, partNumber, prepared.TransferID, expiresIn, signedURLFlow, huggingFaceByDestination[target.DestinationID])
				completedRoutes <- routeSignResult{route: route, err: signErr}
			}()
			pendingRouteCount++
			if pendingRouteCount >= signingConcurrency {
				if err := flushCompletedRoute(); err != nil {
					return nil, failStreaming(err)
				}
			}
		}
	}
	for pendingRouteCount > 0 {
		if err := flushCompletedRoute(); err != nil {
			return nil, failStreaming(err)
		}
	}
	<-multipartDone.done
	if multipartDone.err != nil {
		return nil, failStreaming(multipartDone.err)
	}

	var attached AttachSignedURLsResponse
	if err := stream.complete(streamCtx, &attached); err != nil {
		return nil, failStreaming(err)
	}
	if !attached.Success {
		return nil, failStreaming(fmt.Errorf("route stream failed: %s%s", attached.Error, attached.Message))
	}
	if recovery != nil {
		return prepared, nil
	}
	return prepared, nil
}

type multipartGroupFuture struct {
	done  chan struct{}
	state multipartUploadState
	err   error
}

type multipartManifestFuture struct {
	done chan struct{}
	err  error
}

func (client *Client) startMultipartGroupManifest(
	ctx context.Context,
	prepared *TransferPrepareResponse,
	destinationByID map[string]ProviderDestination,
	uploads map[string]multipartUploadState,
	uploadsMu *sync.RWMutex,
	expiresIn time.Duration,
	stream *routeStreamSender,
	signedURLFlow SignedURLFlow,
) (map[string]*multipartGroupFuture, *multipartManifestFuture, error) {
	type groupJob struct {
		groupID         string
		source          CompactTransferPlanSource
		destinationPlan CompactTransferPlanDestination
		destination     ProviderDestination
		finalObjectKey  string
	}
	jobs := make([]groupJob, 0)
	futures := make(map[string]*multipartGroupFuture)
	for _, source := range prepared.PlanDescriptor.Sources {
		for _, destinationPlan := range prepared.PlanDescriptor.Destinations {
			destination, ok := destinationByID[destinationPlan.DestinationID]
			if !ok {
				return nil, nil, fmt.Errorf("BeamCore returned unknown destination_id: %s", destinationPlan.DestinationID)
			}
			if isDirectPutDestination(destination) {
				continue
			}
			finalObjectKey := destinationPlan.FinalObjectKeys[source.SourceID]
			if finalObjectKey == "" {
				return nil, nil, fmt.Errorf("missing final object key for %s:%s", source.SourceID, destinationPlan.DestinationID)
			}
			groupID := fmt.Sprintf("%s:%s:%s:%s", prepared.TransferID, destinationPlan.DestinationID, source.SourceID, finalObjectKey)
			jobs = append(jobs, groupJob{groupID: groupID, source: source, destinationPlan: destinationPlan, destination: destination, finalObjectKey: finalObjectKey})
			futures[groupID] = &multipartGroupFuture{done: make(chan struct{})}
		}
	}
	manifestFuture := &multipartManifestFuture{done: make(chan struct{})}
	if len(jobs) == 0 {
		close(manifestFuture.done)
		return futures, manifestFuture, nil
	}
	workerCount := client.routeSigningConcurrency
	if len(jobs) < workerCount {
		workerCount = len(jobs)
	}
	jobQueue := make(chan groupJob)
	results := make(chan error, len(jobs))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := 0; worker < workerCount; worker++ {
		go func() {
			defer workers.Done()
			for job := range jobQueue {
				uploadsMu.RLock()
				retainedState, retained := uploads[job.groupID]
				uploadsMu.RUnlock()
				var retainedUpload *multipartUploadState
				if retained {
					retainedUpload = &retainedState
				}
				state, err := createMultipartGroupState(ctx, prepared, job.source, job.destinationPlan, job.destination, job.finalObjectKey, expiresIn, signedURLFlow, retainedUpload)
				if err == nil {
					uploadsMu.Lock()
					uploads[job.groupID] = state
					uploadsMu.Unlock()
					err = validateSignedRouteManifestContract(prepared.TransferID, nil, []MultipartGroupManifest{state.Manifest})
				}
				if err == nil {
					err = stream.addManifestGroups(ctx, []MultipartGroupManifest{state.Manifest})
				}
				future := futures[job.groupID]
				future.state = state
				future.err = err
				close(future.done)
				results <- err
			}
		}()
	}
	go func() {
		for _, job := range jobs {
			jobQueue <- job
		}
		close(jobQueue)
	}()
	go func() {
		workers.Wait()
		for index := 0; index < len(jobs); index++ {
			if resultErr := <-results; resultErr != nil && manifestFuture.err == nil {
				manifestFuture.err = resultErr
			}
		}
		close(manifestFuture.done)
	}()
	return futures, manifestFuture, nil
}

func createMultipartGroupState(
	ctx context.Context,
	prepared *TransferPrepareResponse,
	source CompactTransferPlanSource,
	destinationPlan CompactTransferPlanDestination,
	destination ProviderDestination,
	finalObjectKey string,
	expiresIn time.Duration,
	signedURLFlow SignedURLFlow,
	retained *multipartUploadState,
) (multipartUploadState, error) {
	groupID := fmt.Sprintf("%s:%s:%s:%s", prepared.TransferID, destinationPlan.DestinationID, source.SourceID, finalObjectKey)
	metadata := map[string]string{
		"beam-transfer-id": prepared.TransferID,
	}
	uploadID := ""
	createdUpload := retained == nil
	if retained != nil {
		uploadID = retained.UploadID
	} else {
		var err error
		uploadID, err = createMultipartUpload(ctx, destination, finalObjectKey, metadata)
		if err != nil {
			return multipartUploadState{}, err
		}
	}
	fail := func(cause error) (multipartUploadState, error) {
		if !createdUpload {
			return multipartUploadState{}, cause
		}
		if cleanupErr := abortMultipartUploadForCleanup(destination, finalObjectKey, uploadID); cleanupErr != nil {
			return multipartUploadState{}, fmt.Errorf("multipart group setup failed (%w) and cleanup failed: %v", cause, cleanupErr)
		}
		return multipartUploadState{}, cause
	}
	maxPartNumber, err := multipartPartNumber(source.ChunkCount-1, multipartAttemptSlots-1)
	if err != nil {
		return fail(err)
	}
	urlsExpiresAt := time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano)
	listPageURLs := make([]string, 0, (maxPartNumber+999)/1000)
	for marker := 0; marker < maxPartNumber; marker += 1000 {
		listURL, signErr := signListMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn, 1000, marker)
		if signErr != nil {
			return fail(signErr)
		}
		listPageURLs = append(listPageURLs, listURL)
	}
	completeURL, err := signCompleteMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn)
	if err != nil {
		return fail(err)
	}
	abortURL, err := signAbortMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn)
	if err != nil {
		return fail(err)
	}
	finalHeadURL, err := signFinalObjectHead(ctx, destination, finalObjectKey, expiresIn)
	if err != nil {
		return fail(err)
	}
	manifest := MultipartGroupManifest{
		MultipartGroupID: groupID, SourceID: source.SourceID, DestinationID: destinationPlan.DestinationID,
		FinalObjectKey: finalObjectKey, UploadID: uploadID, ExpectedObjectSize: source.Size,
		ExpectedPartCount: source.ChunkCount, MaxPartNumber: maxPartNumber, CompleteURL: completeURL,
		AbortURL: abortURL, ListPageURLs: listPageURLs,
		FinalHeadURL: finalHeadURL, FinalObjectMetadata: metadata,
		URLsExpiresAt: urlsExpiresAt,
	}
	return multipartUploadState{Destination: destination, ObjectKey: finalObjectKey, UploadID: uploadID, Manifest: manifest}, nil
}

func (client *Client) cancelProviderTransferAfterFailure(transferID string, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), providerFailureCancellationTimeout)
	defer cancel()
	result, cancelErr := client.CancelTransfer(cleanupCtx, transferID)
	if cancelErr != nil {
		return fmt.Errorf("provider transfer failed (%w) and transfer cancellation failed: %v", cause, cancelErr)
	}
	if result == nil || !result.Success {
		message := "Beam rejected cancellation"
		if result != nil && result.Message != "" {
			message = result.Message
		}
		return fmt.Errorf("provider transfer failed (%w) and transfer cancellation failed: %s", cause, message)
	}
	return nil
}

func (client *Client) cancelAndAbortProviderTransferAfterFailure(
	transferID string,
	cause error,
	uploads map[string]multipartUploadState,
) error {
	cancelErr := client.cancelProviderTransferAfterFailure(transferID, cause)
	cleanupErr := abortCreatedUploads(uploads)
	if cancelErr != nil && cleanupErr != nil {
		return errors.Join(cancelErr, fmt.Errorf("multipart cleanup failed: %w", cleanupErr))
	}
	if cancelErr != nil {
		return cancelErr
	}
	if cleanupErr != nil {
		return fmt.Errorf("provider transfer failed (%w) and multipart cleanup failed: %v", cause, cleanupErr)
	}
	return cause
}

func (client *Client) signProviderRouteForAttempt(
	ctx context.Context,
	chunk ChunkSigningPlanItem,
	target ChunkDestinationSigningTarget,
	source ProviderSource,
	destination ProviderDestination,
	upload multipartUploadState,
	finalObjectKey string,
	partNumber int,
	transferID string,
	expiresIn time.Duration,
	signedURLFlow SignedURLFlow,
	huggingFaceUpload *huggingFaceUploadState,
) (SignedChunkRoute, error) {
	if _, huggingFace := destination.(HuggingFaceProviderDestination); huggingFace {
		if huggingFaceUpload == nil {
			return SignedChunkRoute{}, fmt.Errorf("huggingface upload state is missing for %s", target.DestinationID)
		}
		// The Hub presigns its own part targets; chunk N carries the URL for part N + 1.
		destURL := huggingFaceUpload.UploadHref
		if huggingFaceUpload.ChunkSize > 0 {
			destURL = ""
			if chunk.SourceChunkIndex < len(huggingFaceUpload.PartURLs) {
				destURL = huggingFaceUpload.PartURLs[chunk.SourceChunkIndex]
			}
		}
		if destURL == "" {
			return SignedChunkRoute{}, fmt.Errorf(
				"the Hub issued no upload URL for chunk %d of %s",
				chunk.SourceChunkIndex, huggingFaceUpload.Config.describe(),
			)
		}
		target.ObjectKey = finalObjectKey
		// BeamCore rejects part_number on a destination it does not treat as an S3 multipart target.
		metadata := map[string]any{}
		for key, value := range target.Metadata {
			if key != "part_number" {
				metadata[key] = value
			}
		}
		target.Metadata = metadata
		return signDestinationRoute(ctx, client.httpClient, destinationRouteInput{
			Chunk: chunk, Target: target, Source: source, Destination: destination,
			ExpiresIn: expiresIn, TransferID: transferID, DestURL: destURL,
		})
	}
	if _, hippius := destination.(HippiusProviderDestination); hippius {
		return signDestinationRoute(ctx, client.httpClient, destinationRouteInput{
			Chunk: chunk, Target: target, Source: source, Destination: destination,
			ExpiresIn: expiresIn, TransferID: transferID,
		})
	}
	if upload.UploadID == "" {
		return SignedChunkRoute{}, fmt.Errorf("multipart group manifest is required for provider destinations")
	}
	if partNumber < 1 || partNumber > upload.Manifest.MaxPartNumber {
		return SignedChunkRoute{}, fmt.Errorf("multipart part_number %d is outside group %s range 1-%d", partNumber, upload.Manifest.MultipartGroupID, upload.Manifest.MaxPartNumber)
	}
	target.ObjectKey = finalObjectKey
	listPageIndex := (partNumber - 1) / 1000
	if listPageIndex < 0 || listPageIndex >= len(upload.Manifest.ListPageURLs) {
		return SignedChunkRoute{}, fmt.Errorf("multipart list_page_url missing for part_number %d", partNumber)
	}
	return signDestinationRoute(ctx, client.httpClient, destinationRouteInput{
		Chunk:               chunk,
		Target:              target,
		Source:              source,
		Destination:         destination,
		ExpiresIn:           expiresIn,
		MultipartGroupID:    upload.Manifest.MultipartGroupID,
		UploadID:            upload.UploadID,
		PartNumber:          partNumber,
		MultipartObjectKey:  finalObjectKey,
		TransferID:          transferID,
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
	destinations := make([]ChunkDestinationSigningTarget, 0, len(descriptor.Destinations))
	for _, destination := range descriptor.Destinations {
		finalObjectKey := destination.FinalObjectKeys[sourceID]
		if finalObjectKey == "" {
			return ChunkSigningPlanItem{}, false
		}
		partNumber, err := multipartPartNumber(sourceChunkIndex)
		if err != nil {
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
	switch source := source.(type) {
	case S3ProviderSource:
		client := s3Client(source.Region, source.EndpointURL, source.AccessKeyID, source.SecretAccessKey, source.SessionToken)
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(source.Bucket), Key: aws.String(source.Key)})
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		presigned := s3.NewPresignClient(client)
		getURL, err := presigned.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(source.Bucket), Key: aws.String(source.Key)}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		return PreparedHTTPSource{
			SourceID:  defaultID(source.SourceID, "src", index),
			Type:      "http",
			Provider:  "s3",
			URL:       getURL.URL,
			Size:      *head.ContentLength,
			Filename:  path.Base(source.Key),
			ExpiresAt: time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano),
			Metadata:  map[string]any{"bucket": source.Bucket, "key": source.Key, "region": defaultString(source.Region, "us-east-1")},
		}, nil
	case R2ProviderSource:
		client := r2Client(source.EndpointURL, source.AccountID, source.AccessKeyID, source.SecretAccessKey)
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(source.Bucket), Key: aws.String(source.Key)})
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		presigned := s3.NewPresignClient(client)
		getURL, err := presigned.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(source.Bucket), Key: aws.String(source.Key)}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return PreparedHTTPSource{}, err
		}
		return PreparedHTTPSource{
			SourceID:  defaultID(source.SourceID, "src", index),
			Type:      "http",
			Provider:  "r2",
			URL:       getURL.URL,
			Size:      *head.ContentLength,
			Filename:  path.Base(source.Key),
			ExpiresAt: time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano),
			Metadata:  map[string]any{"bucket": source.Bucket, "key": source.Key, "endpoint_url": r2Endpoint(source.EndpointURL, source.AccountID)},
		}, nil
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
			Metadata: huggingFaceMetadata(config, metadata.ETag, metadata.CommitHash),
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
			Metadata:  map[string]any{"bucket": source.Bucket, "key": source.Key, "base_url": baseURL},
		}, nil
	default:
		return PreparedHTTPSource{}, fmt.Errorf("provider source signing is not implemented for %T", source)
	}
}

func prepareProviderDestination(destination ProviderDestination, index int) (PreparedDestination, error) {
	switch destination := destination.(type) {
	case S3ProviderDestination:
		metadata := map[string]any{"bucket": destination.Bucket, "key": destination.Key, "region": defaultString(destination.Region, "us-east-1")}
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "s3",
			LogicalPrefix: destination.Key,
			Metadata:      metadata,
		}, nil
	case R2ProviderDestination:
		metadata := map[string]any{"bucket": destination.Bucket, "key": destination.Key, "endpoint_url": r2Endpoint(destination.EndpointURL, destination.AccountID), "account_id": destination.AccountID}
		return PreparedDestination{
			DestinationID: defaultID(destination.DestinationID, "dst", index),
			Provider:      "r2",
			LogicalPrefix: destination.Key,
			Metadata:      metadata,
		}, nil
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
			Metadata:      huggingFaceMetadata(config, "", ""),
		}, nil
	case HippiusProviderDestination:
		metadata := map[string]any{"bucket": destination.Bucket, "key": destination.Key, "base_url": defaultString(destination.BaseURL, "https://api.hippius.com")}
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

func createMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, metadata map[string]string) (string, error) {
	client, bucket, err := destinationS3Client(destination)
	if err != nil {
		return "", err
	}
	response, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(objectKey), Metadata: metadata})
	if err != nil {
		return "", err
	}
	if response.UploadId == nil || *response.UploadId == "" {
		return "", fmt.Errorf("provider did not return UploadId for %s", objectKey)
	}
	return *response.UploadId, nil
}

func signFinalObjectHead(ctx context.Context, destination ProviderDestination, objectKey string, expiresIn time.Duration) (string, error) {
	return presignS3Operation(ctx, destination, http.MethodHead, objectKey, "HeadObject", expiresIn, nil, nil)
}

func signCompleteMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	return presignS3Operation(ctx, destination, http.MethodPost, objectKey, "CompleteMultipartUpload", expiresIn, map[string]string{
		"uploadId": uploadID,
	}, nil)
}

func signAbortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration) (string, error) {
	return presignS3Operation(ctx, destination, http.MethodDelete, objectKey, "AbortMultipartUpload", expiresIn, map[string]string{
		"uploadId": uploadID,
	}, nil)
}

func signListMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string, expiresIn time.Duration, listOptions ...int) (string, error) {
	bucket, endpoint, region, accessKeyID, secretAccessKey, sessionToken, err := destinationSigningInfo(destination)
	if err != nil {
		return "", err
	}
	maxParts := 0
	partNumberMarker := 0
	if len(listOptions) > 0 {
		maxParts = listOptions[0]
	}
	if len(listOptions) > 1 {
		partNumberMarker = listOptions[1]
	}
	requestURL, err := listPartsRequestURL(endpoint, bucket, objectKey, uploadID, expiresIn, maxParts, partNumberMarker)
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", err
	}
	signer := v4.NewSigner()
	signedURL, _, err := signer.PresignHTTP(
		ctx,
		aws.Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
			Source:          "beam-network-sdk",
		},
		request,
		"UNSIGNED-PAYLOAD",
		"s3",
		region,
		time.Now().UTC(),
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	)
	if err != nil {
		return "", err
	}
	return signedURL, nil
}

func abortCreatedUploads(uploads map[string]multipartUploadState) error {
	jobs := make(chan multipartUploadState)
	errorsByUpload := make(chan error, len(uploads))
	workerCount := providerUploadAbortConcurrency
	if len(uploads) < workerCount {
		workerCount = len(uploads)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := 0; worker < workerCount; worker++ {
		go func() {
			defer workers.Done()
			for upload := range jobs {
				if upload.UploadID == "" {
					continue
				}
				if err := abortMultipartUploadForCleanup(upload.Destination, upload.ObjectKey, upload.UploadID); err != nil {
					errorsByUpload <- err
				}
			}
		}()
	}
	for _, upload := range uploads {
		jobs <- upload
	}
	close(jobs)
	workers.Wait()
	close(errorsByUpload)
	var cleanupErrors []error
	for err := range errorsByUpload {
		cleanupErrors = append(cleanupErrors, err)
	}
	return errors.Join(cleanupErrors...)
}

func abortMultipartUploadForCleanup(destination ProviderDestination, objectKey string, uploadID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), providerUploadAbortTimeout)
	defer cancel()
	return abortMultipartUpload(cleanupCtx, destination, objectKey, uploadID)
}

func abortMultipartUpload(ctx context.Context, destination ProviderDestination, objectKey string, uploadID string) error {
	client, bucket, err := destinationS3Client(destination)
	if err != nil {
		return err
	}
	_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	return err
}

type destinationRouteInput struct {
	Chunk               ChunkSigningPlanItem
	Target              ChunkDestinationSigningTarget
	Source              ProviderSource
	Destination         ProviderDestination
	ExpiresIn           time.Duration
	MultipartGroupID    string
	UploadID            string
	PartNumber          int
	MultipartObjectKey  string
	TransferID          string
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
}

func rangeHeaderForChunk(chunk ChunkSigningPlanItem) string {
	start := chunk.SourceOffset
	if start < 0 {
		start = 0
	}
	size := chunk.ChunkSize
	if size < 1 {
		size = 1
	}
	return fmt.Sprintf("bytes=%d-%d", start, start+size-1)
}

func signSourceRoute(ctx context.Context, httpClient *http.Client, source ProviderSource, chunk ChunkSigningPlanItem, expiresIn time.Duration) (string, map[string]string, error) {
	rangeHeader := rangeHeaderForChunk(chunk)
	headers := map[string]string{"Range": rangeHeader}
	if source == nil {
		return chunk.SourceURL, headers, nil
	}
	switch source := source.(type) {
	case S3ProviderSource:
		client := s3Client(source.Region, source.EndpointURL, source.AccessKeyID, source.SecretAccessKey, source.SessionToken)
		presigned := s3.NewPresignClient(client)
		result, err := presigned.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(source.Bucket),
			Key:    aws.String(source.Key),
			Range:  aws.String(rangeHeader),
		}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return "", nil, err
		}
		return result.URL, headers, nil
	case R2ProviderSource:
		client := r2Client(source.EndpointURL, source.AccountID, source.AccessKeyID, source.SecretAccessKey)
		presigned := s3.NewPresignClient(client)
		result, err := presigned.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(source.Bucket),
			Key:    aws.String(source.Key),
			Range:  aws.String(rangeHeader),
		}, func(options *s3.PresignOptions) {
			options.Expires = expiresIn
		})
		if err != nil {
			return "", nil, err
		}
		return result.URL, headers, nil
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

func signDestinationRoute(ctx context.Context, httpClient *http.Client, input destinationRouteInput) (SignedChunkRoute, error) {
	objectKey := input.Target.ObjectKey
	if objectKey == "" {
		return SignedChunkRoute{}, fmt.Errorf("destination signing target is missing object_key")
	}
	destURL := input.DestURL
	if destURL != "" {
		// Providers such as Hugging Face presign their own upload targets.
		return finishDestinationRoute(ctx, httpClient, input, destURL)
	}
	switch destination := input.Destination.(type) {
	case S3ProviderDestination, R2ProviderDestination:
		client, bucket, err := destinationS3Client(destination)
		if err != nil {
			return SignedChunkRoute{}, err
		}
		presigned := s3.NewPresignClient(client)
		var resultURL string
		if input.UploadID != "" && input.PartNumber > 0 {
			result, err := presigned.PresignUploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(objectKey),
				UploadId:   aws.String(input.UploadID),
				PartNumber: aws.Int32(int32(input.PartNumber)),
			}, func(options *s3.PresignOptions) {
				options.Expires = input.ExpiresIn
			})
			if err != nil {
				return SignedChunkRoute{}, err
			}
			resultURL = result.URL
		} else {
			result, err := presigned.PresignPutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(objectKey),
			}, func(options *s3.PresignOptions) {
				options.Expires = input.ExpiresIn
			})
			if err != nil {
				return SignedChunkRoute{}, err
			}
			resultURL = result.URL
		}
		destURL = resultURL
	case HippiusProviderDestination:
		var err error
		destURL, err = hippiusPresign(ctx, httpClient, defaultString(destination.BaseURL, "https://api.hippius.com"), destination.APIToken, destination.Bucket, objectKey, "put", input.ExpiresIn)
		if err != nil {
			return SignedChunkRoute{}, err
		}
	case HuggingFaceProviderDestination:
		return SignedChunkRoute{}, fmt.Errorf("huggingface destination routes must carry the Hub's presigned part URL as DestURL")
	default:
		return SignedChunkRoute{}, fmt.Errorf("destination signing is not implemented for %T", destination)
	}

	return finishDestinationRoute(ctx, httpClient, input, destURL)
}

func finishDestinationRoute(ctx context.Context, httpClient *http.Client, input destinationRouteInput, destURL string) (SignedChunkRoute, error) {
	sourceURL, sourceHeaders, err := signSourceRoute(ctx, httpClient, input.Source, input.Chunk, input.ExpiresIn)
	if err != nil {
		return SignedChunkRoute{}, err
	}

	metadata := map[string]any{}
	for key, value := range input.Target.Metadata {
		metadata[key] = value
	}
	if input.TransferID != "" {
		metadata["transfer_id"] = input.TransferID
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
	if input.MultipartObjectKey != "" {
		metadata["final_object_key"] = input.MultipartObjectKey
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
		ExpiresAt:     time.Now().UTC().Add(input.ExpiresIn).Format(time.RFC3339Nano),
		Headers:       sourceHeaders,
		Metadata:      metadata,
	}, nil
}

func destinationS3Client(destination ProviderDestination) (*s3.Client, string, error) {
	switch destination := destination.(type) {
	case S3ProviderDestination:
		return s3Client(destination.Region, destination.EndpointURL, destination.AccessKeyID, destination.SecretAccessKey, destination.SessionToken), destination.Bucket, nil
	case R2ProviderDestination:
		return r2Client(destination.EndpointURL, destination.AccountID, destination.AccessKeyID, destination.SecretAccessKey), destination.Bucket, nil
	default:
		return nil, "", fmt.Errorf("multipart upload is not supported for %T", destination)
	}
}

func destinationSigningInfo(destination ProviderDestination) (bucket string, endpoint string, region string, accessKeyID string, secretAccessKey string, sessionToken string, err error) {
	switch destination := destination.(type) {
	case S3ProviderDestination:
		region = defaultString(destination.Region, "us-east-1")
		endpoint = destination.EndpointURL
		if endpoint == "" {
			if region == "us-east-1" {
				endpoint = "https://s3.amazonaws.com"
			} else {
				endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
			}
		}
		return destination.Bucket, endpoint, region, destination.AccessKeyID, destination.SecretAccessKey, destination.SessionToken, nil
	case R2ProviderDestination:
		return destination.Bucket, r2Endpoint(destination.EndpointURL, destination.AccountID), "auto", destination.AccessKeyID, destination.SecretAccessKey, "", nil
	default:
		return "", "", "", "", "", "", fmt.Errorf("multipart list-parts is not supported for %T", destination)
	}
}

func presignS3Operation(
	ctx context.Context,
	destination ProviderDestination,
	method string,
	objectKey string,
	operation string,
	expiresIn time.Duration,
	queryValues map[string]string,
	headers map[string]string,
) (string, error) {
	bucket, endpoint, region, accessKeyID, secretAccessKey, sessionToken, err := destinationSigningInfo(destination)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	baseRawPath := strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path = basePath + "/" + bucket + "/" + objectKey
	parsed.RawPath = baseRawPath + "/" + url.PathEscape(bucket) + "/" + escapeS3Key(objectKey)
	query := parsed.Query()
	query.Set("x-id", operation)
	query.Set("X-Amz-Expires", fmt.Sprintf("%.0f", expiresIn.Seconds()))
	for key, value := range queryValues {
		query.Set(key, value)
	}
	parsed.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, parsed.String(), nil)
	if err != nil {
		return "", err
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	signedURL, _, err := v4.NewSigner().PresignHTTP(
		ctx,
		aws.Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
			Source:          "beam-network-sdk",
		},
		request,
		"UNSIGNED-PAYLOAD",
		"s3",
		region,
		time.Now().UTC(),
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	)
	if err != nil {
		return "", err
	}
	return signedURL, nil
}

func listPartsRequestURL(endpoint string, bucket string, objectKey string, uploadID string, expiresIn time.Duration, maxParts int, partNumberMarker int) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	baseRawPath := strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path = basePath + "/" + bucket + "/" + objectKey
	parsed.RawPath = baseRawPath + "/" + url.PathEscape(bucket) + "/" + escapeS3Key(objectKey)
	query := parsed.Query()
	query.Set("uploadId", uploadID)
	query.Set("x-id", "ListParts")
	query.Set("X-Amz-Expires", fmt.Sprintf("%.0f", expiresIn.Seconds()))
	if maxParts > 0 {
		query.Set("max-parts", fmt.Sprintf("%d", maxParts))
	}
	if partNumberMarker > 0 {
		query.Set("part-number-marker", fmt.Sprintf("%d", partNumberMarker))
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func escapeS3Key(key string) string {
	parts := strings.Split(key, "/")
	for index, part := range parts {
		parts[index] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func s3Client(region string, endpoint string, accessKeyID string, secretAccessKey string, sessionToken string) *s3.Client {
	options := s3.Options{
		Region:      defaultString(region, "us-east-1"),
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)),
	}
	if endpoint != "" {
		options.BaseEndpoint = aws.String(endpoint)
	}
	return s3.New(options)
}

func r2Client(endpoint string, accountID string, accessKeyID string, secretAccessKey string) *s3.Client {
	options := s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(r2Endpoint(endpoint, accountID)),
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	}
	return s3.New(options)
}

func r2Endpoint(endpoint string, accountID string) string {
	if endpoint != "" {
		return endpoint
	}
	if accountID != "" {
		return fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
	}
	return ""
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
	response, err := httpClient.Do(request)
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
	response, err := httpClient.Do(request)
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
