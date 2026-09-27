package beamnetworksdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const providerFailureCancellationTimeout = 2 * time.Minute
const providerUploadAbortTimeout = 30 * time.Second
const providerMultipartCreateTimeout = 2 * time.Minute
const maxRouteSigningConcurrency = 256

// DefaultMultipartControlConcurrency bounds concurrent multipart create and
// abort requests independently from route signing.
const DefaultMultipartControlConcurrency = 2

// providerTransferInput is the internal form of a provider transfer request.
type providerTransferInput struct {
	sources           []ProviderSource
	destinations      []ProviderDestination
	name              string
	testMode          bool
	expiresIn         time.Duration
	distribute        bool
	chunkSize         int64
	idempotencyKey    string
	routeGenerationID string

	ownership               context.Context
	onBeforeTransferPrepare func(context.Context) error
	onPrepared              func(context.Context, *TransferPrepareResponse) error
	onMultipartGroupReady   func(context.Context, ProviderMultipartGroupIdentity) error
	throwIfCancelled        func(context.Context, string) error
}

// providerResume re-prepares an existing transfer with retained multipart uploads.
type providerResume struct {
	transferID      string
	multipartGroups []ProviderMultipartGroupIdentity
}

// providerTransferSession is the retained, in-memory state of one provider
// transfer. It serves the initial route stream, route replay after Runtime
// epoch changes, and route recovery signing, and is scrubbed when the recovery
// lease is released.
type providerTransferSession struct {
	sourceHistory            *sourceSignatureHistory
	clients                  *providerClients
	telemetry                *sdkPerformance
	client                   *Client
	prepared                 *TransferPrepareResponse
	expiresIn                time.Duration
	distribute               bool
	ownership                context.Context
	onMultipartGroupReady    func(context.Context, ProviderMultipartGroupIdentity) error
	huggingFaceByDestination map[string]*huggingFaceUploadState

	// uploads holds every multipart upload this owner created or restored; it is
	// the authority for cleanup. recoveryUploads holds the fully signed group
	// state used by route recovery signing.
	uploads         *multipartUploadRegistry
	recoveryUploads *multipartUploadRegistry

	// streamMu serializes the initial route stream with route replay.
	streamMu                sync.Mutex
	initialThrowIfCancelled func(context.Context, string) error

	secretsMu        sync.RWMutex
	sourcesByID      map[string]ProviderSource
	destinationsByID map[string]ProviderDestination

	// lease and signers are this owner's registrations. A fenced owner releases
	// only these, never a replacement owner's registrations for the transfer.
	lease   *recoveryLease
	signers *transferSigners
}

// releaseOwned stops this owner's signers and releases its recovery lease.
func (session *providerTransferSession) releaseOwned() {
	session.client.stopOwnedRecoverySigner(session.prepared.TransferID, session.signers)
	session.client.control.releaseOwnedRecoveryLease(session.lease)
}

func (session *providerTransferSession) source(sourceID string) ProviderSource {
	session.secretsMu.RLock()
	defer session.secretsMu.RUnlock()
	return session.sourcesByID[sourceID]
}

func (session *providerTransferSession) destination(destinationID string) ProviderDestination {
	session.secretsMu.RLock()
	defer session.secretsMu.RUnlock()
	return session.destinationsByID[destinationID]
}

// disposeSecrets releases every retained provider configuration and upload state.
func (session *providerTransferSession) disposeSecrets() {
	if session.clients != nil {
		session.clients.retire()
	}
	session.secretsMu.Lock()
	clear(session.sourcesByID)
	clear(session.destinationsByID)
	session.secretsMu.Unlock()
	session.uploads.clear()
	session.recoveryUploads.clear()
}

// ownershipErr reports whether the caller relinquished ownership of the transfer.
func ownershipErr(ownership context.Context) error {
	if ownership == nil || ownership.Err() == nil {
		return nil
	}
	return context.Cause(ownership)
}

// withOwnership derives a context that is also cancelled when ownership is lost.
func withOwnership(parent context.Context, ownership context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	if ownership == nil {
		return ctx, func() { cancel(context.Canceled) }
	}
	stop := context.AfterFunc(ownership, func() { cancel(context.Cause(ownership)) })
	return ctx, func() {
		stop()
		cancel(context.Canceled)
	}
}

func (client *Client) executeProviderTransfer(ctx context.Context, input providerTransferInput, resume *providerResume) (*TransferPrepareResponse, error) {
	if err := ownershipErr(input.ownership); err != nil {
		return nil, err
	}
	expiresIn := input.expiresIn
	if expiresIn < 0 {
		return nil, fmt.Errorf("expiresIn must be a positive duration")
	}
	if expiresIn == 0 {
		expiresIn = time.Hour
	}
	throwIfCancelled := func(transferID string) error {
		if input.throwIfCancelled == nil {
			return nil
		}
		return input.throwIfCancelled(ctx, transferID)
	}
	clients := newProviderClients()
	retainedClients := false
	defer func() {
		if !retainedClients {
			clients.retire()
		}
	}()
	ctx = withProviderClients(ctx, clients)
	telemetry := newSDKPerformance()
	ctx = context.WithValue(ctx, performanceContextKey{}, telemetry)
	sources := append([]ProviderSource(nil), input.sources...)
	destinations := append([]ProviderDestination(nil), input.destinations...)

	preparedDestinations := make([]PreparedDestination, len(destinations))
	destinationsByID := make(map[string]ProviderDestination, len(destinations))
	for index, destination := range destinations {
		switch destination.(type) {
		case GCSProviderDestination, AzureProviderDestination:
			// Their models exist for configuration, but no signer does; fail before
			// BeamCore creates a transfer that could never receive routes.
			return nil, fmt.Errorf("provider signing is not implemented for %s destinations", providerName(destination))
		}
		prepared, err := prepareProviderDestination(destination, index)
		if err != nil {
			return nil, err
		}
		preparedDestinations[index] = prepared
		destinationsByID[prepared.DestinationID] = destination
	}

	if err := throwIfCancelled(""); err != nil {
		return nil, err
	}
	sourceCtx, stopSourceCtx := withOwnership(ctx, input.ownership)
	discoveryStarted := time.Now()
	preparedSources, err := client.prepareProviderSources(sourceCtx, sources, expiresIn)
	telemetry.observe("sdk.discovery", discoveryStarted)
	stopSourceCtx()
	if err != nil {
		if ownershipLost := ownershipErr(input.ownership); ownershipLost != nil {
			return nil, ownershipLost
		}
		return nil, err
	}
	if err := throwIfCancelled(""); err != nil {
		return nil, err
	}
	huggingFaceStates, huggingFaceChunkSize, err := client.planHuggingFaceUploads(ctx, sources, preparedSources, destinations, preparedDestinations)
	if err != nil {
		return nil, err
	}
	if err := throwIfCancelled(""); err != nil {
		return nil, err
	}
	if input.onBeforeTransferPrepare != nil {
		if err := input.onBeforeTransferPrepare(ctx); err != nil {
			return nil, err
		}
	}
	if err := ownershipErr(input.ownership); err != nil {
		return nil, err
	}

	request := TransferPrepareRequest{
		Sources:      preparedSources,
		Destinations: preparedDestinations,
		Name:         input.name,
		TestMode:     input.testMode,
		ChunkSize:    input.chunkSize,
	}
	if huggingFaceChunkSize > 0 {
		request.ChunkSize = huggingFaceChunkSize
	}
	var prepared *TransferPrepareResponse
	if resume != nil {
		request.TransferID = resume.transferID
		prepared, err = client.prepareTransferWithRequestKey(ctx, request, fmt.Sprintf("transfer:%s:prepare:resume:%s", resume.transferID, newTransferID()))
		if err == nil && prepared.TransferID != resume.transferID {
			err = fmt.Errorf("resumed provider transfer id mismatch")
		}
	} else {
		request.IdempotencyKey = input.idempotencyKey
		request.RouteGenerationID = input.routeGenerationID
		prepared, err = client.prepareTransferWithRequestKey(ctx, request, "")
	}
	if err != nil {
		return nil, err
	}
	if err := ownershipErr(input.ownership); err != nil {
		return nil, err
	}
	if !prepared.Success {
		return prepared, nil
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

	sourcesByID := make(map[string]ProviderSource, len(sources))
	for index, preparedSource := range preparedSources {
		sourcesByID[preparedSource.SourceID] = sources[index]
	}

	session := &providerTransferSession{
		sourceHistory: &sourceSignatureHistory{incomplete: resume != nil},
		clients:       clients, telemetry: telemetry,
		client:                   client,
		prepared:                 prepared,
		expiresIn:                expiresIn,
		distribute:               input.distribute,
		ownership:                input.ownership,
		onMultipartGroupReady:    input.onMultipartGroupReady,
		huggingFaceByDestination: huggingFaceByDestination,
		uploads:                  newMultipartUploadRegistry(),
		recoveryUploads:          newMultipartUploadRegistry(),
		initialThrowIfCancelled:  input.throwIfCancelled,
		sourcesByID:              sourcesByID,
		destinationsByID:         destinationsByID,
	}
	if resume != nil {
		if err := restoreProviderMultipartIdentities(prepared, destinationsByID, resume.multipartGroups, session.uploads); err != nil {
			return nil, err
		}
	}

	session.streamMu.Lock()
	defer session.streamMu.Unlock()

	signers, err := client.startProviderRouteRecoverySigner(session)
	if err != nil {
		return nil, err
	}
	session.signers = signers

	transferID := prepared.TransferID
	var stopOwnershipWatch atomicStop
	lease := &recoveryLease{
		transferID:         transferID,
		planFingerprint:    prepared.PlanFingerprint,
		coordinateChecksum: prepared.CoordinateChecksum,
		ownership:          input.ownership,
		replayRoutes: func(background context.Context, generationID string) error {
			session.streamMu.Lock()
			defer session.streamMu.Unlock()
			return session.streamRoutes(background, generationID, true)
		},
		dispose: func() {
			stopOwnershipWatch.stop()
			session.disposeSecrets()
		},
	}
	session.lease = lease
	client.control.registerRecoveryLease(lease)
	retainedClients = true
	if input.ownership != nil {
		stopOwnershipWatch.set(context.AfterFunc(input.ownership, session.releaseOwned))
	}

	if err := ownershipErr(input.ownership); err != nil {
		// A fenced owner never continues recovery; the replacement owns it.
		session.releaseOwned()
		return nil, err
	}
	if input.onPrepared != nil {
		if err := input.onPrepared(ctx, prepared); err != nil {
			client.control.continueRecoveryLease(lease)
			return nil, err
		}
	}
	if err := throwIfCancelled(transferID); err != nil {
		client.control.continueRecoveryLease(lease)
		return nil, err
	}
	if err := session.streamRoutes(ctx, prepared.RouteGenerationID, false); err != nil {
		return nil, err
	}
	session.initialThrowIfCancelled = nil
	return prepared, nil
}

// prepareProviderSources resolves every source, bounded by the route signing concurrency.
func (client *Client) prepareProviderSources(ctx context.Context, sources []ProviderSource, expiresIn time.Duration) ([]PreparedHTTPSource, error) {
	prepared := make([]PreparedHTTPSource, len(sources))
	errs := make([]error, len(sources))
	semaphore := make(chan struct{}, max(1, client.routeSigningConcurrency))
	var wait sync.WaitGroup
	for index, source := range sources {
		wait.Add(1)
		semaphore <- struct{}{}
		go func(index int, source ProviderSource) {
			defer wait.Done()
			defer func() { <-semaphore }()
			prepared[index], errs[index] = prepareProviderSource(ctx, client.httpClient, source, index, expiresIn)
		}(index, source)
	}
	wait.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

// streamRoutes signs and streams every planned route under one route
// generation. The initial stream (recoveryReplay false) also decides what a
// failure means: foreground cancellation and recoverable transport failures keep
// the recovery lease; anything else cancels the transfer and aborts the
// multipart uploads this owner created.
func (session *providerTransferSession) streamRoutes(baseCtx context.Context, routeGenerationID string, recoveryReplay bool) error {
	baseCtx = withProviderClients(baseCtx, session.clients)
	client := session.client
	prepared := session.prepared
	transferID := prepared.TransferID
	if err := ownershipErr(session.ownership); err != nil {
		return err
	}
	streamCtx, cancelStream := withOwnership(baseCtx, session.ownership)
	defer cancelStream()
	// Multipart creation is not interrupted by route failures: a create the
	// provider accepted must be recorded so cleanup can abort it.
	createCtx, cancelCreate := withOwnership(context.WithoutCancel(baseCtx), session.ownership)
	defer cancelCreate()

	foregroundCancelled := false
	throwIfCancelled := func() error {
		if err := ownershipErr(session.ownership); err != nil {
			foregroundCancelled = true
			return err
		}
		if !recoveryReplay && session.initialThrowIfCancelled != nil {
			if err := session.initialThrowIfCancelled(baseCtx, transferID); err != nil {
				foregroundCancelled = true
				return err
			}
		}
		return nil
	}

	type routeResult struct {
		route SignedChunkRoute
		err   error
	}
	results := make(chan routeResult, max(client.routeSigningConcurrency, maxRouteSigningConcurrency))
	pendingRoutes := 0
	var activeSigning atomic.Int32
	var stream *routeStreamSender
	beginAttempted := false
	var manifest *multipartManifestTask

	streamErr := func() error {
		stream = newRouteStreamSender(
			client.control, transferID,
			prepared.PlanDescriptor.DeliveryRouteCount, prepared.PlanDescriptor.LogicalChunkCount,
			session.distribute, time.Now().UTC().Add(session.expiresIn).Format(time.RFC3339Nano),
			prepared.PlanDescriptor.PlanNonce+":"+routeGenerationID, SignedURLFlowCanonical, routeGenerationID,
		)
		stream.onDiagnostics = client.onDiagnostics
		if !recoveryReplay && session.telemetry != nil {
			stream.telemetry = session.telemetry
		}
		stream.telemetry.sourceHistory = session.sourceHistory
		streamCtx = context.WithValue(streamCtx, performanceContextKey{}, stream.telemetry)
		createCtx = context.WithValue(createCtx, performanceContextKey{}, stream.telemetry)
		stream.telemetry.gauge("signing_configured_limit", float64(client.routeSigningConcurrency))
		stream.telemetry.gauge("multipart_configured_limit", float64(client.multipartControlConcurrency))
		stream.telemetry.gauge("signing_limit", float64(client.routeSigningConcurrency))
		stream.telemetry.gauge("multipart_limit", float64(client.multipartControlConcurrency))
		beginAttempted = true
		if err := stream.begin(streamCtx); err != nil {
			return err
		}
		var err error
		manifest, err = session.startMultipartGroupManifest(streamCtx, createCtx, stream)
		if err != nil {
			return err
		}

		signingConcurrency := client.routeSigningConcurrency
		signedInWindow := 0
		windowStartedAt := time.Now()
		flushCompletedRoute := func() error {
			producerStarted := time.Now()
			result := <-results
			stream.telemetry.observe("sdk.producer_wait", producerStarted)
			pendingRoutes--
			if result.err != nil {
				return result.err
			}
			if err := ownershipErr(session.ownership); err != nil {
				return err
			}
			if err := stream.addRoute(streamCtx, result.route); err != nil {
				return err
			}
			signedInWindow++
			if signedInWindow == routeStreamBatchRoutes {
				if !client.routeSigningConcurrencyOverridden && time.Since(windowStartedAt) > 4*time.Second && signingConcurrency < maxRouteSigningConcurrency {
					signingConcurrency = min(maxRouteSigningConcurrency, signingConcurrency*2)
					stream.telemetry.increment("concurrency_changes")
					stream.telemetry.gauge("signing_limit", float64(signingConcurrency))
				}
				signedInWindow = 0
				windowStartedAt = time.Now()
			}
			return nil
		}

		iterator := newPlanChunkIterator(prepared.PlanDescriptor, transferID)
		for {
			chunk, hasChunk, err := iterator.next()
			if err != nil {
				return err
			}
			if !hasChunk {
				break
			}
			if err := throwIfCancelled(); err != nil {
				return err
			}
			sourceFuture := &sourceChunkFuture{}
			for _, target := range chunk.Destinations {
				pendingRoutes++
				stream.telemetry.gauge("signing_pending_peak", float64(pendingRoutes))
				queuedAt := time.Now()
				go func(chunk ChunkSigningPlanItem, target ChunkDestinationSigningTarget) {
					started := time.Now()
					stream.telemetry.observe("sdk.signing_queue", queuedAt)
					stream.telemetry.gauge("signing_active_peak", float64(activeSigning.Add(1)))
					defer activeSigning.Add(-1)
					grant, err := sourceFuture.get(streamCtx, session, chunk)
					var route SignedChunkRoute
					if err == nil {
						route, err = session.signPlannedRoute(streamCtx, chunk, target, manifest, grant)
					}
					stream.telemetry.observe("sdk.signing", started)
					results <- routeResult{route: route, err: err}
				}(chunk, target)
				if pendingRoutes >= signingConcurrency {
					if err := flushCompletedRoute(); err != nil {
						return err
					}
				}
			}
		}
		for pendingRoutes > 0 {
			if err := flushCompletedRoute(); err != nil {
				return err
			}
		}
		if err := manifest.wait(); err != nil {
			return err
		}
		if err := ownershipErr(session.ownership); err != nil {
			return err
		}
		var attached AttachSignedURLsResponse
		if err := stream.complete(streamCtx, &attached); err != nil {
			return err
		}
		if !attached.Success {
			return fmt.Errorf("route stream failed: %s", firstNonEmpty(attached.Error, attached.Message, "unknown error"))
		}
		return nil
	}()
	if streamErr == nil {
		return nil
	}

	cancelStream()
	for pendingRoutes > 0 {
		<-results
		pendingRoutes--
	}
	if stream != nil {
		_ = stream.waitForPendingSend()
	}
	if manifest != nil {
		_ = manifest.wait()
	}
	if ownershipLost := ownershipErr(session.ownership); ownershipLost != nil {
		// A replaced owner stops signing and releases its lease, but never cancels
		// the transfer, aborts uploads, or releases what the replacement owns.
		session.releaseOwned()
		return ownershipLost
	}
	if recoveryReplay {
		return streamErr
	}
	if foregroundCancelled || baseCtx.Err() != nil {
		client.control.continueRecoveryLease(session.lease)
		return streamErr
	}
	if isRecoverableRouteStreamError(streamErr) {
		client.control.continueRecoveryLease(session.lease)
		return &RouteRecoveryPendingError{TransferID: transferID, Cause: streamErr}
	}
	// The retained recovery input owns the destination credentials used by
	// multipart cleanup, so the lease is released only after cleanup finishes.
	cleanupErr := client.cancelAndAbortProviderFailure(transferID, streamErr, session.uploads, !beginAttempted)
	// Released secrets can no longer sign, so the recovery signer stops too.
	session.releaseOwned()
	return cleanupErr
}

// signPlannedRoute signs one planned route, waiting for its multipart group when needed.
func (session *providerTransferSession) signPlannedRoute(ctx context.Context, chunk ChunkSigningPlanItem, target ChunkDestinationSigningTarget, manifest *multipartManifestTask, grant *sourceChunkGrant) (SignedChunkRoute, error) {
	transferID := session.prepared.TransferID
	destination := session.destination(target.DestinationID)
	if destination == nil {
		return SignedChunkRoute{}, fmt.Errorf("BeamCore returned unknown destination_id: %s", target.DestinationID)
	}
	source := session.source(chunk.SourceID)
	if source == nil {
		return SignedChunkRoute{}, fmt.Errorf("BeamCore returned unknown source_id: %s", chunk.SourceID)
	}
	finalObjectKey := target.ObjectKey
	if value, ok := target.Metadata["final_object_key"].(string); ok {
		finalObjectKey = value
	}
	if finalObjectKey == "" {
		return SignedChunkRoute{}, fmt.Errorf("destination signing target is missing object_key")
	}
	var upload *multipartUploadState
	if !isDirectPutDestination(destination) {
		future := manifest.future(multipartGroupStateKey(transferID, target.DestinationID, chunk.SourceID, finalObjectKey))
		if future == nil {
			return SignedChunkRoute{}, fmt.Errorf("multipart group manifest is missing for %s:%s", chunk.SourceID, target.DestinationID)
		}
		waitDone := performancePhase(ctx, "sdk.multipart_ready_wait")
		select {
		case <-future.done:
		case <-ctx.Done():
			return SignedChunkRoute{}, context.Cause(ctx)
		}
		waitDone()
		if future.err != nil {
			return SignedChunkRoute{}, future.err
		}
		state := future.state
		upload = &state
	}
	partNumber, integer := exactIntegerValue(target.Metadata["part_number"])
	if !integer {
		var err error
		partNumber, err = MultipartPartNumber(chunk.SourceChunkIndex, 0)
		if err != nil {
			return SignedChunkRoute{}, err
		}
	}
	return session.client.signProviderRoute(ctx, providerRouteInput{
		chunk:             chunk,
		sourceGrant:       grant,
		target:            target,
		source:            source,
		destination:       destination,
		expiresIn:         session.expiresIn,
		upload:            upload,
		huggingFaceUpload: session.huggingFaceByDestination[target.DestinationID],
		finalObjectKey:    finalObjectKey,
		partNumber:        partNumber,
	})
}

type multipartGroupFuture struct {
	done  chan struct{}
	state multipartUploadState
	err   error
}

// multipartManifestTask creates, signs, and publishes every multipart group of
// a route stream with bounded concurrency. Each group resolves its own future
// so routes stream as soon as their group is published.
type multipartManifestTask struct {
	futures map[string]*multipartGroupFuture
	done    chan struct{}
	err     error
}

func (task *multipartManifestTask) future(groupID string) *multipartGroupFuture {
	return task.futures[groupID]
}

func (task *multipartManifestTask) wait() error {
	<-task.done
	return task.err
}

type multipartGroupJob struct {
	groupID        string
	source         CompactTransferPlanSource
	destinationID  string
	destination    ProviderDestination
	finalObjectKey string
}

func (session *providerTransferSession) startMultipartGroupManifest(streamCtx context.Context, createCtx context.Context, stream *routeStreamSender) (*multipartManifestTask, error) {
	prepared := session.prepared
	jobs := make([]multipartGroupJob, 0)
	task := &multipartManifestTask{futures: map[string]*multipartGroupFuture{}, done: make(chan struct{})}
	for _, source := range prepared.PlanDescriptor.Sources {
		for _, destinationPlan := range prepared.PlanDescriptor.Destinations {
			destination := session.destination(destinationPlan.DestinationID)
			if destination == nil {
				return nil, fmt.Errorf("BeamCore returned unknown destination_id: %s", destinationPlan.DestinationID)
			}
			if isDirectPutDestination(destination) {
				continue
			}
			finalObjectKey := destinationPlan.FinalObjectKeys[source.SourceID]
			if finalObjectKey == "" {
				return nil, fmt.Errorf("plan final object key not found: %s:%s", source.SourceID, destinationPlan.DestinationID)
			}
			groupID := multipartGroupStateKey(prepared.TransferID, destinationPlan.DestinationID, source.SourceID, finalObjectKey)
			jobs = append(jobs, multipartGroupJob{groupID: groupID, source: source, destinationID: destinationPlan.DestinationID, destination: destination, finalObjectKey: finalObjectKey})
			task.futures[groupID] = &multipartGroupFuture{done: make(chan struct{})}
		}
	}
	if len(jobs) == 0 {
		close(task.done)
		return task, nil
	}
	queue := make(chan multipartGroupJob)
	errs := make([]error, len(jobs))
	indexByGroup := make(map[string]int, len(jobs))
	for index, job := range jobs {
		indexByGroup[job.groupID] = index
	}
	var workers sync.WaitGroup
	var activeMultipart atomic.Int32
	queuedAt := time.Now()
	for worker := 0; worker < min(max(1, session.client.multipartControlConcurrency), len(jobs)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue {
				stream.telemetry.observe("sdk.multipart_queue", queuedAt)
				stream.telemetry.gauge("multipart_active_peak", float64(activeMultipart.Add(1)))
				var state multipartUploadState
				err := context.Cause(streamCtx)
				if streamCtx.Err() == nil {
					state, err = session.setupMultipartGroup(streamCtx, createCtx, stream, job)
				}
				activeMultipart.Add(-1)
				future := task.futures[job.groupID]
				future.state, future.err = state, err
				close(future.done)
				errs[indexByGroup[job.groupID]] = err
			}
		}()
	}
	go func() {
		for _, job := range jobs {
			queue <- job
		}
		close(queue)
		workers.Wait()
		for _, err := range errs {
			if err != nil {
				task.err = err
				break
			}
		}
		close(task.done)
	}()
	return task, nil
}

// setupMultipartGroup creates (or reuses) one multipart upload, signs its group
// controls, reports its identity, and publishes its manifest. A failure after
// creating the upload aborts it, unless ownership was relinquished.
func (session *providerTransferSession) setupMultipartGroup(streamCtx context.Context, createCtx context.Context, stream *routeStreamSender, job multipartGroupJob) (multipartUploadState, error) {
	transferID := session.prepared.TransferID
	if err := ownershipErr(session.ownership); err != nil {
		return multipartUploadState{}, err
	}
	finalObjectMetadata := map[string]string{"beam-transfer-id": transferID}
	retained, hasRetained := session.uploads.get(job.groupID)
	uploadID := retained.UploadID
	createdUpload := !hasRetained
	if createdUpload {
		ctx, cancel := context.WithTimeout(createCtx, providerMultipartCreateTimeout)
		var err error
		started := time.Now()
		uploadID, err = createMultipartUpload(ctx, job.destination, job.finalObjectKey, finalObjectMetadata)
		stream.telemetry.observe("sdk.multipart_create", started)
		stream.telemetry.observe("sdk.multipart_provider", started)
		cancel()
		if err != nil {
			return multipartUploadState{}, err
		}
		session.uploads.set(job.groupID, multipartUploadState{Destination: job.destination, ObjectKey: job.finalObjectKey, UploadID: uploadID})
	}
	state, err := signMultipartGroupState(streamCtx, transferID, job.groupID, job.source, job.destinationID, job.destination, job.finalObjectKey, uploadID, session.expiresIn)
	if err == nil {
		session.uploads.set(job.groupID, state)
		err = session.publishMultipartGroup(streamCtx, stream, state)
	}
	if err != nil {
		if createdUpload && ownershipErr(session.ownership) == nil {
			if abortErr := abortMultipartUploadForCleanup(job.destination, job.finalObjectKey, uploadID); abortErr != nil {
				return multipartUploadState{}, fmt.Errorf("multipart group setup and cleanup failed for %s: %w", job.groupID, errors.Join(err, abortErr))
			}
			session.uploads.delete(job.groupID)
		}
		return multipartUploadState{}, err
	}
	return state, nil
}

func (session *providerTransferSession) publishMultipartGroup(ctx context.Context, stream *routeStreamSender, state multipartUploadState) error {
	if err := ownershipErr(session.ownership); err != nil {
		return err
	}
	session.recoveryUploads.set(state.Manifest.MultipartGroupID, state)
	if err := validateSignedRouteManifestContract(session.prepared.TransferID, nil, []MultipartGroupManifest{state.Manifest}); err != nil {
		return err
	}
	if session.onMultipartGroupReady != nil {
		started := time.Now()
		err := session.onMultipartGroupReady(ctx, multipartGroupIdentity(session.prepared.TransferID, state.Manifest))
		stream.telemetry.observe("sdk.multipart_callback", started)
		if err != nil {
			return err
		}
	}
	return stream.addManifestGroups(ctx, []MultipartGroupManifest{state.Manifest})
}

// signMultipartGroupState signs the complete, abort, list-page, and final HEAD
// controls of one multipart group around an existing upload id.
func signMultipartGroupState(
	ctx context.Context,
	transferID string,
	groupID string,
	source CompactTransferPlanSource,
	destinationID string,
	destination ProviderDestination,
	finalObjectKey string,
	uploadID string,
	expiresIn time.Duration,
) (multipartUploadState, error) {
	if source.ChunkCount < 1 {
		return multipartUploadState{}, fmt.Errorf("source chunk_count must be a positive integer")
	}
	maxPartNumber, err := MultipartPartNumber(source.ChunkCount-1, 0)
	if err != nil {
		return multipartUploadState{}, err
	}
	urlsExpiresAt := time.Now().UTC().Add(expiresIn).Format(time.RFC3339Nano)
	listPageURLs := make([]string, 0, (maxPartNumber+999)/1000)
	for marker := 0; marker < maxPartNumber; marker += 1000 {
		listURL, err := signListMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn, 1000, marker)
		if err != nil {
			return multipartUploadState{}, err
		}
		listPageURLs = append(listPageURLs, listURL)
	}
	completeURL, err := signCompleteMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn)
	if err != nil {
		return multipartUploadState{}, err
	}
	abortURL, err := signAbortMultipartUpload(ctx, destination, finalObjectKey, uploadID, expiresIn)
	if err != nil {
		return multipartUploadState{}, err
	}
	finalHeadURL, err := signFinalObjectHead(ctx, destination, finalObjectKey, expiresIn)
	if err != nil {
		return multipartUploadState{}, err
	}
	manifest := MultipartGroupManifest{
		MultipartGroupID: groupID, SourceID: source.SourceID, DestinationID: destinationID,
		FinalObjectKey: finalObjectKey, UploadID: uploadID, ExpectedObjectSize: source.Size,
		ExpectedPartCount: source.ChunkCount, MaxPartNumber: maxPartNumber, CompleteURL: completeURL,
		AbortURL: abortURL, ListPageURLs: listPageURLs, FinalHeadURL: finalHeadURL,
		FinalObjectMetadata: map[string]string{"beam-transfer-id": transferID},
		URLsExpiresAt:       urlsExpiresAt,
	}
	return multipartUploadState{Destination: destination, ObjectKey: finalObjectKey, UploadID: uploadID, Manifest: manifest}, nil
}

func multipartGroupStateKey(transferID string, destinationID string, sourceID string, finalObjectKey string) string {
	return fmt.Sprintf("%s:%s:%s:%s", transferID, destinationID, sourceID, finalObjectKey)
}

func multipartGroupIdentity(transferID string, manifest MultipartGroupManifest) ProviderMultipartGroupIdentity {
	return ProviderMultipartGroupIdentity{
		TransferID:         transferID,
		MultipartGroupID:   manifest.MultipartGroupID,
		SourceID:           manifest.SourceID,
		DestinationID:      manifest.DestinationID,
		ObjectKey:          manifest.FinalObjectKey,
		UploadID:           manifest.UploadID,
		ExpectedObjectSize: manifest.ExpectedObjectSize,
		ExpectedPartCount:  manifest.ExpectedPartCount,
		ExpiresAt:          manifest.URLsExpiresAt,
	}
}

// restoreProviderMultipartIdentities validates that the saved identities cover
// exactly the plan's multipart groups and seeds them as retained uploads.
func restoreProviderMultipartIdentities(
	prepared *TransferPrepareResponse,
	destinations map[string]ProviderDestination,
	identities []ProviderMultipartGroupIdentity,
	uploads *multipartUploadRegistry,
) error {
	type expectedGroup struct {
		destination   ProviderDestination
		objectKey     string
		source        CompactTransferPlanSource
		destinationID string
	}
	expected := map[string]expectedGroup{}
	for _, source := range prepared.PlanDescriptor.Sources {
		for _, target := range prepared.PlanDescriptor.Destinations {
			destination := destinations[target.DestinationID]
			if destination == nil {
				return fmt.Errorf("provider_multipart_recovery_destination_invalid")
			}
			if isDirectPutDestination(destination) {
				continue
			}
			objectKey := target.FinalObjectKeys[source.SourceID]
			if objectKey == "" {
				return fmt.Errorf("provider_multipart_recovery_object_invalid")
			}
			expected[multipartGroupStateKey(prepared.TransferID, target.DestinationID, source.SourceID, objectKey)] = expectedGroup{
				destination: destination, objectKey: objectKey, source: source, destinationID: target.DestinationID,
			}
		}
	}
	if len(expected) != len(identities) {
		return fmt.Errorf("provider_multipart_recovery_incomplete")
	}
	for _, identity := range identities {
		group, known := expected[identity.MultipartGroupID]
		_, duplicate := uploads.get(identity.MultipartGroupID)
		if !known || duplicate || identity.TransferID != prepared.TransferID ||
			identity.SourceID != group.source.SourceID || identity.DestinationID != group.destinationID ||
			identity.ObjectKey != group.objectKey || identity.ExpectedObjectSize != group.source.Size ||
			identity.ExpectedPartCount != group.source.ChunkCount || trimmedEmpty(identity.UploadID) {
			return fmt.Errorf("provider_multipart_recovery_identity_invalid")
		}
		uploads.set(identity.MultipartGroupID, multipartUploadState{Destination: group.destination, ObjectKey: group.objectKey, UploadID: identity.UploadID})
	}
	return nil
}

// multipartUploadRegistry is a concurrency-safe map of multipart group state.
type multipartUploadRegistry struct {
	mu     sync.Mutex
	states map[string]multipartUploadState
}

func newMultipartUploadRegistry() *multipartUploadRegistry {
	return &multipartUploadRegistry{states: map[string]multipartUploadState{}}
}

func (registry *multipartUploadRegistry) get(groupID string) (multipartUploadState, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state, ok := registry.states[groupID]
	return state, ok
}

func (registry *multipartUploadRegistry) set(groupID string, state multipartUploadState) {
	registry.mu.Lock()
	registry.states[groupID] = state
	registry.mu.Unlock()
}

func (registry *multipartUploadRegistry) delete(groupID string) {
	registry.mu.Lock()
	delete(registry.states, groupID)
	registry.mu.Unlock()
}

func (registry *multipartUploadRegistry) snapshot() map[string]multipartUploadState {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	out := make(map[string]multipartUploadState, len(registry.states))
	for key, value := range registry.states {
		out[key] = value
	}
	return out
}

// clone returns an independent registry holding the current states, including
// the destination credentials each one needs for abort.
func (registry *multipartUploadRegistry) clone() *multipartUploadRegistry {
	return &multipartUploadRegistry{states: registry.snapshot()}
}

func (registry *multipartUploadRegistry) clear() {
	registry.mu.Lock()
	clear(registry.states)
	registry.mu.Unlock()
}

// cancelAndAbortProviderFailure cancels a transfer after a non-recoverable
// provider failure and aborts the multipart uploads this owner created or
// restored. When the
// route stream never began, uploads are aborted first; a failed abort is retried
// after cancellation.
func (client *Client) cancelAndAbortProviderFailure(transferID string, cause error, retained *multipartUploadRegistry, abortBeforeCancel bool) error {
	// Cleanup owns a snapshot taken before cancellation: once Beam reports the
	// transfer cancelled, a concurrent TransferStatus or recovery loop may
	// release the lease, and its dispose clears the retained registry.
	uploads := retained.clone()
	var cleanupErr error
	if abortBeforeCancel {
		cleanupErr = abortCreatedUploads(uploads, client.multipartControlConcurrency)
	}
	cancelErr := client.cancelTransferAfterProviderFailure(transferID, cause)
	if !abortBeforeCancel || cleanupErr != nil {
		cleanupErr = abortCreatedUploads(uploads, client.multipartControlConcurrency)
	}
	return newProviderTransferError(transferID, cause, cancelErr, cleanupErr)
}

func (client *Client) cancelTransferAfterProviderFailure(transferID string, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), providerFailureCancellationTimeout)
	defer cancel()
	result, err := client.requestTransferCancellation(ctx, transferID)
	if err == nil && (result == nil || !result.Success) {
		message := fmt.Sprintf("Beam rejected cancellation for %s", transferID)
		if result != nil && result.Message != "" {
			message = result.Message
		}
		err = errors.New(message)
	}
	if err != nil {
		return fmt.Errorf("provider transfer failed (%s) and transfer cancellation failed (%s)", safeErrorCode(cause), safeErrorCode(err))
	}
	return nil
}

// abortCreatedUploads aborts every retained multipart upload and removes the
// ones that were aborted, so a retry only touches the remainder.
func abortCreatedUploads(uploads *multipartUploadRegistry, concurrency int) error {
	type candidate struct {
		groupID string
		upload  multipartUploadState
	}
	candidates := make([]candidate, 0)
	for groupID, upload := range uploads.snapshot() {
		if upload.UploadID == "" {
			continue
		}
		if _, hippius := upload.Destination.(HippiusProviderDestination); hippius {
			continue
		}
		candidates = append(candidates, candidate{groupID: groupID, upload: upload})
	}
	if len(candidates) == 0 {
		return nil
	}
	errs := make([]error, len(candidates))
	semaphore := make(chan struct{}, min(max(1, concurrency), len(candidates)))
	var wait sync.WaitGroup
	for index, item := range candidates {
		wait.Add(1)
		semaphore <- struct{}{}
		go func(index int, item candidate) {
			defer wait.Done()
			defer func() { <-semaphore }()
			if err := abortMultipartUploadForCleanup(item.upload.Destination, item.upload.ObjectKey, item.upload.UploadID); err != nil {
				errs[index] = err
				return
			}
			uploads.delete(item.groupID)
		}(index, item)
	}
	wait.Wait()
	failures := make([]error, 0)
	for _, err := range errs {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return &multipartCleanupError{errs: failures}
}

// multipartCleanupError reports multipart uploads that could not be aborted.
type multipartCleanupError struct {
	errs []error
}

func (err *multipartCleanupError) Error() string {
	return fmt.Sprintf("failed to abort %d multipart upload(s)", len(err.errs))
}

func (err *multipartCleanupError) Unwrap() []error {
	return err.errs
}

func abortMultipartUploadForCleanup(destination ProviderDestination, objectKey string, uploadID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), providerUploadAbortTimeout)
	defer cancel()
	return abortMultipartUpload(ctx, destination, objectKey, uploadID)
}

// atomicStop holds a context.AfterFunc stop function that is installed after
// the callback that may invoke it has been created.
type atomicStop struct {
	mu sync.Mutex
	fn func() bool
}

func (holder *atomicStop) set(fn func() bool) {
	holder.mu.Lock()
	holder.fn = fn
	holder.mu.Unlock()
}

func (holder *atomicStop) stop() {
	holder.mu.Lock()
	fn := holder.fn
	holder.mu.Unlock()
	if fn != nil {
		fn()
	}
}
