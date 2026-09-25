package beamnetworksdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DevURL  = "nats://127.0.0.1:4222"
	ProdURL = "tls://orch-gateway.b1m.ai:4222"
)

type Client struct {
	natsURL                           string
	environment                       string
	apiKey                            string
	httpClient                        *http.Client
	control                           transferControl
	routeSigningConcurrency           int
	routeSigningConcurrencyOverridden bool
	maxPayloadBytes                   int

	huggingFaceMu      sync.Mutex
	huggingFaceUploads map[string][]*huggingFaceUploadState
}

// RouteRecoveryPendingError means a prepared transfer still has an active,
// process-local route recovery lease after a recoverable streaming failure.
type RouteRecoveryPendingError struct {
	TransferID string
	Cause      error
}

func (error *RouteRecoveryPendingError) Error() string {
	return fmt.Sprintf("transfer %s is prepared and route recovery is continuing in the background: %v", error.TransferID, error.Cause)
}

func (error *RouteRecoveryPendingError) Unwrap() error {
	return error.Cause
}

type Option func(*Client)

type transferControl interface {
	request(ctx context.Context, messageType string, payload map[string]any, transferID string, output any, idempotencyKey ...string) error
	splitRoutes(messageType string, basePayload map[string]any, routes []SignedChunkRoute) ([][]SignedChunkRoute, error)
	openTerminalSignalWaiter(ctx context.Context, transferID string) (*TransferTerminalSignalWaiter, error)
	registerRecoveryLease(lease *recoveryLease)
	releaseRecoveryLease(transferID string)
	continueRecoveryLease(transferID string)
	close()
}

func NewClient(options ...Option) *Client {
	client := &Client{
		natsURL:                 os.Getenv("BEAM_NATS_URL"),
		environment:             firstNonEmpty(os.Getenv("BEAM_ENV"), "prod"),
		apiKey:                  os.Getenv("BEAM_API_KEY"),
		httpClient:              http.DefaultClient,
		routeSigningConcurrency: 64,
		maxPayloadBytes:         defaultMaxPayloadBytes,
	}

	for _, option := range options {
		option(client)
	}

	if strings.TrimSpace(client.natsURL) == "" {
		if client.environment == "prod" {
			client.natsURL = ProdURL
		} else {
			client.natsURL = DevURL
		}
	}
	client.natsURL = strings.TrimRight(client.natsURL, "/")
	control := newNatsControl(client.apiKey, client.natsURL, client.environment, parseEnvInt("TRANSFER_RUNTIME_SHARD_COUNT", 1))
	control.maxPayloadBytes = client.maxPayloadBytes
	client.control = control
	return client
}

func WithNATSURL(natsURL string) Option {
	return func(client *Client) {
		client.natsURL = natsURL
	}
}

func WithEnvironment(environment string) Option {
	return func(client *Client) {
		client.environment = environment
		if environment == "prod" {
			client.natsURL = ProdURL
			return
		}
		client.natsURL = DevURL
	}
}

func WithAPIKey(apiKey string) Option {
	return func(client *Client) {
		client.apiKey = apiKey
	}
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		if httpClient != nil {
			client.httpClient = httpClient
		}
	}
}

func WithRouteSigningConcurrency(concurrency int) Option {
	return func(client *Client) {
		if concurrency > 0 {
			client.routeSigningConcurrency = concurrency
			client.routeSigningConcurrencyOverridden = true
		}
	}
}

func WithMaxPayloadBytes(maxPayloadBytes int) Option {
	return func(client *Client) {
		if maxPayloadBytes > 0 {
			client.maxPayloadBytes = maxPayloadBytes
		}
	}
}

func (client *Client) Close() {
	if client.control != nil {
		client.control.close()
	}
}

func CalculateOptimalChunkSize(totalSize int64) int64 {
	const mb = 1024 * 1024
	const gb = 1024 * mb
	const minChunk = 5 * mb
	const maxChunk = 5 * mb
	const targetChunks = 10

	if totalSize <= minChunk {
		return totalSize
	}
	if totalSize < gb {
		ideal := totalSize / targetChunks
		if ideal < minChunk {
			return minChunk
		}
		if ideal > maxChunk {
			return maxChunk
		}
		return ideal
	}
	return maxChunk
}

func (client *Client) CreateTransfer(ctx context.Context, input TransferCreateRequest) (*TransferCreateResponse, error) {
	if input.ChunkSize == 0 {
		input.ChunkSize = CalculateOptimalChunkSize(input.TotalSize)
	}
	var result TransferCreateResponse
	if input.TransferID == "" {
		input.TransferID = transferIDForIdempotencyKey(input.IdempotencyKey)
	}
	if input.SignedURLFlow == "" {
		input.SignedURLFlow = SignedURLFlowCanonical
	}
	if err := client.control.request(ctx, "transfer.create", structToMap(input), input.TransferID, &result, firstNonEmpty(input.IdempotencyKey, "transfer:"+input.TransferID+":create")); err != nil {
		return nil, err
	}
	return &result, nil
}

func (client *Client) TransferStatus(ctx context.Context, transferID string) (*TransferStatusInfo, error) {
	if err := validateID(transferID, "transferID"); err != nil {
		return nil, err
	}
	var result TransferStatusInfo
	if err := client.control.request(ctx, "transfer.status", map[string]any{"transfer_id": transferID}, transferID, &result); err != nil {
		return nil, err
	}
	if result.Status == "completed" || result.Status == "failed" || result.Status == "cancelled" {
		client.control.releaseRecoveryLease(transferID)
	}
	return &result, nil
}

// OpenTransferTerminalWaiter subscribes to the authenticated, transfer-owned
// terminal signal before callers perform their first authoritative status read.
func (client *Client) OpenTransferTerminalWaiter(ctx context.Context, transferID string) (*TransferTerminalSignalWaiter, error) {
	if err := validateID(transferID, "transferID"); err != nil {
		return nil, err
	}
	return client.control.openTerminalSignalWaiter(ctx, transferID)
}

func (client *Client) DistributeTransfer(ctx context.Context, transferID string) (*DistributeResponse, error) {
	if err := validateID(transferID, "transferID"); err != nil {
		return nil, err
	}
	var result DistributeResponse
	if err := client.control.request(ctx, "transfer.distribute", map[string]any{"transfer_id": transferID}, transferID, &result, "transfer:"+transferID+":distribute"); err != nil {
		return nil, err
	}
	return &result, nil
}

func (client *Client) CancelTransfer(ctx context.Context, transferID string) (*TransferCancelResponse, error) {
	if err := validateID(transferID, "transferID"); err != nil {
		return nil, err
	}
	var result TransferCancelResponse
	if err := client.control.request(ctx, "transfer.cancel", map[string]any{"transfer_id": transferID}, transferID, &result, "transfer:"+transferID+":cancel"); err != nil {
		return nil, err
	}
	if result.Success {
		client.control.releaseRecoveryLease(transferID)
	}
	return &result, nil
}
func (client *Client) PrepareTransfer(
	ctx context.Context,
	sources []PreparedHTTPSource,
	destinations []PreparedDestination,
	name string,
	testMode bool,
	urlsExpiresAt string,
	routeGenerationID string,
	idempotencyKey ...string,
) (*TransferPrepareResponse, error) {
	return client.prepareTransferWithChunkSize(ctx, sources, destinations, name, testMode, urlsExpiresAt, routeGenerationID, 0, idempotencyKey...)
}

// prepareTransferWithChunkSize requests a specific plan chunk size. BeamCore may raise it, so
// callers that depend on the exact value must check the response.
func (client *Client) prepareTransferWithChunkSize(
	ctx context.Context,
	sources []PreparedHTTPSource,
	destinations []PreparedDestination,
	name string,
	testMode bool,
	urlsExpiresAt string,
	routeGenerationID string,
	chunkSize int64,
	idempotencyKey ...string,
) (*TransferPrepareResponse, error) {
	key := ""
	if len(idempotencyKey) > 0 {
		key = idempotencyKey[0]
	}
	transferID := transferIDForIdempotencyKey(key)
	prepareIdempotencyKey := firstNonEmpty(key, "transfer:"+transferID+":prepare")
	body := map[string]any{
		"sources":      sources,
		"destinations": destinations,
		"route_generation_id": firstNonEmpty(
			routeGenerationID,
			stableIDFromIdentity("beam-route-generation:"+prepareIdempotencyKey),
		),
		"transfer_id": transferID,
	}
	if name != "" {
		body["name"] = name
	}
	if testMode {
		body["test_mode"] = true
	}
	if urlsExpiresAt != "" {
		body["urls_expires_at"] = urlsExpiresAt
	}
	if chunkSize > 0 {
		body["chunk_size"] = chunkSize
	}
	body["signed_url_flow"] = SignedURLFlowCanonical

	var result TransferPrepareResponse
	if err := client.control.request(ctx, "transfer.prepare", body, transferID, &result, prepareIdempotencyKey); err != nil {
		return nil, err
	}
	if result.Success {
		if err := validateCompactTransferPlan(result.SignedURLFlow, result.PlanDescriptor); err != nil {
			return nil, err
		}
	}
	return &result, nil
}

func (client *Client) AttachSignedURLs(
	ctx context.Context,
	transferID string,
	chunkRoutes []SignedChunkRoute,
	multipartGroupManifest []MultipartGroupManifest,
	transferKey string,
	urlsExpiresAt string,
	routeGenerationID string,
	recovery ManualRouteRecovery,
) (*AttachSignedURLsResponse, error) {
	_ = transferKey
	if err := validateID(transferID, "transferID"); err != nil {
		return nil, err
	}
	if routeGenerationID == "" || recovery.PlanFingerprint == "" || recovery.CoordinateChecksum == "" || recovery.Regenerate == nil {
		return nil, fmt.Errorf("route generation and recovery factory are required by transfer-client-control/v6")
	}
	streamRoutes := func(streamCtx context.Context, routes []SignedChunkRoute, manifests []MultipartGroupManifest, expiresAt string, generationID string) (*AttachSignedURLsResponse, error) {
		if err := validateSignedRouteManifestContract(transferID, routes, manifests); err != nil {
			return nil, err
		}
		orderedRoutes := append([]SignedChunkRoute(nil), routes...)
		destinationIDs := make(map[string]struct{})
		for _, route := range orderedRoutes {
			destinationIDs[route.DestinationID] = struct{}{}
		}
		for index, route := range orderedRoutes {
			if _, present := signedRouteDeliveryIndex(route); present {
				continue
			}
			if len(destinationIDs) != 1 {
				return nil, fmt.Errorf("delivery_index is required when manually attaching routes for multiple destinations")
			}
			if orderedRoutes[index].Metadata == nil {
				orderedRoutes[index].Metadata = make(map[string]any)
			}
			orderedRoutes[index].Metadata["delivery_index"] = route.ChunkIndex
		}
		sort.SliceStable(orderedRoutes, func(left, right int) bool {
			leftIndex, leftHasIndex := signedRouteDeliveryIndex(orderedRoutes[left])
			rightIndex, rightHasIndex := signedRouteDeliveryIndex(orderedRoutes[right])
			if leftHasIndex && rightHasIndex && leftIndex != rightIndex {
				return leftIndex < rightIndex
			}
			return routeKeyForSignedRoute(orderedRoutes[left]) < routeKeyForSignedRoute(orderedRoutes[right])
		})
		stream := newRouteStreamSender(
			client.control,
			transferID,
			len(orderedRoutes),
			countDistinctRouteChunks(orderedRoutes),
			true,
			expiresAt,
			checksumForRouteCoordinates(orderedRoutes)+":"+generationID,
			SignedURLFlowCanonical,
			generationID,
		)
		if err := stream.begin(streamCtx); err != nil {
			return nil, err
		}
		if err := stream.addManifestGroups(streamCtx, manifests); err != nil {
			return nil, err
		}
		for _, route := range orderedRoutes {
			if err := stream.addRoute(streamCtx, route); err != nil {
				return nil, err
			}
		}
		var result AttachSignedURLsResponse
		if err := stream.complete(streamCtx, &result); err != nil {
			return nil, err
		}
		return &result, nil
	}

	streamMu := &sync.Mutex{}
	streamMu.Lock()
	client.control.registerRecoveryLease(&recoveryLease{
		transferID:         transferID,
		planFingerprint:    recovery.PlanFingerprint,
		coordinateChecksum: recovery.CoordinateChecksum,
		replayRoutes: func(background context.Context, generationID string) error {
			streamMu.Lock()
			defer streamMu.Unlock()
			routes, manifests, expiresAt, err := recovery.Regenerate(background, generationID)
			if err != nil {
				return err
			}
			result, err := streamRoutes(background, routes, manifests, expiresAt, generationID)
			if err == nil && !result.Success {
				return fmt.Errorf("route stream failed: %s%s", result.Error, result.Message)
			}
			return err
		},
	})
	result, err := streamRoutes(ctx, chunkRoutes, multipartGroupManifest, urlsExpiresAt, routeGenerationID)
	streamMu.Unlock()
	if err != nil {
		if ctx.Err() != nil {
			client.control.continueRecoveryLease(transferID)
			return nil, err
		}
		if isRecoverableRouteStreamError(err) {
			client.control.continueRecoveryLease(transferID)
			return nil, &RouteRecoveryPendingError{TransferID: transferID, Cause: err}
		}
		client.control.releaseRecoveryLease(transferID)
		return nil, err
	}
	if !result.Success {
		client.control.releaseRecoveryLease(transferID)
	}
	return result, nil
}

const routeStreamBatchRoutes = 2048

type routeStreamSender struct {
	control             transferControl
	transferID          string
	streamID            string
	totalRoutes         int
	totalChunks         int
	autoDistribute      bool
	urlsExpiresAt       string
	signedURLFlow       SignedURLFlow
	routeGenerationID   string
	checksum            routeKeysChecksum
	batch               []SignedChunkRoute
	seenDeliveryIndices map[int]struct{}
	batchIndex          int
	routeCount          int
	sendTail            <-chan error
}

func newRouteStreamSender(control transferControl, transferID string, totalRoutes int, totalChunks int, autoDistribute bool, urlsExpiresAt string, planIdentity string, signedURLFlow SignedURLFlow, routeGenerationID string) *routeStreamSender {
	if signedURLFlow == "" {
		signedURLFlow = SignedURLFlowCanonical
	}
	return &routeStreamSender{
		control:             control,
		transferID:          transferID,
		streamID:            stableRouteStreamID(transferID, planIdentity, totalRoutes, totalChunks, signedURLFlow),
		totalRoutes:         totalRoutes,
		totalChunks:         totalChunks,
		autoDistribute:      autoDistribute,
		urlsExpiresAt:       urlsExpiresAt,
		signedURLFlow:       signedURLFlow,
		routeGenerationID:   routeGenerationID,
		batch:               make([]SignedChunkRoute, 0, routeStreamBatchRoutes),
		seenDeliveryIndices: make(map[int]struct{}),
	}
}

func (s *routeStreamSender) begin(ctx context.Context) error {
	payload := map[string]any{
		"transfer_id":            s.transferID,
		"stream_id":              s.streamID,
		"route_generation_id":    s.routeGenerationID,
		"total_routes":           s.totalRoutes,
		"total_chunks":           s.totalChunks,
		"route_contract_version": s.signedURLFlow,
		"signed_url_flow":        s.signedURLFlow,
		"auto_distribute":        s.autoDistribute,
	}
	if s.urlsExpiresAt != "" {
		payload["urls_expires_at"] = s.urlsExpiresAt
	}
	return s.control.request(ctx, "transfer.route_stream.begin", payload, s.transferID, nil, fmt.Sprintf("transfer:%s:route-stream:%s:begin", s.transferID, s.streamID))
}

func (s *routeStreamSender) addRoute(ctx context.Context, route SignedChunkRoute) error {
	deliveryIndex, present := signedRouteDeliveryIndex(route)
	if !present {
		return fmt.Errorf("route delivery_index is required")
	}
	if deliveryIndex < 0 || deliveryIndex >= s.totalRoutes {
		return fmt.Errorf("route delivery_index %d is outside the declared route stream", deliveryIndex)
	}
	if _, duplicate := s.seenDeliveryIndices[deliveryIndex]; duplicate {
		return fmt.Errorf("duplicate route delivery_index %d", deliveryIndex)
	}
	s.seenDeliveryIndices[deliveryIndex] = struct{}{}
	s.routeCount++
	s.checksum.add(routeKeyForSignedRoute(route))
	s.batch = append(s.batch, route)
	if len(s.batch) >= routeStreamBatchRoutes {
		if err := s.enqueueFlush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *routeStreamSender) addManifestGroups(ctx context.Context, groups []MultipartGroupManifest) error {
	if len(groups) == 0 {
		return nil
	}
	identity := stableManifestBatchIdentity(groups)
	return s.control.request(ctx, "transfer.route_stream.manifest", map[string]any{
		"transfer_id":         s.transferID,
		"stream_id":           s.streamID,
		"route_generation_id": s.routeGenerationID,
		"manifest_batch_id":   identity,
		"groups":              groups,
	}, s.transferID, nil, fmt.Sprintf("transfer:%s:route-stream:%s:manifest:%s", s.transferID, s.streamID, identity))
}

func (s *routeStreamSender) complete(ctx context.Context, output any) error {
	if s.routeCount != s.totalRoutes || len(s.seenDeliveryIndices) != s.totalRoutes {
		return fmt.Errorf("route stream has incomplete or duplicate delivery indices: received %d of %d routes", s.routeCount, s.totalRoutes)
	}
	if err := s.enqueueFlush(ctx); err != nil {
		return err
	}
	if err := s.waitForPendingSend(); err != nil {
		return err
	}
	return s.control.request(ctx, "transfer.route_stream.complete", map[string]any{
		"transfer_id":         s.transferID,
		"stream_id":           s.streamID,
		"route_generation_id": s.routeGenerationID,
		"expected_batches":    s.batchIndex,
		"expected_routes":     s.totalRoutes,
		"route_keys_checksum": s.checksum.value(),
	}, s.transferID, output, fmt.Sprintf("transfer:%s:route-stream:%s:complete", s.transferID, s.streamID))
}

func (s *routeStreamSender) waitForPendingSend() error {
	if s.sendTail == nil {
		return nil
	}
	pending := s.sendTail
	s.sendTail = nil
	return <-pending
}

func (s *routeStreamSender) enqueueFlush(ctx context.Context) error {
	if len(s.batch) == 0 {
		return nil
	}
	if err := s.waitForPendingSend(); err != nil {
		return err
	}
	routes := append([]SignedChunkRoute{}, s.batch...)
	s.batch = s.batch[:0]
	chunks, err := s.control.splitRoutes("transfer.route_stream.batch", map[string]any{"transfer_id": s.transferID, "stream_id": s.streamID, "route_generation_id": s.routeGenerationID, "batch_id": s.streamID + ":estimate", "batch_index": s.batchIndex}, routes)
	if err != nil {
		return err
	}
	type scheduledChunk struct {
		batchIndex int
		routes     []SignedChunkRoute
	}
	scheduled := make([]scheduledChunk, 0, len(chunks))
	for _, chunk := range chunks {
		batchIndex := s.batchIndex
		s.batchIndex++
		scheduled = append(scheduled, scheduledChunk{batchIndex: batchIndex, routes: chunk})
	}
	done := make(chan error, 1)
	s.sendTail = done
	go func() {
		for _, item := range scheduled {
			routeChecksum := checksumForRoutes(item.routes)
			coordinateChecksum := checksumForRouteCoordinates(item.routes)
			batchID := fmt.Sprintf("%s:%d:%s", s.streamID, item.batchIndex, coordinateChecksum)
			if err := s.control.request(ctx, "transfer.route_stream.batch", map[string]any{
				"transfer_id":         s.transferID,
				"stream_id":           s.streamID,
				"route_generation_id": s.routeGenerationID,
				"batch_id":            batchID,
				"batch_index":         item.batchIndex,
				"route_batch":         compactSignedRoutes(item.routes),
				"route_count":         len(item.routes),
				"route_keys_checksum": routeChecksum,
			}, s.transferID, nil, fmt.Sprintf("transfer:%s:route-stream:%s:batch:%d:%s", s.transferID, s.streamID, item.batchIndex, coordinateChecksum)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return nil
}

func stableRouteStreamID(transferID string, planIdentity string, totalRoutes int, totalChunks int, signedURLFlow SignedURLFlow) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("beam:route-stream:%s:%s:%s:%d:%d", signedURLFlow, transferID, planIdentity, totalRoutes, totalChunks)))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}

func signedRouteDeliveryIndex(route SignedChunkRoute) (int, bool) {
	if value, present := route.Metadata["delivery_index"]; present {
		return intValue(value), true
	}
	if route.DeliveryIndex > 0 {
		return route.DeliveryIndex, true
	}
	return 0, false
}

type routeKeysChecksum struct {
	bytes [32]byte
	count int
}

func (c *routeKeysChecksum) add(routeKey string) {
	digest := sha256.Sum256([]byte(routeKey))
	for index := range c.bytes {
		c.bytes[index] ^= digest[index]
	}
	c.count++
}

func (c *routeKeysChecksum) value() string {
	return fmt.Sprintf("sha256-xor-v1:%d:%s", c.count, hex.EncodeToString(c.bytes[:]))
}

func checksumForRoutes(routes []SignedChunkRoute) string {
	checksum := routeKeysChecksum{}
	for _, route := range routes {
		checksum.add(routeKeyForSignedRoute(route))
	}
	return checksum.value()
}

func routeKeyForSignedRoute(route SignedChunkRoute) string {
	return fmt.Sprintf("%s:%s:%d", route.SourceID, route.DestinationID, route.ChunkIndex)
}

func countDistinctRouteChunks(routes []SignedChunkRoute) int {
	seen := map[string]struct{}{}
	for _, route := range routes {
		seen[fmt.Sprintf("%s:%d", route.SourceID, route.ChunkIndex)] = struct{}{}
	}
	return len(seen)
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func (client *Client) CreateAndDistribute(ctx context.Context, input TransferCreateRequest) (*TransferCreateResponse, error) {
	result, err := client.CreateTransfer(ctx, input)
	if err != nil {
		return nil, err
	}
	if result.Success {
		if _, err := client.DistributeTransfer(ctx, result.TransferID); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (client *Client) WaitForTransfer(
	ctx context.Context,
	transferID string,
	timeout time.Duration,
	pollInterval time.Duration,
) (*TransferStatusInfo, error) {
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	if pollInterval == 0 {
		pollInterval = 15 * time.Second
	}
	initialPollInterval := pollInterval
	waiter, _ := client.OpenTransferTerminalWaiter(ctx, transferID)
	defer func() {
		if waiter != nil {
			_ = waiter.Close()
		}
	}()
	deadline := time.Now().Add(timeout)
	for {
		status, err := client.TransferStatus(ctx, transferID)
		if err != nil {
			return nil, err
		}
		switch status.Status {
		case "completed":
			return status, nil
		case "failed":
			message := ""
			if status.ErrorMessage != nil {
				message = *status.ErrorMessage
			}
			return nil, fmt.Errorf("transfer failed: %s", message)
		case "cancelled":
			return nil, fmt.Errorf("transfer cancelled")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("transfer %s did not complete within %s", transferID, timeout)
		}
		jitter := 0.8 + rand.Float64()*0.4
		waitFor := time.Duration(float64(pollInterval) * jitter)
		if waitFor > remaining {
			waitFor = remaining
		}
		var terminalEvent *TransferTerminalEvent
		if waiter != nil {
			waitStartedAt := time.Now()
			terminalEvent, err = waiter.Wait(ctx, waitFor)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				_ = waiter.Close()
				waiter = nil
				remainingWait := waitFor - time.Since(waitStartedAt)
				if remainingWait > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(remainingWait):
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitFor):
			}
		}
		if terminalEvent != nil {
			pollInterval = initialPollInterval
		} else {
			pollInterval = min(30*time.Second, time.Duration(float64(pollInterval)*1.5))
		}
	}
}

func checksumForRouteCoordinates(routes []SignedChunkRoute) string {
	checksum := routeKeysChecksum{}
	for _, route := range routes {
		deliveryIndex, present := signedRouteDeliveryIndex(route)
		if !present {
			checksum.add(routeKeyForSignedRoute(route) + ":missing")
			continue
		}
		checksum.add(fmt.Sprintf("%s:%d", routeKeyForSignedRoute(route), deliveryIndex))
	}
	return checksum.value()
}

func stableManifestBatchIdentity(groups []MultipartGroupManifest) string {
	identities := make([]string, 0, len(groups))
	for _, group := range groups {
		identities = append(identities, fmt.Sprintf("%s:%s:%s", group.MultipartGroupID, group.SourceID, group.DestinationID))
	}
	sort.Strings(identities)
	digest := sha256.Sum256([]byte(strings.Join(identities, "\n")))
	return hex.EncodeToString(digest[:16])
}

func validateSignedRouteManifestContract(transferID string, routes []SignedChunkRoute, manifest []MultipartGroupManifest) error {
	groups := make(map[string]MultipartGroupManifest, len(manifest))
	for _, group := range manifest {
		if group.MultipartGroupID == "" {
			return fmt.Errorf("multipart_group_id is required")
		}
		if _, exists := groups[group.MultipartGroupID]; exists {
			return fmt.Errorf("duplicate multipart_group_id: %s", group.MultipartGroupID)
		}
		if group.SourceID == "" || group.DestinationID == "" || group.FinalObjectKey == "" ||
			group.UploadID == "" || group.CompleteURL == "" || group.AbortURL == "" ||
			group.FinalHeadURL == "" || group.URLsExpiresAt == "" {
			return fmt.Errorf("multipart group %s has missing required controls", group.MultipartGroupID)
		}
		if group.ExpectedObjectSize <= 0 || group.ExpectedPartCount < 1 || group.ExpectedPartCount > multipartMaxSourceChunks {
			return fmt.Errorf("multipart group %s has invalid expected object or part count", group.MultipartGroupID)
		}
		expectedMaxPartNumber, err := multipartPartNumber(group.ExpectedPartCount-1, multipartAttemptSlots-1)
		if err != nil {
			return err
		}
		if group.MaxPartNumber != expectedMaxPartNumber {
			return fmt.Errorf("multipart group %s max_part_number must equal highest reserved recovery slot", group.MultipartGroupID)
		}
		expectedListPageCount := (group.MaxPartNumber + 999) / 1_000
		uniqueListPageURLs := make(map[string]struct{}, len(group.ListPageURLs))
		for _, listPageURL := range group.ListPageURLs {
			if listPageURL == "" {
				return fmt.Errorf("multipart group %s has an empty list_page_url", group.MultipartGroupID)
			}
			uniqueListPageURLs[listPageURL] = struct{}{}
		}
		if len(group.ListPageURLs) != expectedListPageCount || len(uniqueListPageURLs) != expectedListPageCount {
			return fmt.Errorf("multipart group %s requires %d unique list_page_urls", group.MultipartGroupID, expectedListPageCount)
		}
		if len(group.FinalObjectMetadata) != 1 || group.FinalObjectMetadata["beam-transfer-id"] != transferID {
			return fmt.Errorf("multipart group %s has invalid final_object_metadata", group.MultipartGroupID)
		}
		groups[group.MultipartGroupID] = group
	}
	for _, route := range routes {
		groupID := stringValue(route.Metadata["multipart_group_id"])
		uploadID := stringValue(route.Metadata["upload_id"])
		if uploadID != "" && groupID == "" {
			return fmt.Errorf("signed multipart route %s:%s:%d is missing multipart_group_id", route.SourceID, route.DestinationID, route.ChunkIndex)
		}
		if groupID == "" {
			continue
		}
		group, ok := groups[groupID]
		if !ok {
			return fmt.Errorf("signed route references unknown multipart group %s", groupID)
		}
		if route.SourceID != group.SourceID || route.DestinationID != group.DestinationID {
			return fmt.Errorf("signed route identity does not match multipart group %s", groupID)
		}
		if uploadID != group.UploadID || stringValue(route.Metadata["final_object_key"]) != group.FinalObjectKey {
			return fmt.Errorf("signed route controls do not match multipart group %s", groupID)
		}
		partNumber, integer := exactIntegerValue(route.Metadata["part_number"])
		if !integer || partNumber < 1 || partNumber > group.MaxPartNumber {
			return fmt.Errorf("multipart part_number %d is outside group %s range 1-%d", partNumber, groupID, group.MaxPartNumber)
		}
	}
	return nil
}

func validateCompactTransferPlan(signedURLFlow SignedURLFlow, descriptor CompactTransferPlanDescriptor) error {
	if signedURLFlow != SignedURLFlowCanonical {
		return fmt.Errorf("BeamCore returned unsupported signed_url_flow: %s", signedURLFlow)
	}
	if descriptor.Version != "compact-transfer-plan/v1" {
		return fmt.Errorf("BeamCore returned unsupported plan version: %s", descriptor.Version)
	}
	if descriptor.MultipartAttemptSlots != multipartAttemptSlots {
		return fmt.Errorf("BeamCore returned unsupported multipart_attempt_slots: %d", descriptor.MultipartAttemptSlots)
	}
	expected := CompactTransferPlanFormulas{
		SourceOffset:      "source_chunk_index * chunk_size",
		DeliveryIndex:     "chunk_index * destination_count + destination_index",
		PartNumber:        "source_chunk_index * 3 + attempt_slot + 1",
		RouteGenerationID: "initial-{chunk_index}-{destination_id}",
	}
	if descriptor.Formulas != expected {
		return fmt.Errorf("BeamCore returned unsupported compact transfer plan formulas")
	}
	return nil
}

func exactIntegerValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case uint:
		return int(typed), uint(int(typed)) == typed
	case uint8:
		return int(typed), true
	case uint16:
		return int(typed), true
	case uint32:
		return int(typed), uint32(int(typed)) == typed
	case uint64:
		return int(typed), uint64(int(typed)) == typed
	case float32:
		converted := int(typed)
		return converted, float32(converted) == typed
	case float64:
		converted := int(typed)
		return converted, float64(converted) == typed
	default:
		return 0, false
	}
}

func transferIDForIdempotencyKey(key string) string {
	if strings.TrimSpace(key) == "" {
		return newTransferID()
	}
	return stableIDFromIdentity("beam-transfer:" + strings.TrimSpace(key))
}

func stableIDFromIdentity(identity string) string {
	digest := sha256.Sum256([]byte(identity))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}

func validateID(value string, name string) error {
	if value == "" || strings.Contains(value, "/") || strings.Contains(value, "..") {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func parseEnvInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	return parsed
}

func structToMap(value any) map[string]any {
	encoded, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(encoded, &out)
	return out
}
