package beamnetworksdk

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

const transferClientSchemaVersion = "transfer-client-control/v6"
const sdkMaxReconnects = -1

// NATS enforces max_payload per message. This SDK guard splits signed-route
// control messages before the broker rejects them; transfer bytes never use NATS.
const defaultMaxPayloadBytes = 8 * 1024 * 1024

func newTransferID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		digest := sha256.Sum256([]byte(fmt.Sprintf("beam-sdk-uuid:%d", time.Now().UnixNano())))
		copy(b[:], digest[:16])
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type sdkAuthResolveResponse struct {
	OK    bool   `json:"ok"`
	Token string `json:"token"`
	Error string `json:"error"`
}

type sdkAuthClaims struct {
	Exp int64 `json:"exp"`
}

type natsControl struct {
	apiKey            string
	keyPrefix         string
	natsURL           string
	environment       string
	subjectPrefix     string
	shardCount        int
	requestTimeout    time.Duration
	maxPayloadBytes   int
	nc                *nats.Conn
	authToken         string
	authExpiresAt     int64
	authRefresh       chan struct{}
	connectionMu      sync.Mutex
	authMu            sync.Mutex
	terminalWaiters   map[*TransferTerminalSignalWaiter]struct{}
	recoveryMu        sync.Mutex
	recoveryLeases    map[string]*recoveryLease
	recoveryRunning   map[string]bool
	recoveryRequested map[string][2]string
	runtimeEpochs     map[int][2]string
	helloMonitors     map[int]context.CancelFunc
	backgroundCtx     context.Context
	backgroundStop    context.CancelFunc
	closed            bool
}

type lifecycleRequestError struct {
	status int
	body   string
}

func (e *lifecycleRequestError) Error() string {
	return fmt.Sprintf("beam lifecycle request failed (%d): %s", e.status, e.body)
}

type recoveryLease struct {
	transferID         string
	shardID            int
	planFingerprint    string
	coordinateChecksum string
	replayRoutes       func(context.Context, string) error
	dispose            func()
}

func newNatsControl(apiKey, natsURL, environment string, shardCount int) *natsControl {
	if environment == "" {
		environment = "prod"
	}
	if shardCount < 1 {
		shardCount = 1
	}
	backgroundCtx, backgroundStop := context.WithCancel(context.Background())
	return &natsControl{
		apiKey:            apiKey,
		keyPrefix:         keyPrefix(apiKey),
		natsURL:           strings.TrimRight(natsURL, "/"),
		environment:       environment,
		subjectPrefix:     "beam.transfer.client",
		shardCount:        shardCount,
		requestTimeout:    30 * time.Second,
		maxPayloadBytes:   defaultMaxPayloadBytes,
		terminalWaiters:   make(map[*TransferTerminalSignalWaiter]struct{}),
		recoveryLeases:    make(map[string]*recoveryLease),
		recoveryRunning:   make(map[string]bool),
		recoveryRequested: make(map[string][2]string),
		runtimeEpochs:     make(map[int][2]string),
		helloMonitors:     make(map[int]context.CancelFunc),
		backgroundCtx:     backgroundCtx,
		backgroundStop:    backgroundStop,
	}
}

func (c *natsControl) close() {
	c.connectionMu.Lock()
	if c.closed {
		c.connectionMu.Unlock()
		return
	}
	c.closed = true
	c.backgroundStop()
	nc := c.nc
	c.nc = nil
	waiters := make([]*TransferTerminalSignalWaiter, 0, len(c.terminalWaiters))
	for waiter := range c.terminalWaiters {
		waiters = append(waiters, waiter)
	}
	clear(c.terminalWaiters)
	c.connectionMu.Unlock()
	c.recoveryMu.Lock()
	for _, lease := range c.recoveryLeases {
		if lease.dispose != nil {
			lease.dispose()
		}
	}
	clear(c.recoveryLeases)
	clear(c.recoveryRequested)
	clear(c.helloMonitors)
	c.recoveryMu.Unlock()
	for _, waiter := range waiters {
		_ = waiter.Close()
	}
	if nc != nil {
		_ = nc.Drain()
	}
}

// TransferTerminalSignalWaiter retains an owned terminal subscription until it
// receives a signal, its caller closes it, or the parent client closes.
type TransferTerminalSignalWaiter struct {
	control      *natsControl
	transferID   string
	subscription *nats.Subscription
	closeOnce    sync.Once
	waitMu       sync.Mutex
	closed       chan struct{}
}

// Wait waits for one terminal signal. A nil event and nil error means the
// supplied timeout elapsed; the subscription remains active for the next wait.
func (w *TransferTerminalSignalWaiter) Wait(ctx context.Context, timeout time.Duration) (*TransferTerminalEvent, error) {
	if timeout <= 0 {
		return nil, nil
	}
	select {
	case <-w.closed:
		return nil, fmt.Errorf("transfer terminal waiter is closed")
	default:
	}
	w.waitMu.Lock()
	defer w.waitMu.Unlock()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	message, err := w.subscription.NextMsgWithContext(waitCtx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if waitCtx.Err() != nil {
			return nil, nil
		}
		select {
		case <-w.closed:
			return nil, fmt.Errorf("transfer terminal waiter is closed")
		default:
			return nil, err
		}
	}
	var event TransferTerminalEvent
	if err := msgpack.Unmarshal(message.Data, &event); err != nil {
		return nil, fmt.Errorf("invalid transfer terminal signal: %w", err)
	}
	if event.SchemaVersion != transferClientSchemaVersion || event.Producer != "transfer-runtime" || event.TransferID != w.transferID {
		return nil, fmt.Errorf("invalid transfer terminal signal identity")
	}
	switch event.Status {
	case "completed", "failed", "cancelled":
	default:
		return nil, fmt.Errorf("invalid transfer terminal signal status: %s", event.Status)
	}
	if _, err := time.Parse(time.RFC3339Nano, event.OccurredAt); err != nil {
		return nil, fmt.Errorf("invalid transfer terminal signal occurred_at: %w", err)
	}
	return &event, nil
}

// Close unsubscribes immediately and releases the waiter from its parent client.
func (w *TransferTerminalSignalWaiter) Close() error {
	var unsubscribeErr error
	w.closeOnce.Do(func() {
		close(w.closed)
		if w.subscription != nil {
			unsubscribeErr = w.subscription.Unsubscribe()
		}
		if w.control != nil {
			w.control.connectionMu.Lock()
			delete(w.control.terminalWaiters, w)
			w.control.connectionMu.Unlock()
		}
	})
	return unsubscribeErr
}

func (c *natsControl) openTerminalSignalWaiter(ctx context.Context, transferID string) (*TransferTerminalSignalWaiter, error) {
	nc, err := c.connection()
	if err != nil {
		return nil, err
	}
	subscription, err := nc.SubscribeSync(c.terminalSubject(transferID))
	if err != nil {
		return nil, err
	}
	flushCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	if err := nc.FlushWithContext(flushCtx); err != nil {
		_ = subscription.Unsubscribe()
		return nil, err
	}
	waiter := &TransferTerminalSignalWaiter{
		control:      c,
		transferID:   transferID,
		subscription: subscription,
		closed:       make(chan struct{}),
	}
	c.connectionMu.Lock()
	if c.closed {
		c.connectionMu.Unlock()
		_ = subscription.Unsubscribe()
		return nil, fmt.Errorf("NATS lifecycle control is closed")
	}
	c.terminalWaiters[waiter] = struct{}{}
	c.connectionMu.Unlock()
	return waiter, nil
}

func (c *natsControl) request(ctx context.Context, messageType string, payload map[string]any, transferID string, output any, idempotencyKeys ...string) error {
	shardID := 0
	if transferID != "" {
		shardID = transferShardID(transferID, c.shardCount)
	}
	return c.requestOnShard(ctx, messageType, payload, transferID, shardID, output, idempotencyKeys...)
}

func (c *natsControl) requestOnShard(ctx context.Context, messageType string, payload map[string]any, transferID string, shardID int, output any, idempotencyKeys ...string) error {
	if strings.TrimSpace(c.apiKey) == "" {
		return fmt.Errorf("api key is required")
	}
	token, err := c.authTokenValue(ctx)
	if err != nil {
		return err
	}
	idempotencyKey := ""
	if len(idempotencyKeys) > 0 {
		idempotencyKey = idempotencyKeys[0]
	}
	requestID := lifecycleRequestID(messageType, idempotencyKey)
	envelope := map[string]any{
		"message_id":     fmt.Sprintf("%s:%s:%s:%s:%s", transferClientSchemaVersion, c.environment, c.keyPrefix, messageType, requestID),
		"schema_version": transferClientSchemaVersion,
		"environment":    c.environment,
		"key_prefix":     c.keyPrefix,
		"shard_id":       shardID,
		"message_type":   messageType,
		"request_id":     requestID,
		"auth_token":     token,
		"occurred_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"producer":       "sdk",
		"payload":        payload,
	}
	data, err := marshalMsgpack(envelope)
	if err != nil {
		return err
	}
	if len(data) > c.maxPayloadBytes {
		return fmt.Errorf("NATS lifecycle request is %d bytes, above maxPayloadBytes=%d", len(data), c.maxPayloadBytes)
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		nc, connectionErr := c.connection()
		if connectionErr != nil {
			if !isRetryableNATSError(connectionErr) {
				return connectionErr
			}
			lastErr = connectionErr
			if attempt == 2 {
				break
			}
			base := []time.Duration{150 * time.Millisecond, 500 * time.Millisecond}[attempt]
			delay := time.Duration(float64(base) * (0.8 + rand.Float64()*0.4))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		msg, requestErr := nc.RequestWithContext(requestCtx, c.requestSubject(messageType, shardID), data)
		cancel()
		if requestErr == nil {
			var decoded map[string]any
			if decodeErr := msgpack.Unmarshal(msg.Data, &decoded); decodeErr != nil {
				return decodeErr
			}
			c.observeRuntimeEpochs(shardID, decoded)
			if ok, _ := decoded["ok"].(bool); ok {
				if output == nil {
					return nil
				}
				encoded, encodeErr := json.Marshal(decoded["payload"])
				if encodeErr != nil {
					return encodeErr
				}
				return json.Unmarshal(encoded, output)
			} else {
				body, _ := json.Marshal(decoded["error"])
				status := intValue(decoded["status"])
				if status == 0 {
					status = 500
				}
				lastErr = &lifecycleRequestError{status: status, body: string(body)}
				if status != 408 && status != 425 && status != 429 && status < 500 {
					return lastErr
				}
			}
		} else {
			if !isRetryableNATSError(requestErr) {
				return requestErr
			}
			lastErr = requestErr
		}
		if attempt == 2 {
			break
		}
		base := []time.Duration{150 * time.Millisecond, 500 * time.Millisecond}[attempt]
		delay := time.Duration(float64(base) * (0.8 + rand.Float64()*0.4))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}

func (c *natsControl) registerRecoveryLease(lease *recoveryLease) {
	lease.shardID = transferShardID(lease.transferID, c.shardCount)
	c.recoveryMu.Lock()
	if previous := c.recoveryLeases[lease.transferID]; previous != nil && previous.dispose != nil {
		previous.dispose()
	}
	c.recoveryLeases[lease.transferID] = lease
	if _, exists := c.helloMonitors[lease.shardID]; !exists {
		monitorCtx, cancel := context.WithCancel(c.backgroundCtx)
		c.helloMonitors[lease.shardID] = cancel
		go c.monitorRuntime(monitorCtx, lease.shardID)
	}
	c.recoveryMu.Unlock()
}

func (c *natsControl) releaseRecoveryLease(transferID string) {
	c.recoveryMu.Lock()
	lease := c.recoveryLeases[transferID]
	delete(c.recoveryLeases, transferID)
	delete(c.recoveryRequested, transferID)
	if lease != nil {
		hasShardLease := false
		for _, candidate := range c.recoveryLeases {
			if candidate.shardID == lease.shardID {
				hasShardLease = true
				break
			}
		}
		if !hasShardLease {
			if cancel := c.helloMonitors[lease.shardID]; cancel != nil {
				cancel()
			}
			delete(c.helloMonitors, lease.shardID)
		}
	}
	c.recoveryMu.Unlock()
	if lease != nil && lease.dispose != nil {
		lease.dispose()
	}
}

func (c *natsControl) continueRecoveryLease(transferID string) {
	c.recoveryMu.Lock()
	lease := c.recoveryLeases[transferID]
	if lease == nil {
		c.recoveryMu.Unlock()
		return
	}
	requestedEpoch, exists := c.runtimeEpochs[lease.shardID]
	if !exists {
		requestedEpoch = [2]string{"foreground", newTransferID()}
	}
	c.recoveryRequested[transferID] = requestedEpoch
	if c.recoveryRunning[transferID] {
		c.recoveryMu.Unlock()
		return
	}
	c.recoveryRunning[transferID] = true
	c.recoveryMu.Unlock()
	go c.recoverTransfer(lease)
}

func (c *natsControl) observeRuntimeEpochs(shardID int, envelope map[string]any) {
	runtimeEpoch, runtimeOK := envelope["runtime_epoch"].(string)
	transportEpoch, transportOK := envelope["transport_epoch"].(string)
	if !runtimeOK || !transportOK || runtimeEpoch == "" || transportEpoch == "" {
		return
	}
	c.recoveryMu.Lock()
	previous, existed := c.runtimeEpochs[shardID]
	current := [2]string{runtimeEpoch, transportEpoch}
	c.runtimeEpochs[shardID] = current
	changed := existed && previous != current
	leases := make([]*recoveryLease, 0)
	if changed {
		for _, lease := range c.recoveryLeases {
			if lease.shardID == shardID {
				c.recoveryRequested[lease.transferID] = current
				if !c.recoveryRunning[lease.transferID] {
					c.recoveryRunning[lease.transferID] = true
					leases = append(leases, lease)
				}
			}
		}
	}
	c.recoveryMu.Unlock()
	if changed && previous[0] != runtimeEpoch {
		c.authMu.Lock()
		c.authToken = ""
		c.authExpiresAt = 0
		c.authMu.Unlock()
	}
	for _, lease := range leases {
		go c.recoverTransfer(lease)
	}
}

func (c *natsControl) monitorRuntime(ctx context.Context, shardID int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		_ = c.requestOnShard(requestCtx, "runtime.hello", map[string]any{}, "", shardID, nil, fmt.Sprintf("runtime-hello:%d", shardID))
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *natsControl) recoverTransfer(lease *recoveryLease) {
	defer func() {
		c.recoveryMu.Lock()
		delete(c.recoveryRunning, lease.transferID)
		c.recoveryMu.Unlock()
	}()
	for attempt := 0; ; attempt++ {
		c.recoveryMu.Lock()
		current := c.recoveryLeases[lease.transferID]
		requestedEpoch := c.recoveryRequested[lease.transferID]
		c.recoveryMu.Unlock()
		if current != lease {
			return
		}
		generationID := newTransferID()
		var result struct {
			Recovery            string `json:"recovery"`
			RouteReplayRequired bool   `json:"route_replay_required"`
		}
		err := c.request(
			c.backgroundCtx,
			"transfer.resume",
			map[string]any{
				"transfer_id":         lease.transferID,
				"plan_fingerprint":    lease.planFingerprint,
				"coordinate_checksum": lease.coordinateChecksum,
				"route_generation_id": generationID,
			},
			lease.transferID,
			&result,
			fmt.Sprintf("transfer:%s:resume:%s", lease.transferID, generationID),
		)
		if err == nil && result.Recovery == "terminal" {
			c.releaseRecoveryLease(lease.transferID)
			return
		}
		if err == nil && result.RouteReplayRequired {
			err = lease.replayRoutes(c.backgroundCtx, generationID)
		}
		if err == nil {
			c.recoveryMu.Lock()
			latestEpoch := c.recoveryRequested[lease.transferID]
			if latestEpoch != requestedEpoch {
				c.recoveryMu.Unlock()
				attempt = 0
				continue
			}
			delete(c.recoveryRunning, lease.transferID)
			c.recoveryMu.Unlock()
			return
		}
		if !isRetryableLifecycleError(err) {
			c.releaseRecoveryLease(lease.transferID)
			return
		}
		delay := time.Duration(1<<min(attempt, 6)) * 500 * time.Millisecond
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		delay = time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
		select {
		case <-c.backgroundCtx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func lifecycleRequestID(messageType string, idempotencyKey string) string {
	if strings.TrimSpace(idempotencyKey) == "" {
		return newTransferID()
	}
	digest := sha256.Sum256([]byte("beam:" + messageType + ":" + strings.TrimSpace(idempotencyKey)))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}

func (c *natsControl) splitRoutes(messageType string, basePayload map[string]any, routes []SignedChunkRoute) ([][]SignedChunkRoute, error) {
	encodedSize := func(candidate []SignedChunkRoute) (int, error) {
		payload := map[string]any{}
		for k, v := range basePayload {
			payload[k] = v
		}
		payload["route_batch"] = compactSignedRoutes(candidate)
		envelope := map[string]any{"schema_version": transferClientSchemaVersion, "auth_token": strings.Repeat("x", 512), "payload": payload, "message_type": messageType}
		data, encodeErr := marshalMsgpack(envelope)
		if encodeErr != nil {
			return 0, encodeErr
		}
		return len(data), nil
	}
	if len(routes) == 0 {
		return nil, nil
	}
	chunks := make([][]SignedChunkRoute, 0, 1)
	for offset := 0; offset < len(routes); {
		low, high, accepted := 1, len(routes)-offset, 0
		for low <= high {
			count := (low + high) / 2
			size, err := encodedSize(routes[offset : offset+count])
			if err != nil {
				return nil, err
			}
			if size <= c.maxPayloadBytes {
				accepted = count
				low = count + 1
			} else {
				high = count - 1
			}
		}
		if accepted == 0 {
			size, err := encodedSize(routes[offset : offset+1])
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("single signed route is %d bytes, above max_payload_bytes=%d", size, c.maxPayloadBytes)
		}
		chunks = append(chunks, routes[offset:offset+accepted])
		offset += accepted
	}
	return chunks, nil
}

var routeAttemptMetadataKeys = map[string]struct{}{
	"part_number": {}, "logical_attempt_index": {}, "attempt_slot": {}, "route_generation_id": {},
}

func compactSignedRoutes(routes []SignedChunkRoute) map[string]any {
	sourceChunks := make([]map[string]any, 0)
	sourceRefs := map[string]int{}
	compactRoutes := make([]map[string]any, 0, len(routes))
	for routeIndex, route := range routes {
		sourceKeyBytes, _ := json.Marshal([]any{route.SourceID, route.ChunkIndex, route.SourceURL, route.SourceOffset, route.ChunkSize, route.ExpiresAt, route.Headers})
		sourceKey := string(sourceKeyBytes)
		sourceRef, exists := sourceRefs[sourceKey]
		if !exists {
			sourceRef = len(sourceChunks)
			sourceRefs[sourceKey] = sourceRef
			source := map[string]any{
				"source_ref": sourceRef, "source_id": route.SourceID, "chunk_index": route.ChunkIndex,
				"source_url": route.SourceURL, "source_offset": route.SourceOffset, "chunk_size": route.ChunkSize,
			}
			if route.ExpiresAt != "" {
				source["expires_at"] = route.ExpiresAt
			}
			if len(route.Headers) > 0 {
				source["headers"] = route.Headers
			}
			sourceChunks = append(sourceChunks, source)
		}
		metadata := map[string]any{}
		for key, value := range route.Metadata {
			metadata[key] = value
		}
		deliveryIndex := routeIndex
		if route.DeliveryIndex > 0 {
			deliveryIndex = route.DeliveryIndex
		}
		if value, ok := metadata["delivery_index"]; ok {
			deliveryIndex = intValue(value)
		}
		for _, key := range []string{"source_id", "destination_id", "chunk_index", "route_chunk_index", "delivery_index"} {
			delete(metadata, key)
		}
		groupID := stringValue(metadata["multipart_group_id"])
		if groupID != "" {
			for key := range metadata {
				if _, routeSpecific := routeAttemptMetadataKeys[key]; !routeSpecific {
					delete(metadata, key)
				}
			}
		}
		compactRoute := map[string]any{
			"source_ref": sourceRef, "destination_id": route.DestinationID,
			"delivery_index": deliveryIndex, "dest_url": route.DestURL,
		}
		if route.ExpiresAt != "" {
			compactRoute["expires_at"] = route.ExpiresAt
		}
		if len(route.DestHeaders) > 0 {
			compactRoute["dest_headers"] = route.DestHeaders
		}
		if groupID != "" {
			compactRoute["multipart_group_id"] = groupID
		}
		if len(metadata) > 0 {
			compactRoute["metadata"] = metadata
		}
		compactRoutes = append(compactRoutes, compactRoute)
	}
	return map[string]any{"source_chunks": sourceChunks, "routes": compactRoutes}
}

func marshalMsgpack(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.SetCustomStructTag("json")
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (c *natsControl) connection() (*nats.Conn, error) {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("NATS lifecycle control is closed")
	}
	if strings.HasPrefix(c.natsURL, "http://") || strings.HasPrefix(c.natsURL, "https://") || strings.HasPrefix(c.natsURL, "ws://") || strings.HasPrefix(c.natsURL, "wss://") {
		return nil, fmt.Errorf("natsURL must use nats:// or tls:// for Go lifecycle transport")
	}
	if c.nc != nil && !c.nc.IsClosed() {
		return c.nc, nil
	}
	nc, err := nats.Connect(
		c.natsURL,
		nats.UserInfo(c.keyPrefix, c.apiKey),
		nats.Name("beam-go-sdk-"+c.keyPrefix),
		nats.MaxReconnects(sdkMaxReconnects),
		nats.ReconnectWait(time.Second),
		nats.ReconnectJitter(250*time.Millisecond, time.Second),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, err
	}
	c.nc = nc
	return nc, nil
}

func (c *natsControl) authTokenValue(ctx context.Context) (string, error) {
	for {
		c.authMu.Lock()
		if c.authToken != "" && c.authExpiresAt-5 > time.Now().Unix() {
			token := c.authToken
			c.authMu.Unlock()
			return token, nil
		}
		if c.authRefresh != nil {
			refresh := c.authRefresh
			c.authMu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-refresh:
				continue
			}
		}
		refresh := make(chan struct{})
		c.authRefresh = refresh
		c.authMu.Unlock()

		token, expiresAt, err := c.resolveAuthToken(ctx)
		c.authMu.Lock()
		if err == nil {
			c.authToken = token
			c.authExpiresAt = expiresAt
		}
		c.authRefresh = nil
		close(refresh)
		c.authMu.Unlock()
		return token, err
	}
}

func (c *natsControl) resolveAuthToken(ctx context.Context) (string, int64, error) {
	var msg *nats.Msg
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		nc, connectionErr := c.connection()
		if connectionErr != nil {
			if !isRetryableNATSError(connectionErr) {
				return "", 0, connectionErr
			}
			lastErr = connectionErr
		} else {
			requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
			var requestErr error
			msg, requestErr = nc.RequestWithContext(requestCtx, c.authSubject(), []byte("{}"))
			cancel()
			if requestErr == nil {
				break
			}
			if !isRetryableNATSError(requestErr) {
				return "", 0, requestErr
			}
			lastErr = requestErr
		}
		if attempt < 2 {
			base := []time.Duration{150 * time.Millisecond, 500 * time.Millisecond}[attempt]
			delay := time.Duration(float64(base) * (0.8 + rand.Float64()*0.4))
			select {
			case <-ctx.Done():
				return "", 0, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	if msg == nil {
		return "", 0, lastErr
	}
	var parsed sdkAuthResolveResponse
	if decodeErr := json.Unmarshal(msg.Data, &parsed); decodeErr != nil {
		return "", 0, decodeErr
	}
	if !parsed.OK || parsed.Token == "" {
		return "", 0, fmt.Errorf("NATS auth resolve failed: %s", parsed.Error)
	}
	claims, err := decodeJWTClaims(parsed.Token)
	if err != nil {
		return "", 0, err
	}
	return parsed.Token, claims.Exp, nil
}

func isRetryableNATSError(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(err.Error())
	for _, token := range []string{
		"timeout",
		"deadline exceeded",
		"no responders",
		"no servers",
		"connection closed",
		"disconnected",
		"connection reset",
		"connection refused",
		"broken pipe",
		"socket",
		"network",
	} {
		if strings.Contains(normalized, token) {
			return true
		}
	}
	return false
}

func isRetryableLifecycleError(err error) bool {
	var lifecycleErr *lifecycleRequestError
	if errors.As(err, &lifecycleErr) {
		return lifecycleErr.status == 408 || lifecycleErr.status == 425 || lifecycleErr.status == 429 || lifecycleErr.status >= 500
	}
	return isRetryableNATSError(err)
}

func isRecoverableRouteStreamError(err error) bool {
	if isRetryableLifecycleError(err) {
		return true
	}
	var lifecycleErr *lifecycleRequestError
	return errors.As(err, &lifecycleErr) && (lifecycleErr.status == 404 || lifecycleErr.status == 409)
}

func (c *natsControl) authSubject() string {
	return fmt.Sprintf("%s.%s.auth.%s.resolve", c.subjectPrefix, c.environment, c.keyPrefix)
}

func (c *natsControl) requestSubject(messageType string, shardID int) string {
	return fmt.Sprintf("%s.%s.sdk.%s.shard.%d.%s", c.subjectPrefix, c.environment, c.keyPrefix, shardID, strings.ReplaceAll(messageType, ".", "_"))
}

func (c *natsControl) terminalSubject(transferID string) string {
	return fmt.Sprintf("%s.%s.events.%s.%s.terminal", c.subjectPrefix, c.environment, c.keyPrefix, transferID)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}
func keyPrefix(apiKey string) string {
	if len(apiKey) < 12 {
		return apiKey
	}
	return apiKey[:12]
}

func transferShardID(transferID string, shardCount int) int {
	h := uint32(2166136261)
	for _, ch := range transferID {
		h ^= uint32(ch)
		h *= 16777619
	}
	return int(h % uint32(shardCount))
}

func decodeJWTClaims(token string) (sdkAuthClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return sdkAuthClaims{}, fmt.Errorf("invalid JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return sdkAuthClaims{}, err
	}
	var claims sdkAuthClaims
	return claims, json.Unmarshal(payload, &claims)
}
