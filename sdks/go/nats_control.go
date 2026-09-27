package beamnetworksdk

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

const transferClientSchemaVersion = "transfer-client-control/v7"
const sdkMaxReconnects = -1

// NATS enforces max_payload per message. This SDK guard splits signed-route
// control messages before the broker rejects them; transfer bytes never use NATS.
const defaultMaxPayloadBytes = 8 * 1024 * 1024

// Route batches target 4 MiB physical messages below the maxPayloadBytes guard.
const routeTargetPayloadBytes = 4 * 1024 * 1024

// Route batch estimates reserve room for the live SDK auth token.
const routeBatchAuthTokenEstimateBytes = 64 * 1024

// Cached SDK auth tokens are refreshed before their final 30 seconds.
const authTokenRefreshSafetySeconds = 30

const lifecycleRequestMaxAttempts = 3

const runtimeHelloInterval = 5 * time.Second

const defaultSubjectPrefix = "beam.transfer.client"

const defaultRequestTimeout = 30 * time.Second

var lifecycleRequestRetryDelays = []time.Duration{150 * time.Millisecond, 500 * time.Millisecond}

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
	recoveryRunning   map[string]*recoveryRun
	recoveryRequested map[string][2]string
	performanceV2     map[int]time.Time
	runtimeEpochs     map[int][2]string
	helloMonitors     map[int]context.CancelFunc
	backgroundCtx     context.Context
	backgroundStop    context.CancelFunc
	closed            bool
	signerMu          sync.Mutex
	recoverySigners   map[*func()]func()
	signersClosed     bool
	// requestOverride replaces the NATS request/reply exchange in tests.
	requestOverride func(ctx context.Context, subject string, data []byte) ([]byte, error)
}

func (c *natsControl) serveIntegritySigner(transferID string, handler func(context.Context, *IntegrityAuditChallenge) (map[string]any, error)) (func(), error) {
	nc, err := c.connection()
	if err != nil {
		return nil, err
	}
	subject := fmt.Sprintf("%s.%s.sdk.%s.transfer.%s.integrity_sign", c.subjectPrefix, c.environment, c.keyPrefix, transferID)
	sub, err := nc.Subscribe(subject, func(message *nats.Msg) {
		var request struct {
			Schema      string                  `json:"schema_version"`
			Capability  string                  `json:"capability"`
			Producer    string                  `json:"producer"`
			Environment string                  `json:"environment"`
			KeyPrefix   string                  `json:"key_prefix"`
			TransferID  string                  `json:"transfer_id"`
			RequestID   string                  `json:"request_id"`
			Challenge   IntegrityAuditChallenge `json:"challenge"`
		}
		if json.Unmarshal(message.Data, &request) != nil || request.Schema != transferClientSchemaVersion || request.Capability != "integrity-signing/v1" || request.Producer != "transfer-runtime" || request.Environment != c.environment || request.KeyPrefix != c.keyPrefix || request.TransferID != transferID || request.Challenge.TransferID != transferID || request.RequestID == "" {
			_ = message.Respond([]byte(`{"retry":true}`))
			return
		}
		ctx, cancel := context.WithTimeout(c.backgroundCtx, 30*time.Second)
		defer cancel()
		payload, err := handler(ctx, &request.Challenge)
		if err != nil {
			_ = message.Respond([]byte(`{"retry":true}`))
			return
		}
		body, err := json.Marshal(map[string]any{"schema_version": transferClientSchemaVersion, "capability": "integrity-signing/v1", "environment": c.environment, "key_prefix": c.keyPrefix, "transfer_id": transferID, "request_id": request.RequestID, "payload": payload})
		if err == nil {
			_ = message.Respond(body)
		}
	})
	if err != nil {
		return nil, err
	}
	_ = sub.SetPendingLimits(32, c.maxPayloadBytes*2)
	if err := nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// LifecycleRequestError is a non-success reply from Beam lifecycle control.
// Status is the reply status (408, 425, 429, and 5xx are retried), Code and
// Message come from the reply error, and Body is the JSON-encoded error.
type LifecycleRequestError struct {
	Status  int
	Code    string
	Message string
	Body    string
}

func (e *LifecycleRequestError) Error() string {
	return fmt.Sprintf("beam lifecycle request failed (%d): %s", e.Status, e.Body)
}

type recoveryLease struct {
	transferID         string
	shardID            int
	planFingerprint    string
	coordinateChecksum string
	// ownership is the owner's fence; once it is done the lease never sends
	// transfer.resume or replays routes again.
	ownership    context.Context
	replayRoutes func(context.Context, string) error
	dispose      func()
}

// recoveryRun is one background recovery loop for one lease. Its identity,
// not the transfer id, decides whether a finishing loop may clear the running
// registration, and cancelling it stops a loop whose lease was replaced.
type recoveryRun struct {
	lease  *recoveryLease
	ctx    context.Context
	cancel context.CancelFunc
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
		subjectPrefix:     defaultSubjectPrefix,
		shardCount:        shardCount,
		requestTimeout:    defaultRequestTimeout,
		maxPayloadBytes:   defaultMaxPayloadBytes,
		terminalWaiters:   make(map[*TransferTerminalSignalWaiter]struct{}),
		recoveryLeases:    make(map[string]*recoveryLease),
		recoveryRunning:   make(map[string]*recoveryRun),
		recoveryRequested: make(map[string][2]string),
		runtimeEpochs:     make(map[int][2]string),
		helloMonitors:     make(map[int]context.CancelFunc),
		recoverySigners:   make(map[*func()]func()),
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
	c.signerMu.Lock()
	c.signersClosed = true
	signerStops := make([]func(), 0, len(c.recoverySigners))
	for _, stop := range c.recoverySigners {
		signerStops = append(signerStops, stop)
	}
	c.signerMu.Unlock()
	for _, stop := range signerStops {
		stop()
	}
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
	idempotencyKey := ""
	if len(idempotencyKeys) > 0 {
		idempotencyKey = idempotencyKeys[0]
	}
	// The request identity and timestamp stay fixed across attempts so BeamCore
	// deduplicates retries; only an expired auth token is replaced.
	requestID := lifecycleRequestID(messageType, idempotencyKey)
	occurredAt := time.Now().UTC().Format(time.RFC3339Nano)
	subject := c.requestSubject(messageType, shardID)
	var lastErr error
	for attempt := 0; attempt < lifecycleRequestMaxAttempts; attempt++ {
		retry, err := c.requestAttempt(ctx, messageType, payload, shardID, subject, requestID, occurredAt, output, attempt)
		if err == nil {
			return nil
		}
		if !retry || attempt+1 >= lifecycleRequestMaxAttempts {
			return err
		}
		lastErr = err
		if sleepErr := sleepWithJitter(ctx, lifecycleRetryDelay(attempt)); sleepErr != nil {
			return sleepErr
		}
	}
	return lastErr
}

// requestAttempt sends one lifecycle request and reports whether its failure may be retried.
func (c *natsControl) requestAttempt(
	ctx context.Context,
	messageType string,
	payload map[string]any,
	shardID int,
	subject string,
	requestID string,
	occurredAt string,
	output any,
	attempt int,
) (bool, error) {
	token, err := c.authTokenValue(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return isRetryableNATSError(err), err
	}
	encodeStarted := time.Now()
	data, err := marshalMsgpack(map[string]any{
		"message_id":     fmt.Sprintf("%s:%s:%s:%s:%s", transferClientSchemaVersion, c.environment, c.keyPrefix, messageType, requestID),
		"schema_version": transferClientSchemaVersion,
		"environment":    c.environment,
		"key_prefix":     c.keyPrefix,
		"shard_id":       shardID,
		"message_type":   messageType,
		"request_id":     requestID,
		"auth_token":     token,
		"occurred_at":    occurredAt,
		"producer":       "sdk",
		"payload":        payload,
	})
	if err != nil {
		return false, err
	}
	if metrics := performanceFromContext(ctx); metrics != nil && messageType == "transfer.route_stream.batch" {
		metrics.observe("sdk.batch_encode", encodeStarted)
		metrics.gauge("batch_bytes_max", float64(len(data)))
	}
	if len(data) > c.maxPayloadBytes {
		return false, fmt.Errorf("NATS lifecycle request is %d bytes, above maxPayloadBytes=%d", len(data), c.maxPayloadBytes)
	}
	reply, err := c.natsRequest(ctx, subject, data)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return isRetryableNATSError(err), err
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(reply, &decoded); err != nil {
		return false, err
	}
	c.observeRuntimeEpochs(shardID, decoded)
	if ok, _ := decoded["ok"].(bool); ok {
		if messageType == "runtime.hello" {
			payload, _ := decoded["payload"].(map[string]any)
			capabilities, _ := payload["capabilities"].([]any)
			c.recoveryMu.Lock()
			if c.performanceV2 == nil {
				c.performanceV2 = map[int]time.Time{}
			}
			delete(c.performanceV2, shardID)
			for _, capability := range capabilities {
				if capability == "sdk-performance/v2" {
					c.performanceV2[shardID] = time.Now().Add(15 * time.Second)
				}
			}
			c.recoveryMu.Unlock()
		}
		if output == nil {
			return false, nil
		}
		encoded, err := json.Marshal(decoded["payload"])
		if err != nil {
			return false, err
		}
		return false, json.Unmarshal(encoded, output)
	}
	replyErr := newLifecycleRequestError(decoded)
	if replyErr.Status == 401 && replyErr.Code == "auth_token_expired" && attempt+1 < lifecycleRequestMaxAttempts {
		c.authMu.Lock()
		if c.authToken == token {
			c.authToken = ""
			c.authExpiresAt = 0
		}
		c.authMu.Unlock()
		return true, replyErr
	}
	return isRetryableLifecycleStatus(replyErr.Status), replyErr
}

// natsRequest performs one request/reply exchange bounded by the request timeout.
func (c *natsControl) natsRequest(ctx context.Context, subject string, data []byte) ([]byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	if c.requestOverride != nil {
		return c.requestOverride(requestCtx, subject, data)
	}
	nc, err := c.connection()
	if err != nil {
		return nil, err
	}
	msg, err := nc.RequestWithContext(requestCtx, subject, data)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func newLifecycleRequestError(decoded map[string]any) *LifecycleRequestError {
	errorBody := decoded["error"]
	body := []byte("{}")
	if errorBody != nil {
		body, _ = json.Marshal(errorBody)
	}
	status := intValue(decoded["status"])
	if status == 0 {
		status = 500
	}
	code, message := "", ""
	if fields, ok := errorBody.(map[string]any); ok {
		code = stringValue(fields["code"])
		message = stringValue(fields["message"])
	}
	return &LifecycleRequestError{Status: status, Code: code, Message: message, Body: string(body)}
}

func lifecycleRetryDelay(attempt int) time.Duration {
	if attempt < len(lifecycleRequestRetryDelays) {
		return lifecycleRequestRetryDelays[attempt]
	}
	return time.Second
}

func sleepWithJitter(ctx context.Context, base time.Duration) error {
	timer := time.NewTimer(time.Duration(float64(base) * (0.8 + rand.Float64()*0.4)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *natsControl) registerRecoveryLease(lease *recoveryLease) {
	lease.shardID = transferShardID(lease.transferID, c.shardCount)
	c.recoveryMu.Lock()
	if previous := c.recoveryLeases[lease.transferID]; previous != nil && previous != lease && previous.dispose != nil {
		previous.dispose()
	}
	c.recoveryLeases[lease.transferID] = lease
	// A loop recovering the replaced lease stops, so the new lease's recovery
	// is scheduled rather than coalesced into the old loop.
	c.stopRecoveryRunLocked(lease.transferID, lease)
	if _, exists := c.helloMonitors[lease.shardID]; !exists {
		monitorCtx, cancel := context.WithCancel(c.backgroundCtx)
		c.helloMonitors[lease.shardID] = cancel
		go c.monitorRuntime(monitorCtx, lease.shardID)
	}
	c.recoveryMu.Unlock()
}

// releaseRecoveryLease releases whichever lease the transfer holds. Terminal
// status and explicit cancellation use it; owners use releaseOwnedRecoveryLease.
func (c *natsControl) releaseRecoveryLease(transferID string) {
	c.releaseLease(transferID, nil)
}

// releaseOwnedRecoveryLease releases the lease only while it is still the
// transfer's current lease, so a fenced owner never releases its replacement.
func (c *natsControl) releaseOwnedRecoveryLease(lease *recoveryLease) {
	if lease != nil {
		c.releaseLease(lease.transferID, lease)
	}
}

func (c *natsControl) releaseLease(transferID string, expected *recoveryLease) {
	c.recoveryMu.Lock()
	lease := c.recoveryLeases[transferID]
	if expected != nil && lease != expected {
		c.recoveryMu.Unlock()
		return
	}
	delete(c.recoveryLeases, transferID)
	delete(c.recoveryRequested, transferID)
	c.stopRecoveryRunLocked(transferID, nil)
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

// continueRecoveryLease schedules background recovery for the lease while it
// is still the transfer's current lease.
func (c *natsControl) continueRecoveryLease(lease *recoveryLease) {
	if lease == nil {
		return
	}
	c.recoveryMu.Lock()
	if c.recoveryLeases[lease.transferID] != lease {
		c.recoveryMu.Unlock()
		return
	}
	requestedEpoch, exists := c.runtimeEpochs[lease.shardID]
	if !exists {
		requestedEpoch = [2]string{"foreground", newTransferID()}
	}
	c.recoveryRequested[lease.transferID] = requestedEpoch
	run := c.startRecoveryRunLocked(lease)
	c.recoveryMu.Unlock()
	if run != nil {
		go c.recoverTransfer(run)
	}
}

// startRecoveryRunLocked registers a new recovery loop for the lease, unless
// one for this same lease is already running. A loop for a replaced lease is
// cancelled. The caller holds recoveryMu and starts the returned run.
func (c *natsControl) startRecoveryRunLocked(lease *recoveryLease) *recoveryRun {
	if existing := c.recoveryRunning[lease.transferID]; existing != nil {
		if existing.lease == lease {
			return nil
		}
		existing.cancel()
	}
	ctx, cancel := context.WithCancel(c.backgroundCtx)
	run := &recoveryRun{lease: lease, ctx: ctx, cancel: cancel}
	c.recoveryRunning[lease.transferID] = run
	return run
}

// stopRecoveryRunLocked cancels and unregisters the transfer's recovery loop
// unless it recovers keep. The caller holds recoveryMu.
func (c *natsControl) stopRecoveryRunLocked(transferID string, keep *recoveryLease) {
	if run := c.recoveryRunning[transferID]; run != nil && (keep == nil || run.lease != keep) {
		run.cancel()
		delete(c.recoveryRunning, transferID)
	}
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
	if previous != current {
		delete(c.performanceV2, shardID)
	}
	changed := existed && previous != current
	runs := make([]*recoveryRun, 0)
	if changed {
		for _, lease := range c.recoveryLeases {
			if lease.shardID == shardID {
				c.recoveryRequested[lease.transferID] = current
				if run := c.startRecoveryRunLocked(lease); run != nil {
					runs = append(runs, run)
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
	for _, run := range runs {
		go c.recoverTransfer(run)
	}
}

func (c *natsControl) monitorRuntime(ctx context.Context, shardID int) {
	ticker := time.NewTicker(runtimeHelloInterval)
	defer ticker.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		if err := c.requestOnShard(requestCtx, "runtime.hello", map[string]any{"capabilities": []string{"integrity-signing/v1", "sdk-performance/v2"}}, "", shardID, nil, fmt.Sprintf("runtime-hello:%d:integrity", shardID)); err != nil {
			_ = c.requestOnShard(requestCtx, "runtime.hello", map[string]any{}, "", shardID, nil, fmt.Sprintf("runtime-hello:%d", shardID))
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// recoveryRunCurrent reports whether the run is still the transfer's
// registered loop, its lease is still current, and its owner is not fenced.
// It returns the newest requested epoch.
func (c *natsControl) recoveryRunCurrent(run *recoveryRun) ([2]string, bool) {
	lease := run.lease
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	current := c.recoveryRunning[lease.transferID] == run && c.recoveryLeases[lease.transferID] == lease &&
		run.ctx.Err() == nil && ownershipErr(lease.ownership) == nil
	return c.recoveryRequested[lease.transferID], current
}

func (c *natsControl) recoverTransfer(run *recoveryRun) {
	lease := run.lease
	defer func() {
		c.recoveryMu.Lock()
		if c.recoveryRunning[lease.transferID] == run {
			delete(c.recoveryRunning, lease.transferID)
		}
		c.recoveryMu.Unlock()
		run.cancel()
	}()
	for attempt := 0; ; attempt++ {
		requestedEpoch, current := c.recoveryRunCurrent(run)
		if !current {
			if ownershipErr(lease.ownership) != nil {
				c.releaseOwnedRecoveryLease(lease)
			}
			return
		}
		generationID := newTransferID()
		var result struct {
			Recovery            string `json:"recovery"`
			RouteReplayRequired bool   `json:"route_replay_required"`
		}
		err := c.request(
			run.ctx,
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
			c.releaseOwnedRecoveryLease(lease)
			return
		}
		if err == nil && (result.RouteReplayRequired || result.Recovery == "route_replay_required") {
			if _, current := c.recoveryRunCurrent(run); !current {
				if ownershipErr(lease.ownership) != nil {
					c.releaseOwnedRecoveryLease(lease)
				}
				return
			}
			err = lease.replayRoutes(run.ctx, generationID)
		}
		if err == nil {
			c.recoveryMu.Lock()
			if c.recoveryRunning[lease.transferID] != run {
				c.recoveryMu.Unlock()
				return
			}
			if c.recoveryRequested[lease.transferID] != requestedEpoch {
				c.recoveryMu.Unlock()
				attempt = 0
				continue
			}
			// Clearing the registration under the same lock as the epoch check
			// means a request arriving afterwards starts a new loop.
			delete(c.recoveryRunning, lease.transferID)
			c.recoveryMu.Unlock()
			return
		}
		if run.ctx.Err() != nil {
			// Superseded, released, or closed: the replacement decides.
			return
		}
		if !isRetryableLifecycleError(err) {
			c.releaseOwnedRecoveryLease(lease)
			return
		}
		delay := time.Duration(1<<min(attempt, 6)) * 500 * time.Millisecond
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		delay = time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
		timer := time.NewTimer(delay)
		select {
		case <-run.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
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

// splitRoutes packs routes into physical batches that target 4 MiB, or
// maxPayloadBytes when that is lower. A single route above the target but within
// maxPayloadBytes is sent alone; one above maxPayloadBytes fails before publication.
func (c *natsControl) splitRoutes(messageType string, basePayload map[string]any, routes []SignedChunkRoute) ([][]SignedChunkRoute, error) {
	targetPayloadBytes := min(c.maxPayloadBytes, routeTargetPayloadBytes)
	authTokenEstimate := strings.Repeat("x", min(routeBatchAuthTokenEstimateBytes, max(512, c.maxPayloadBytes/128)))
	occurredAt := time.Now().UTC().Format(time.RFC3339Nano)
	encodedSize := func(candidate []SignedChunkRoute) (int, error) {
		payload := make(map[string]any, len(basePayload)+1)
		for key, value := range basePayload {
			payload[key] = value
		}
		payload["route_batch"] = compactSignedRoutes(candidate)
		// The live JWT is larger than a compact placeholder once claims and signatures
		// are encoded, so the estimate reserves room for token and envelope growth.
		data, err := marshalMsgpack(map[string]any{
			"schema_version": transferClientSchemaVersion,
			"environment":    c.environment,
			"key_prefix":     c.keyPrefix,
			"shard_id":       0,
			"message_type":   messageType,
			"request_id":     "00000000-0000-4000-8000-000000000000",
			"auth_token":     authTokenEstimate,
			"occurred_at":    occurredAt,
			"producer":       "sdk",
			"payload":        payload,
		})
		if err != nil {
			return 0, err
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
			if size <= targetPayloadBytes {
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
			if size > c.maxPayloadBytes {
				return nil, fmt.Errorf("single signed route is %d bytes, above maxPayloadBytes=%d", size, c.maxPayloadBytes)
			}
			accepted = 1
		}
		chunks = append(chunks, routes[offset:offset+accepted])
		offset += accepted
	}
	return chunks, nil
}

var routeAttemptMetadataKeys = map[string]struct{}{
	"etag_required": {}, "part_number": {}, "logical_attempt_index": {}, "attempt_slot": {}, "route_generation_id": {},
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
		if route.DeliveryIndex != nil {
			deliveryIndex = *route.DeliveryIndex
		} else if value, ok := exactIntegerValue(metadata["delivery_index"]); ok && value >= 0 {
			deliveryIndex = value
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
	// Sorted keys keep encoding deterministic, so a retried request is byte-identical.
	encoder.SetSortMapKeys(true)
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
	nc, err := nats.Connect(c.natsURL, natsConnectOptions(c.natsURL, c.apiKey, c.keyPrefix)...)
	if err != nil {
		return nil, err
	}
	c.nc = nc
	return nc, nil
}

// natsConnectOptions builds the connection options. The tls:// gateway expects
// the TLS handshake before it sends INFO, so the client starts TLS first and
// presents the gateway hostname for SNI and certificate verification.
func natsConnectOptions(natsURL string, apiKey string, keyPrefix string) []nats.Option {
	options := []nats.Option{
		nats.UserInfo(keyPrefix, apiKey),
		nats.Name("beam-go-sdk-" + keyPrefix),
		nats.MaxReconnects(sdkMaxReconnects),
		nats.ReconnectWait(time.Second),
		nats.ReconnectJitter(500*time.Millisecond, time.Second),
		// A failed first connect fails fast, as the TypeScript connect() does,
		// so a rejected API key surfaces immediately. Reconnects after a
		// successful connect stay unbounded.
		nats.RetryOnFailedConnect(false),
	}
	if strings.HasPrefix(strings.ToLower(natsURL), "tls://") {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if parsed, err := url.Parse(natsURL); err == nil && parsed.Hostname() != "" {
			tlsConfig.ServerName = parsed.Hostname()
		}
		options = append(options, nats.Secure(tlsConfig), nats.TLSHandshakeFirst())
	}
	return options
}

func (c *natsControl) authTokenValue(ctx context.Context) (string, error) {
	for {
		c.authMu.Lock()
		if c.authToken != "" && c.authExpiresAt-authTokenRefreshSafetySeconds > time.Now().Unix() {
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
	var reply []byte
	var lastErr error
	for attempt := 0; attempt < lifecycleRequestMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		var requestErr error
		reply, requestErr = c.natsRequest(ctx, c.authSubject(), []byte("{}"))
		if requestErr == nil {
			break
		}
		if !isRetryableNATSError(requestErr) {
			return "", 0, requestErr
		}
		lastErr = requestErr
		if attempt+1 < lifecycleRequestMaxAttempts {
			if err := sleepWithJitter(ctx, lifecycleRetryDelay(attempt)); err != nil {
				return "", 0, err
			}
		}
	}
	if reply == nil {
		return "", 0, lastErr
	}
	var parsed sdkAuthResolveResponse
	if decodeErr := json.Unmarshal(reply, &parsed); decodeErr != nil {
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
	if errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) || errors.Is(err, nats.ErrAuthRevoked) {
		return false
	}
	normalized := strings.ToLower(err.Error())
	// Credential rejections never succeed on retry, even when the server also
	// closed the connection.
	for _, token := range []string{"authorization violation", "authentication expired", "authentication revoked", "permissions violation"} {
		if strings.Contains(normalized, token) {
			return false
		}
	}
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

func isRetryableLifecycleStatus(status int) bool {
	return status == 408 || status == 425 || status == 429 || status >= 500
}

func isRetryableLifecycleError(err error) bool {
	var lifecycleErr *LifecycleRequestError
	if errors.As(err, &lifecycleErr) {
		return isRetryableLifecycleStatus(lifecycleErr.Status)
	}
	return isRetryableNATSError(err)
}

func isRecoverableRouteStreamError(err error) bool {
	if isRetryableLifecycleError(err) {
		return true
	}
	var lifecycleErr *LifecycleRequestError
	return errors.As(err, &lifecycleErr) && (lifecycleErr.Status == 404 || lifecycleErr.Status == 409)
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

func (c *natsControl) supportsPerformanceV2(transferID string) bool {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	return time.Now().Before(c.performanceV2[transferShardID(transferID, c.shardCount)])
}
