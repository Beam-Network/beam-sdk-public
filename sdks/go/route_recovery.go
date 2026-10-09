package beamnetworksdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

const routeRecoverySignMessageType = "transfer.route_recovery.sign"

// routeRecoverySignChunk is one route Runtime asks the owning SDK to re-sign
// for a new logical attempt. Under transfer-client-control/v7 the attempt slot
// is always 0 and the part number is source_chunk_index + 1.
type routeRecoverySignChunk struct {
	SourceID            string         `msgpack:"source_id"`
	DestinationID       string         `msgpack:"destination_id"`
	ChunkIndex          int            `msgpack:"chunk_index"`
	DeliveryIndex       int            `msgpack:"delivery_index"`
	SourceOffset        int64          `msgpack:"source_offset"`
	ChunkSize           int64          `msgpack:"chunk_size"`
	LogicalAttemptIndex int            `msgpack:"logical_attempt_index"`
	AttemptSlot         int            `msgpack:"attempt_slot"`
	PartNumber          int            `msgpack:"part_number"`
	RouteGenerationID   string         `msgpack:"route_generation_id"`
	MultipartGroupID    string         `msgpack:"multipart_group_id"`
	FinalObjectKey      string         `msgpack:"final_object_key"`
	UploadID            string         `msgpack:"upload_id"`
	URLsExpiresAt       string         `msgpack:"urls_expires_at"`
	MultipartCreatedAt  string         `msgpack:"multipart_created_at"`
	SourceMetadata      map[string]any `msgpack:"source_metadata"`
	DestinationMetadata map[string]any `msgpack:"destination_metadata"`
	// Recovery is Runtime's optional multipart recovery directive (staged
	// UploadPartCopy recovery or control renewal).
	Recovery *multipartRecoveryRequest `msgpack:"recovery,omitempty"`
}

type routeRecoverySignRequest struct {
	TransferID        string                   `msgpack:"transfer_id"`
	RouteGenerationID string                   `msgpack:"route_generation_id"`
	RequestedAt       string                   `msgpack:"requested_at"`
	Chunks            []routeRecoverySignChunk `msgpack:"chunks"`
}

type routeRecoverySignReply struct {
	TransferID        string             `json:"transfer_id"`
	RouteGenerationID string             `json:"route_generation_id"`
	SignedAt          string             `json:"signed_at,omitempty"`
	ChunkRoutes       []SignedChunkRoute `json:"chunk_routes"`
}

type routeRecoverySignEnvelope struct {
	MessageID     string                    `msgpack:"message_id"`
	SchemaVersion string                    `msgpack:"schema_version"`
	Environment   string                    `msgpack:"environment"`
	KeyPrefix     string                    `msgpack:"key_prefix"`
	TransferID    string                    `msgpack:"transfer_id"`
	MessageType   string                    `msgpack:"message_type"`
	RequestID     string                    `msgpack:"request_id"`
	OccurredAt    string                    `msgpack:"occurred_at"`
	Producer      string                    `msgpack:"producer"`
	Payload       *routeRecoverySignRequest `msgpack:"payload"`
}

type routeRecoverySignHandler func(ctx context.Context, request routeRecoverySignRequest) (*routeRecoverySignReply, error)

func (c *natsControl) routeRecoverySignSubject(transferID string) string {
	return fmt.Sprintf("%s.%s.sdk.%s.transfer.%s.route_recovery_sign", c.subjectPrefix, c.environment, c.keyPrefix, transferID)
}

// serveRouteRecoverySigner answers Runtime's route recovery signing requests
// for one transfer until the returned stop function runs or the control closes.
func (c *natsControl) serveRouteRecoverySigner(transferID string, handler routeRecoverySignHandler) (func(), error) {
	nc, err := c.connection()
	if err != nil {
		return nil, err
	}
	signerCtx, cancel := context.WithCancel(c.backgroundCtx)
	var active sync.Mutex
	stopped := false
	subscription, err := nc.Subscribe(c.routeRecoverySignSubject(transferID), func(msg *nats.Msg) {
		active.Lock()
		skip := stopped
		active.Unlock()
		// Only request messages carry a reply subject; anything else is ignored.
		if skip || msg.Reply == "" {
			return
		}
		go func() {
			_ = msg.Respond(c.routeRecoveryReply(signerCtx, transferID, msg.Data, handler))
		}()
	})
	if err != nil {
		cancel()
		return nil, err
	}
	// A failed flush is not fatal: the subscription is re-registered on reconnect.
	flushCtx, flushCancel := context.WithTimeout(c.backgroundCtx, c.requestTimeout)
	_ = nc.FlushWithContext(flushCtx)
	flushCancel()

	var once sync.Once
	var stop func()
	stop = func() {
		once.Do(func() {
			active.Lock()
			stopped = true
			active.Unlock()
			cancel()
			_ = subscription.Unsubscribe()
			c.signerMu.Lock()
			delete(c.recoverySigners, &stop)
			c.signerMu.Unlock()
		})
	}
	c.signerMu.Lock()
	if c.signersClosed {
		c.signerMu.Unlock()
		stop()
		return nil, errors.New("NATS lifecycle control is closed")
	}
	c.recoverySigners[&stop] = stop
	c.signerMu.Unlock()
	return stop, nil
}

// routeRecoveryReply validates one request envelope, runs the handler, and
// encodes the reply envelope. Failures reply ok=false with status 500.
func (c *natsControl) routeRecoveryReply(ctx context.Context, transferID string, data []byte, handler routeRecoverySignHandler) []byte {
	request := routeRecoverySignEnvelope{MessageID: "unknown", RequestID: "unknown", TransferID: transferID}
	payload, err := func() (*routeRecoverySignReply, error) {
		var decoded routeRecoverySignEnvelope
		if err := msgpack.Unmarshal(data, &decoded); err != nil {
			return nil, err
		}
		request = decoded
		if decoded.SchemaVersion != transferClientSchemaVersion ||
			decoded.Environment != c.environment ||
			decoded.KeyPrefix != c.keyPrefix ||
			decoded.TransferID != transferID ||
			decoded.MessageType != routeRecoverySignMessageType ||
			decoded.Producer != "transfer-runtime" ||
			decoded.Payload == nil ||
			decoded.Payload.TransferID != transferID ||
			decoded.Payload.RouteGenerationID == "" ||
			len(decoded.Payload.Chunks) == 0 {
			return nil, errors.New("route recovery request envelope mismatch")
		}
		return handler(ctx, *decoded.Payload)
	}()
	reply := map[string]any{
		"message_id":     request.MessageID,
		"schema_version": transferClientSchemaVersion,
		"environment":    c.environment,
		"key_prefix":     c.keyPrefix,
		"transfer_id":    request.TransferID,
		"message_type":   routeRecoverySignMessageType,
		"request_id":     request.RequestID,
		"occurred_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"producer":       "sdk",
		"ok":             err == nil,
		"status":         200,
	}
	if err != nil {
		reply["status"] = 500
		reply["error"] = map[string]any{"code": "route_recovery_sign_failed", "message": err.Error()}
	} else {
		reply["payload"] = payload
	}
	encoded, encodeErr := marshalMsgpack(reply)
	if encodeErr != nil {
		encoded, _ = marshalMsgpack(map[string]any{
			"message_id": request.MessageID, "schema_version": transferClientSchemaVersion, "environment": c.environment,
			"key_prefix": c.keyPrefix, "transfer_id": request.TransferID, "message_type": routeRecoverySignMessageType,
			"request_id": request.RequestID, "occurred_at": time.Now().UTC().Format(time.RFC3339Nano), "producer": "sdk",
			"ok": false, "status": 500, "error": map[string]any{"code": "route_recovery_sign_failed", "message": "route recovery reply encoding failed"},
		})
	}
	return encoded
}

// startProviderRouteRecoverySigner serves route recovery signing and registers
// the integrity audit signer for a provider transfer. The returned token lets
// the session stop only its own signers.
func (client *Client) startProviderRouteRecoverySigner(session *providerTransferSession) (*transferSigners, error) {
	transferID := session.prepared.TransferID
	client.stopRecoverySigner(transferID)
	stop, err := client.control.serveRouteRecoverySigner(transferID, func(ctx context.Context, request routeRecoverySignRequest) (*routeRecoverySignReply, error) {
		signCtx, cancel := withOwnership(ctx, session.ownership)
		defer cancel()
		return client.signRouteRecovery(signCtx, session, request)
	})
	if err != nil {
		return nil, err
	}
	grants := client.providerIntegritySigner(session)
	signers := &transferSigners{
		stop: stop,
		integrity: func(ctx context.Context, challenge *IntegrityAuditChallenge) error {
			payload, err := grants(ctx, challenge)
			if err != nil {
				return err
			}
			var receipt struct {
				Published bool `json:"published"`
			}
			if err := client.control.request(ctx, "transfer.integrity_audit_grants", payload, challenge.TransferID, &receipt, "transfer:"+challenge.TransferID+":integrity-audit:"+challenge.AuditID+":"+payload["submitted_at"].(string)); err != nil {
				return err
			}
			if !receipt.Published {
				return errors.New("integrity audit delivery unavailable")
			}
			return nil
		},
	}
	if immediate, ok := client.control.(interface {
		serveIntegritySigner(string, func(context.Context, *IntegrityAuditChallenge) (map[string]any, error)) (func(), error)
	}); ok {
		if stopIntegrity, signErr := immediate.serveIntegritySigner(transferID, grants); signErr == nil {
			signers.stop = func() { stop(); stopIntegrity() }
		}
	}
	if err := ownershipErr(session.ownership); err != nil {
		signers.stop()
		return nil, err
	}
	client.registerTransferSigners(transferID, signers)
	return signers, nil
}

// signRouteRecovery re-signs requested routes, reusing the existing multipart
// uploads, and applies any multipart recovery directive to the signed route
// (as TypeScript client.ts does with signMultipartRecovery).
func (client *Client) signRouteRecovery(ctx context.Context, session *providerTransferSession, request routeRecoverySignRequest) (*routeRecoverySignReply, error) {
	if err := ownershipErr(session.ownership); err != nil {
		return nil, err
	}
	if request.RouteGenerationID == "" {
		return nil, errors.New("route recovery generation is required")
	}
	ctx = withProviderClients(ctx, session.clients)
	var sourceMu sync.Mutex
	sourceFutures := map[string]*sourceChunkFuture{}
	prepared := session.prepared
	routes, err := mapOrderedWithConcurrency(ctx, request.Chunks, client.routeSigningConcurrency, func(ctx context.Context, requested routeRecoverySignChunk) (SignedChunkRoute, error) {
		if err := ownershipErr(session.ownership); err != nil {
			return SignedChunkRoute{}, err
		}
		if requested.RouteGenerationID != request.RouteGenerationID {
			return SignedChunkRoute{}, errors.New("route recovery chunk generation mismatch")
		}
		chunk, ok := materializePlanChunk(prepared.PlanDescriptor, prepared.TransferID, requested.SourceID, requested.ChunkIndex)
		if !ok {
			return SignedChunkRoute{}, fmt.Errorf("plan chunk is outside source range: %s:%d", requested.SourceID, requested.ChunkIndex)
		}
		if partNumber, err := MultipartPartNumber(chunk.SourceChunkIndex, requested.AttemptSlot); err != nil || partNumber != requested.PartNumber {
			return SignedChunkRoute{}, errors.New("route recovery multipart slot mapping mismatch")
		}
		if requested.LogicalAttemptIndex < 0 {
			return SignedChunkRoute{}, errors.New("route recovery logical attempt index is invalid")
		}
		if chunk.SourceOffset != requested.SourceOffset || chunk.ChunkSize != requested.ChunkSize {
			return SignedChunkRoute{}, errors.New("route recovery source coordinate mismatch")
		}
		var target *ChunkDestinationSigningTarget
		for index := range chunk.Destinations {
			candidate := &chunk.Destinations[index]
			if deliveryIndex, _ := exactIntegerValue(candidate.Metadata["delivery_index"]); candidate.DestinationID == requested.DestinationID && deliveryIndex == requested.DeliveryIndex {
				target = candidate
				break
			}
		}
		if target == nil {
			return SignedChunkRoute{}, errors.New("route recovery destination coordinate mismatch")
		}
		source := session.source(requested.SourceID)
		destination := session.destination(requested.DestinationID)
		if source == nil || destination == nil {
			return SignedChunkRoute{}, errors.New("route recovery provider configuration is unavailable")
		}
		signingTarget := *target
		var upload *multipartUploadState
		if !isDirectPutDestination(destination) {
			state, err := session.recoveryMultipartUploadState(ctx, destination, requested)
			if err != nil {
				return SignedChunkRoute{}, err
			}
			upload = &state
			signingTarget.ObjectKey = requested.FinalObjectKey
		}
		metadata := make(map[string]any, len(target.Metadata)+len(requested.DestinationMetadata)+9)
		for key, value := range target.Metadata {
			metadata[key] = value
		}
		for key, value := range requested.DestinationMetadata {
			metadata[key] = value
		}
		metadata["transfer_id"] = prepared.TransferID
		metadata["final_object_key"] = requested.FinalObjectKey
		metadata["upload_id"] = requested.UploadID
		metadata["multipart_group_id"] = requested.MultipartGroupID
		metadata["delivery_index"] = requested.DeliveryIndex
		metadata["part_number"] = requested.PartNumber
		metadata["logical_attempt_index"] = requested.LogicalAttemptIndex
		metadata["attempt_slot"] = requested.AttemptSlot
		metadata["route_generation_id"] = requested.RouteGenerationID
		signingTarget.Metadata = metadata
		sourceKey := fmt.Sprintf("%s:%d", chunk.SourceID, chunk.ChunkIndex)
		sourceMu.Lock()
		future := sourceFutures[sourceKey]
		if future == nil {
			future = &sourceChunkFuture{}
			sourceFutures[sourceKey] = future
		}
		sourceMu.Unlock()
		grant, err := future.get(ctx, session, chunk)
		if err != nil {
			return SignedChunkRoute{}, err
		}
		route, err := client.signProviderRoute(ctx, providerRouteInput{
			sourceGrant:       grant,
			chunk:             chunk,
			target:            signingTarget,
			source:            source,
			destination:       destination,
			expiresIn:         session.expiresIn,
			upload:            upload,
			huggingFaceUpload: session.huggingFaceByDestination[requested.DestinationID],
			finalObjectKey:    requested.FinalObjectKey,
			partNumber:        requested.PartNumber,
		})
		if err != nil {
			return SignedChunkRoute{}, err
		}
		return signMultipartRecovery(ctx, destination, prepared.TransferID, requested, route, session.expiresIn)
	})
	if err != nil {
		return nil, err
	}
	return &routeRecoverySignReply{
		TransferID:        request.TransferID,
		RouteGenerationID: request.RouteGenerationID,
		SignedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		ChunkRoutes:       routes,
	}, nil
}

// recoveryMultipartUploadState returns the signed state of an existing
// multipart group, rebuilding its controls from the upload id when this
// process has not signed it yet. It never creates an upload.
func (session *providerTransferSession) recoveryMultipartUploadState(ctx context.Context, destination ProviderDestination, requested routeRecoverySignChunk) (multipartUploadState, error) {
	if existing, ok := session.recoveryUploads.get(requested.MultipartGroupID); ok {
		if existing.UploadID != requested.UploadID || existing.ObjectKey != requested.FinalObjectKey ||
			existing.Manifest.SourceID != requested.SourceID || existing.Manifest.DestinationID != requested.DestinationID {
			return multipartUploadState{}, errors.New("route recovery multipart identity mismatch")
		}
		return existing, nil
	}
	if requested.UploadID == "" {
		return multipartUploadState{}, errors.New("route recovery multipart upload id is required")
	}
	prepared := session.prepared
	var source *CompactTransferPlanSource
	for index := range prepared.PlanDescriptor.Sources {
		if prepared.PlanDescriptor.Sources[index].SourceID == requested.SourceID {
			source = &prepared.PlanDescriptor.Sources[index]
			break
		}
	}
	if source == nil {
		return multipartUploadState{}, errors.New("route recovery source plan is unavailable")
	}
	if requested.MultipartGroupID != multipartGroupStateKey(prepared.TransferID, requested.DestinationID, requested.SourceID, requested.FinalObjectKey) {
		return multipartUploadState{}, errors.New("route recovery multipart group identity mismatch")
	}
	state, err := signMultipartGroupState(ctx, prepared.TransferID, requested.MultipartGroupID, *source, requested.DestinationID, destination, requested.FinalObjectKey, requested.UploadID, session.expiresIn)
	if err != nil {
		return multipartUploadState{}, err
	}
	if err := validateSignedRouteManifestContract(prepared.TransferID, nil, []MultipartGroupManifest{state.Manifest}); err != nil {
		return multipartUploadState{}, err
	}
	session.recoveryUploads.set(requested.MultipartGroupID, state)
	return state, nil
}
