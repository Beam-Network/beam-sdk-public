package beamnetworksdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// fakeTransferControl records lifecycle requests and answers them like
// BeamCore. Provider prepares derive a one-chunk-per-source plan from the
// request: a single destination plans out/file.bin, several plan output-N.bin.
type fakeTransferControl struct {
	mu                          sync.Mutex
	calls                       []fakeTransferCall
	recoveryLeases              map[string]*recoveryLease
	recoveryPresentAtRouteBegin bool
	continuedRecovery           []string
	failOnceAt                  string
	failOnceError               error
	cancelResponse              map[string]any
	statusResponse              map[string]any
	routeRecoverySigner         *fakeRouteRecoverySigner
	// beforeRequest runs, without the fake's lock, before each request is answered.
	beforeRequest func(messageType string, payload map[string]any)
	// onServeSigner runs when a route recovery signer starts serving.
	onServeSigner func(transferID string)
}

type fakeTransferCall struct {
	messageType    string
	payload        map[string]any
	transferID     string
	idempotencyKey string
}

func (fake *fakeTransferControl) request(_ context.Context, messageType string, payload map[string]any, transferID string, output any, idempotencyKeys ...string) error {
	idempotencyKey := ""
	if len(idempotencyKeys) > 0 {
		idempotencyKey = idempotencyKeys[0]
	}
	fake.mu.Lock()
	fake.calls = append(fake.calls, fakeTransferCall{messageType: messageType, payload: payload, transferID: transferID, idempotencyKey: idempotencyKey})
	if messageType == "transfer.route_stream.begin" {
		_, fake.recoveryPresentAtRouteBegin = fake.recoveryLeases[payload["transfer_id"].(string)]
	}
	if fake.failOnceAt == messageType {
		fake.failOnceAt = ""
		err := fake.failOnceError
		fake.failOnceError = nil
		fake.mu.Unlock()
		if err == nil {
			err = errors.New("connection closed during route streaming")
		}
		return err
	}
	cancelResponse, statusResponse, beforeRequest := fake.cancelResponse, fake.statusResponse, fake.beforeRequest
	fake.mu.Unlock()
	if beforeRequest != nil {
		beforeRequest(messageType, payload)
	}

	var response any = map[string]any{"success": true}
	switch messageType {
	case "transfer.integrity_audit_grants":
		response = map[string]any{"published": true}
	case "transfer.create":
		response = map[string]any{
			"success":            true,
			"transfer_id":        payload["transfer_id"],
			"total_chunks":       2,
			"total_sources":      1,
			"total_destinations": 1,
		}
	case "transfer.plan":
		descriptor, err := fakePlanDescriptor(payload)
		if err != nil {
			return err
		}
		response = map[string]any{
			"success": true, "chunk_size": descriptor["chunk_size"], "total_size": 1024,
			"total_sources": 1, "total_destinations": 1, "logical_chunks": 1, "total_chunks": 1,
			"signed_url_flow":     "signed_url",
			"plan_fingerprint":    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"coordinate_checksum": "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
			"plan_descriptor":     descriptor,
		}
	case "transfer.prepare":
		descriptor, err := fakePlanDescriptor(payload)
		if err != nil {
			return err
		}
		response = map[string]any{
			"success":             true,
			"transfer_id":         payload["transfer_id"],
			"transfer_key":        "tk_go",
			"chunk_size":          descriptor["chunk_size"],
			"total_size":          4096,
			"total_sources":       1,
			"total_destinations":  descriptor["delivery_route_count"],
			"logical_chunks":      1,
			"total_chunks":        descriptor["delivery_route_count"],
			"signed_url_flow":     "signed_url",
			"plan_fingerprint":    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"coordinate_checksum": "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
			"route_generation_id": payload["route_generation_id"],
			"plan_descriptor":     descriptor,
		}
	case "transfer.route_stream.complete":
		response = map[string]any{"success": true, "transfer_id": payload["transfer_id"], "total_routes": 2}
	case "transfer.distribute":
		response = map[string]any{"success": true, "transfer_id": payload["transfer_id"], "orchestrators_assigned": 1}
	case "transfer.status":
		response = map[string]any{
			"transfer_id":   payload["transfer_id"],
			"status":        "completed",
			"error_message": nil,
			"started_at":    "2026-06-22T22:40:20.000Z",
			"completed_at":  "2026-06-22T22:41:20.000Z",
		}
		if statusResponse != nil {
			response = statusResponse
		}
	case "transfer.cancel":
		response = map[string]any{"success": true, "message": "cancelled"}
		if cancelResponse != nil {
			response = cancelResponse
		}
	}
	if output == nil {
		return nil
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, output)
}

// fakePlanDescriptor builds a compact plan from a prepare or plan payload.
func fakePlanDescriptor(payload map[string]any) (map[string]any, error) {
	var sources []map[string]any
	var destinations []map[string]any
	for key, target := range map[string]*[]map[string]any{"sources": &sources, "destinations": &destinations} {
		encoded, err := json.Marshal(payload[key])
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(encoded, target); err != nil {
			return nil, err
		}
	}
	if len(sources) == 0 || len(destinations) == 0 {
		return nil, fmt.Errorf("fake plan requires sources and destinations")
	}
	source := sources[0]
	size := int64(4096)
	if value, ok := source["size"].(float64); ok && value > 0 {
		size = int64(value)
	}
	source["global_chunk_start"] = 0
	source["chunk_count"] = 1
	for index, destination := range destinations {
		finalKey := "out/file.bin"
		if len(destinations) > 1 {
			finalKey = fmt.Sprintf("output-%d.bin", index)
		}
		destination["destination_index"] = index
		destination["final_object_keys"] = map[string]string{source["source_id"].(string): finalKey}
	}
	return map[string]any{
		"version": "compact-transfer-plan/v1", "plan_nonce": "testplan", "chunk_size": size,
		"multipart_attempt_slots": 1,
		"sources":                 []any{source}, "destinations": destinations,
		"logical_chunk_count": 1, "delivery_route_count": len(destinations),
		"formulas": map[string]string{
			"source_offset":       "source_chunk_index * chunk_size",
			"delivery_index":      "chunk_index * destination_count + destination_index",
			"part_number":         "source_chunk_index + 1",
			"route_generation_id": "initial-{chunk_index}-{destination_id}",
		},
	}, nil
}

func (fake *fakeTransferControl) splitRoutes(_ string, _ map[string]any, routes []SignedChunkRoute) ([][]SignedChunkRoute, error) {
	return [][]SignedChunkRoute{routes}, nil
}

func (fake *fakeTransferControl) openTerminalSignalWaiter(_ context.Context, _ string) (*TransferTerminalSignalWaiter, error) {
	return &TransferTerminalSignalWaiter{closed: make(chan struct{})}, nil
}

func (fake *fakeTransferControl) registerRecoveryLease(lease *recoveryLease) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.recoveryLeases == nil {
		fake.recoveryLeases = make(map[string]*recoveryLease)
	}
	// A replaced owner's retained secrets are disposed, as the NATS control does.
	if previous := fake.recoveryLeases[lease.transferID]; previous != nil && previous != lease && previous.dispose != nil {
		previous.dispose()
	}
	fake.recoveryLeases[lease.transferID] = lease
}

// releaseRecoveryLease disposes retained secrets like the NATS control does.
func (fake *fakeTransferControl) releaseRecoveryLease(transferID string) {
	fake.mu.Lock()
	lease := fake.recoveryLeases[transferID]
	delete(fake.recoveryLeases, transferID)
	fake.mu.Unlock()
	if lease != nil && lease.dispose != nil {
		lease.dispose()
	}
}

func (fake *fakeTransferControl) releaseOwnedRecoveryLease(lease *recoveryLease) {
	fake.mu.Lock()
	if lease == nil || fake.recoveryLeases[lease.transferID] != lease {
		fake.mu.Unlock()
		return
	}
	delete(fake.recoveryLeases, lease.transferID)
	fake.mu.Unlock()
	if lease.dispose != nil {
		lease.dispose()
	}
}

func (fake *fakeTransferControl) continueRecoveryLease(lease *recoveryLease) {
	if lease == nil {
		return
	}
	fake.mu.Lock()
	if fake.recoveryLeases[lease.transferID] == lease {
		fake.continuedRecovery = append(fake.continuedRecovery, lease.transferID)
	}
	fake.mu.Unlock()
}

func (fake *fakeTransferControl) close() {}

type fakeRouteRecoverySigner struct {
	transferID string
	handler    routeRecoverySignHandler
}

func (fake *fakeTransferControl) serveRouteRecoverySigner(transferID string, handler routeRecoverySignHandler) (func(), error) {
	signer := &fakeRouteRecoverySigner{transferID: transferID, handler: handler}
	fake.mu.Lock()
	fake.routeRecoverySigner = signer
	onServeSigner := fake.onServeSigner
	fake.mu.Unlock()
	if onServeSigner != nil {
		onServeSigner(transferID)
	}
	return func() {
		fake.mu.Lock()
		if fake.routeRecoverySigner == signer {
			fake.routeRecoverySigner = nil
		}
		fake.mu.Unlock()
	}, nil
}

func (fake *fakeTransferControl) signer() *fakeRouteRecoverySigner {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.routeRecoverySigner
}

func (fake *fakeTransferControl) lease(transferID string) *recoveryLease {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.recoveryLeases[transferID]
}

func (fake *fakeTransferControl) callsOf(messageType string) []fakeTransferCall {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	out := make([]fakeTransferCall, 0)
	for _, call := range fake.calls {
		if call.messageType == messageType {
			out = append(out, call)
		}
	}
	return out
}

func intPtr(value int) *int {
	return &value
}
