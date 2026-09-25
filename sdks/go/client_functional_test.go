package beamnetworksdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRouteMessageDefaultOverrideAndRecoveryError(t *testing.T) {
	if defaultMaxPayloadBytes != 8*1024*1024 {
		t.Fatalf("defaultMaxPayloadBytes = %d", defaultMaxPayloadBytes)
	}
	client := NewClient(WithMaxPayloadBytes(10 * 1024 * 1024))
	defer client.Close()
	control := client.control.(*natsControl)
	if control.maxPayloadBytes != 10*1024*1024 {
		t.Fatalf("maxPayloadBytes = %d", control.maxPayloadBytes)
	}
	cause := errors.New("route generation mismatch")
	pending := &RouteRecoveryPendingError{TransferID: "transfer-1", Cause: cause}
	if !errors.Is(pending, cause) || pending.TransferID != "transfer-1" {
		t.Fatalf("recovery error did not retain identity and cause: %v", pending)
	}
}

type fakeTransferControl struct {
	calls                       []fakeTransferCall
	recoveryLeases              map[string]*recoveryLease
	recoveryPresentAtRouteBegin bool
	continuedRecovery           []string
	failOnceAt                  string
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
	fake.calls = append(fake.calls, fakeTransferCall{messageType: messageType, payload: payload, transferID: transferID, idempotencyKey: idempotencyKey})
	if messageType == "transfer.route_stream.begin" {
		_, fake.recoveryPresentAtRouteBegin = fake.recoveryLeases[payload["transfer_id"].(string)]
	}
	if fake.failOnceAt == messageType {
		fake.failOnceAt = ""
		return errors.New("connection closed during route streaming")
	}
	var response any = map[string]any{"success": true}
	switch messageType {
	case "transfer.create":
		response = map[string]any{
			"success":            true,
			"transfer_id":        payload["transfer_id"],
			"total_chunks":       2,
			"total_sources":      1,
			"total_destinations": 1,
		}
	case "transfer.prepare":
		source := map[string]any{"source_id": "src_0", "type": "http", "url": "https://source.example/file.bin", "size": 4096, "global_chunk_start": 0, "chunk_count": 1}
		destination := map[string]any{"destination_id": "dst_0", "provider": "http", "mode": "http_chunks", "destination_index": 0, "final_object_keys": map[string]string{"src_0": "out/file.bin"}}
		response = map[string]any{
			"success":             true,
			"transfer_id":         payload["transfer_id"],
			"transfer_key":        "tk_go",
			"chunk_size":          4096,
			"total_size":          4096,
			"total_sources":       1,
			"total_destinations":  1,
			"logical_chunks":      1,
			"total_chunks":        1,
			"signed_url_flow":     "signed_url",
			"plan_fingerprint":    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"coordinate_checksum": "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
			"route_generation_id": payload["route_generation_id"],
			"plan_descriptor": map[string]any{
				"version": "compact-transfer-plan/v1", "plan_nonce": "testplan", "chunk_size": 4096,
				"multipart_attempt_slots": 3,
				"sources":                 []any{source}, "destinations": []any{destination},
				"logical_chunk_count": 1, "delivery_route_count": 1,
				"formulas": map[string]string{
					"source_offset":       "source_chunk_index * chunk_size",
					"delivery_index":      "chunk_index * destination_count + destination_index",
					"part_number":         "source_chunk_index * 3 + attempt_slot + 1",
					"route_generation_id": "initial-{chunk_index}-{destination_id}",
				},
			},
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
	case "transfer.cancel":
		response = map[string]any{"success": true, "message": "cancelled"}
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

func (fake *fakeTransferControl) splitRoutes(_ string, _ map[string]any, routes []SignedChunkRoute) ([][]SignedChunkRoute, error) {
	return [][]SignedChunkRoute{routes}, nil
}

func (fake *fakeTransferControl) openTerminalSignalWaiter(_ context.Context, _ string) (*TransferTerminalSignalWaiter, error) {
	return &TransferTerminalSignalWaiter{closed: make(chan struct{})}, nil
}

func (fake *fakeTransferControl) registerRecoveryLease(lease *recoveryLease) {
	if fake.recoveryLeases == nil {
		fake.recoveryLeases = make(map[string]*recoveryLease)
	}
	fake.recoveryLeases[lease.transferID] = lease
}
func (fake *fakeTransferControl) releaseRecoveryLease(transferID string) {
	delete(fake.recoveryLeases, transferID)
}
func (fake *fakeTransferControl) continueRecoveryLease(transferID string) {
	fake.continuedRecovery = append(fake.continuedRecovery, transferID)
}

func (fake *fakeTransferControl) close() {}

func TestClientUsesNATSControlForLifecycle(t *testing.T) {
	client := NewClient(WithNATSURL("nats://127.0.0.1:4222"), WithAPIKey("b1m_go"))
	fake := &fakeTransferControl{}
	client.control = fake

	created, err := client.CreateTransfer(context.Background(), TransferCreateRequest{
		Sources:      []SourceConfig{{"type": "http", "url": "https://source.example/file.bin"}},
		Destinations: []DestConfig{{"type": "http", "url": "https://dest.example/file.bin"}},
		TotalSize:    10_485_760,
		ChunkSize:    5_242_880,
		TestMode:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created.Success || created.TransferID == "" {
		t.Fatalf("unexpected create response: %+v", created)
	}

	if _, err := client.DistributeTransfer(context.Background(), created.TransferID); err != nil {
		t.Fatal(err)
	}
	status, err := client.WaitForTransfer(context.Background(), created.TransferID, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "completed" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if _, err := client.CancelTransfer(context.Background(), created.TransferID); err != nil {
		t.Fatal(err)
	}

	want := []string{"transfer.create", "transfer.distribute", "transfer.status", "transfer.cancel"}
	if got := callTypes(fake.calls); !sameStrings(got, want) {
		t.Fatalf("message types mismatch: got %v want %v", got, want)
	}
	if fake.calls[0].payload["chunk_size"] == nil {
		t.Fatalf("create payload omitted chunk_size: %+v", fake.calls[0].payload)
	}
}

func TestIdempotentPrepareReusesRouteGeneration(t *testing.T) {
	client := NewClient(WithNATSURL("nats://127.0.0.1:4222"), WithAPIKey("b1m_go"))
	fake := &fakeTransferControl{}
	client.control = fake

	for i := 0; i < 2; i++ {
		if _, err := client.PrepareTransfer(
			context.Background(),
			[]PreparedHTTPSource{{SourceID: "src_0", Type: "http", URL: "https://source.example/file.bin", Size: 4096}},
			[]PreparedDestination{{DestinationID: "dst_0", Provider: "http", Mode: "http_chunks", LogicalPrefix: "imports/file.bin"}},
			"",
			false,
			"",
			"",
			"studio-step-retry",
		); err != nil {
			t.Fatal(err)
		}
	}

	if len(fake.calls) != 2 {
		t.Fatalf("prepare calls = %d", len(fake.calls))
	}
	if fake.calls[0].transferID != fake.calls[1].transferID {
		t.Fatalf("transferID mismatch: %s != %s", fake.calls[0].transferID, fake.calls[1].transferID)
	}
	if fake.calls[0].payload["route_generation_id"] != fake.calls[1].payload["route_generation_id"] {
		t.Fatalf("route_generation_id mismatch: %v != %v", fake.calls[0].payload["route_generation_id"], fake.calls[1].payload["route_generation_id"])
	}
	if fake.calls[0].idempotencyKey != fake.calls[1].idempotencyKey {
		t.Fatalf("idempotency key mismatch: %s != %s", fake.calls[0].idempotencyKey, fake.calls[1].idempotencyKey)
	}
}

func TestClientChunksSignedURLAttachmentOverNATSControl(t *testing.T) {
	client := NewClient(WithNATSURL("nats://127.0.0.1:4222"), WithAPIKey("b1m_go"))
	fake := &fakeTransferControl{}
	client.control = fake

	prepared, err := client.PrepareTransfer(
		context.Background(),
		[]PreparedHTTPSource{{SourceID: "src_0", Type: "http", URL: "https://source.example/file.bin", Size: 4096}},
		[]PreparedDestination{{DestinationID: "dst_0", Provider: "http", Mode: "http_chunks", LogicalPrefix: "imports/file.bin"}},
		"",
		false,
		"",
		"11111111-1111-4111-8111-111111111111",
	)
	if err != nil {
		t.Fatal(err)
	}
	routes := make([]SignedChunkRoute, 5_120)
	for index := range routes {
		routes[index] = SignedChunkRoute{
			SourceID: "src_0", DestinationID: "dst_0", ChunkIndex: index,
			SourceURL: "https://source.example/file.bin", DestURL: "https://dest.example/file.bin.part",
			SourceOffset: int64(index * 2048), ChunkSize: 2048,
		}
	}
	recovery := ManualRouteRecovery{
		PlanFingerprint:    prepared.PlanFingerprint,
		CoordinateChecksum: prepared.CoordinateChecksum,
		Regenerate: func(_ context.Context, _ string) ([]SignedChunkRoute, []MultipartGroupManifest, string, error) {
			return routes, nil, "", nil
		},
	}
	attached, err := client.AttachSignedURLs(context.Background(), prepared.TransferID, routes, []MultipartGroupManifest{}, prepared.TransferKey, "", prepared.RouteGenerationID, recovery)
	if err != nil {
		t.Fatal(err)
	}
	if !attached.Success {
		t.Fatalf("unexpected attach response: %+v", attached)
	}
	if !fake.recoveryPresentAtRouteBegin {
		t.Fatal("recovery lease was not registered before route-stream begin")
	}

	want := []string{
		"transfer.prepare",
		"transfer.route_stream.begin",
		"transfer.route_stream.batch",
		"transfer.route_stream.batch",
		"transfer.route_stream.batch",
		"transfer.route_stream.complete",
	}
	if got := callTypes(fake.calls); !sameStrings(got, want) {
		t.Fatalf("message types mismatch: got %v want %v", got, want)
	}
	if fake.calls[1].payload["total_routes"] != 5_120 {
		t.Fatalf("unexpected begin payload: %+v", fake.calls[1].payload)
	}
	wantBatchSizes := []any{2_048, 2_048, 1_024}
	for index, wantSize := range wantBatchSizes {
		if fake.calls[2+index].payload["route_count"] != wantSize {
			t.Fatalf("unexpected route batch %d size: got=%v want=%v", index, fake.calls[2+index].payload["route_count"], wantSize)
		}
	}
}

func TestRouteStreamInterruptionsRetainRecovery(t *testing.T) {
	transferID := "11111111-1111-4111-8111-111111111111"
	routes := []SignedChunkRoute{{
		SourceID: "src_0", DestinationID: "dst_0", ChunkIndex: 0, DeliveryIndex: 0,
		SourceURL: "https://source.example/file.bin", DestURL: "https://dest.example/file.bin.part0",
		SourceOffset: 0, ChunkSize: 512,
	}}
	recovery := ManualRouteRecovery{
		PlanFingerprint:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CoordinateChecksum: "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
		Regenerate: func(context.Context, string) ([]SignedChunkRoute, []MultipartGroupManifest, string, error) {
			return routes, nil, "", nil
		},
	}
	for _, boundary := range []string{"transfer.route_stream.begin", "transfer.route_stream.batch"} {
		t.Run(boundary, func(t *testing.T) {
			client := NewClient(WithNATSURL("nats://127.0.0.1:4222"), WithAPIKey("b1m_go"))
			fake := &fakeTransferControl{failOnceAt: boundary}
			client.control = fake
			_, err := client.AttachSignedURLs(context.Background(), transferID, routes, nil, "", "", "22222222-2222-4222-8222-222222222222", recovery)
			var pending *RouteRecoveryPendingError
			if !errors.As(err, &pending) || pending.TransferID != transferID || pending.Cause == nil || pending.Cause.Error() != "connection closed during route streaming" {
				t.Fatalf("unexpected route interruption result: %v", err)
			}
			if !fake.recoveryPresentAtRouteBegin {
				t.Fatal("recovery lease was not registered before route-stream begin")
			}
			if len(fake.continuedRecovery) != 1 || fake.continuedRecovery[0] != transferID {
				t.Fatalf("foreground interruption did not continue recovery: %v", fake.continuedRecovery)
			}
			if fake.recoveryLeases[transferID] == nil {
				t.Fatal("recoverable route interruption released the lease")
			}
		})
	}
}

func TestRuntimeStateLossRetainsRouteRecovery(t *testing.T) {
	for _, status := range []int{404, 409} {
		if !isRecoverableRouteStreamError(&lifecycleRequestError{status: status}) {
			t.Fatalf("runtime state-loss status %d did not retain recovery", status)
		}
	}
	if isRecoverableRouteStreamError(&lifecycleRequestError{status: 400}) {
		t.Fatal("ordinary route validation failure incorrectly retained recovery")
	}
}

func TestCompactSignedRoutesReferencesMultipartGroupManifest(t *testing.T) {
	routes := []SignedChunkRoute{
		{
			SourceID: "src", DestinationID: "dst", ChunkIndex: 0,
			SourceURL: "https://source.example/file", DestURL: "https://dest.example/part-1",
			ChunkSize: 512, Metadata: map[string]any{
				"multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin",
				"bucket": "dest-bucket", "part_number": 1, "delivery_index": 0,
			},
		},
		{
			SourceID: "src", DestinationID: "dst", ChunkIndex: 1, DeliveryIndex: 1,
			SourceURL: "https://source.example/file", DestURL: "https://dest.example/part-2",
			SourceOffset: 512, ChunkSize: 512, Metadata: map[string]any{
				"multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin",
				"bucket": "dest-bucket", "part_number": 4, "delivery_index": 1,
			},
		},
	}
	batch := compactSignedRoutes(routes)
	if _, repeated := batch["multipart_groups"]; repeated {
		t.Fatal("compact route batch repeated multipart group manifest data")
	}
	entries := batch["routes"].([]map[string]any)
	if len(entries) != 2 || entries[0]["multipart_group_id"] != "group" || entries[0]["metadata"].(map[string]any)["part_number"] != 1 {
		t.Fatalf("unexpected route attempt metadata: %#v", entries)
	}
	if _, repeated := entries[0]["metadata"].(map[string]any)["bucket"]; repeated {
		t.Fatal("shared multipart metadata was repeated in the route entry")
	}
}

func TestMultipartUploadRouteRequiresGroupIdentity(t *testing.T) {
	routes := []SignedChunkRoute{{
		SourceID: "src", DestinationID: "dst", ChunkIndex: 0,
		Metadata: map[string]any{"upload_id": "upload", "final_object_key": "file.bin", "part_number": 1},
	}}
	if err := validateSignedRouteManifestContract("transfer", routes, nil); err == nil || !strings.Contains(err.Error(), "missing multipart_group_id") {
		t.Fatalf("expected missing multipart group rejection, got %v", err)
	}
}

func callTypes(calls []fakeTransferCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.messageType)
	}
	return out
}

func sameStrings(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRuntimeEpochRecoveryCoalescesAndClearsSecrets(t *testing.T) {
	control := newNatsControl("b1m_recovery", "nats://127.0.0.1:4222", "dev", 1)
	transferID := "11111111-1111-4111-8111-111111111111"
	disposed := 0
	lease := &recoveryLease{
		transferID:         transferID,
		shardID:            0,
		planFingerprint:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		coordinateChecksum: "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
		replayRoutes:       func(context.Context, string) error { return nil },
		dispose:            func() { disposed++ },
	}
	control.recoveryLeases[transferID] = lease
	control.recoveryRunning[transferID] = true
	control.runtimeEpochs[0] = [2]string{"runtime-a", "transport-a"}
	control.observeRuntimeEpochs(0, map[string]any{
		"runtime_epoch":   "runtime-b",
		"transport_epoch": "transport-b",
	})
	expectedEpoch := [2]string{"runtime-b", "transport-b"}
	if got := control.recoveryRequested[transferID]; got != expectedEpoch {
		t.Fatalf("running recovery did not retain the newest epoch: %#v", got)
	}
	if sdkMaxReconnects >= 0 {
		t.Fatalf("SDK reconnects must remain unbounded, got %d", sdkMaxReconnects)
	}
	control.releaseRecoveryLease(transferID)
	if disposed != 1 {
		t.Fatalf("retained provider secrets were disposed %d times", disposed)
	}
	control.close()
}
