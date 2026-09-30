package beamnetworksdk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestResumeProviderTransferReusesUploadsAndFencesReplacedOwners(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake, WithRouteSigningConcurrency(4))
	transferID := "77777777-7777-4777-8777-777777777777"
	groupID := transferID + ":dst_0:src_0:out/file.bin"
	groups := []ProviderMultipartGroupIdentity{{
		TransferID: transferID, MultipartGroupID: groupID, SourceID: "src_0", DestinationID: "dst_0",
		ObjectKey: "out/file.bin", UploadID: "upload-existing", ExpectedObjectSize: 1024, ExpectedPartCount: 1,
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
	}}
	ownership, relinquish := context.WithCancelCause(context.Background())
	transfer := ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		Ownership:    ownership,
	}
	onPreparedCalls := 0
	first := transfer
	first.OnPrepared = func(context.Context, *TransferPrepareResponse) error {
		onPreparedCalls++
		if len(fake.callsOf("transfer.route_stream.begin")) != 0 {
			t.Error("OnPrepared must run before routes stream")
		}
		return nil
	}
	prepared, err := client.ResumeProviderTransfer(context.Background(), ProviderTransferResumeOptions{ProviderTransferOptions: first, TransferID: transferID, MultipartGroups: groups})
	if err != nil {
		t.Fatal(err)
	}
	heads, creates, _ := storage.counts()
	if prepared.TransferID != transferID || heads != 1 || creates != 0 || onPreparedCalls != 1 {
		t.Fatalf("resume: id=%s heads=%d creates=%d onPrepared=%d", prepared.TransferID, heads, creates, onPreparedCalls)
	}
	if signer := fake.signer(); signer == nil || signer.transferID != transferID || fake.lease(transferID) == nil {
		t.Fatal("resume must serve route recovery signing and hold a recovery lease")
	}

	second := newTestClient(t, fake, WithRouteSigningConcurrency(4))
	if _, err := second.ResumeProviderTransfer(context.Background(), ProviderTransferResumeOptions{ProviderTransferOptions: transfer, TransferID: transferID, MultipartGroups: groups}); err != nil {
		t.Fatal(err)
	}
	prepares := fake.callsOf("transfer.prepare")
	if len(prepares) != 2 || prepares[0].idempotencyKey == prepares[1].idempotencyKey ||
		!strings.HasPrefix(prepares[0].idempotencyKey, "transfer:"+transferID+":prepare:resume:") ||
		prepares[0].payload["route_generation_id"] == prepares[1].payload["route_generation_id"] || prepares[0].payload["transfer_id"] != transferID {
		t.Fatalf("each resume must re-prepare the explicit transfer with a fresh request and generation: %+v", prepares)
	}

	signer := fake.signer()
	reply, err := signer.handler(context.Background(), routeRecoverySignRequest{
		TransferID: transferID, RouteGenerationID: "recover-1",
		Chunks: []routeRecoverySignChunk{{
			SourceID: "src_0", DestinationID: "dst_0", ChunkIndex: 0, DeliveryIndex: 0, SourceOffset: 0, ChunkSize: 1024,
			LogicalAttemptIndex: 1, AttemptSlot: 0, PartNumber: 1, RouteGenerationID: "recover-1",
			MultipartGroupID: groupID, FinalObjectKey: "out/file.bin", UploadID: "upload-existing",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.TransferID != transferID || reply.RouteGenerationID != "recover-1" || len(reply.ChunkRoutes) != 1 {
		t.Fatalf("reply = %+v", reply)
	}
	route := reply.ChunkRoutes[0]
	if route.Metadata["upload_id"] != "upload-existing" || route.Metadata["multipart_group_id"] != groupID || route.Metadata["part_number"] != 1 || route.Metadata["attempt_slot"] != 0 || route.Metadata["logical_attempt_index"] != 1 || !strings.Contains(route.DestURL, "uploadId=upload-existing") || !strings.Contains(route.DestURL, "partNumber=1") {
		t.Fatalf("recovery route = %+v", route)
	}
	// The route recovery signer applies Runtime's multipart recovery directive
	// to the signed route: a staged upload targets its own staging object and
	// carries the UploadPartCopy grant into the original upload.
	staged, err := signer.handler(context.Background(), routeRecoverySignRequest{
		TransferID: transferID, RouteGenerationID: "recover-staged",
		Chunks: []routeRecoverySignChunk{{
			SourceID: "src_0", DestinationID: "dst_0", ChunkIndex: 0, DeliveryIndex: 0, SourceOffset: 0, ChunkSize: 1024,
			LogicalAttemptIndex: 2, AttemptSlot: 0, PartNumber: 1, RouteGenerationID: "recover-staged",
			MultipartGroupID: groupID, FinalObjectKey: "out/file.bin", UploadID: "upload-existing",
			Recovery: &multipartRecoveryRequest{Operation: "upload", Mode: "staged", AttemptID: "44444444-4444-4444-8444-444444444444"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stagedRoute := staged.ChunkRoutes[0]
	stage, _ := stagedRoute.Metadata["recovery_staging"].(map[string]any)
	if stage == nil || strings.Contains(stagedRoute.DestURL, "uploadId=") || !strings.Contains(stagedRoute.DestURL, "out/file.bin.beam-recovery/"+transferID+"/") ||
		!strings.Contains(stage["copy_url"].(string), "uploadId=upload-existing") || stagedRoute.Metadata["upload_id"] != "upload-existing" || stagedRoute.Metadata["part_number"] != 1 {
		t.Fatalf("staged recovery route = %+v", stagedRoute)
	}
	// v7 reserves one part per chunk: a non-zero slot or a shifted part is rejected.
	for _, mismatch := range []struct{ slot, part int }{{1, 1}, {1, 2}, {0, 2}} {
		if _, err := signer.handler(context.Background(), routeRecoverySignRequest{
			TransferID: transferID, RouteGenerationID: "recover-2",
			Chunks: []routeRecoverySignChunk{{SourceID: "src_0", DestinationID: "dst_0", ChunkSize: 1024, AttemptSlot: mismatch.slot, PartNumber: mismatch.part, RouteGenerationID: "recover-2", MultipartGroupID: groupID, FinalObjectKey: "out/file.bin", UploadID: "upload-existing"}},
		}); err == nil || !strings.Contains(err.Error(), "slot mapping mismatch") {
			t.Fatalf("attempt slot %d part %d must be rejected, got %v", mismatch.slot, mismatch.part, err)
		}
	}
	if got := len(fake.callsOf("transfer.route_stream.complete")); got != 2 {
		t.Fatalf("route stream completes = %d", got)
	}
	lease := fake.lease(transferID)
	if err := lease.replayRoutes(context.Background(), "33333333-3333-4333-8333-333333333333"); err != nil {
		t.Fatal(err)
	}
	if _, creates, _ := storage.counts(); creates != 0 || len(fake.callsOf("transfer.route_stream.complete")) != 3 {
		t.Fatal("route replay must reuse the existing upload")
	}

	invalidObject := groups[0]
	invalidObject.ObjectKey = "different"
	emptyUpload := groups[0]
	emptyUpload.UploadID = " "
	for _, invalid := range [][]ProviderMultipartGroupIdentity{nil, {}, {groups[0], groups[0]}, {invalidObject}, {emptyUpload}} {
		plain := transfer
		plain.Ownership = nil
		_, err := second.ResumeProviderTransfer(context.Background(), ProviderTransferResumeOptions{ProviderTransferOptions: plain, TransferID: transferID, MultipartGroups: invalid})
		if err == nil || !strings.Contains(err.Error(), "provider_multipart_recovery") {
			t.Fatalf("invalid groups %v must be rejected, got %v", invalid, err)
		}
	}
	if _, creates, _ := storage.counts(); creates != 0 {
		t.Fatal("invalid retained state must never create another upload")
	}

	replaced := errors.New("owner replaced")
	relinquish(replaced)
	if _, err := signer.handler(context.Background(), routeRecoverySignRequest{TransferID: transferID, RouteGenerationID: "later"}); !errors.Is(err, replaced) {
		t.Fatalf("a replaced owner must stop recovery signing, got %v", err)
	}
	if err := lease.replayRoutes(context.Background(), "44444444-4444-4444-8444-444444444444"); !errors.Is(err, replaced) {
		t.Fatalf("a replaced owner must stop route replay, got %v", err)
	}
	if len(fake.callsOf("transfer.cancel")) != 0 {
		t.Fatal("ownership loss must never cancel the transfer")
	}
	if _, err := client.ResumeProviderTransfer(context.Background(), ProviderTransferResumeOptions{TransferID: "../escape"}); err == nil {
		t.Fatal("resume must validate the transfer id")
	}
}

func TestRouteRecoveryReplyValidatesEnvelope(t *testing.T) {
	control := newNatsControl("b1m_test_key", "nats://127.0.0.1:4222", "prod", 1)
	defer control.close()
	transferID := "transfer-1"
	handled := 0
	var recovery *multipartRecoveryRequest
	handler := func(_ context.Context, request routeRecoverySignRequest) (*routeRecoverySignReply, error) {
		handled++
		recovery = request.Chunks[0].Recovery
		return &routeRecoverySignReply{TransferID: request.TransferID, RouteGenerationID: request.RouteGenerationID, ChunkRoutes: []SignedChunkRoute{{SourceID: "src_0", DestinationID: "dst_0", DestURL: "https://dest.example/part"}}}, nil
	}
	envelope := func(producer string) []byte {
		data, err := marshalMsgpack(map[string]any{
			"message_id": "message-1", "schema_version": transferClientSchemaVersion, "environment": "prod",
			"key_prefix": control.keyPrefix, "transfer_id": transferID, "message_type": routeRecoverySignMessageType,
			"request_id": "request-1", "occurred_at": "2026-09-25T00:00:00Z", "producer": producer,
			"payload": map[string]any{"transfer_id": transferID, "route_generation_id": "generation-1", "chunks": []any{map[string]any{"source_id": "src_0", "attempt_slot": 0, "recovery": map[string]any{"operation": "upload", "mode": "staged", "attempt_id": "22222222-2222-4222-8222-222222222222", "etag": "abc"}}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	decode := func(data []byte) map[string]any {
		var reply map[string]any
		if err := msgpack.Unmarshal(data, &reply); err != nil {
			t.Fatal(err)
		}
		return reply
	}
	ok := decode(control.routeRecoveryReply(context.Background(), transferID, envelope("transfer-runtime"), handler))
	if ok["ok"] != true || intValue(ok["status"]) != 200 || ok["message_id"] != "message-1" || ok["request_id"] != "request-1" || ok["producer"] != "sdk" || ok["message_type"] != routeRecoverySignMessageType {
		t.Fatalf("reply = %#v", ok)
	}
	if routes := ok["payload"].(map[string]any)["chunk_routes"].([]any); len(routes) != 1 {
		t.Fatalf("reply routes = %#v", routes)
	}
	if recovery == nil || recovery.Operation != "upload" || recovery.Mode != "staged" || recovery.AttemptID != "22222222-2222-4222-8222-222222222222" || recovery.ETag != "abc" {
		t.Fatalf("recovery directive was not decoded: %#v", recovery)
	}
	rejected := decode(control.routeRecoveryReply(context.Background(), transferID, envelope("sdk"), handler))
	if rejected["ok"] != false || intValue(rejected["status"]) != 500 || rejected["error"].(map[string]any)["code"] != "route_recovery_sign_failed" {
		t.Fatalf("mismatched envelope reply = %#v", rejected)
	}
	garbage := decode(control.routeRecoveryReply(context.Background(), transferID, []byte{0xc1}, handler))
	if garbage["ok"] != false || garbage["message_id"] != "unknown" || garbage["transfer_id"] != transferID {
		t.Fatalf("undecodable request reply = %#v", garbage)
	}
	if handled != 1 {
		t.Fatalf("handler must only run for valid envelopes, ran %d times", handled)
	}
	if subject := control.routeRecoverySignSubject(transferID); subject != "beam.transfer.client.prod.sdk.b1m_test_key.transfer.transfer-1.route_recovery_sign" {
		t.Fatalf("subject = %s", subject)
	}
}

func TestPlanTransferValidatesCompactPlan(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	source, err := PrepareProviderSourceForPlan(context.Background(), r2Source(storage.URL), ProviderSourceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if source.URL != "" || source.Size != 1024 || source.Metadata["etag"] != `"source-etag"` {
		t.Fatalf("planning source = %+v", source)
	}
	planned, err := client.PlanTransfer(context.Background(), TransferPlanRequest{
		Sources:      []PlanningHTTPSource{source},
		Destinations: []PreparedDestination{{DestinationID: "dst_0", Provider: "http", Mode: "http_chunks", LogicalPrefix: "out/file.bin"}},
	})
	if err != nil || !planned.Success || planned.PlanDescriptor.Version != "compact-transfer-plan/v1" {
		t.Fatalf("plan = %+v, %v", planned, err)
	}
	call := fake.callsOf("transfer.plan")[0]
	if call.transferID != "" || call.idempotencyKey != "" || call.payload["signed_url_flow"] != SignedURLFlowCanonical {
		t.Fatalf("plan call = %+v", call)
	}
}
