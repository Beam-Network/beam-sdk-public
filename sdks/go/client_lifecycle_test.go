package beamnetworksdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLifecycleIdempotencyKeysFollowTransferDerivations(t *testing.T) {
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	created, err := client.CreateRawTransfer(context.Background(), TransferCreateRequest{
		Sources:        []SourceConfig{{"type": "http", "url": "https://source.example/file.bin"}},
		Destinations:   []DestConfig{{"type": "http", "url": "https://dest.example/file.bin"}},
		TotalSize:      1024,
		IdempotencyKey: "caller-step",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.TransferID != transferIDForIdempotencyKey("caller-step") || fake.calls[0].idempotencyKey != "transfer:"+created.TransferID+":create" {
		t.Fatalf("create key = %q for %s", fake.calls[0].idempotencyKey, created.TransferID)
	}
	prepared, err := client.PrepareTransferWithRequest(context.Background(), TransferPrepareRequest{
		Sources:        []PreparedHTTPSource{{SourceID: "src_0", Type: "http", URL: "https://source.example/file.bin", Size: 4096}},
		Destinations:   []PreparedDestination{{DestinationID: "dst_0", Provider: "http", Mode: "http_chunks"}},
		IdempotencyKey: "caller-step",
	})
	if err != nil {
		t.Fatal(err)
	}
	prepareKey := "transfer:" + prepared.TransferID + ":prepare"
	call := fake.callsOf("transfer.prepare")[0]
	if call.idempotencyKey != prepareKey || call.payload["route_generation_id"] != stableIDFromIdentity("beam-route-generation:"+prepareKey) {
		t.Fatalf("prepare key=%q generation=%v", call.idempotencyKey, call.payload["route_generation_id"])
	}
	for _, key := range []string{"chunk_size", "provider_part_size"} {
		if _, sent := call.payload[key]; sent {
			t.Fatalf("prepare payload must not carry %s: %+v", key, call.payload)
		}
	}
}

func TestCancelTransferReleasesLocalRecoveryOnAnyReply(t *testing.T) {
	fake := &fakeTransferControl{cancelResponse: map[string]any{"success": false, "message": "already terminal"}}
	client := newTestClient(t, fake)
	transferID := "11111111-1111-4111-8111-111111111111"
	disposed := false
	fake.registerRecoveryLease(&recoveryLease{transferID: transferID, dispose: func() { disposed = true }})
	client.recoverySigners = map[string]*transferSigners{transferID: {integrity: func(context.Context, *IntegrityAuditChallenge) error { return nil }}}
	result, err := client.CancelTransfer(context.Background(), transferID)
	if err != nil || result.Success {
		t.Fatalf("cancel = %+v, %v", result, err)
	}
	if !disposed || fake.lease(transferID) != nil || client.recoverySigners[transferID] != nil {
		t.Fatal("cancel must release the lease and integrity signer once BeamCore answers")
	}
}

func TestAttachSignedURLsSendsOneManifestRequestPerGroup(t *testing.T) {
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	transferID := "11111111-1111-4111-8111-111111111111"
	manifest := func(groupID string, destinationID string) MultipartGroupManifest {
		return MultipartGroupManifest{
			MultipartGroupID: groupID, SourceID: "src_0", DestinationID: destinationID, FinalObjectKey: "file.bin",
			UploadID: "upload-" + groupID, ExpectedObjectSize: 1024, ExpectedPartCount: 1, MaxPartNumber: 1,
			CompleteURL: "https://dest.example/complete", AbortURL: "https://dest.example/abort",
			ListPageURLs: []string{"https://dest.example/list"}, FinalHeadURL: "https://dest.example/head",
			FinalObjectMetadata: map[string]string{"beam-transfer-id": transferID}, URLsExpiresAt: "2026-07-16T00:00:00.000Z",
		}
	}
	groups := []MultipartGroupManifest{manifest("group-a", "dst_0"), manifest("group-b", "dst_1")}
	routes := []SignedChunkRoute{
		{SourceID: "src_0", DestinationID: "dst_0", ChunkIndex: 0, DeliveryIndex: intPtr(0), Metadata: map[string]any{"multipart_group_id": "group-a", "upload_id": "upload-group-a", "final_object_key": "file.bin", "part_number": 1}},
		{SourceID: "src_0", DestinationID: "dst_1", ChunkIndex: 0, DeliveryIndex: intPtr(1), Metadata: map[string]any{"multipart_group_id": "group-b", "upload_id": "upload-group-b", "final_object_key": "file.bin", "part_number": 1}},
	}
	recovery := ManualRouteRecovery{
		PlanFingerprint: strings.Repeat("a", 64), CoordinateChecksum: "sha256-xor-v1:2:" + strings.Repeat("0", 64),
		Regenerate: func(context.Context, string) ([]SignedChunkRoute, []MultipartGroupManifest, string, error) {
			return routes, groups, "", nil
		},
	}
	if _, err := client.AttachSignedURLs(context.Background(), transferID, routes, groups, "", "", "22222222-2222-4222-8222-222222222222", recovery); err != nil {
		t.Fatal(err)
	}
	manifests := fake.callsOf("transfer.route_stream.manifest")
	if len(manifests) != 2 {
		t.Fatalf("manifest requests = %d", len(manifests))
	}
	for _, call := range manifests {
		if sent := call.payload["groups"].([]MultipartGroupManifest); len(sent) != 1 {
			t.Fatalf("each manifest request must carry one group, got %d", len(sent))
		}
	}
}

func TestWaitForTransferValidatesAndFinalizesHuggingFace(t *testing.T) {
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	transferID := "11111111-1111-4111-8111-111111111111"
	for _, options := range []WaitForTransferOptions{{Timeout: -1}, {PollInterval: -1}, {MaxPollInterval: -1}} {
		if _, err := client.WaitForTransferWithOptions(context.Background(), transferID, options); err == nil {
			t.Fatalf("negative wait option must fail: %+v", options)
		}
	}
	commits := 0
	hub := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/commit/main") {
			commits++
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"commitOid":"cafe"}`))
	}))
	defer hub.Close()
	client.huggingFaceUploads = map[string][]*huggingFaceUploadState{transferID: {{
		Config: huggingFaceConfig{RepoID: "acme/net", Path: "out/file.bin", RepoType: "model", Revision: "main", Token: huggingFaceTestToken, Endpoint: hub.URL},
		OID:    strings.Repeat("a", 64), Size: 4096,
	}}}
	status, err := client.WaitForTransferWithOptions(context.Background(), transferID, WaitForTransferOptions{Timeout: time.Second, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond})
	if err != nil || status.Status != "completed" {
		t.Fatalf("wait = %+v, %v", status, err)
	}
	if commits != 1 {
		t.Fatalf("completed wait must commit Hugging Face uploads, commits=%d", commits)
	}
}

func TestS3AddressingIsConsistentAcrossSigners(t *testing.T) {
	ctx := context.Background()
	aws := S3ProviderDestination{Bucket: "bucket", Key: "k", Region: "eu-west-1", AccessKeyID: "ak", SecretAccessKey: "sk"}
	r2 := R2ProviderDestination{Bucket: "bucket", Key: "k", AccountID: "acct", AccessKeyID: "ak", SecretAccessKey: "sk"}
	forced := S3ProviderDestination{Bucket: "bucket", Key: "k", EndpointURL: "https://minio.example.com", ForcePathStyle: Bool(true), AccessKeyID: "ak", SecretAccessKey: "sk"}
	for _, test := range []struct {
		destination ProviderDestination
		prefix      string
	}{
		{aws, "https://bucket.s3.eu-west-1.amazonaws.com/out/file.bin?"},
		{r2, "https://acct.r2.cloudflarestorage.com/bucket/out/file.bin?"},
		{forced, "https://minio.example.com/bucket/out/file.bin?"},
	} {
		head, err := signFinalObjectHead(ctx, test.destination, "out/file.bin", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		complete, err := signCompleteMultipartUpload(ctx, test.destination, "out/file.bin", "upload", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		list, err := signListMultipartUpload(ctx, test.destination, "out/file.bin", "upload", time.Minute, 1000, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, signed := range []string{head, complete, list} {
			if !strings.HasPrefix(signed, test.prefix) {
				t.Fatalf("signed URL %q does not use %q", signed, test.prefix)
			}
		}
	}
	if _, err := prepareProviderDestination(R2ProviderDestination{Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk"}, 0); err == nil || !strings.Contains(err.Error(), "account_id or endpoint_url") {
		t.Fatalf("r2 without an endpoint must fail, got %v", err)
	}
}
