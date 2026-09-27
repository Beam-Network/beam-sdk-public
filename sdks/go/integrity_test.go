package beamnetworksdk

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func challengeStatus(transferID string, chunks []map[string]any) map[string]any {
	return map[string]any{
		"transfer_id": transferID, "status": "in_progress", "error_message": nil,
		"integrity_audit_challenge": map[string]any{
			"audit_id": "audit-1", "transfer_id": transferID, "requested_at": "2026-09-25T00:00:00.000Z",
			"range_bytes": 512, "chunks": chunks,
		},
	}
}

func TestIntegrityGrantFailuresAreVisibleWithoutSignedURLs(t *testing.T) {
	transferID := "c61fdfd2-3c09-44c7-bbb4-048a9d0e9c7c"
	fake := &fakeTransferControl{statusResponse: challengeStatus(transferID, []map[string]any{})}
	client := newTestClient(t, fake)
	status, err := client.TransferStatus(context.Background(), transferID)
	if err != nil || status.IntegrityAuditSubmissionError != "integrity audit signer unavailable" {
		t.Fatalf("status = %+v, %v", status, err)
	}
	client.signerMu.Lock()
	client.recoverySigners = map[string]*transferSigners{transferID: {integrity: func(context.Context, *IntegrityAuditChallenge) error {
		return errors.New("failed to sign https://private.example/?X-Amz-Signature=secret")
	}}}
	client.signerMu.Unlock()
	status, err = client.TransferStatus(context.Background(), transferID)
	if err != nil || status.IntegrityAuditSubmissionError != "Error" {
		t.Fatalf("sensitive failure must be summarized by name, got %q", status.IntegrityAuditSubmissionError)
	}
	long := strings.Repeat("a", 300)
	if summary := integrityAuditErrorSummary(errors.New(long)); summary != long[:200] {
		t.Fatalf("summary must cap at 200 characters, got %d", len(summary))
	}
}

func TestIntegrityGrantSubmissionsCoalescePerAudit(t *testing.T) {
	transferID := "c61fdfd2-3c09-44c7-bbb4-048a9d0e9c7c"
	fake := &fakeTransferControl{statusResponse: challengeStatus(transferID, []map[string]any{})}
	client := newTestClient(t, fake)
	var calls atomic.Int32
	release := make(chan struct{})
	client.recoverySigners = map[string]*transferSigners{transferID: {integrity: func(context.Context, *IntegrityAuditChallenge) error {
		calls.Add(1)
		<-release
		return nil
	}}}
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if status, err := client.TransferStatus(context.Background(), transferID); err != nil || status.IntegrityAuditSubmissionError != "" {
				t.Errorf("status = %+v, %v", status, err)
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wait.Wait()
	if _, err := client.TransferStatus(context.Background(), transferID); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("one audit must be signed once, got %d", got)
	}
}

func TestS3SourceMetadataPinsIntegrityGrants(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	prepared, err := client.PrepareProviderTransfer(context.Background(),
		[]ProviderSource{r2Source(storage.URL)},
		[]ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		"", false, 0, false, "")
	if err != nil {
		t.Fatal(err)
	}
	source := fake.callsOf("transfer.prepare")[0].payload["sources"].([]PreparedHTTPSource)[0].Metadata
	for key, want := range map[string]any{
		"driver": "s3-compatible", "endpoint_url": storage.URL, "content_length": int64(1024),
		"etag": `"source-etag"`, "last_modified": "2026-10-21T07:28:00.000Z", "version_id": "source-version",
		"region": "auto", "bucket": "source", "key": "input.bin",
	} {
		if source[key] != want {
			t.Fatalf("source metadata %s = %#v, want %#v", key, source[key], want)
		}
	}
	destination := fake.callsOf("transfer.prepare")[0].payload["destinations"].([]PreparedDestination)[0].Metadata
	if destination["driver"] != "s3-compatible" || destination["endpoint_url"] != storage.URL {
		t.Fatalf("destination metadata = %#v", destination)
	}

	fake.mu.Lock()
	fake.statusResponse = challengeStatus(prepared.TransferID, []map[string]any{{
		"challenge_id": "challenge-1", "task_id": "task-1", "attempt_id": nil,
		"orchestrator_id": "orchestrator-1", "orchestrator_hotkey": "hotkey", "worker_id": "worker-1",
		"source_id": "src_0", "destination_id": "dst_0", "route_chunk_index": 0, "delivery_index": 0,
		"source_offset": 0, "destination_offset": 0, "range_length": 512,
		"final_object_key": "out/file.bin", "final_object_etag": `"final-etag"`,
	}})
	fake.mu.Unlock()
	status, err := client.TransferStatus(context.Background(), prepared.TransferID)
	if err != nil || status.IntegrityAuditSubmissionError != "" {
		t.Fatalf("status = %+v, %v", status, err)
	}
	grants := fake.callsOf("transfer.integrity_audit_grants")
	if len(grants) != 1 {
		t.Fatalf("grant submissions = %d", len(grants))
	}
	chunk := grants[0].payload["chunks"].([]map[string]any)[0]
	if _, leaked := chunk["worker_id"]; leaked {
		t.Fatal("grant chunks must omit orchestrator and worker identity")
	}
	sourceGrant := chunk["source"].(map[string]any)
	signed, err := url.Parse(sourceGrant["url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if signed.Query().Get("versionId") != "source-version" || !strings.Contains(signed.Query().Get("X-Amz-SignedHeaders"), "if-match") {
		t.Fatalf("source grant must pin version and ETag: %s", signed)
	}
	if sourceGrant["headers"].(map[string]string)["If-Match"] != `"source-etag"` {
		t.Fatalf("source grant headers = %#v", sourceGrant["headers"])
	}
	destinationGrant := chunk["destination"].(map[string]any)
	if destinationGrant["headers"].(map[string]string)["If-Match"] != `"final-etag"` {
		t.Fatalf("destination grant headers = %#v", destinationGrant["headers"])
	}
}
