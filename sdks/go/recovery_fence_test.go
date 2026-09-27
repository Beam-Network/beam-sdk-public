package beamnetworksdk

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func fenceResumeOptions(storageURL string, transferID string, ownership context.Context) ProviderTransferResumeOptions {
	return ProviderTransferResumeOptions{
		ProviderTransferOptions: ProviderTransferOptions{
			Sources:      []ProviderSource{r2Source(storageURL)},
			Destinations: []ProviderDestination{r2Destination(storageURL, "out/file.bin")},
			Ownership:    ownership,
		},
		TransferID: transferID,
		MultipartGroups: []ProviderMultipartGroupIdentity{{
			TransferID: transferID, MultipartGroupID: transferID + ":dst_0:src_0:out/file.bin", SourceID: "src_0", DestinationID: "dst_0",
			ObjectKey: "out/file.bin", UploadID: "upload-existing", ExpectedObjectSize: 1024, ExpectedPartCount: 1,
			ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
		}},
	}
}

func (client *Client) hasRecoverySigner(transferID string) bool {
	client.signerMu.Lock()
	defer client.signerMu.Unlock()
	_, ok := client.recoverySigners[transferID]
	return ok
}

// Regression: a fenced owner whose route stream lost ownership released the
// lease and signers by transfer id, which removed the replacement owner's.
func TestFencedOwnerStreamKeepsReplacementLeaseAndSigner(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake, WithRouteSigningConcurrency(4))
	transferID := "88888888-8888-4888-8888-888888888888"
	ownershipA, relinquishA := context.WithCancelCause(context.Background())
	if _, err := client.ResumeProviderTransfer(context.Background(), fenceResumeOptions(storage.URL, transferID, ownershipA)); err != nil {
		t.Fatal(err)
	}
	leaseA := fake.lease(transferID)
	var fired atomic.Bool
	var leaseB *recoveryLease
	var resumeErr error
	fake.mu.Lock()
	fake.beforeRequest = func(messageType string, _ map[string]any) {
		if messageType != "transfer.route_stream.begin" || fired.Swap(true) {
			return
		}
		// Owner B takes over on the same Client while A's replay is streaming.
		relinquishA(errors.New("owner replaced"))
		_, resumeErr = client.ResumeProviderTransfer(context.Background(), fenceResumeOptions(storage.URL, transferID, context.Background()))
		leaseB = fake.lease(transferID)
	}
	fake.mu.Unlock()
	if err := leaseA.replayRoutes(context.Background(), "99999999-9999-4999-8999-999999999999"); err == nil {
		t.Fatal("the fenced owner's replay must fail")
	}
	if resumeErr != nil {
		t.Fatal(resumeErr)
	}
	time.Sleep(50 * time.Millisecond)
	if current := fake.lease(transferID); leaseB == nil || current != leaseB {
		t.Fatalf("the replacement owner's lease was released: current=%p leaseB=%p", current, leaseB)
	}
	if fake.signer() == nil || !client.hasRecoverySigner(transferID) {
		t.Fatal("the replacement owner's recovery signer was stopped")
	}
}

// Regression: ownership lost after the lease registered continued recovery
// instead of releasing the fenced owner's lease.
func TestOwnershipLostAfterLeaseRegistrationReleasesLease(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	ownership, relinquish := context.WithCancelCause(context.Background())
	fake.onServeSigner = func(string) { relinquish(errors.New("owner replaced")) }
	_, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		Ownership:    ownership,
	})
	if err == nil {
		t.Fatal("a relinquished owner must fail")
	}
	fake.mu.Lock()
	continued, leases := len(fake.continuedRecovery), len(fake.recoveryLeases)
	fake.mu.Unlock()
	if continued != 0 || leases != 0 {
		t.Fatalf("a fenced owner must release, not continue: continued=%d leases=%d", continued, leases)
	}
}

// recoveryTestControl answers transfer.resume with route_replay_required and
// records each resume's route generation.
func recoveryTestControl(t *testing.T, resumes chan<- string) *natsControl {
	t.Helper()
	control := controlWithCachedToken("test-token")
	// okReply reports these epochs, so replies never look like an epoch change.
	control.runtimeEpochs[0] = [2]string{"runtime-one", "transport-one"}
	control.requestOverride = func(_ context.Context, _ string, data []byte) ([]byte, error) {
		var envelope map[string]any
		if err := msgpack.Unmarshal(data, &envelope); err != nil {
			return nil, err
		}
		if envelope["message_type"] == "transfer.resume" && resumes != nil {
			resumes <- envelope["payload"].(map[string]any)["route_generation_id"].(string)
		}
		return okReply(t, map[string]any{"recovery": "route_replay_required"}), nil
	}
	t.Cleanup(control.close)
	return control
}

func fenceTestLease(transferID string, replay func(context.Context, string) error) *recoveryLease {
	return &recoveryLease{
		transferID:         transferID,
		planFingerprint:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		coordinateChecksum: "sha256-xor-v1:1:0000000000000000000000000000000000000000000000000000000000000000",
		replayRoutes:       replay,
	}
}

func (c *natsControl) currentLease(transferID string) *recoveryLease {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	return c.recoveryLeases[transferID]
}

// Regression: a fenced owner's failed recovery released the replacement
// owner's lease by transfer id; a replaced lease's recovery was also dropped
// because the old loop still held the running flag.
func TestRecoveryLoopIsFencedByLeaseIdentity(t *testing.T) {
	control := recoveryTestControl(t, nil)
	transferID := "12121212-1212-4121-8121-121212121212"
	replayStarted, releaseA := make(chan struct{}), make(chan struct{})
	replayAReturned := make(chan struct{})
	leaseA := fenceTestLease(transferID, func(context.Context, string) error {
		close(replayStarted)
		<-releaseA
		defer close(replayAReturned)
		return context.Canceled
	})
	control.registerRecoveryLease(leaseA)
	control.continueRecoveryLease(leaseA)
	<-replayStarted

	replayedB := make(chan struct{}, 1)
	var disposedB atomic.Int32
	leaseB := fenceTestLease(transferID, func(context.Context, string) error {
		replayedB <- struct{}{}
		return nil
	})
	leaseB.dispose = func() { disposedB.Add(1) }
	control.registerRecoveryLease(leaseB)
	control.continueRecoveryLease(leaseB)
	select {
	case <-replayedB:
	case <-time.After(3 * time.Second):
		close(releaseA)
		t.Fatal("the replacement lease's recovery was dropped while the old loop ran")
	}
	close(releaseA)
	<-replayAReturned
	time.Sleep(100 * time.Millisecond)
	if control.currentLease(transferID) != leaseB || disposedB.Load() != 0 {
		t.Fatalf("the fenced owner's failure released the replacement lease (disposed=%d)", disposedB.Load())
	}
}

// Regression: a finishing loop cleared the running registration by transfer
// id, wiping the registration of a loop started after it had cleared its own,
// so a third loop could run concurrently with the second.
func TestFinishingRecoveryLoopKeepsNewerLoopRegistration(t *testing.T) {
	resumes := make(chan string, 4)
	control := recoveryTestControl(t, resumes)
	transferID := "13131313-1313-4131-8131-131313131313"
	lease := fenceTestLease(transferID, func(context.Context, string) error { return nil })
	control.registerRecoveryLease(lease)
	staleCtx, staleCancel := context.WithCancel(context.Background())
	stale := &recoveryRun{lease: lease, ctx: staleCtx, cancel: staleCancel}
	control.recoveryMu.Lock()
	newer := control.startRecoveryRunLocked(lease)
	control.recoveryMu.Unlock()
	control.recoverTransfer(stale)
	control.recoveryMu.Lock()
	registered := control.recoveryRunning[transferID]
	control.recoveryMu.Unlock()
	if registered != newer {
		t.Fatal("a finishing loop cleared a newer loop's registration")
	}
	if len(resumes) != 0 {
		t.Fatal("a superseded loop must not send transfer.resume")
	}
	control.recoveryMu.Lock()
	control.startRecoveryRunLocked(lease)
	control.recoveryMu.Unlock()
	if newer.ctx.Err() != nil {
		t.Fatal("a loop for the same lease must be coalesced, not restarted")
	}
	newer.cancel()
}

// Regression: a fenced owner's recovery loop still sent transfer.resume.
func TestFencedLeaseRecoveryNeverSendsResume(t *testing.T) {
	resumes := make(chan string, 4)
	control := recoveryTestControl(t, resumes)
	transferID := "15151515-1515-4151-8151-151515151515"
	ownership, relinquish := context.WithCancelCause(context.Background())
	replayed := make(chan struct{}, 1)
	lease := fenceTestLease(transferID, func(context.Context, string) error {
		replayed <- struct{}{}
		return nil
	})
	lease.ownership = ownership
	control.registerRecoveryLease(lease)
	relinquish(errors.New("owner replaced"))
	control.continueRecoveryLease(lease)
	deadline := time.Now().Add(2 * time.Second)
	for control.currentLease(transferID) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if control.currentLease(transferID) != nil {
		t.Fatal("a fenced owner's lease must be released")
	}
	if len(resumes) != 0 || len(replayed) != 0 {
		t.Fatal("a fenced owner must not resume or replay")
	}
}

// Regression: cancellation ran before the uploads were snapshotted, so a
// concurrent terminal status that released the lease emptied the registry and
// cleanup reported success without aborting anything.
func TestProviderFailureCleanupSurvivesConcurrentLeaseRelease(t *testing.T) {
	storage := newFakeS3(t)
	storage.uploadID = func(int) string { return "upload-cleanup" }
	fake := &fakeTransferControl{failOnceAt: "transfer.route_stream.complete", failOnceError: &LifecycleRequestError{Status: 400, Body: `{"code":"route_contract_rejected"}`}}
	client := newTestClient(t, fake)
	fake.beforeRequest = func(messageType string, payload map[string]any) {
		if messageType == "transfer.cancel" {
			// A concurrent TransferStatus observed the cancellation.
			client.control.releaseRecoveryLease(payload["transfer_id"].(string))
		}
	}
	_, err := client.PrepareProviderTransfer(context.Background(),
		[]ProviderSource{r2Source(storage.URL)},
		[]ProviderDestination{r2Destination(storage.URL, "output.bin")},
		"", false, 0, false, "")
	var providerErr *ProviderTransferError
	if !errors.As(err, &providerErr) || !providerErr.MultipartCleanupComplete {
		t.Fatalf("expected completed cleanup, got %v", err)
	}
	if _, _, aborts := storage.counts(); aborts != 1 {
		t.Fatalf("the created upload must still be aborted, aborts=%d", aborts)
	}
}

// Regression: nil multipart identities were rejected, so plans without
// multipart groups could not resume when identities were built with append.
func TestResumeAcceptsNilMultipartGroupsWithoutMultipartPlan(t *testing.T) {
	prepared := &TransferPrepareResponse{
		TransferID: "14141414-1414-4141-8141-141414141414",
		PlanDescriptor: CompactTransferPlanDescriptor{
			Sources:      []CompactTransferPlanSource{{PreparedHTTPSource: PreparedHTTPSource{SourceID: "src_0", Size: 1024}, ChunkCount: 1}},
			Destinations: []CompactTransferPlanDestination{{PreparedDestination: PreparedDestination{DestinationID: "dst_0"}, FinalObjectKeys: map[string]string{"src_0": "out/file.bin"}}},
		},
	}
	destinations := map[string]ProviderDestination{"dst_0": HuggingFaceProviderDestination{RepoID: "acme/out", Path: "out/file.bin"}}
	if err := restoreProviderMultipartIdentities(prepared, destinations, nil, newMultipartUploadRegistry()); err != nil {
		t.Fatalf("nil identities must equal an empty list: %v", err)
	}
}
