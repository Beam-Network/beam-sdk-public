package beamnetworksdk

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

func newTestClient(t *testing.T, fake *fakeTransferControl, options ...Option) *Client {
	t.Helper()
	client := NewClient(append([]Option{WithNATSURL("nats://127.0.0.1:4222"), WithAPIKey("b1m_test")}, options...)...)
	client.control = fake
	t.Cleanup(client.Close)
	return client
}

// Regression: cancellation used to release the recovery lease first, and its
// dispose emptied the upload registry before abort could read it.
func TestNonRecoverableRouteFailurePreservesMultipartCleanupAuthority(t *testing.T) {
	storage := newFakeS3(t)
	storage.uploadID = func(int) string { return "upload-cleanup" }
	fake := &fakeTransferControl{failOnceAt: "transfer.route_stream.complete", failOnceError: &LifecycleRequestError{Status: 400, Body: `{"code":"route_contract_rejected"}`}}
	client := newTestClient(t, fake)

	_, err := client.PrepareProviderTransfer(context.Background(),
		[]ProviderSource{r2Source(storage.URL)},
		[]ProviderDestination{r2Destination(storage.URL, "output.bin")},
		"", 0, false, "")
	var providerErr *ProviderTransferError
	if !errors.As(err, &providerErr) {
		t.Fatalf("expected ProviderTransferError, got %T %v", err, err)
	}
	if !providerErr.TransferCancelled || !providerErr.MultipartCleanupComplete {
		t.Fatalf("cleanup outcome = %+v", providerErr)
	}
	var lifecycleErr *LifecycleRequestError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Status != 400 {
		t.Fatalf("the original failure must stay reachable: %v", err)
	}
	if strings.Contains(err.Error(), "route_contract_rejected") {
		t.Fatalf("error message must not echo provider or lifecycle bodies: %q", err.Error())
	}
	_, _, aborts := storage.counts()
	if aborts != 1 || !strings.Contains(storage.abortAuthorization, "Credential=cleanup-access/") {
		t.Fatalf("abort count=%d authorization=%q", aborts, storage.abortAuthorization)
	}
	if len(fake.callsOf("transfer.cancel")) != 1 {
		t.Fatal("transfer must be cancelled exactly once")
	}
	if fake.lease(providerErr.TransferID) != nil || fake.signer() != nil {
		t.Fatal("the recovery lease and signer must be released after cleanup")
	}
}

func TestRouteReplayReusesCreatedMultipartUploads(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	prepared, err := client.PrepareProviderTransfer(context.Background(),
		[]ProviderSource{r2Source(storage.URL)},
		[]ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		"", 0, false, "")
	if err != nil {
		t.Fatal(err)
	}
	lease := fake.lease(prepared.TransferID)
	if lease == nil {
		t.Fatal("prepared transfer must retain a recovery lease")
	}
	if err := lease.replayRoutes(context.Background(), "33333333-3333-4333-8333-333333333333"); err != nil {
		t.Fatal(err)
	}
	heads, creates, _ := storage.counts()
	if creates != 1 || heads != 1 {
		t.Fatalf("replay must reuse the upload and plan: heads=%d creates=%d", heads, creates)
	}
	if got := len(fake.callsOf("transfer.route_stream.complete")); got != 2 {
		t.Fatalf("route stream completes = %d", got)
	}
	batches := fake.callsOf("transfer.route_stream.batch")
	last := batches[len(batches)-1].payload["route_batch"].(map[string]any)["routes"].([]map[string]any)
	if last[0]["multipart_group_id"] != multipartGroupStateKey(prepared.TransferID, "dst_0", "src_0", "out/file.bin") {
		t.Fatalf("replayed route lost its multipart group: %#v", last[0])
	}
}

func TestHuggingFaceRouteReplayReusesOriginalPlan(t *testing.T) {
	body := []byte(strings.Repeat("h", 4096))
	cdn := httptest.NewServer(cdnHandler(body))
	defer cdn.Close()
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{cdnOrigin: cdn.URL, size: 4096, sha256: strings.Repeat("a", 64), chunkSize: 4096, partCount: 1})
	defer hub.Close()
	var mu sync.Mutex
	negotiations := 0
	inner := hub.Config.Handler
	hub.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "/preupload/") || strings.HasSuffix(request.URL.Path, "/info/lfs/objects/batch") {
			mu.Lock()
			negotiations++
			mu.Unlock()
		}
		inner.ServeHTTP(writer, request)
	})

	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	prepared, err := client.PrepareProviderTransfer(context.Background(),
		[]ProviderSource{HuggingFaceProviderSource{RepoID: "acme/corpus", Path: "data/train.parquet", RepoType: "dataset", Token: huggingFaceTestToken, Endpoint: hub.URL}},
		[]ProviderDestination{HuggingFaceProviderDestination{RepoID: "acme/out", Path: "out/file.bin", Token: huggingFaceTestToken, Endpoint: hub.URL}},
		"", 0, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.lease(prepared.TransferID).replayRoutes(context.Background(), "44444444-4444-4444-8444-444444444444"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if negotiations != 2 {
		t.Fatalf("replay must reuse the negotiated Hub plan, saw %d negotiation requests", negotiations)
	}
	routes := fake.callsOf("transfer.route_stream.batch")
	metadata := routes[len(routes)-1].payload["route_batch"].(map[string]any)["routes"].([]map[string]any)[0]["metadata"].(map[string]any)
	if _, leaked := metadata["part_number"]; leaked {
		t.Fatalf("direct-PUT routes must not carry part_number: %#v", metadata)
	}
	for key := range metadata {
		if _, allowed := partRouteMetadataKeys[key]; !allowed {
			t.Fatalf("direct-PUT route leaked metadata key %q", key)
		}
	}
}

// Beam chooses the chunk size: no provider prepare carries chunk_size, and only
// a Hugging Face multipart destination carries the part size its Hub dictated.
func TestProviderPrepareCarriesOnlyTheHubPartSize(t *testing.T) {
	preparePayload := func(t *testing.T, fake *fakeTransferControl) map[string]any {
		t.Helper()
		calls := fake.callsOf("transfer.prepare")
		if len(calls) != 1 {
			t.Fatalf("prepare calls = %d", len(calls))
		}
		if _, sent := calls[0].payload["chunk_size"]; sent {
			t.Fatalf("prepare payload must leave chunk size to Beam: %+v", calls[0].payload)
		}
		return calls[0].payload
	}
	prepareHuggingFace := func(t *testing.T, hubPartSize int64, partCount int) (*fakeTransferControl, *TransferPrepareResponse, *huggingFaceHub) {
		t.Helper()
		cdn := httptest.NewServer(cdnHandler([]byte(strings.Repeat("h", 4096))))
		t.Cleanup(cdn.Close)
		hub := newHuggingFaceHub(t, huggingFaceHubOptions{cdnOrigin: cdn.URL, size: 4096, sha256: strings.Repeat("a", 64), chunkSize: hubPartSize, partCount: partCount})
		t.Cleanup(hub.Close)
		fake := &fakeTransferControl{}
		client := newTestClient(t, fake)
		prepared, err := client.PrepareProviderTransfer(context.Background(),
			[]ProviderSource{HuggingFaceProviderSource{RepoID: "acme/corpus", Path: "data/train.parquet", RepoType: "dataset", Token: huggingFaceTestToken, Endpoint: hub.URL}},
			[]ProviderDestination{HuggingFaceProviderDestination{RepoID: "acme/out", Path: "out/file.bin", Token: huggingFaceTestToken, Endpoint: hub.URL}},
			"", 0, false, "")
		if err != nil {
			t.Fatal(err)
		}
		return fake, prepared, hub
	}

	t.Run("s3 compatible", func(t *testing.T) {
		storage := newFakeS3(t)
		fake := &fakeTransferControl{}
		client := newTestClient(t, fake)
		if _, err := client.PrepareProviderTransfer(context.Background(),
			[]ProviderSource{r2Source(storage.URL)},
			[]ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
			"", 0, false, ""); err != nil {
			t.Fatal(err)
		}
		if _, sent := preparePayload(t, fake)["provider_part_size"]; sent {
			t.Fatal("a prepare without a Hugging Face destination must not carry provider_part_size")
		}
	})

	t.Run("hugging face multipart", func(t *testing.T) {
		fake, prepared, hub := prepareHuggingFace(t, 2048, 2)
		if got := preparePayload(t, fake)["provider_part_size"]; got != int64(2048) {
			t.Fatalf("provider_part_size = %#v, want the Hub part size 2048", got)
		}
		if prepared.PlanDescriptor.ChunkSize != 2048 {
			t.Fatalf("plan chunk size = %d, want the Hub part size", prepared.PlanDescriptor.ChunkSize)
		}
		destURLs := make([]string, 0)
		for _, batch := range fake.callsOf("transfer.route_stream.batch") {
			for _, route := range batch.payload["route_batch"].(map[string]any)["routes"].([]map[string]any) {
				destURLs = append(destURLs, route["dest_url"].(string))
			}
		}
		sort.Strings(destURLs)
		if want := []string{hub.URL + "/part/1", hub.URL + "/part/2"}; !reflect.DeepEqual(destURLs, want) {
			t.Fatalf("routes target %v, want the Hub part URLs %v", destURLs, want)
		}
	})

	t.Run("hugging face single part", func(t *testing.T) {
		fake, prepared, _ := prepareHuggingFace(t, 0, 0)
		if _, sent := preparePayload(t, fake)["provider_part_size"]; sent {
			t.Fatal("a single-part Hugging Face upload must not carry provider_part_size")
		}
		if prepared.PlanDescriptor.Sources[0].ChunkCount != 1 {
			t.Fatalf("single-part plan chunk count = %d", prepared.PlanDescriptor.Sources[0].ChunkCount)
		}
	})
}
