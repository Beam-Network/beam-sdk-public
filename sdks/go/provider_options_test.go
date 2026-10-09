package beamnetworksdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCreateProviderTransferReportsMultipartIdentitiesWithoutSecrets(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	var mu sync.Mutex
	identities := []ProviderMultipartGroupIdentity{}
	destinations := []ProviderDestination{r2Destination(storage.URL, "a.bin"), r2Destination(storage.URL, "b.bin")}
	prepared, err := client.CreateProviderTransfer(context.Background(), ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: destinations,
		OnMultipartGroupReady: func(_ context.Context, identity ProviderMultipartGroupIdentity) error {
			mu.Lock()
			identities = append(identities, identity)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("identities = %d", len(identities))
	}
	for _, identity := range identities {
		serialized, _ := json.Marshal(identity)
		for _, secret := range []string{"cleanup-access", "cleanup-secret", "http://", "https://"} {
			if strings.Contains(string(serialized), secret) {
				t.Fatalf("identity leaked %q: %s", secret, serialized)
			}
		}
		if identity.TransferID != prepared.TransferID || identity.UploadID == "" || identity.ExpectedObjectSize != 1024 || identity.ExpectedPartCount != 1 || identity.ExpiresAt == "" {
			t.Fatalf("incomplete identity: %+v", identity)
		}
	}
	if begin := fake.callsOf("transfer.route_stream.begin")[0]; begin.payload["auto_distribute"] != true {
		t.Fatalf("CreateProviderTransfer must distribute by default: %v", begin.payload["auto_distribute"])
	}
}

func TestMultipartGroupCallbackFailureAbortsUploadAndFailsClosed(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	callbackErr := errors.New("durable journal unavailable")
	_, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{
		Sources:               []ProviderSource{r2Source(storage.URL)},
		Destinations:          []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		Distribute:            Bool(false),
		OnMultipartGroupReady: func(context.Context, ProviderMultipartGroupIdentity) error { return callbackErr },
	})
	var providerErr *ProviderTransferError
	if !errors.As(err, &providerErr) || !errors.Is(err, callbackErr) || !providerErr.MultipartCleanupComplete {
		t.Fatalf("callback failure must fail closed: %v", err)
	}
	if _, creates, aborts := storage.counts(); creates != 1 || aborts != 1 {
		t.Fatalf("creates=%d aborts=%d", creates, aborts)
	}
	if len(fake.callsOf("transfer.route_stream.manifest")) != 0 || len(fake.callsOf("transfer.route_stream.complete")) != 0 {
		t.Fatal("a rejected group must not be published")
	}
}

func TestOnPreparedFailureKeepsRecoveryLease(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	callbackErr := errors.New("caller could not record the transfer")
	var preparedID string
	_, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		OnPrepared: func(_ context.Context, prepared *TransferPrepareResponse) error {
			preparedID = prepared.TransferID
			return callbackErr
		},
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("error = %v", err)
	}
	if fake.lease(preparedID) == nil || len(fake.continuedRecovery) != 1 || len(fake.callsOf("transfer.route_stream.begin")) != 0 {
		t.Fatal("OnPrepared failure must keep the lease and continue recovery without streaming")
	}
}

func TestThrowIfCancelledStopsForegroundAndContinuesRecovery(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	stopErr := errors.New("caller cancelled")
	var preparedID string
	_, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		OnPrepared: func(_ context.Context, prepared *TransferPrepareResponse) error {
			preparedID = prepared.TransferID
			return nil
		},
		ThrowIfCancelled: func(_ context.Context, transferID string) error {
			if transferID != "" && len(fake.callsOf("transfer.route_stream.begin")) > 0 {
				return stopErr
			}
			return nil
		},
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("error = %v", err)
	}
	if len(fake.callsOf("transfer.cancel")) != 0 || fake.lease(preparedID) == nil || len(fake.continuedRecovery) != 1 {
		t.Fatal("foreground cancellation must keep background recovery and never cancel the transfer")
	}
	if _, _, aborts := storage.counts(); aborts != 0 {
		t.Fatal("foreground cancellation must keep retained multipart uploads")
	}
}

func TestOwnershipLossStopsWithoutCancellingOrAborting(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	ownership, relinquish := context.WithCancelCause(context.Background())
	replaced := errors.New("owner replaced")
	var preparedID string
	_, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{
		Sources:      []ProviderSource{r2Source(storage.URL)},
		Destinations: []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		Ownership:    ownership,
		OnPrepared: func(_ context.Context, prepared *TransferPrepareResponse) error {
			preparedID = prepared.TransferID
			return nil
		},
		OnMultipartGroupReady: func(context.Context, ProviderMultipartGroupIdentity) error {
			relinquish(replaced)
			return nil
		},
	})
	if !errors.Is(err, replaced) {
		t.Fatalf("error = %v", err)
	}
	if _, creates, aborts := storage.counts(); creates != 1 || aborts != 0 {
		t.Fatalf("a replaced owner must not abort uploads: creates=%d aborts=%d", creates, aborts)
	}
	if len(fake.callsOf("transfer.cancel")) != 0 || fake.lease(preparedID) != nil {
		t.Fatal("a replaced owner must release its lease without cancelling the transfer")
	}
	if _, err := client.PrepareProviderTransferWithOptions(context.Background(), ProviderTransferOptions{Ownership: ownership}); !errors.Is(err, replaced) {
		t.Fatalf("a relinquished ownership fence must stop new work, got %v", err)
	}
}

func TestUnsupportedProvidersFailBeforePrepare(t *testing.T) {
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	for _, destination := range []ProviderDestination{GCSProviderDestination{Bucket: "b", Key: "k"}, AzureProviderDestination{Container: "c", Blob: "b"}} {
		if _, err := client.CreateProviderTransfer(context.Background(), ProviderTransferOptions{
			Sources:      []ProviderSource{HippiusProviderSource{Bucket: "b", Key: "k", APIToken: "t"}},
			Destinations: []ProviderDestination{destination},
		}); err == nil || !strings.Contains(err.Error(), "not implemented") {
			t.Fatalf("%T must be rejected, got %v", destination, err)
		}
	}
	if len(fake.callsOf("transfer.prepare")) != 0 {
		t.Fatal("unsupported providers must fail before a transfer exists")
	}
}

func TestS3CompatibleProvidersSignWithPathStyleEndpoints(t *testing.T) {
	storage := newFakeS3(t)
	source, err := NewS3CompatibleProviderSource(S3CompatibleProviderSource{
		Provider: " Wasabi ", Bucket: "custom-source-bucket", Key: "objects/custom.bin", Region: "us-east-1",
		AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: storage.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareProviderSource(context.Background(), nil, source, 0, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Provider != "wasabi" || prepared.Metadata["driver"] != "s3-compatible" || prepared.Metadata["endpoint_url"] != storage.URL || prepared.Metadata["region"] != "us-east-1" {
		t.Fatalf("prepared source = %+v", prepared)
	}
	if !strings.HasPrefix(prepared.URL, storage.URL+"/custom-source-bucket/objects/custom.bin") {
		t.Fatalf("S3-compatible sources must use path style: %s", prepared.URL)
	}
	if !strings.Contains(storage.requestLog(), "HEAD /custom-source-bucket/objects/custom.bin") {
		t.Fatalf("HEAD must be path style: %s", storage.requestLog())
	}
	destination, err := NewS3CompatibleProviderDestination(S3CompatibleProviderDestination{
		Provider: "minio", Bucket: "archive", Key: "file.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: storage.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	preparedDestination, err := prepareProviderDestination(destination, 0)
	if err != nil || preparedDestination.Provider != "minio" || preparedDestination.LogicalPrefix != "file.bin" || preparedDestination.Metadata["driver"] != "s3-compatible" {
		t.Fatalf("prepared destination = %+v, %v", preparedDestination, err)
	}
	route, err := signDestinationRoute(context.Background(), nil, DestinationRouteInput{
		Chunk:       ChunkSigningPlanItem{ChunkIndex: 1, SourceID: "src_0", SourceChunkIndex: 1, SourceOffset: 1234, ChunkSize: 512, SourceURL: "https://source.example/read"},
		Target:      ChunkDestinationSigningTarget{DestinationID: "dst_custom", ObjectKey: "file.bin", Metadata: map[string]any{"final_object_key": "file.bin"}},
		Destination: destination,
		ExpiresIn:   10 * time.Minute,
	})
	if err != nil || !strings.HasPrefix(route.DestURL, storage.URL+"/archive/file.bin") || route.Headers["Range"] != "bytes=1234-1745" {
		t.Fatalf("route = %+v, %v", route, err)
	}
}

func TestS3CompatibleResolutionIsProviderAware(t *testing.T) {
	r2 := R2ProviderSource{Bucket: "bucket", Key: "file.bin", AccountID: "account123", AccessKeyID: "ak", SecretAccessKey: "sk"}
	custom := S3CompatibleProviderSource{Provider: "wasabi", Driver: "s3-compatible", Bucket: "bucket", Key: "file.bin", EndpointURL: "https://s3.us-east-1.wasabisys.com", AccessKeyID: "ak", SecretAccessKey: "sk"}
	aws := S3ProviderSource{Bucket: "bucket", Key: "file.bin", AccessKeyID: "ak", SecretAccessKey: "sk"}
	check := func(config any, endpoint string, region string, pathStyle *bool) {
		t.Helper()
		gotEndpoint, err := S3CompatibleEndpoint(config)
		if err != nil || gotEndpoint != endpoint {
			t.Fatalf("endpoint = %q, %v", gotEndpoint, err)
		}
		if gotRegion, _ := S3CompatibleRegion(config); gotRegion != region {
			t.Fatalf("region = %q", gotRegion)
		}
		gotPathStyle, _ := S3CompatibleForcePathStyle(config)
		if (gotPathStyle == nil) != (pathStyle == nil) || (gotPathStyle != nil && *gotPathStyle != *pathStyle) {
			t.Fatalf("path style = %v, want %v", gotPathStyle, pathStyle)
		}
	}
	check(r2, "https://account123.r2.cloudflarestorage.com", "auto", Bool(true))
	check(custom, "https://s3.us-east-1.wasabisys.com", "us-east-1", Bool(true))
	custom.ForcePathStyle = Bool(false)
	check(custom, "https://s3.us-east-1.wasabisys.com", "us-east-1", Bool(false))
	check(aws, "", "us-east-1", nil)
	if _, err := S3CompatibleEndpoint(S3CompatibleProviderSource{Provider: "minio", Bucket: "b", Key: "k"}); err == nil || !strings.Contains(err.Error(), "minio config requires endpoint_url") {
		t.Fatalf("custom provider without endpoint = %v", err)
	}
}

func TestProviderConstructorsValidateLikeTypeScript(t *testing.T) {
	cases := map[string]error{}
	_, cases["s3 config requires bucket."] = NewS3ProviderSource(S3ProviderSource{Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk"})
	_, cases["r2 config requires account_id or endpoint_url."] = NewR2ProviderDestination(R2ProviderDestination{Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk"})
	_, cases["s3-compatible config requires provider."] = NewS3CompatibleProviderSource(S3CompatibleProviderSource{Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk"})
	_, cases["wasabi config requires endpoint_url."] = NewS3CompatibleProviderDestination(S3CompatibleProviderDestination{Provider: "Wasabi", Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk"})
	_, cases["hippius config requires api_token."] = NewHippiusProviderSource(HippiusProviderSource{Bucket: "b", Key: "k"})
	_, cases["huggingface config repo_type must be one of model, dataset, space, kernel, bucket."] = NewHuggingFaceProviderSource(HuggingFaceProviderSource{RepoID: "org/repo", Path: "p", Token: "t", RepoType: "models"})
	_, cases["huggingface config repo_id must be `namespace/name`."] = NewHuggingFaceProviderDestination(HuggingFaceProviderDestination{RepoID: "repo", Path: "p", Token: "t"})
	for want, err := range cases {
		if err == nil || err.Error() != want {
			t.Fatalf("want %q, got %v", want, err)
		}
	}
	valid, err := NewS3CompatibleProviderSource(S3CompatibleProviderSource{Provider: " MinIO ", Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "https://minio.example"})
	if err != nil || valid.Provider != "minio" || valid.Driver != "s3-compatible" {
		t.Fatalf("valid = %+v, %v", valid, err)
	}
	r2, err := NewR2ProviderSource(R2ProviderSource{Bucket: "b", Key: "k", AccessKeyID: "ak", SecretAccessKey: "sk", AccountID: "acct"})
	if err != nil || r2.Provider != "r2" {
		t.Fatalf("r2 = %+v, %v", r2, err)
	}
}

func TestS3ClientsAreCachedAndRetryTransientFailures(t *testing.T) {
	storage := newFakeS3(t)
	storage.abortFailures = 2
	destination := r2Destination(storage.URL, "output.bin")
	settings, _, err := s3SettingsFor(destination)
	if err != nil {
		t.Fatal(err)
	}
	if s3ClientFor(settings) != s3ClientFor(settings) {
		t.Fatal("identical settings must reuse one client")
	}
	if err := abortMultipartUpload(context.Background(), destination, "output.bin", "upload-retry"); err != nil {
		t.Fatal(err)
	}
	if _, _, aborts := storage.counts(); aborts != 3 {
		t.Fatalf("abort attempts = %d", aborts)
	}
}
