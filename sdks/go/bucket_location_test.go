package beamnetworksdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const testR2Endpoint = "https://example-account.r2.cloudflarestorage.com"

type locationTransport struct {
	mu       sync.Mutex
	requests []*http.Request
	started  []time.Time
	respond  func(*http.Request) (*http.Response, error)
}

func (transport *locationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	transport.requests = append(transport.requests, request)
	transport.started = append(transport.started, time.Now())
	respond := transport.respond
	transport.mu.Unlock()
	if respond == nil {
		return locationResponse(http.StatusOK, locationBody("ENAM")), nil
	}
	return respond(request)
}

func (transport *locationTransport) count() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return len(transport.requests)
}

func locationBody(value string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` + value + `</LocationConstraint>`
}

func locationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

func locationClient(t *testing.T, transport *locationTransport) *Client {
	t.Helper()
	return newTestClient(t, &fakeTransferControl{}, WithHTTPClient(&http.Client{Transport: transport}))
}

func r2LocationSource(bucket string) R2ProviderSource {
	return R2ProviderSource{Bucket: bucket, Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint}
}

func TestBucketLocationAsksAnR2BucketWithASignedGet(t *testing.T) {
	transport := &locationTransport{}
	client := locationClient(t, transport)
	if got := client.bucketLocation(r2LocationSource("media")).wait(context.Background()); got != "ENAM" {
		t.Fatalf("location = %q", got)
	}
	if transport.count() != 1 {
		t.Fatalf("requests = %d", transport.count())
	}
	request := transport.requests[0]
	if request.Method != http.MethodGet || request.URL.Host != "example-account.r2.cloudflarestorage.com" || request.URL.Path != "/media" || request.URL.RawQuery != "location=" {
		t.Fatalf("request = %s %s", request.Method, request.URL)
	}
	authorization := request.Header.Get("Authorization")
	if !regexp.MustCompile(`^AWS4-HMAC-SHA256 Credential=ak/\d{8}/auto/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$`).MatchString(authorization) {
		t.Fatalf("authorization = %q", authorization)
	}
	if request.Header.Get("X-Amz-Content-Sha256") != emptyPayloadSHA256 || request.Header.Get("X-Amz-Date") == "" {
		t.Fatalf("headers = %v", request.Header)
	}
}

func TestBucketLocationAsksR2JurisdictionAndAccountEndpoints(t *testing.T) {
	transport := &locationTransport{respond: func(*http.Request) (*http.Response, error) {
		return locationResponse(http.StatusOK, locationBody(" WEUR ")), nil
	}}
	client := locationClient(t, transport)
	jurisdiction := r2LocationSource("media")
	jurisdiction.EndpointURL = "https://example-account.eu.r2.cloudflarestorage.com"
	account := R2ProviderDestination{Bucket: "media", Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", AccountID: "other-account"}
	for _, config := range []any{jurisdiction, account} {
		if got := client.bucketLocation(config).wait(context.Background()); got != "WEUR" {
			t.Fatalf("location = %q", got)
		}
	}
	if transport.requests[0].URL.Host != "example-account.eu.r2.cloudflarestorage.com" || transport.requests[1].URL.Host != "other-account.r2.cloudflarestorage.com" {
		t.Fatalf("hosts = %s, %s", transport.requests[0].URL.Host, transport.requests[1].URL.Host)
	}
}

func TestBucketLocationAsksGoogleCloudStorageEndpoints(t *testing.T) {
	transport := &locationTransport{respond: func(*http.Request) (*http.Response, error) {
		return locationResponse(http.StatusOK, locationBody("US-EAST1")), nil
	}}
	client := locationClient(t, transport)
	for _, endpoint := range []string{"https://storage.googleapis.com", "https://media.storage.googleapis.com"} {
		config := S3CompatibleProviderSource{Provider: "gcs-hmac", Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: endpoint}
		if got := client.bucketLocation(config).wait(context.Background()); got != "US-EAST1" {
			t.Fatalf("%s location = %q", endpoint, got)
		}
	}
	if transport.requests[0].URL.Host != "storage.googleapis.com" || transport.requests[1].URL.Host != "media.storage.googleapis.com" {
		t.Fatalf("hosts = %s, %s", transport.requests[0].URL.Host, transport.requests[1].URL.Host)
	}
	if !strings.Contains(transport.requests[0].Header.Get("Authorization"), "/us-east-1/s3/aws4_request") {
		t.Fatalf("authorization = %q", transport.requests[0].Header.Get("Authorization"))
	}
}

func TestBucketLocationFollowsTheEndpointHostNotTheProviderID(t *testing.T) {
	transport := &locationTransport{}
	client := locationClient(t, transport)
	for _, provider := range []string{"minio", "custom-s3", "s3-compatible"} {
		config := S3CompatibleProviderDestination{Provider: provider, Bucket: "media-" + provider, Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint}
		if got := client.bucketLocation(config).wait(context.Background()); got != "ENAM" {
			t.Fatalf("%s location = %q", provider, got)
		}
	}
	unasked := []any{
		R2ProviderSource{Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "https://r2.example.test"},
		R2ProviderSource{Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint + ".example.test"},
		S3ProviderSource{Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk"},
		S3ProviderSource{Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "https://s3.us-west-2.amazonaws.com"},
		S3CompatibleProviderSource{Provider: "minio", Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "http://127.0.0.1:9000"},
		HippiusProviderSource{Bucket: "media", Key: "input.bin", APIToken: "token"},
		HuggingFaceProviderSource{RepoID: "org/model", Path: "weights.bin"},
	}
	for _, config := range unasked {
		if lookup := client.bucketLocation(config); lookup != nil {
			t.Fatalf("%T was looked up", config)
		}
	}
	if transport.count() != 3 {
		t.Fatalf("requests = %d", transport.count())
	}
}

func TestBucketLocationSkipsADeclaredStorageLocation(t *testing.T) {
	transport := &locationTransport{}
	client := locationClient(t, transport)
	config := r2LocationSource("media")
	config.StorageLocation = "weur"
	if lookup := client.bucketLocation(config); lookup != nil || transport.count() != 0 {
		t.Fatal("a declared storage location must not be looked up")
	}
}

func TestBucketLocationAsksEachBucketOncePerClient(t *testing.T) {
	var status sync.Map
	transport := &locationTransport{respond: func(request *http.Request) (*http.Response, error) {
		if _, failing := status.Load(request.URL.Path); failing {
			return locationResponse(http.StatusInternalServerError, "<Error/>"), nil
		}
		return locationResponse(http.StatusOK, locationBody("ENAM")), nil
	}}
	client := locationClient(t, transport)
	first, second := client.bucketLocation(r2LocationSource("media")), client.bucketLocation(R2ProviderDestination{Bucket: "media", Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint})
	if first != second || first.wait(context.Background()) != "ENAM" {
		t.Fatal("identical lookups must share one request")
	}
	status.Store("/missing", true)
	if got := client.bucketLocation(r2LocationSource("missing")).wait(context.Background()); got != "" {
		t.Fatalf("location = %q", got)
	}
	status.Delete("/missing")
	if got := client.bucketLocation(r2LocationSource("missing")).wait(context.Background()); got != "" {
		t.Fatalf("a failed lookup must stay cached, got %q", got)
	}
	if transport.count() != 2 {
		t.Fatalf("requests = %d", transport.count())
	}
	if got := locationClient(t, transport).bucketLocation(r2LocationSource("missing")).wait(context.Background()); got != "ENAM" {
		t.Fatalf("another client asks again, got %q", got)
	}
}

func TestBucketLocationFailuresLeaveTheLocationOut(t *testing.T) {
	answers := []func(*http.Request) (*http.Response, error){
		func(*http.Request) (*http.Response, error) { return locationResponse(http.StatusForbidden, locationBody("ENAM")), nil },
		func(*http.Request) (*http.Response, error) {
			return locationResponse(http.StatusMovedPermanently, locationBody("ENAM")), nil
		},
		func(*http.Request) (*http.Response, error) { return locationResponse(http.StatusOK, "<LocationConstraint/>"), nil },
		func(*http.Request) (*http.Response, error) { return locationResponse(http.StatusOK, locationBody("")), nil },
		func(*http.Request) (*http.Response, error) {
			return locationResponse(http.StatusOK, locationBody("not a location")), nil
		},
		func(*http.Request) (*http.Response, error) { return locationResponse(http.StatusOK, "<html>gateway</html>"), nil },
		func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") },
	}
	for index, answer := range answers {
		client := locationClient(t, &locationTransport{respond: answer})
		if got := client.bucketLocation(r2LocationSource("media")).wait(context.Background()); got != "" {
			t.Fatalf("answer %d: location = %q", index, got)
		}
	}
}

func TestBucketLocationTimesOutAfterThreeSeconds(t *testing.T) {
	transport := &locationTransport{respond: func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}}
	client := locationClient(t, transport)
	started := time.Now()
	if got := client.bucketLocation(r2LocationSource("media")).wait(context.Background()); got != "" {
		t.Fatalf("location = %q", got)
	}
	if elapsed := time.Since(started); elapsed < 2900*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("elapsed = %s", elapsed)
	}
}

func TestProviderTransfersAddBucketLocationWhileDiscoveringSources(t *testing.T) {
	var headMu sync.Mutex
	var headEnd time.Time
	storage := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(500 * time.Millisecond)
		headMu.Lock()
		headEnd = time.Now()
		headMu.Unlock()
		writer.Header().Set("Content-Length", "1024")
		writer.Header().Set("ETag", `"source-etag"`)
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(storage.Close)
	transport := &locationTransport{respond: func(request *http.Request) (*http.Response, error) {
		time.Sleep(500 * time.Millisecond)
		value := "ENAM"
		if request.URL.Host == "storage.googleapis.com" {
			value = "NAM4"
		}
		return locationResponse(http.StatusOK, locationBody(value)), nil
	}}
	fake := &fakeTransferControl{failOnceAt: "transfer.prepare", failOnceError: errors.New("prepare captured")}
	client := newTestClient(t, fake, WithHTTPClient(&http.Client{Transport: transport}))
	declared := R2ProviderDestination{Bucket: "declared", Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint, StorageLocation: "weur"}
	destinations := []ProviderDestination{
		S3CompatibleProviderDestination{Provider: "minio", Bucket: "archive", Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: testR2Endpoint},
		S3CompatibleProviderDestination{Provider: "custom-s3", Bucket: "archive", Key: "out/", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "https://storage.googleapis.com"},
		declared,
	}
	started := time.Now()
	_, err := client.PrepareProviderTransfer(context.Background(), []ProviderSource{r2Source(storage.URL)}, destinations, "", 0, false, "")
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "prepare captured") {
		t.Fatalf("err = %v", err)
	}
	if elapsed > 900*time.Millisecond {
		t.Fatalf("lookups ran after source discovery: %s", elapsed)
	}
	headMu.Lock()
	for _, lookupStart := range transport.started {
		if !lookupStart.Before(headEnd) {
			t.Fatal("a lookup started after the source HEAD finished")
		}
	}
	headMu.Unlock()
	calls := fake.callsOf("transfer.prepare")
	if len(calls) != 1 || transport.count() != 2 {
		t.Fatalf("prepares = %d lookups = %d", len(calls), transport.count())
	}
	sources := calls[0].payload["sources"].([]PreparedHTTPSource)
	if _, present := sources[0].Metadata["bucket_location"]; present {
		t.Fatal("a source on another endpoint must carry no bucket_location")
	}
	prepared := calls[0].payload["destinations"].([]PreparedDestination)
	got := []any{prepared[0].Metadata["bucket_location"], prepared[1].Metadata["bucket_location"], prepared[2].Metadata["bucket_location"]}
	if got[0] != "ENAM" || got[1] != "NAM4" || got[2] != nil || prepared[2].Metadata["storage_location"] != "weur" {
		t.Fatalf("destination locations = %v", got)
	}

	fake.mu.Lock()
	fake.failOnceAt, fake.failOnceError = "transfer.prepare", errors.New("prepare captured")
	fake.mu.Unlock()
	if _, err := client.PrepareProviderTransfer(context.Background(), []ProviderSource{r2Source(storage.URL)}, destinations, "", 0, false, ""); err == nil {
		t.Fatal("the second prepare must be captured")
	}
	if transport.count() != 2 {
		t.Fatalf("a repeated transfer must reuse the lookups, requests = %d", transport.count())
	}
	if again := fake.callsOf("transfer.prepare")[1].payload["destinations"].([]PreparedDestination); again[0].Metadata["bucket_location"] != "ENAM" {
		t.Fatalf("repeated destination metadata = %v", again[0].Metadata)
	}
}

func TestProviderTransferSourcesWaitForTheirLookup(t *testing.T) {
	transport := &locationTransport{}
	client := locationClient(t, transport)
	sources := []any{r2LocationSource("media"), S3CompatibleProviderSource{Provider: "custom-s3", Bucket: "media", Key: "input.bin", AccessKeyID: "ak", SecretAccessKey: "sk", EndpointURL: "https://storage.googleapis.com"}}
	for _, source := range sources {
		prepared := PreparedHTTPSource{Metadata: map[string]any{"bucket": "media"}}
		prepared.Metadata = withBucketLocation(prepared.Metadata, client.bucketLocation(source).wait(context.Background()))
		if prepared.Metadata["bucket_location"] != "ENAM" || prepared.Metadata["bucket"] != "media" {
			t.Fatalf("%T metadata = %v", source, prepared.Metadata)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	slow := locationClient(t, &locationTransport{respond: func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}})
	if got := slow.bucketLocation(r2LocationSource("media")).wait(cancelled); got != "" {
		t.Fatalf("a cancelled wait = %q", got)
	}
	if withBucketLocation(nil, "") != nil {
		t.Fatal("an empty location must leave metadata untouched")
	}
}
