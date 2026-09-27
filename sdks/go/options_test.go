package beamnetworksdk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewValidatesConfigurationLikeTypeScript(t *testing.T) {
	t.Setenv("BEAM_API_KEY", "")
	if _, err := New(WithNATSURL("nats://127.0.0.1:4222")); err == nil || !strings.Contains(err.Error(), "apiKey is required") {
		t.Fatalf("missing API key error = %v", err)
	}
	if _, err := New(WithAPIKey("b1m_test"), WithNATSURL("http://beamcore.test")); err == nil || !strings.Contains(err.Error(), "nats:// or tls://") {
		t.Fatalf("HTTP lifecycle URL error = %v", err)
	}
	for name, option := range map[string]Option{
		"routeSigningConcurrency":     WithRouteSigningConcurrency(0),
		"multipartControlConcurrency": WithMultipartControlConcurrency(0),
		"transferRuntimeShardCount":   WithTransferRuntimeShardCount(0),
		"requestTimeout":              WithRequestTimeout(0),
		"maxPayloadBytes":             WithMaxPayloadBytes(-1),
		"subjectPrefix":               WithSubjectPrefix("..."),
		"httpClient":                  WithHTTPClient(nil),
	} {
		if _, err := New(WithAPIKey("b1m_test"), WithNATSURL("nats://127.0.0.1:4222"), option); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("%s must be validated, got %v", name, err)
		}
	}

	client, err := New(
		WithAPIKey("b1m_test"), WithNATSURL("tls://orch-gateway.b1m.ai:4222"),
		WithSubjectPrefix(".custom.prefix."), WithTransferRuntimeShardCount(4),
		WithRequestTimeout(5*time.Second), WithMultipartControlConcurrency(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	control := client.control.(*natsControl)
	if control.subjectPrefix != "custom.prefix" || control.shardCount != 4 || control.requestTimeout != 5*time.Second || client.multipartControlConcurrency != 3 {
		t.Fatalf("options not applied: prefix=%q shards=%d timeout=%s multipart=%d", control.subjectPrefix, control.shardCount, control.requestTimeout, client.multipartControlConcurrency)
	}
	if !strings.HasPrefix(control.authSubject(), "custom.prefix.") {
		t.Fatalf("subject prefix not used: %s", control.authSubject())
	}

	lenient := NewClient(WithAPIKey("b1m_test"), WithRouteSigningConcurrency(0))
	defer lenient.Close()
	if lenient.routeSigningConcurrency != 64 || lenient.routeSigningConcurrencyOverridden {
		t.Fatal("NewClient must keep defaults for invalid option values")
	}
}

func TestLifecycleRequestErrorExposesReplyFields(t *testing.T) {
	control := controlWithCachedToken("test-token")
	control.requestOverride = func(context.Context, string, []byte) ([]byte, error) {
		return marshalMsgpack(map[string]any{
			"ok": false, "status": 409, "runtime_epoch": "runtime-one", "transport_epoch": "transport-one",
			"error": map[string]any{"code": "route_generation_mismatch", "message": "stale generation"},
		})
	}
	err := control.request(context.Background(), "transfer.route_stream.begin", map[string]any{}, "transfer-one", nil, "begin")
	var lifecycleErr *LifecycleRequestError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Status != 409 || lifecycleErr.Code != "route_generation_mismatch" || lifecycleErr.Message != "stale generation" || !strings.Contains(lifecycleErr.Body, "route_generation_mismatch") {
		t.Fatalf("lifecycle error = %#v", err)
	}
	if !isRecoverableRouteStreamError(err) {
		t.Fatal("409 must keep route recovery")
	}
}

func TestProviderTransferErrorRetainsCausesAndCleanupOutcomes(t *testing.T) {
	providerErr := errors.New("R2 request throttled")
	cleanupErr := &multipartCleanupError{errs: []error{errors.New("abort throttled")}}
	err := newProviderTransferError("transfer-1", providerErr, nil, cleanupErr)
	if err.TransferID != "transfer-1" || !err.TransferCancelled || err.MultipartCleanupComplete {
		t.Fatalf("outcome = %+v", err)
	}
	if len(err.Errors) != 2 || err.Errors[0] != providerErr || err.Errors[1] != cleanupErr {
		t.Fatalf("errors = %v", err.Errors)
	}
	if !errors.Is(err, providerErr) || err.Cause != providerErr {
		t.Fatal("the provider failure must stay reachable")
	}
	if strings.Contains(err.Error(), "throttled") {
		t.Fatalf("message must not echo provider errors: %q", err.Error())
	}
}

func TestMultipartControlIsBoundedIndependentlyFromRouteSigning(t *testing.T) {
	storage := newFakeS3(t)
	storage.createDelay = 20 * time.Millisecond
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake, WithRouteSigningConcurrency(64))
	destinations := make([]ProviderDestination, 5)
	for index := range destinations {
		destinations[index] = r2Destination(storage.URL, "output.bin")
	}
	prepared, err := client.PrepareProviderTransfer(context.Background(), []ProviderSource{r2Source(storage.URL)}, destinations, "", false, 0, false, "")
	if err != nil || !prepared.Success {
		t.Fatalf("prepare = %+v, %v", prepared, err)
	}
	_, creates, _ := storage.counts()
	storage.mu.Lock()
	maxActive := storage.maxActiveCreates
	storage.mu.Unlock()
	if DefaultMultipartControlConcurrency != 2 || creates != 5 || maxActive != 2 {
		t.Fatalf("creates=%d maxActive=%d", creates, maxActive)
	}
	manifestGroups := map[string]bool{}
	for _, call := range fake.callsOf("transfer.route_stream.manifest") {
		manifestGroups[call.payload["groups"].([]MultipartGroupManifest)[0].MultipartGroupID] = true
	}
	routeGroups := map[string]bool{}
	for _, call := range fake.callsOf("transfer.route_stream.batch") {
		for _, route := range call.payload["route_batch"].(map[string]any)["routes"].([]map[string]any) {
			routeGroups[route["multipart_group_id"].(string)] = true
		}
	}
	if len(manifestGroups) != 5 || len(routeGroups) != 5 {
		t.Fatalf("manifest groups=%d route groups=%d", len(manifestGroups), len(routeGroups))
	}
	for groupID := range routeGroups {
		if !manifestGroups[groupID] {
			t.Fatalf("route references unpublished group %s", groupID)
		}
	}
}
