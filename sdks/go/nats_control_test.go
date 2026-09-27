package beamnetworksdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

func natsOptionsFor(t *testing.T, natsURL string) nats.Options {
	t.Helper()
	options := nats.GetDefaultOptions()
	for _, option := range natsConnectOptions(natsURL, "b1m_test_key", "b1m_test_key") {
		if err := option(&options); err != nil {
			t.Fatal(err)
		}
	}
	return options
}

func TestTLSGatewayUsesHandshakeFirstAndServerName(t *testing.T) {
	options := natsOptionsFor(t, "tls://orch-gateway.b1m.ai:4222")
	if !options.TLSHandshakeFirst || !options.Secure {
		t.Fatalf("tls:// must perform the TLS handshake first: %+v", options)
	}
	if options.TLSConfig == nil || options.TLSConfig.ServerName != "orch-gateway.b1m.ai" {
		t.Fatalf("tls:// must present the gateway hostname: %+v", options.TLSConfig)
	}
	if options.MaxReconnect != -1 {
		t.Fatalf("reconnects must stay unbounded, got %d", options.MaxReconnect)
	}
	plain := natsOptionsFor(t, "nats://127.0.0.1:4222")
	if plain.TLSHandshakeFirst || plain.TLSConfig != nil {
		t.Fatalf("nats:// must not force TLS: %+v", plain)
	}
}

// A handshake-first gateway sends no plaintext INFO. The client must open with
// a TLS ClientHello carrying the gateway hostname, or it waits forever.
func TestTLSGatewayClientSendsClientHelloBeforeINFO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverNames := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		tlsConn := tls.Server(conn, &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			serverNames <- hello.ServerName
			return nil, errors.New("test server stops after ClientHello")
		}})
		_ = tlsConn.Handshake()
	}()
	natsURL := "tls://localhost:" + strings.Split(listener.Addr().String(), ":")[1]
	options := append(natsConnectOptions(natsURL, "b1m_test_key", "b1m_test_key"), nats.RetryOnFailedConnect(false), nats.Timeout(3*time.Second))
	if nc, err := nats.Connect(natsURL, options...); err == nil {
		nc.Close()
		t.Fatal("connect unexpectedly succeeded against a test server")
	}
	select {
	case serverName := <-serverNames:
		if serverName != "localhost" {
			t.Fatalf("ClientHello server name = %q", serverName)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client never started the TLS handshake")
	}
}

func testRoute(index int, sourceURLBytes int) SignedChunkRoute {
	return SignedChunkRoute{
		SourceID: "src_0", DestinationID: "dest_0", ChunkIndex: index,
		SourceURL:    "https://storage.example/" + strings.Repeat("x", sourceURLBytes),
		DestURL:      "https://storage.example/destination",
		SourceOffset: int64(index * 1024), ChunkSize: 1024,
	}
}

func splitSizes(t *testing.T, control *natsControl, routes []SignedChunkRoute) []int {
	t.Helper()
	batches, err := control.splitRoutes("transfer.route_stream.batch", map[string]any{}, routes)
	if err != nil {
		t.Fatal(err)
	}
	sizes := make([]int, 0, len(batches))
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

func TestRouteSplittingTargetsFourMiBAndHonorsGuards(t *testing.T) {
	defaults := newNatsControl("beam_test", "nats://127.0.0.1:4222", "prod", 1)
	overridden := newNatsControl("beam_test", "nats://127.0.0.1:4222", "prod", 1)
	overridden.maxPayloadBytes = 10 * 1024 * 1024
	routes := []SignedChunkRoute{testRoute(0, 4_500_000), testRoute(1, 4_500_000)}
	if got := splitSizes(t, defaults, routes); !reflect.DeepEqual(got, []int{1, 1}) {
		t.Fatalf("default split = %v", got)
	}
	if got := splitSizes(t, overridden, routes); !reflect.DeepEqual(got, []int{1, 1}) {
		t.Fatalf("overridden split = %v", got)
	}
	if got := splitSizes(t, defaults, []SignedChunkRoute{testRoute(3, 4_190_000), testRoute(4, 4_190_000)}); !reflect.DeepEqual(got, []int{1, 1}) {
		t.Fatalf("near-limit split = %v", got)
	}
	if _, err := defaults.splitRoutes("transfer.route_stream.batch", map[string]any{}, []SignedChunkRoute{testRoute(2, 8*1024*1024)}); err == nil ||
		!regexp.MustCompile(`single signed route is \d+ bytes, above maxPayloadBytes=8388608`).MatchString(err.Error()) {
		t.Fatalf("oversized route error = %v", err)
	}
	if got := splitSizes(t, defaults, []SignedChunkRoute{testRoute(5, 6*1024*1024)}); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("a route above the target but under the guard must publish alone, got %v", got)
	}
	lowerGuard := newNatsControl("beam_test", "nats://127.0.0.1:4222", "prod", 1)
	lowerGuard.maxPayloadBytes = 3 * 1024 * 1024
	if _, err := lowerGuard.splitRoutes("transfer.route_stream.batch", map[string]any{}, []SignedChunkRoute{testRoute(6, 3*1024*1024)}); err == nil ||
		!regexp.MustCompile(`single signed route is \d+ bytes, above maxPayloadBytes=3145728`).MatchString(err.Error()) {
		t.Fatalf("lower guard error = %v", err)
	}
}

func okReply(t *testing.T, payload any) []byte {
	t.Helper()
	data, err := marshalMsgpack(map[string]any{
		"ok": true, "status": 200, "runtime_epoch": "runtime-one", "transport_epoch": "transport-one", "payload": payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func controlWithCachedToken(token string) *natsControl {
	control := newNatsControl("beam_test", "nats://127.0.0.1:4222", "prod", 1)
	control.authToken = token
	control.authExpiresAt = time.Now().Unix() + 3600
	return control
}

func TestLifecycleTransportRetriesReuseIdenticalEnvelope(t *testing.T) {
	control := controlWithCachedToken("test-token")
	var sent [][]byte
	control.requestOverride = func(_ context.Context, _ string, data []byte) ([]byte, error) {
		sent = append(sent, append([]byte(nil), data...))
		if len(sent) == 1 {
			return nil, errors.New("connection closed")
		}
		return okReply(t, map[string]any{"success": true}), nil
	}
	if err := control.request(context.Background(), "transfer.prepare", map[string]any{"transfer_id": "transfer-one", "route_generation_id": "generation-one"}, "transfer-one", nil, "resume-generation-one"); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || !bytes.Equal(sent[0], sent[1]) {
		t.Fatalf("retry must resend the identical envelope: %d sends", len(sent))
	}
}

func unsignedTestJWT(t *testing.T, exp int64) string {
	t.Helper()
	encode := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return encode(map[string]any{"alg": "none", "typ": "JWT"}) + "." + encode(map[string]any{"token_type": "beam-sdk-auth", "exp": exp}) + ".signature"
}

func TestExpiredLifecycleAuthRetriesWithFreshTokenAndStableIdentity(t *testing.T) {
	control := controlWithCachedToken("stale-token")
	var sent []map[string]any
	authResolves := 0
	control.requestOverride = func(_ context.Context, subject string, data []byte) ([]byte, error) {
		if strings.Contains(subject, ".auth.") {
			authResolves++
			return json.Marshal(map[string]any{"ok": true, "token": unsignedTestJWT(t, time.Now().Unix()+3600)})
		}
		var envelope map[string]any
		if err := msgpack.Unmarshal(data, &envelope); err != nil {
			return nil, err
		}
		sent = append(sent, envelope)
		if len(sent) == 1 {
			return marshalMsgpack(map[string]any{
				"ok": false, "status": 401, "runtime_epoch": "runtime-one", "transport_epoch": "transport-one",
				"error": map[string]any{"code": "auth_token_expired", "message": "auth token expired"},
			})
		}
		return okReply(t, map[string]any{"success": true}), nil
	}
	payload := map[string]any{"transfer_id": "transfer-one", "route_generation_id": "generation-one", "batch_index": 1}
	if err := control.request(context.Background(), "transfer.route_stream.batch", payload, "transfer-one", nil, "route-batch-one"); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || authResolves != 1 {
		t.Fatalf("sends=%d authResolves=%d", len(sent), authResolves)
	}
	if sent[0]["request_id"] != sent[1]["request_id"] || !reflect.DeepEqual(sent[0]["payload"], sent[1]["payload"]) {
		t.Fatal("auth retry must keep the request identity and payload")
	}
	if sent[0]["auth_token"] != "stale-token" || sent[1]["auth_token"] == "stale-token" {
		t.Fatalf("auth retry must use a fresh token: %v then %v", sent[0]["auth_token"], sent[1]["auth_token"])
	}
}

func TestAuthTokenRefreshesBeforeFinalThirtySeconds(t *testing.T) {
	control := newNatsControl("beam_test", "nats://127.0.0.1:4222", "prod", 1)
	control.authToken = "nearly-expired"
	control.authExpiresAt = time.Now().Unix() + 20
	resolves := 0
	control.requestOverride = func(_ context.Context, subject string, _ []byte) ([]byte, error) {
		resolves++
		return json.Marshal(map[string]any{"ok": true, "token": unsignedTestJWT(t, time.Now().Unix()+3600)})
	}
	token, err := control.authTokenValue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resolves != 1 || token == "nearly-expired" {
		t.Fatalf("a token inside the 30-second margin must refresh (resolves=%d)", resolves)
	}
}

func TestResumeReplaysWhenRecoveryReportsRouteReplayRequired(t *testing.T) {
	control := controlWithCachedToken("test-token")
	defer control.close()
	transferID := "11111111-1111-4111-8111-111111111111"
	var mu sync.Mutex
	helloRequestIDs := []string{}
	control.requestOverride = func(_ context.Context, subject string, data []byte) ([]byte, error) {
		var envelope map[string]any
		if err := msgpack.Unmarshal(data, &envelope); err != nil {
			return nil, err
		}
		switch envelope["message_type"] {
		case "runtime.hello":
			mu.Lock()
			helloRequestIDs = append(helloRequestIDs, envelope["request_id"].(string))
			mu.Unlock()
			return okReply(t, map[string]any{"ready": true}), nil
		case "transfer.resume":
			// Only the recovery enum requests replay; the legacy boolean is absent.
			return okReply(t, map[string]any{"recovery": "route_replay_required"}), nil
		}
		return okReply(t, map[string]any{}), nil
	}
	replayed := make(chan string, 1)
	control.registerRecoveryLease(&recoveryLease{
		transferID:         transferID,
		planFingerprint:    strings.Repeat("a", 64),
		coordinateChecksum: "sha256-xor-v1:1:" + strings.Repeat("0", 64),
		replayRoutes: func(_ context.Context, generationID string) error {
			replayed <- generationID
			return nil
		},
	})
	control.continueRecoveryLease(control.currentLease(transferID))
	select {
	case generationID := <-replayed:
		if generationID == "" {
			t.Fatal("replay must carry a fresh route generation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("route_replay_required recovery did not replay routes")
	}
	// The hello monitor runs on its own goroutine, so it may trail the replay.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		sent := len(helloRequestIDs) > 0
		mu.Unlock()
		if sent || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if runtimeHelloInterval != 5*time.Second || len(helloRequestIDs) == 0 || helloRequestIDs[0] != lifecycleRequestID("runtime.hello", "runtime-hello:0:integrity") {
		t.Fatalf("runtime.hello must use key runtime:hello:<shard> every 5s: %v", helloRequestIDs)
	}
}

func TestDeliveryIndexZeroIsPresent(t *testing.T) {
	route := SignedChunkRoute{SourceID: "src", DestinationID: "dst", ChunkIndex: 3, DeliveryIndex: intPtr(0)}
	if index, present := signedRouteDeliveryIndex(route); !present || index != 0 {
		t.Fatalf("delivery index 0 must be present, got %d %v", index, present)
	}
	if _, present := signedRouteDeliveryIndex(SignedChunkRoute{}); present {
		t.Fatal("an unset delivery index must be absent")
	}
	batch := compactSignedRoutes([]SignedChunkRoute{
		{SourceID: "src", DestinationID: "dst", ChunkIndex: 1, DeliveryIndex: intPtr(1)},
		{SourceID: "src", DestinationID: "dst", ChunkIndex: 0, DeliveryIndex: intPtr(0)},
	})
	routes := batch["routes"].([]map[string]any)
	if routes[0]["delivery_index"] != 1 || routes[1]["delivery_index"] != 0 {
		t.Fatalf("compacted delivery indices = %v, %v", routes[0]["delivery_index"], routes[1]["delivery_index"])
	}
}

// Regression: RetryOnFailedConnect turned a rejected first connect into a
// reconnecting connection, so a bad API key surfaced as a 30s timeout or a
// retried "connection closed" instead of the authorization error.
func TestAuthorizationViolationOnFirstConnectFailsFast(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	connections := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections++
			mu.Unlock()
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				_, _ = conn.Write([]byte(`INFO {"server_id":"fake","version":"2.10.0","proto":1,"max_payload":1048576,"auth_required":true}` + "\r\n"))
				buffer := make([]byte, 4096)
				received := ""
				for !strings.Contains(received, "PING") {
					n, err := conn.Read(buffer)
					if err != nil {
						return
					}
					received += string(buffer[:n])
				}
				_, _ = conn.Write([]byte("-ERR 'Authorization Violation'\r\n"))
			}(conn)
		}
	}()
	control := newNatsControl("b1m_fake_review_key", "nats://"+listener.Addr().String(), "prod", 1)
	control.requestTimeout = 3 * time.Second
	defer control.close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	started := time.Now()
	err = control.request(ctx, "transfer.status", map[string]any{"transfer_id": "5f0d2a4e-8b1c-4f5e-9a3d-2c7b6e1f0a99"}, "5f0d2a4e-8b1c-4f5e-9a3d-2c7b6e1f0a99", &map[string]any{})
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "authorization violation") {
		t.Fatalf("expected an authorization violation, got %v after %v", err, elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("authorization failure took %v", elapsed)
	}
	if isRetryableNATSError(err) || isRetryableNATSError(errors.New("nats: connection closed: nats: authorization violation")) {
		t.Fatal("authorization violations must not be retried")
	}
	mu.Lock()
	defer mu.Unlock()
	if connections != 1 {
		t.Fatalf("a rejected first connect must not be retried, saw %d connections", connections)
	}
}
