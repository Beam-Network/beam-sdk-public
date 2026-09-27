package beamnetworksdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Explicitly opt in; credentials and signed URLs never enter test diagnostics.
func TestLiveR2MultipartRecovery(t *testing.T) {
	if os.Getenv("BEAM_RUN_MULTIPART_DEV") != "1" {
		t.Skip("DEV storage experiment is opt-in")
	}
	data, err := os.ReadFile("../../../beam-core/.env.live.local")
	if err != nil {
		t.Fatal("DEV configuration unavailable")
	}
	env := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			env[key] = strings.Trim(strings.TrimSpace(value), "\"'")
		}
	}
	if os.Getenv("BEAM_DEV_CORE_URL") == "" || env["BEAMCORE_HTTP_URL"] != os.Getenv("BEAM_DEV_CORE_URL") || !strings.HasPrefix(env["LIVE_TEST_SDK_QUALIFIED_SOURCE"], "r2://beam-xfer-test/") {
		t.Fatal("not the qualified DEV environment")
	}
	destination := R2ProviderDestination{Provider: "r2", Bucket: "beam-xfer-test", Key: "dev-multipart-v7/go-" + newTransferID() + "/file.bin", AccessKeyID: env["R2_ACCESS_KEY_ID"], SecretAccessKey: env["R2_SECRET_ACCESS_KEY"], EndpointURL: env["R2_ENDPOINT_URL"]}
	ctx := context.Background()
	upload, err := createMultipartUpload(ctx, destination, destination.Key, nil)
	if err != nil {
		t.Fatal("DEV multipart creation failed")
	}
	defer func() { _ = abortMultipartUpload(ctx, destination, destination.Key, upload) }()
	httpClient := &http.Client{Timeout: 120 * time.Second}
	fetch := func(method, grant string, body []byte, headers map[string]string) (http.Header, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, grant, bytes.NewReader(body))
		if err != nil {
			t.Fatal("invalid signed request")
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		response, err := httpClient.Do(req)
		if err != nil {
			t.Fatal("DEV signed request failed")
		}
		defer response.Body.Close()
		result, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode >= 300 {
			t.Fatalf("DEV signed %s returned HTTP %d", method, response.StatusCode)
		}
		return response.Header, result
	}
	sign := func(method, key, operation string) string {
		t.Helper()
		grant, err := presignS3Operation(ctx, destination, method, key, operation, 10*time.Minute, nil, nil)
		if err != nil {
			t.Fatal("signing failed")
		}
		return grant
	}
	defer func() { fetch("DELETE", sign("DELETE", destination.Key, "DeleteObject"), nil, nil) }()
	transfer := newTransferID()
	request := routeRecoveryChunk{MultipartGroupID: "group:one/(a)", FinalObjectKey: destination.Key, PartNumber: 1, UploadID: upload, Recovery: &multipartRecoveryRequest{Operation: "upload", Mode: "staged", AttemptID: newTransferID()}}
	recovery := func() SignedChunkRoute {
		t.Helper()
		route, err := signMultipartRecovery(ctx, destination, transfer, request, SignedChunkRoute{Metadata: map[string]any{"expected_part_count": 1}}, 10*time.Minute)
		if err != nil {
			t.Fatal("recovery signing failed")
		}
		return route
	}
	route := recovery()
	stage := route.Metadata["recovery_staging"].(map[string]any)
	defer func() { fetch("DELETE", stage["delete_url"].(string), nil, nil) }()
	payload := bytes.Repeat([]byte{0x5b}, 8<<20)
	fetch("PUT", route.DestURL, payload, nil)
	head, _ := fetch("HEAD", stage["head_url"].(string), nil, nil)
	if head.Get("Content-Length") != fmt.Sprint(len(payload)) {
		t.Fatal("staging size mismatch")
	}
	_, copied := fetch("PUT", stage["copy_url"].(string), nil, stage["copy_headers"].(map[string]string))
	var receipt struct {
		ETag string `xml:"ETag"`
	}
	if xml.Unmarshal(copied, &receipt) != nil || receipt.ETag == "" {
		t.Fatal("copy receipt missing")
	}
	request.Recovery.Operation = "list"
	listing := recovery().Metadata["recovery_listing"].(map[string]any)
	_, listed := fetch("GET", listing["url"].(string), nil, nil)
	if !bytes.Contains(listed, []byte(stage["object_key"].(string))) {
		t.Fatal("staging object missing from signed listing")
	}
	request.Recovery.Operation = "renew"
	request.Recovery.Mode = "direct"
	controls := recovery().Metadata
	_, complete := fetch("POST", controls["complete_url"].(string), []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"+receipt.ETag+"</ETag></Part></CompleteMultipartUpload>"), nil)
	if bytes.Contains(complete, []byte("<Error>")) {
		t.Fatal("completion returned an embedded error")
	}
	_, actual := fetch("GET", sign("GET", destination.Key, "GetObject"), nil, nil)
	if sha256.Sum256(actual) != sha256.Sum256(payload) {
		t.Fatal("final bytes differ")
	}
}
