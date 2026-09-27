package beamnetworksdk

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestMultipartRecoveryGrants(t *testing.T) {
	destination := R2ProviderDestination{Bucket: "dev", Key: "file.bin", AccessKeyID: "dev", SecretAccessKey: "dev", EndpointURL: "https://dev.example"}
	request := routeRecoveryChunk{MultipartGroupID: "group:one/(a)", FinalObjectKey: "file.bin", PartNumber: 1001, UploadID: "original", Recovery: &multipartRecoveryRequest{Operation: "upload", Mode: "staged", AttemptID: "22222222-2222-4222-8222-222222222222"}}
	sign := func() SignedChunkRoute {
		t.Helper()
		route, err := signMultipartRecovery(context.Background(), destination, "transfer", request, SignedChunkRoute{DestURL: "https://dev.example/direct", Metadata: map[string]any{"upload_id": "original", "part_number": 1001, "expected_part_count": 10000}}, 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return route
	}
	route := sign()
	stage := route.Metadata["recovery_staging"].(map[string]any)
	if stage["object_key"] != "file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/1001/22222222-2222-4222-8222-222222222222" {
		t.Fatal("incorrect staging scope")
	}
	copyURL, _ := url.Parse(stage["copy_url"].(string))
	if copyURL.Query().Get("uploadId") != "original" || copyURL.Query().Get("partNumber") != "1001" {
		t.Fatal("copy changed final multipart identity")
	}
	putURL, _ := url.Parse(route.DestURL)
	if putURL.Query().Has("uploadId") {
		t.Fatal("staging upload must be PutObject")
	}
	request.Recovery.Operation = "list"
	request.Recovery.ContinuationToken = "a+/=&"
	listing := sign().Metadata["recovery_listing"].(map[string]any)
	listURL, _ := url.Parse(listing["url"].(string))
	if listURL.Query().Get("prefix") != listing["prefix"] || listURL.Query().Get("continuation-token") != "a+/=&" {
		t.Fatal("listing scope lost")
	}
	request.Recovery.Operation = "renew"
	request.Recovery.Mode = "direct"
	renewed := sign()
	pages := renewed.Metadata["list_page_urls"].([]string)
	if len(pages) != 10 || renewed.Metadata["list_page_url"] != pages[1] {
		t.Fatal("incomplete renewal pagination")
	}
	for _, page := range pages {
		u, _ := url.Parse(page)
		if u.Query().Get("uploadId") != "original" {
			t.Fatal("renewal changed upload ID")
		}
	}
	request.Recovery.Operation = "controls"
	request.Recovery.Mode = "staged"
	request.Recovery.ObjectKey = "another-transfer"
	if _, err := signMultipartRecovery(context.Background(), destination, "transfer", request, SignedChunkRoute{Metadata: map[string]any{}}, time.Minute); err == nil {
		t.Fatal("wrong staging object accepted")
	}
}

// Staged recovery grants use the SDK's SigV4 key escaping and addressing, so
// special characters in the final object key survive into the staging object,
// the listing prefix, and the UploadPartCopy source.
func TestMultipartRecoveryGrantsEscapeSpecialKeys(t *testing.T) {
	ctx := context.Background()
	for _, pathStyle := range []bool{false, true} {
		destination := S3ProviderDestination{
			Bucket: "beam-bucket", Region: "us-east-1", AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
			ForcePathStyle: aws.Bool(pathStyle),
		}
		for _, key := range s3SpecialKeys {
			request := routeRecoveryChunk{MultipartGroupID: "group", FinalObjectKey: key, PartNumber: 2, UploadID: "upload+id=1", Recovery: &multipartRecoveryRequest{Operation: "upload", Mode: "staged", AttemptID: "22222222-2222-4222-8222-222222222222", ETag: "\"etag\""}}
			route, err := signMultipartRecovery(ctx, destination, "transfer", request, SignedChunkRoute{Metadata: map[string]any{}}, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			stage := route.Metadata["recovery_staging"].(map[string]any)
			stagingKey := stage["object_key"].(string)
			if stagingKey != key+".beam-recovery/transfer/group/2/22222222-2222-4222-8222-222222222222" {
				t.Fatalf("staging key = %q", stagingKey)
			}
			verifyS3PresignedURL(t, http.MethodPut, route.DestURL, destination.SecretAccessKey)
			verifyS3PresignedURL(t, http.MethodHead, stage["head_url"].(string), destination.SecretAccessKey)
			verifyS3PresignedURL(t, http.MethodDelete, stage["delete_url"].(string), destination.SecretAccessKey)
			if putURL, _ := url.Parse(route.DestURL); !strings.HasSuffix(putURL.Path, "/"+stagingKey) {
				t.Fatalf("staging PUT path %q does not address %q", putURL.Path, stagingKey)
			}
			headers := stage["copy_headers"].(map[string]string)
			if headers["x-amz-copy-source"] != "beam-bucket/"+sigV4URIEncode(stagingKey, false) || headers["x-amz-copy-source-if-match"] != `"etag"` {
				t.Fatalf("copy headers = %#v", headers)
			}
			copyURL, _ := url.Parse(stage["copy_url"].(string))
			if signed := copyURL.Query().Get("X-Amz-SignedHeaders"); !strings.Contains(signed, "x-amz-copy-source") || copyURL.Query().Has("x-amz-copy-source") {
				t.Fatalf("copy source must be a signed header, not a hoisted query value: %s", copyURL.RawQuery)
			}
			if copyURL.Query().Get("partNumber") != "2" || copyURL.Query().Get("uploadId") != "upload+id=1" || !strings.HasSuffix(copyURL.Path, "/"+key) {
				t.Fatalf("copy target changed: %s", copyURL)
			}
			request.Recovery.Operation = "list"
			listing := func() map[string]any {
				route, err := signMultipartRecovery(ctx, destination, "transfer", request, SignedChunkRoute{Metadata: map[string]any{}}, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				return route.Metadata["recovery_listing"].(map[string]any)
			}()
			verifyS3PresignedURL(t, http.MethodGet, listing["url"].(string), destination.SecretAccessKey)
		}
	}
}

// R2 does not promise to enforce copy source conditions, and direct or absent
// directives leave the signed route untouched apart from stale staging data.
func TestMultipartRecoveryDirectAndConditions(t *testing.T) {
	ctx := context.Background()
	r2 := R2ProviderDestination{Bucket: "dev", AccessKeyID: "dev", SecretAccessKey: "dev", EndpointURL: "https://dev.example"}
	request := routeRecoveryChunk{MultipartGroupID: "g", FinalObjectKey: "file.bin", PartNumber: 1, UploadID: "u", Recovery: &multipartRecoveryRequest{Operation: "controls", Mode: "staged", AttemptID: "22222222-2222-4222-8222-222222222222", ETag: "etag"}}
	original := SignedChunkRoute{DestURL: "https://dev.example/direct", Metadata: map[string]any{"recovery_staging": "stale"}}
	route, err := signMultipartRecovery(ctx, r2, "transfer", request, original, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, conditioned := route.Metadata["recovery_staging"].(map[string]any)["copy_headers"].(map[string]string)["x-amz-copy-source-if-match"]; conditioned {
		t.Fatal("R2 copy must not carry a source condition")
	}
	if route.DestURL != original.DestURL || original.Metadata["recovery_staging"] != "stale" {
		t.Fatal("controls must keep the direct upload URL and not mutate the input route")
	}
	request.Recovery = nil
	route, err = signMultipartRecovery(ctx, r2, "transfer", request, original, time.Minute)
	if err != nil || route.DestURL != original.DestURL {
		t.Fatal(err)
	}
	if _, stale := route.Metadata["recovery_staging"]; stale {
		t.Fatal("stale staging metadata must be removed")
	}
	request.Recovery = &multipartRecoveryRequest{Operation: "upload", Mode: "staged", AttemptID: "22222222-2222-4222-8222-222222222222"}
	if _, err := signMultipartRecovery(ctx, HippiusProviderDestination{Bucket: "b", Key: "k"}, "transfer", request, original, time.Minute); err == nil {
		t.Fatal("staged recovery requires an S3-compatible destination")
	}
	request.Recovery.AttemptID = "not-a-uuid"
	if _, err := signMultipartRecovery(ctx, r2, "transfer", request, original, time.Minute); err == nil {
		t.Fatal("invalid attempt id accepted")
	}
}
