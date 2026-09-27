package beamnetworksdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// multipartRecoveryRequest is the optional recovery directive Runtime attaches
// to a route recovery chunk (TypeScript RouteRecoverySignChunk.recovery).
// Staged recovery uploads an attempt to its own staging object and copies it
// into the original multipart upload with UploadPartCopy; renew re-signs the
// group controls of the existing upload.
type multipartRecoveryRequest struct {
	Operation         string `msgpack:"operation" json:"operation"`
	Mode              string `msgpack:"mode" json:"mode"`
	AttemptID         string `msgpack:"attempt_id" json:"attempt_id"`
	ObjectKey         string `msgpack:"object_key,omitempty" json:"object_key,omitempty"`
	ETag              string `msgpack:"etag,omitempty" json:"etag,omitempty"`
	ContinuationToken string `msgpack:"continuation_token,omitempty" json:"continuation_token,omitempty"`
}

// routeRecoveryChunk is the route recovery chunk shape signMultipartRecovery
// reads; it is the same type the route recovery signer decodes.
type routeRecoveryChunk = routeRecoverySignChunk

var recoveryAttemptIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// presignS3Operation presigns one S3 operation for an S3, R2, or S3-compatible
// destination with the same addressing and key escaping as every other
// SDK-signed operation.
func presignS3Operation(
	ctx context.Context,
	destination ProviderDestination,
	method string,
	objectKey string,
	operation string,
	expiresIn time.Duration,
	queryValues map[string]string,
	headers map[string]string,
) (string, error) {
	settings, err := destinationS3Settings(destination, "multipart recovery")
	if err != nil {
		return "", err
	}
	return presignS3Request(ctx, settings, method, objectKey, operation, expiresIn, queryValues, headers)
}

// signMultipartRecovery applies Runtime's multipart recovery directive to a
// freshly signed recovery route, mirroring TypeScript signMultipartRecovery.
// Core-only grants: the worker receives only the ordinary upload URL.
func signMultipartRecovery(ctx context.Context, destination ProviderDestination, transferID string, requested routeRecoveryChunk, route SignedChunkRoute, expiry time.Duration) (SignedChunkRoute, error) {
	r := requested.Recovery
	metadata := make(map[string]any, len(route.Metadata)+4)
	for key, value := range route.Metadata {
		metadata[key] = value
	}
	delete(metadata, "recovery_staging")
	route.Metadata = metadata
	if r != nil && r.Operation == "renew" {
		count := intValue(metadata["expected_part_count"])
		if count < 1 || count > MultipartMaxPartNumber {
			return route, errors.New("invalid multipart count")
		}
		if requested.PartNumber < 1 || requested.PartNumber > count {
			return route, errors.New("recovery part number is outside the multipart group")
		}
		complete, err := signCompleteMultipartUpload(ctx, destination, requested.FinalObjectKey, requested.UploadID, expiry)
		if err != nil {
			return route, err
		}
		abort, err := signAbortMultipartUpload(ctx, destination, requested.FinalObjectKey, requested.UploadID, expiry)
		if err != nil {
			return route, err
		}
		head, err := signFinalObjectHead(ctx, destination, requested.FinalObjectKey, expiry)
		if err != nil {
			return route, err
		}
		pages := make([]string, 0, (count+999)/1000)
		for marker := 0; marker < count; marker += 1000 {
			page, err := signListMultipartUpload(ctx, destination, requested.FinalObjectKey, requested.UploadID, expiry, 1000, marker)
			if err != nil {
				return route, err
			}
			pages = append(pages, page)
		}
		metadata["complete_url"] = complete
		metadata["abort_url"] = abort
		metadata["final_head_url"] = head
		metadata["list_page_urls"] = pages
		metadata["list_page_url"] = pages[(requested.PartNumber-1)/1000]
		metadata["control_urls_expires_at"] = time.Now().UTC().Add(expiry).Format(time.RFC3339Nano)
		return route, nil
	}
	if r == nil || r.Mode == "direct" {
		return route, nil
	}
	settings, s3Compatible, err := s3SettingsFor(destination)
	if err != nil {
		return route, err
	}
	if !s3Compatible {
		return route, errors.New("multipart recovery requires an S3-compatible destination")
	}
	if r.Mode != "staged" || !recoveryAttemptIDPattern.MatchString(r.AttemptID) {
		return route, errors.New("invalid staging attempt")
	}
	prefix := requested.FinalObjectKey + ".beam-recovery/" + transferID + "/" + strings.ReplaceAll(url.QueryEscape(requested.MultipartGroupID), "+", "%20") + "/"
	key := fmt.Sprintf("%s%d/%s", prefix, requested.PartNumber, r.AttemptID)
	if r.ObjectKey != "" && r.ObjectKey != key {
		return route, errors.New("recovery staging identity mismatch")
	}
	sign := func(method, object, operation string, query, headers map[string]string) (string, error) {
		return presignS3Request(ctx, settings, method, object, operation, expiry, query, headers)
	}
	if r.Operation == "list" {
		query := map[string]string{"list-type": "2", "prefix": prefix, "max-keys": "1000"}
		if r.ContinuationToken != "" {
			query["continuation-token"] = r.ContinuationToken
		}
		grant, err := sign(http.MethodGet, "", "ListObjectsV2", query, nil)
		if err != nil {
			return route, err
		}
		metadata["recovery_listing"] = map[string]any{"prefix": prefix, "url": grant}
		return route, nil
	}
	remove, err := sign(http.MethodDelete, key, "DeleteObject", nil, nil)
	if err != nil {
		return route, err
	}
	if r.Operation == "delete" {
		metadata["recovery_delete"] = map[string]any{"object_key": key, "url": remove}
		return route, nil
	}
	if r.Operation != "upload" && r.Operation != "controls" {
		return route, errors.New("unsupported recovery operation")
	}
	head, err := sign(http.MethodHead, key, "HeadObject", nil, nil)
	if err != nil {
		return route, err
	}
	// The copy source uses the same key escaping as the signed object paths.
	headers := map[string]string{"x-amz-copy-source": url.PathEscape(settings.bucket) + "/" + escapeS3Key(key)}
	// R2 does not promise to enforce copy source conditions; each attempt has
	// its own staging object.
	if settings.provider == "s3" && r.ETag != "" {
		headers["x-amz-copy-source-if-match"] = `"` + strings.Trim(r.ETag, `"`) + `"`
	}
	copyURL, err := sign(http.MethodPut, requested.FinalObjectKey, "UploadPartCopy", map[string]string{"partNumber": fmt.Sprint(requested.PartNumber), "uploadId": requested.UploadID}, headers)
	if err != nil {
		return route, err
	}
	metadata["recovery_staging"] = map[string]any{
		"object_key": key, "attempt_id": r.AttemptID, "head_url": head, "copy_url": copyURL,
		"copy_headers": headers, "delete_url": remove, "expires_at": time.Now().UTC().Add(expiry).Format(time.RFC3339Nano),
	}
	if r.Operation == "upload" {
		route.DestURL, err = sign(http.MethodPut, key, "PutObject", nil, nil)
	}
	return route, err
}
