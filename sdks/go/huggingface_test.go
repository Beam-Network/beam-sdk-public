package beamnetworksdk

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const huggingFaceTestToken = "hf_test_token"

func TestHuggingFaceURLsFollowRepoTypePrefix(t *testing.T) {
	dataset := huggingFaceSourceConfig(HuggingFaceProviderSource{
		RepoID:   "acme/corpus",
		Path:     "data/train.parquet",
		RepoType: "dataset",
		Revision: "refs/pr/4",
		Token:    huggingFaceTestToken,
	})
	if got, want := huggingFaceResolveURL(dataset), "https://huggingface.co/datasets/acme/corpus/resolve/refs%2Fpr%2F4/data/train.parquet"; got != want {
		t.Fatalf("resolve url = %q, want %q", got, want)
	}
	if got, want := huggingFaceAPIBase(dataset), "https://huggingface.co/api/datasets/acme/corpus"; got != want {
		t.Fatalf("api base = %q, want %q", got, want)
	}
	if got, want := huggingFaceLFSBatchURL(dataset), "https://huggingface.co/datasets/acme/corpus.git/info/lfs/objects/batch"; got != want {
		t.Fatalf("lfs batch url = %q, want %q", got, want)
	}

	// A model repo carries no prefix and defaults to `main`.
	model := huggingFaceSourceConfig(HuggingFaceProviderSource{
		RepoID: "acme/net", Path: "model.safetensors", Token: huggingFaceTestToken,
	})
	if got, want := huggingFaceResolveURL(model), "https://huggingface.co/acme/net/resolve/main/model.safetensors"; got != want {
		t.Fatalf("resolve url = %q, want %q", got, want)
	}
	if got, want := huggingFaceAPIBase(model), "https://huggingface.co/api/models/acme/net"; got != want {
		t.Fatalf("api base = %q, want %q", got, want)
	}
}

func TestHuggingFaceBucketIsUnversionedAndEscapesWholeKey(t *testing.T) {
	bucket := huggingFaceSourceConfig(HuggingFaceProviderSource{
		RepoID:   "acme/store",
		Path:     "nested/data.bin",
		RepoType: "bucket",
		Revision: "v2",
		Token:    huggingFaceTestToken,
	})
	// No revision segment, and the key is escaped whole -- see HfApi.get_bucket_file_metadata.
	if got, want := huggingFaceResolveURL(bucket), "https://huggingface.co/buckets/acme/store/resolve/nested%2Fdata.bin"; got != want {
		t.Fatalf("bucket resolve url = %q, want %q", got, want)
	}
}

func TestHuggingFaceSourceResolvesToCDNRedirect(t *testing.T) {
	cdn := httptest.NewServer(cdnHandler(nil))
	defer cdn.Close()
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{cdnOrigin: cdn.URL, size: 4096, sha256: strings.Repeat("a", 64)})
	defer hub.Close()

	prepared, err := prepareProviderSource(context.Background(), http.DefaultClient, HuggingFaceProviderSource{
		RepoID:   "acme/corpus",
		Path:     "data/train.parquet",
		RepoType: "dataset",
		Token:    huggingFaceTestToken,
		Endpoint: hub.URL,
	}, 0, 0)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Provider != "huggingface" {
		t.Fatalf("provider = %q", prepared.Provider)
	}
	// X-Linked-Size wins over the redirect body's Content-Length.
	if prepared.Size != 4096 {
		t.Fatalf("size = %d, want 4096", prepared.Size)
	}
	if prepared.Filename != "train.parquet" {
		t.Fatalf("filename = %q", prepared.Filename)
	}
	if !strings.HasPrefix(prepared.URL, cdn.URL) {
		t.Fatalf("expected a CDN url, got %q", prepared.URL)
	}
	// No credential may travel with the prepared source.
	encoded, _ := json.Marshal(prepared)
	if strings.Contains(string(encoded), huggingFaceTestToken) {
		t.Fatalf("prepared source leaked the token: %s", encoded)
	}
	if prepared.Metadata["sha256"] != strings.Repeat("a", 64) {
		t.Fatalf("sha256 = %v", prepared.Metadata["sha256"])
	}
	if prepared.Metadata["commit_hash"] != "deadbeef" {
		t.Fatalf("commit_hash = %v", prepared.Metadata["commit_hash"])
	}
}

func TestHuggingFaceSourceWithoutRedirectIsRejected(t *testing.T) {
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{size: 512})
	defer hub.Close()

	_, err := prepareProviderSource(context.Background(), http.DefaultClient, HuggingFaceProviderSource{
		RepoID:   "acme/corpus",
		Path:     "README.md",
		RepoType: "dataset",
		Token:    huggingFaceTestToken,
		Endpoint: hub.URL,
	}, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "A small regular file is served inline from the Hub") {
		t.Fatalf("expected the inline-file explanation, got %v", err)
	}
}

func TestHuggingFaceDestinationPreparesAsDirectPut(t *testing.T) {
	prepared, err := prepareProviderDestination(HuggingFaceProviderDestination{
		RepoID:   "acme/corpus",
		Path:     "data/out.parquet",
		RepoType: "dataset",
		Token:    huggingFaceTestToken,
	}, 1)
	if err != nil {
		t.Fatalf("prepare destination: %v", err)
	}
	if prepared.DestinationID != "dst_1" || prepared.Provider != "huggingface" {
		t.Fatalf("prepared = %+v", prepared)
	}
	if prepared.LogicalPrefix != "data/out.parquet" {
		t.Fatalf("logical prefix = %q", prepared.LogicalPrefix)
	}
	if prepared.Metadata["driver"] != "huggingface" {
		t.Fatalf("driver = %v", prepared.Metadata["driver"])
	}
	encoded, _ := json.Marshal(prepared)
	if strings.Contains(string(encoded), huggingFaceTestToken) {
		t.Fatalf("prepared destination leaked the token: %s", encoded)
	}
}

func TestHuggingFaceUploadProtocol(t *testing.T) {
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{chunkSize: 1024, partCount: 3})
	defer hub.Close()

	config := huggingFaceDestinationConfig(HuggingFaceProviderDestination{
		RepoID:        "acme/corpus",
		Path:          "data/out.parquet",
		RepoType:      "dataset",
		Token:         huggingFaceTestToken,
		Endpoint:      hub.URL,
		CommitMessage: "Add out.parquet",
	})
	ctx := context.Background()

	uploadMode, shouldIgnore, err := huggingFacePreupload(ctx, http.DefaultClient, config, 3000, "AAAA")
	if err != nil || uploadMode != "lfs" || shouldIgnore {
		t.Fatalf("preupload = %q, %v, %v", uploadMode, shouldIgnore, err)
	}
	assertJSONBody(t, hub, "/api/datasets/acme/corpus/preupload/main", map[string]any{
		"files": []any{map[string]any{"path": "data/out.parquet", "sample": "AAAA", "size": float64(3000)}},
	})

	plan, err := huggingFaceLFSBatch(ctx, http.DefaultClient, config, strings.Repeat("b", 64), 3000)
	if err != nil {
		t.Fatalf("lfs batch: %v", err)
	}
	assertJSONBody(t, hub, "/datasets/acme/corpus.git/info/lfs/objects/batch", map[string]any{
		"operation": "upload",
		"transfers": []any{"basic", "multipart"},
		"hash_algo": "sha256",
		"ref":       map[string]any{"name": "main"},
		"objects":   []any{map[string]any{"oid": strings.Repeat("b", 64), "size": float64(3000)}},
	})
	if plan.ChunkSize != 1024 {
		t.Fatalf("chunk size = %d", plan.ChunkSize)
	}
	// Zero-padded part keys must sort numerically, not lexically.
	wantParts := []string{hub.URL + "/part/1", hub.URL + "/part/2", hub.URL + "/part/3"}
	if !reflect.DeepEqual(plan.PartURLs, wantParts) {
		t.Fatalf("part urls = %v, want %v", plan.PartURLs, wantParts)
	}

	if err := huggingFaceCompleteLFSUpload(ctx, http.DefaultClient, config, plan.UploadHref, plan.OID, []string{"e1", "e2", "e3"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	assertJSONBody(t, hub, "/lfs/complete", map[string]any{
		"oid": strings.Repeat("b", 64),
		"parts": []any{
			map[string]any{"partNumber": float64(1), "etag": "e1"},
			map[string]any{"partNumber": float64(2), "etag": "e2"},
			map[string]any{"partNumber": float64(3), "etag": "e3"},
		},
	})

	if err := huggingFaceCommit(ctx, http.DefaultClient, config, plan.OID, 3000); err != nil {
		t.Fatalf("commit: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(hub.bodies["/api/datasets/acme/corpus/commit/main"]), "\n")
	if len(lines) != 2 {
		t.Fatalf("commit ndjson = %v", lines)
	}
	assertJSON(t, lines[0], map[string]any{
		"key":   "header",
		"value": map[string]any{"summary": "Add out.parquet", "description": ""},
	})
	assertJSON(t, lines[1], map[string]any{
		"key": "lfsFile",
		"value": map[string]any{
			"path": "data/out.parquet", "algo": "sha256",
			"oid": strings.Repeat("b", 64), "size": float64(3000),
		},
	})
}

func TestHuggingFacePartCountMismatchIsRejected(t *testing.T) {
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{chunkSize: 1024, partCount: 2})
	defer hub.Close()

	config := huggingFaceDestinationConfig(HuggingFaceProviderDestination{
		RepoID: "acme/corpus", Path: "data/out.parquet", RepoType: "dataset",
		Token: huggingFaceTestToken, Endpoint: hub.URL,
	})
	_, err := huggingFaceLFSBatch(context.Background(), http.DefaultClient, config, strings.Repeat("c", 64), 3000)
	if err == nil || !strings.Contains(err.Error(), "returned 2 part URLs") ||
		!strings.Contains(err.Error(), "expected 3 at chunk_size 1024") {
		t.Fatalf("expected a part-count mismatch, got %v", err)
	}
}

func TestHuggingFaceAlreadyStoredObjectHasNoUploadActions(t *testing.T) {
	hub := newHuggingFaceHub(t, huggingFaceHubOptions{alreadyUploaded: true})
	defer hub.Close()

	config := huggingFaceDestinationConfig(HuggingFaceProviderDestination{
		RepoID: "acme/corpus", Path: "data/out.parquet", RepoType: "dataset",
		Token: huggingFaceTestToken, Endpoint: hub.URL,
	})
	plan, err := huggingFaceLFSBatch(context.Background(), http.DefaultClient, config, strings.Repeat("d", 64), 3000)
	if err != nil {
		t.Fatalf("lfs batch: %v", err)
	}
	if plan.UploadHref != "" || plan.OID != strings.Repeat("d", 64) {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestHuggingFaceHashSourceYieldsSHA256AndPartMD5s(t *testing.T) {
	body := make([]byte, 2500)
	for index := range body {
		body[index] = byte(index % 251)
	}
	cdn := httptest.NewServer(cdnHandler(body))
	defer cdn.Close()
	ctx := context.Background()

	sample, err := huggingFaceSourceSample(ctx, http.DefaultClient, cdn.URL, 512)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString(body[:512]); sample != want {
		t.Fatalf("sample = %q, want %q", sample, want)
	}

	digest, parts, err := huggingFaceHashSource(ctx, http.DefaultClient, cdn.URL, 0, true)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	wholeSum := sha256.Sum256(body)
	if digest != hex.EncodeToString(wholeSum[:]) {
		t.Fatalf("sha256 = %q", digest)
	}
	if len(parts) != 0 {
		t.Fatalf("parts = %v", parts)
	}

	digest, parts, err = huggingFaceHashSource(ctx, http.DefaultClient, cdn.URL, 1024, false)
	if err != nil {
		t.Fatalf("hash parts: %v", err)
	}
	if digest != "" {
		t.Fatalf("expected no whole-object digest, got %q", digest)
	}
	want := []string{md5Hex(body[:1024]), md5Hex(body[1024:2048]), md5Hex(body[2048:])}
	if !reflect.DeepEqual(parts, want) {
		t.Fatalf("part etags = %v, want %v", parts, want)
	}
}

func md5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// cdnHandler stands in for the presigned CDN the Hub redirects to, and for a plain source object.
func cdnHandler(body []byte) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		chunk := body
		status := http.StatusOK
		if raw := request.Header.Get("Range"); strings.HasPrefix(raw, "bytes=") {
			var start, end int
			if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err == nil {
				if end+1 > len(body) {
					end = len(body) - 1
				}
				chunk = body[start : end+1]
				status = http.StatusPartialContent
			}
		}
		writer.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		writer.WriteHeader(status)
		_, _ = writer.Write(chunk)
	})
}

type huggingFaceHubOptions struct {
	cdnOrigin       string
	size            int64
	sha256          string
	chunkSize       int64
	partCount       int
	alreadyUploaded bool
}

type huggingFaceHub struct {
	*httptest.Server
	bodies map[string]string
}

func newHuggingFaceHub(t *testing.T, options huggingFaceHubOptions) *huggingFaceHub {
	t.Helper()
	hub := &huggingFaceHub{bodies: map[string]string{}}
	hub.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead {
			if options.cdnOrigin != "" {
				writer.Header().Set("Location", options.cdnOrigin+"/cas/blob?sig=abc")
				writer.Header().Set("X-Linked-Size", strconv.FormatInt(options.size, 10))
				writer.Header().Set("X-Linked-Etag", `"`+options.sha256+`"`)
				writer.Header().Set("X-Repo-Commit", "deadbeef")
				// A redirect body length that must not be mistaken for the object size.
				writer.Header().Set("Content-Length", "0")
				writer.WriteHeader(http.StatusFound)
				return
			}
			// Served inline, the way a Xet blob or a small regular file is.
			writer.Header().Set("Content-Length", strconv.FormatInt(options.size, 10))
			writer.Header().Set("ETag", `"0123456789abcdef"`)
			writer.WriteHeader(http.StatusOK)
			return
		}

		raw, _ := io.ReadAll(request.Body)
		path := request.URL.Path
		hub.bodies[path] = string(raw)

		switch {
		case strings.Contains(path, "/preupload/"):
			writeJSON(writer, map[string]any{"files": []any{map[string]any{
				"path": "data/out.parquet", "uploadMode": "lfs", "shouldIgnore": false,
			}}})
		case strings.HasSuffix(path, "/info/lfs/objects/batch"):
			var payload struct {
				Objects []struct {
					OID string `json:"oid"`
				} `json:"objects"`
			}
			_ = json.Unmarshal(raw, &payload)
			oid := payload.Objects[0].OID
			if options.alreadyUploaded {
				writeJSON(writer, map[string]any{"objects": []any{map[string]any{
					"oid": oid, "size": options.size,
				}}})
				return
			}
			header := map[string]string{"chunk_size": strconv.FormatInt(options.chunkSize, 10)}
			// Emit the parts out of order so the numeric sort is actually exercised.
			for part := options.partCount; part >= 1; part-- {
				header[fmt.Sprintf("%05d", part)] = fmt.Sprintf("%s/part/%d", hub.URL, part)
			}
			writeJSON(writer, map[string]any{"objects": []any{map[string]any{
				"oid": oid, "size": options.size,
				"actions": map[string]any{
					"upload": map[string]any{"href": hub.URL + "/lfs/complete", "header": header},
					"verify": map[string]any{"href": hub.URL + "/lfs/verify"},
				},
			}}})
		default:
			writeJSON(writer, map[string]any{"commitOid": "cafe"})
		}
	}))
	return hub
}

func writeJSON(writer http.ResponseWriter, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(payload)
}

func assertJSONBody(t *testing.T, hub *huggingFaceHub, path string, want any) {
	t.Helper()
	assertJSON(t, hub.bodies[path], want)
}

func assertJSON(t *testing.T, raw string, want any) {
	t.Helper()
	var got any
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %#v, want %#v", got, want)
	}
}
