package beamnetworksdk

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is a minimal S3-compatible endpoint for HEAD, CreateMultipartUpload,
// and AbortMultipartUpload. It never serves object bytes.
type fakeS3 struct {
	*httptest.Server
	mu                 sync.Mutex
	requests           []string
	headCount          int
	createCount        int
	abortCount         int
	activeCreates      int
	maxActiveCreates   int
	createDelay        time.Duration
	uploadID           func(count int) string
	abortAuthorization string
	abortFailures      int
	abortedUploadIDs   []string
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	server := &fakeS3{uploadID: func(count int) string { return fmt.Sprintf("upload-%d", count) }}
	server.Server = httptest.NewServer(http.HandlerFunc(server.serve))
	t.Cleanup(server.Close)
	return server
}

func (server *fakeS3) serve(writer http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	server.requests = append(server.requests, request.Method+" "+request.URL.RequestURI())
	server.mu.Unlock()
	query := request.URL.Query()
	switch {
	case request.Method == http.MethodHead:
		server.mu.Lock()
		server.headCount++
		server.mu.Unlock()
		writer.Header().Set("Content-Length", "1024")
		writer.Header().Set("ETag", `"source-etag"`)
		writer.Header().Set("Last-Modified", "Wed, 21 Oct 2026 07:28:00 GMT")
		writer.Header().Set("x-amz-version-id", "source-version")
		writer.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && query.Has("uploads"):
		server.mu.Lock()
		server.createCount++
		count := server.createCount
		server.activeCreates++
		server.maxActiveCreates = max(server.maxActiveCreates, server.activeCreates)
		delay := server.createDelay
		server.mu.Unlock()
		time.Sleep(delay)
		server.mu.Lock()
		server.activeCreates--
		server.mu.Unlock()
		writer.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(writer, "<CreateMultipartUploadResult><Bucket>destination</Bucket><Key>out</Key><UploadId>%s</UploadId></CreateMultipartUploadResult>", server.uploadID(count))
	case request.Method == http.MethodDelete && query.Get("uploadId") != "":
		server.mu.Lock()
		server.abortCount++
		server.abortAuthorization = request.Header.Get("Authorization")
		fail := server.abortFailures > 0
		if fail {
			server.abortFailures--
		} else {
			server.abortedUploadIDs = append(server.abortedUploadIDs, query.Get("uploadId"))
		}
		server.mu.Unlock()
		if fail {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte("<Error><Code>SlowDown</Code><Message>retry later</Message></Error>"))
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusNotFound)
		_, _ = writer.Write([]byte("not found"))
	}
}

func (server *fakeS3) counts() (heads int, creates int, aborts int) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.headCount, server.createCount, server.abortCount
}

func (server *fakeS3) requestLog() string {
	server.mu.Lock()
	defer server.mu.Unlock()
	return strings.Join(server.requests, "\n")
}

func r2Source(endpoint string) R2ProviderSource {
	return R2ProviderSource{Bucket: "source", Key: "input.bin", AccessKeyID: "cleanup-access", SecretAccessKey: "cleanup-secret", EndpointURL: endpoint}
}

func r2Destination(endpoint string, key string) R2ProviderDestination {
	return R2ProviderDestination{Bucket: "destination", Key: key, AccessKeyID: "cleanup-access", SecretAccessKey: "cleanup-secret", EndpointURL: endpoint}
}
