package beamnetworksdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("signing must not read source data")
}

func TestHybridSigningComposesFrozenRangesAndChecksumBoundUploads(t *testing.T) {
	noRead := &http.Client{Transport: failingTransport{}}
	for _, provider := range []string{"r2", "hippius", "huggingface"} {
		config := S3CompatibleProviderSource{
			Provider: provider, Driver: "s3-compatible", Bucket: "bucket", Key: "file.bin",
			AccessKeyID: "fixture-key", SecretAccessKey: "fixture-secret", Region: "us-east-1",
			EndpointURL: "https://storage.example.test", ForcePathStyle: Bool(true),
		}
		source, err := SignSourceReadRange(context.Background(), SourceReadRangeInput{
			Source: config, Offset: 10, Length: 20, ExpiresIn: time.Minute,
			IfMatch: `"frozen-etag"`, VersionID: "version-1", HTTPClient: noRead,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceURL, _ := url.Parse(source.URL)
		if source.Headers["Range"] != "bytes=10-29" || source.Headers["If-Match"] != `"frozen-etag"` ||
			sourceURL.Query().Get("versionId") != "version-1" || !strings.Contains(sourceURL.Query().Get("X-Amz-SignedHeaders"), "if-match") {
			t.Fatalf("%s source range = %+v", provider, source)
		}
		destination := S3CompatibleProviderDestination{
			Provider: provider, Driver: "s3-compatible", Bucket: "bucket", Key: "file.bin",
			AccessKeyID: "fixture-key", SecretAccessKey: "fixture-secret", Region: "us-east-1",
			EndpointURL: "https://storage.example.test", ForcePathStyle: Bool(true),
		}
		signed, err := SignDestinationURL(context.Background(), DestinationURLInput{
			Destination: destination, ObjectKey: "final.bin", UploadID: "upload-1", PartNumber: 1,
			ExpiresIn: time.Minute, ContentMD5: "1B2M2Y8AsgTpgAmY7PhCfg==", HTTPClient: noRead,
		})
		if err != nil {
			t.Fatal(err)
		}
		destinationURL, _ := url.Parse(signed)
		if destinationURL.Path != "/bucket/final.bin" || destinationURL.Query().Get("uploadId") != "upload-1" ||
			destinationURL.Query().Get("partNumber") != "1" || !strings.Contains(destinationURL.Query().Get("X-Amz-SignedHeaders"), "content-md5") {
			t.Fatalf("%s destination URL = %s", provider, signed)
		}
	}
	hippius := HippiusProviderSource{Bucket: "bucket", Key: "file.bin", APIToken: "token"}
	if _, err := SignSourceReadRange(context.Background(), SourceReadRangeInput{Source: hippius, IfMatch: `"etag"`, Length: 1, ExpiresIn: time.Minute, HTTPClient: noRead}); err == nil || !strings.Contains(err.Error(), "S3-compatible") {
		t.Fatalf("non-S3 sources must reject conditions, got %v", err)
	}
	if _, err := SignDestinationURL(context.Background(), DestinationURLInput{Destination: HippiusProviderDestination{Bucket: "b", Key: "k", APIToken: "t"}, ObjectKey: "k", ContentMD5: "x", ExpiresIn: time.Minute, HTTPClient: noRead}); err == nil || !strings.Contains(err.Error(), "S3-compatible") {
		t.Fatalf("non-S3 destinations must reject checksum binding, got %v", err)
	}
	if _, err := SignDestinationReadRange(context.Background(), DestinationReadRangeInput{Destination: HippiusProviderDestination{Bucket: "b", Key: "k", APIToken: "t"}, ObjectKey: "k", IfMatch: `"etag"`, Length: 1, ExpiresIn: time.Minute, HTTPClient: noRead}); err == nil {
		t.Fatal("non-S3 destinations must reject conditional ranges")
	}
}

func TestProviderVerificationPaginatesPartsAndCompletesWithoutPayloadReads(t *testing.T) {
	var mu sync.Mutex
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		requests = append(requests, request.Method+" "+request.URL.RequestURI())
		mu.Unlock()
		if request.Method == http.MethodHead {
			writer.Header().Set("Content-Length", "12")
			writer.Header().Set("ETag", `"final"`)
			writer.Header().Set("x-amz-meta-beam-room-operation-id", "operation")
			return
		}
		writer.Header().Set("Content-Type", "application/xml")
		if request.Method == http.MethodGet && request.URL.Query().Has("uploadId") {
			second := request.URL.Query().Get("part-number-marker") == "1"
			partNumber, etag, next := 1, "a", "<NextPartNumberMarker>1</NextPartNumberMarker>"
			if second {
				partNumber, etag, next = 2, "b", ""
			}
			fmt.Fprintf(writer, "<ListPartsResult><IsTruncated>%t</IsTruncated>%s<Part><PartNumber>%d</PartNumber><ETag>%s</ETag><Size>6</Size></Part></ListPartsResult>", !second, next, partNumber, etag)
			return
		}
		if request.Method == http.MethodPost {
			_, _ = writer.Write([]byte("<CompleteMultipartUploadResult><ETag>final</ETag></CompleteMultipartUploadResult>"))
			return
		}
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	destination := S3ProviderDestination{Bucket: "bucket", Key: "file", EndpointURL: server.URL, ForcePathStyle: Bool(true), AccessKeyID: "test", SecretAccessKey: "test"}
	parts, err := ListMultipartParts(context.Background(), destination, "file", "upload")
	if err != nil {
		t.Fatal(err)
	}
	if want := []MultipartPart{{PartNumber: 1, ETag: "a", Size: 6}, {PartNumber: 2, ETag: "b", Size: 6}}; !reflect.DeepEqual(parts, want) {
		t.Fatalf("parts = %+v", parts)
	}
	completed, err := CompleteMultipartUpload(context.Background(), destination, "file", "upload", []MultipartPart{parts[1], parts[0]})
	if err != nil || completed.ETag != "final" {
		t.Fatalf("complete = %+v, %v", completed, err)
	}
	head, err := InspectDestinationObject(context.Background(), destination, "file")
	if err != nil || head.Size != 12 || head.Metadata["beam-room-operation-id"] != "operation" {
		t.Fatalf("head = %+v, %v", head, err)
	}
	mu.Lock()
	defer mu.Unlock()
	gets := 0
	for _, request := range requests {
		if strings.HasPrefix(request, "GET ") {
			gets++
		}
	}
	if len(requests) != 4 || gets != 2 {
		t.Fatalf("requests = %v", requests)
	}
}

func TestProviderMetadataCancellationStopsInFlightRequests(t *testing.T) {
	operations := map[string]func(context.Context, ProviderDestination) error{
		"create": func(ctx context.Context, destination ProviderDestination) error {
			_, err := CreateMultipartUpload(ctx, destination, "file", map[string]string{})
			return err
		},
		"list": func(ctx context.Context, destination ProviderDestination) error {
			_, err := ListMultipartParts(ctx, destination, "file", "upload")
			return err
		},
		"complete": func(ctx context.Context, destination ProviderDestination) error {
			_, err := CompleteMultipartUpload(ctx, destination, "file", "upload", []MultipartPart{{PartNumber: 1, ETag: "part"}})
			return err
		},
		"inspect": func(ctx context.Context, destination ProviderDestination) error {
			_, err := InspectDestinationObject(ctx, destination, "file")
			return err
		},
		"abort": func(ctx context.Context, destination ProviderDestination) error {
			return AbortMultipartUpload(ctx, destination, "file", "upload")
		},
	}
	for name, operation := range operations {
		ctx, cancel := context.WithCancel(context.Background())
		received := make(chan struct{}, 1)
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			// Draining the body lets the server observe the client disconnect.
			_, _ = io.Copy(io.Discard, request.Body)
			select {
			case received <- struct{}{}:
			default:
			}
			cancel()
			<-request.Context().Done()
		}))
		destination := S3ProviderDestination{Bucket: "bucket", Key: "file", Region: "us-east-1", AccessKeyID: "test", SecretAccessKey: "test", EndpointURL: server.URL}
		err := operation(ctx, destination)
		server.CloseClientConnections()
		server.Close()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: cancellation error = %v", name, err)
		}
		select {
		case <-received:
		default:
			t.Fatalf("%s: request must reach the provider before cancellation", name)
		}
	}
}
