package beamnetworksdk

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const bucketLocationTimeout = 3 * time.Second

const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

var (
	bucketLocationElement = regexp.MustCompile(`<LocationConstraint(?:\s[^>]*)?>([^<]*)</LocationConstraint>`)
	bucketLocationValue   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

type bucketLocationLookup struct {
	done  chan struct{}
	value string
}

func (lookup *bucketLocationLookup) wait(ctx context.Context) string {
	if lookup == nil {
		return ""
	}
	select {
	case <-lookup.done:
		return lookup.value
	case <-ctx.Done():
		return ""
	}
}

func (client *Client) bucketLocation(config any) *bucketLocationLookup {
	settings, ok := bucketLocationSettings(config)
	if !ok {
		return nil
	}
	key := settings.endpoint + "\n" + settings.bucket
	client.bucketLocationsMu.Lock()
	defer client.bucketLocationsMu.Unlock()
	if lookup := client.bucketLocations[key]; lookup != nil {
		return lookup
	}
	if client.bucketLocations == nil {
		client.bucketLocations = map[string]*bucketLocationLookup{}
	}
	lookup := &bucketLocationLookup{done: make(chan struct{})}
	client.bucketLocations[key] = lookup
	httpClient := client.httpClient
	go func() {
		defer close(lookup.done)
		ctx, cancel := context.WithTimeout(context.Background(), bucketLocationTimeout)
		defer cancel()
		lookup.value = readBucketLocation(ctx, httpClient, settings)
	}()
	return lookup
}

func bucketLocationSettings(config any) (s3Settings, bool) {
	settings, s3Compatible, err := s3SettingsFor(config)
	if err != nil || !s3Compatible || strings.TrimSpace(settings.storageLocation) != "" || settings.endpoint == "" {
		return s3Settings{}, false
	}
	endpoint, err := url.Parse(settings.endpoint)
	if err != nil {
		return s3Settings{}, false
	}
	host := strings.TrimSuffix(strings.ToLower(endpoint.Hostname()), ".")
	if strings.HasSuffix(host, ".r2.cloudflarestorage.com") || host == "storage.googleapis.com" || strings.HasSuffix(host, ".storage.googleapis.com") {
		return settings, true
	}
	return s3Settings{}, false
}

func readBucketLocation(ctx context.Context, httpClient *http.Client, settings s3Settings) string {
	target, err := s3ObjectURL(ctx, settings, "")
	if err != nil {
		return ""
	}
	target.RawQuery = "location="
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return ""
	}
	request.Header.Set("X-Amz-Content-Sha256", emptyPayloadSHA256)
	credentials := aws.Credentials{AccessKeyID: settings.accessKeyID, SecretAccessKey: settings.secretAccessKey, SessionToken: settings.sessionToken, Source: "beam-network-sdk"}
	signer := v4.NewSigner(func(options *v4.SignerOptions) { options.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(ctx, credentials, request, emptyPayloadSHA256, "s3", settings.region, time.Now().UTC()); err != nil {
		return ""
	}
	response, err := httpClientOrDefault(httpClient).Do(request)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil || response.StatusCode < 200 || response.StatusCode > 299 {
		return ""
	}
	match := bucketLocationElement.FindStringSubmatch(string(body))
	if match == nil {
		return ""
	}
	value := strings.TrimSpace(match[1])
	if !bucketLocationValue.MatchString(value) {
		return ""
	}
	return value
}

func withBucketLocation(metadata map[string]any, location string) map[string]any {
	if location == "" {
		return metadata
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["bucket_location"] = location
	return metadata
}
