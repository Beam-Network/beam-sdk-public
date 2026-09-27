package beamnetworksdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/encoding/httpbinding"
)

// s3ProviderMaxAttempts bounds S3-compatible control requests, retrying
// transient provider failures such as throttling.
const s3ProviderMaxAttempts = 5

// s3ClientCacheLimit bounds the per-process client cache; it is cleared whole
// when exceeded so rotated credentials do not accumulate.
const s3ClientCacheLimit = 256

// s3Settings is a resolved S3-compatible provider configuration.
type s3Settings struct {
	provider        string
	id              string
	bucket          string
	key             string
	region          string
	endpoint        string
	forcePathStyle  *bool
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	accountID       string
}

// usePathStyle reports the addressing style shared by SDK-signed and
// self-presigned operations. nil keeps the AWS SDK default (virtual-hosted).
func (settings s3Settings) usePathStyle() bool {
	return settings.forcePathStyle != nil && *settings.forcePathStyle
}

// s3SettingsFor resolves an S3-compatible provider config. The boolean is false
// for providers that are not S3-compatible.
func s3SettingsFor(config any) (s3Settings, bool, error) {
	var raw s3Settings
	switch typed := config.(type) {
	case S3ProviderSource:
		raw = s3Settings{provider: "s3", id: typed.SourceID, bucket: typed.Bucket, key: typed.Key, region: typed.Region, endpoint: typed.EndpointURL, forcePathStyle: typed.ForcePathStyle, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, sessionToken: typed.SessionToken}
	case S3ProviderDestination:
		raw = s3Settings{provider: "s3", id: typed.DestinationID, bucket: typed.Bucket, key: typed.Key, region: typed.Region, endpoint: typed.EndpointURL, forcePathStyle: typed.ForcePathStyle, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, sessionToken: typed.SessionToken}
	case R2ProviderSource:
		raw = s3Settings{provider: "r2", id: typed.SourceID, bucket: typed.Bucket, key: typed.Key, endpoint: typed.EndpointURL, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, accountID: typed.AccountID}
	case R2ProviderDestination:
		raw = s3Settings{provider: "r2", id: typed.DestinationID, bucket: typed.Bucket, key: typed.Key, endpoint: typed.EndpointURL, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, accountID: typed.AccountID}
	case S3CompatibleProviderSource:
		raw = s3Settings{provider: s3CompatibleProviderName(typed.Provider), id: typed.SourceID, bucket: typed.Bucket, key: typed.Key, region: typed.Region, endpoint: typed.EndpointURL, forcePathStyle: typed.ForcePathStyle, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, sessionToken: typed.SessionToken, accountID: typed.AccountID}
	case S3CompatibleProviderDestination:
		raw = s3Settings{provider: s3CompatibleProviderName(typed.Provider), id: typed.DestinationID, bucket: typed.Bucket, key: typed.Key, region: typed.Region, endpoint: typed.EndpointURL, forcePathStyle: typed.ForcePathStyle, accessKeyID: typed.AccessKeyID, secretAccessKey: typed.SecretAccessKey, sessionToken: typed.SessionToken, accountID: typed.AccountID}
	default:
		return s3Settings{}, false, nil
	}
	settings, err := resolveS3Settings(raw)
	return settings, true, err
}

func s3CompatibleProviderName(provider string) string {
	return defaultString(strings.ToLower(strings.TrimSpace(provider)), "s3-compatible")
}

// S3CompatibleEndpoint resolves the endpoint an S3, R2, or S3-compatible config
// signs against. It is empty for AWS S3 without an explicit endpoint.
func S3CompatibleEndpoint(config any) (string, error) {
	settings, err := s3SettingsForExport(config)
	return settings.endpoint, err
}

// S3CompatibleRegion resolves the signing region of an S3-compatible config.
func S3CompatibleRegion(config any) (string, error) {
	settings, err := s3SettingsForExport(config)
	return settings.region, err
}

// S3CompatibleForcePathStyle resolves path-style addressing: nil means the AWS
// SDK default (virtual-hosted), used for AWS S3 unless overridden.
func S3CompatibleForcePathStyle(config any) (*bool, error) {
	settings, err := s3SettingsForExport(config)
	return settings.forcePathStyle, err
}

func s3SettingsForExport(config any) (s3Settings, error) {
	settings, s3Compatible, err := s3SettingsFor(config)
	if err != nil {
		return s3Settings{}, err
	}
	if !s3Compatible {
		return s3Settings{}, fmt.Errorf("%s is not an S3-compatible provider config", providerName(config))
	}
	return settings, nil
}

func resolveS3Settings(raw s3Settings) (s3Settings, error) {
	endpoint, err := s3CompatibleEndpoint(raw.provider, raw.endpoint, raw.accountID)
	if err != nil {
		return s3Settings{}, err
	}
	raw.endpoint = endpoint
	raw.region = s3CompatibleRegion(raw.provider, raw.region)
	raw.forcePathStyle = s3CompatibleForcePathStyle(raw.provider, raw.forcePathStyle, endpoint)
	return raw, nil
}

// s3CompatibleEndpoint resolves the endpoint: an explicit endpoint URL, the R2
// account endpoint, or empty for AWS S3. Other providers require an endpoint.
func s3CompatibleEndpoint(provider string, endpointURL string, accountID string) (string, error) {
	if strings.TrimSpace(endpointURL) != "" {
		return endpointURL, nil
	}
	if provider == "r2" {
		if strings.TrimSpace(accountID) != "" {
			return fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID), nil
		}
		return "", fmt.Errorf("r2 config requires account_id or endpoint_url.")
	}
	if provider != "s3" {
		return "", fmt.Errorf("%s config requires endpoint_url.", provider)
	}
	return "", nil
}

// s3CompatibleRegion resolves the signing region: explicit, "auto" for R2, else us-east-1.
func s3CompatibleRegion(provider string, region string) string {
	if strings.TrimSpace(region) != "" {
		return region
	}
	if provider == "r2" {
		return "auto"
	}
	return "us-east-1"
}

// s3CompatibleForcePathStyle resolves path-style addressing: explicit, the SDK
// default (nil) for AWS S3, else path style whenever a custom endpoint is set.
func s3CompatibleForcePathStyle(provider string, forcePathStyle *bool, endpoint string) *bool {
	if forcePathStyle != nil {
		return aws.Bool(*forcePathStyle)
	}
	if provider == "s3" {
		return nil
	}
	return aws.Bool(endpoint != "")
}

var (
	s3ClientCacheMu sync.Mutex
	s3ClientCache   = map[string]*s3.Client{}
)

// s3ClientFor returns a cached S3 client for the resolved settings.
func s3ClientFor(settings s3Settings) *s3.Client {
	keyBytes, _ := json.Marshal([]any{
		settings.provider, settings.endpoint, settings.region, settings.usePathStyle(),
		settings.accessKeyID, settings.secretAccessKey, settings.sessionToken,
	})
	cacheKey := string(keyBytes)
	s3ClientCacheMu.Lock()
	defer s3ClientCacheMu.Unlock()
	if client := s3ClientCache[cacheKey]; client != nil {
		return client
	}
	client := newS3ClientFor(settings)
	if len(s3ClientCache) >= s3ClientCacheLimit {
		clear(s3ClientCache)
	}
	s3ClientCache[cacheKey] = client
	return client
}

func newS3ClientFor(settings s3Settings) *s3.Client {
	options := s3.Options{
		Region:           settings.region,
		UsePathStyle:     settings.usePathStyle(),
		RetryMaxAttempts: s3ProviderMaxAttempts,
		Credentials:      aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(settings.accessKeyID, settings.secretAccessKey, settings.sessionToken)),
	}
	if settings.endpoint != "" {
		options.BaseEndpoint = aws.String(settings.endpoint)
	}
	return s3.New(options)
}

func s3ClientForContext(ctx context.Context, settings s3Settings) *s3.Client {
	if clients, ok := ctx.Value(providerClientsKey{}).(*providerClients); !ok || clients == nil {
		return s3ClientFor(settings)
	}
	return cachedProviderClient(ctx, []string{settings.provider, settings.endpoint, settings.region, fmt.Sprint(settings.usePathStyle()), settings.accessKeyID, settings.secretAccessKey, settings.sessionToken}, func() *s3.Client { return newS3ClientFor(settings) })
}

// s3ObjectURL resolves the object URL with the same endpoint rules, and so the
// same virtual-hosted or path-style addressing, as the SDK client.
func s3ObjectURL(ctx context.Context, settings s3Settings, objectKey string) (*url.URL, error) {
	params := s3.EndpointParameters{
		Bucket:            aws.String(settings.bucket),
		Region:            aws.String(settings.region),
		ForcePathStyle:    aws.Bool(settings.usePathStyle()),
		UseFIPS:           aws.Bool(false),
		UseDualStack:      aws.Bool(false),
		Accelerate:        aws.Bool(false),
		UseGlobalEndpoint: aws.Bool(false),
	}
	if settings.endpoint != "" {
		params.Endpoint = aws.String(settings.endpoint)
	}
	resolved, err := s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
	if err != nil {
		return nil, err
	}
	objectURL := resolved.URI
	basePath := strings.TrimRight(objectURL.Path, "/")
	baseRawPath := strings.TrimRight(objectURL.EscapedPath(), "/")
	if objectKey == "" && basePath != "" {
		// Bucket-level operation (ListObjectsV2) on a path-style endpoint.
		objectURL.Path = basePath
		objectURL.RawPath = baseRawPath
		return &objectURL, nil
	}
	objectURL.Path = basePath + "/" + objectKey
	objectURL.RawPath = baseRawPath + "/" + escapeS3Key(objectKey)
	return &objectURL, nil
}

// presignS3Request presigns an S3 operation the SDK presigner does not expose
// (CompleteMultipartUpload, AbortMultipartUpload, ListParts).
func presignS3Request(
	ctx context.Context,
	settings s3Settings,
	method string,
	objectKey string,
	operation string,
	expiresIn time.Duration,
	queryValues map[string]string,
	headers map[string]string,
) (string, error) {
	objectURL, err := s3ObjectURL(ctx, settings, objectKey)
	if err != nil {
		return "", err
	}
	query := objectURL.Query()
	query.Set("x-id", operation)
	query.Set("X-Amz-Expires", fmt.Sprintf("%.0f", expiresIn.Seconds()))
	for key, value := range queryValues {
		query.Set(key, value)
	}
	objectURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, objectURL.String(), nil)
	if err != nil {
		return "", err
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	signedURL, _, err := v4.NewSigner().PresignHTTP(
		ctx,
		aws.Credentials{
			AccessKeyID:     settings.accessKeyID,
			SecretAccessKey: settings.secretAccessKey,
			SessionToken:    settings.sessionToken,
			Source:          "beam-network-sdk",
		},
		request,
		"UNSIGNED-PAYLOAD",
		"s3",
		settings.region,
		time.Now().UTC(),
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	)
	if err != nil {
		return "", err
	}
	return signedURL, nil
}

// escapeS3Key encodes an object key exactly as the AWS SDK serializes it and
// as S3 canonicalizes it for SigV4: every byte except the unreserved
// characters and "/" is percent-encoded. url.PathEscape is not equivalent: it
// leaves characters such as + = @ : $ & unescaped, which S3 re-encodes when it
// verifies the signature, so the signature would not match.
func escapeS3Key(key string) string {
	return httpbinding.EscapePath(key, false)
}
