package beamnetworksdk

// Hugging Face Hub protocol helpers.
//
// The Hub is reached over plain HTTP: a resolve URL that redirects to a presigned CDN URL for
// reads, and the preupload / LFS batch / completion / commit sequence for writes. The token
// never leaves this process; only the presigned URLs the Hub hands back are passed on.

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	huggingFaceDefaultEndpoint = "https://huggingface.co"
	huggingFaceDefaultRevision = "main"
	huggingFaceDefaultRepoType = "model"
	huggingFaceLFSContentType  = "application/vnd.git-lfs+json"
)

// huggingFaceRepoTypeURLPrefixes mirrors constants.REPO_TYPES_URL_PREFIXES; models carry no prefix.
var huggingFaceRepoTypeURLPrefixes = map[string]string{
	"model":   "",
	"dataset": "datasets/",
	"space":   "spaces/",
	"kernel":  "kernels/",
	"bucket":  "buckets/",
}

var huggingFacePartKey = regexp.MustCompile(`^\d+$`)

// huggingFaceConfig is the subset of the source and destination configs the protocol needs.
type huggingFaceConfig struct {
	RepoID   string
	Path     string
	RepoType string
	Revision string
	Token    string
	Endpoint string

	CommitMessage     string
	CommitDescription string
	CreatePR          bool
}

func huggingFaceSourceConfig(source HuggingFaceProviderSource) huggingFaceConfig {
	return huggingFaceConfig{
		RepoID:   source.RepoID,
		Path:     source.Path,
		RepoType: defaultString(source.RepoType, huggingFaceDefaultRepoType),
		Revision: defaultString(source.Revision, huggingFaceDefaultRevision),
		Token:    source.Token,
		Endpoint: defaultString(source.Endpoint, huggingFaceDefaultEndpoint),
	}
}

func huggingFaceDestinationConfig(destination HuggingFaceProviderDestination) huggingFaceConfig {
	return huggingFaceConfig{
		RepoID:            destination.RepoID,
		Path:              destination.Path,
		RepoType:          defaultString(destination.RepoType, huggingFaceDefaultRepoType),
		Revision:          defaultString(destination.Revision, huggingFaceDefaultRevision),
		Token:             destination.Token,
		Endpoint:          defaultString(destination.Endpoint, huggingFaceDefaultEndpoint),
		CommitMessage:     destination.CommitMessage,
		CommitDescription: destination.CommitDescription,
		CreatePR:          destination.CreatePR,
	}
}

func (config huggingFaceConfig) endpoint() string {
	return strings.TrimRight(defaultString(config.Endpoint, huggingFaceDefaultEndpoint), "/")
}

func (config huggingFaceConfig) describe() string {
	return fmt.Sprintf("%s %s@%s/%s", config.RepoType, config.RepoID, config.Revision, config.Path)
}

// huggingFaceResolveURL builds {endpoint}/{prefix}{repo_id}/resolve/{revision}/{path}, as hf_hub_url does.
//
// Buckets are unversioned and take no revision segment, and the Hub escapes their whole key as
// one component -- see HfApi.get_bucket_file_metadata.
func huggingFaceResolveURL(config huggingFaceConfig) string {
	prefix := huggingFaceRepoTypeURLPrefixes[config.RepoType]
	if config.RepoType == "bucket" {
		return fmt.Sprintf(
			"%s/%s%s/resolve/%s", config.endpoint(), prefix, config.RepoID, url.QueryEscape(config.Path),
		)
	}
	segments := strings.Split(config.Path, "/")
	for index, segment := range segments {
		segments[index] = url.PathEscape(segment)
	}
	return fmt.Sprintf(
		"%s/%s%s/resolve/%s/%s",
		config.endpoint(), prefix, config.RepoID,
		url.QueryEscape(config.Revision), strings.Join(segments, "/"),
	)
}

// huggingFaceAPIBase builds {endpoint}/api/{repo_type}s/{repo_id}.
func huggingFaceAPIBase(config huggingFaceConfig) string {
	return fmt.Sprintf("%s/api/%ss/%s", config.endpoint(), config.RepoType, config.RepoID)
}

// huggingFaceLFSBatchURL builds {endpoint}/{prefix}{repo_id}.git/info/lfs/objects/batch.
func huggingFaceLFSBatchURL(config huggingFaceConfig) string {
	prefix := huggingFaceRepoTypeURLPrefixes[config.RepoType]
	return fmt.Sprintf("%s/%s%s.git/info/lfs/objects/batch", config.endpoint(), prefix, config.RepoID)
}

type huggingFaceFileMetadata struct {
	// URL is the credential-free presigned CDN URL the workers read from.
	URL string
	Size int64
	// ETag is the sha256 for an LFS blob, the git sha1 otherwise.
	ETag       string
	CommitHash string
}

type huggingFaceUploadPlan struct {
	OID  string
	Size int64
	// UploadHref is the multipart completion endpoint, or the single-part PUT target. Empty
	// when the Hub already stores this content and no upload is needed.
	UploadHref string
	// ChunkSize is the part size the Hub requires; zero for a single-part upload.
	ChunkSize int64
	// PartURLs are presigned part PUT URLs, ordered by part number.
	PartURLs   []string
	VerifyHref string
}

// huggingFaceFileMetadataFor HEADs the resolve URL and requires the Hub to redirect to its CDN.
//
// The redirect target is presigned and carries no credential, so it is the only form of this
// URL that may be handed to BeamCore and the workers.
func huggingFaceFileMetadataFor(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
) (huggingFaceFileMetadata, error) {
	resolveURL := huggingFaceResolveURL(config)
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, resolveURL, nil)
	if err != nil {
		return huggingFaceFileMetadata{}, err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	// Compression would report a transformed length instead of the real object size.
	request.Header.Set("Accept-Encoding", "identity")

	client := *httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return huggingFaceFileMetadata{}, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return huggingFaceFileMetadata{}, fmt.Errorf(
			"hugging face file lookup failed with status %d for %s", response.StatusCode, config.describe(),
		)
	}

	location := response.Header.Get("Location")
	if location == "" || huggingFaceSameHost(resolveURL, location) {
		return huggingFaceFileMetadata{}, fmt.Errorf(
			"hugging face did not redirect %s to a presigned CDN URL. Beam reads this file over "+
				"plain HTTP without forwarding your token, and the Hub only redirects for "+
				"large-file (LFS or Xet) content. A small regular file is served inline from the "+
				"Hub instead and cannot be transferred", config.describe(),
		)
	}

	rawSize := response.Header.Get("X-Linked-Size")
	if rawSize == "" {
		rawSize = response.Header.Get("Content-Length")
	}
	size, err := strconv.ParseInt(rawSize, 10, 64)
	if err != nil || size <= 0 {
		return huggingFaceFileMetadata{}, fmt.Errorf(
			"hugging face did not report a size for %s", config.describe(),
		)
	}

	etag := response.Header.Get("X-Linked-Etag")
	if etag == "" {
		etag = response.Header.Get("ETag")
	}
	return huggingFaceFileMetadata{
		URL:        location,
		Size:       size,
		ETag:       huggingFaceNormalizeETag(etag),
		CommitHash: response.Header.Get("X-Repo-Commit"),
	}, nil
}

// huggingFacePreupload asks the Hub whether a path is stored as an LFS blob or a regular git blob.
//
// The sample is the base64 of the first 512 bytes, exactly as _fetch_upload_modes sends it.
func huggingFacePreupload(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
	size int64,
	sample string,
) (uploadMode string, shouldIgnore bool, err error) {
	endpoint := fmt.Sprintf(
		"%s/preupload/%s", huggingFaceAPIBase(config), url.QueryEscape(config.Revision),
	)
	if config.CreatePR {
		endpoint += "?create_pr=1"
	}
	body := map[string]any{
		"files": []map[string]any{{"path": config.Path, "sample": sample, "size": size}},
	}
	var payload struct {
		Files []struct {
			Path         string `json:"path"`
			UploadMode   string `json:"uploadMode"`
			ShouldIgnore bool   `json:"shouldIgnore"`
		} `json:"files"`
	}
	if err := huggingFaceJSON(ctx, httpClient, endpoint, config, "preupload", body, &payload); err != nil {
		return "", false, err
	}
	if len(payload.Files) == 0 ||
		(payload.Files[0].UploadMode != "lfs" && payload.Files[0].UploadMode != "regular") {
		return "", false, fmt.Errorf(
			"hugging face preupload returned a malformed response for %s", config.describe(),
		)
	}
	return payload.Files[0].UploadMode, payload.Files[0].ShouldIgnore, nil
}

// huggingFaceLFSBatch requests upload instructions for one object.
//
// The Hub answers with either a single-part PUT target or a completion endpoint plus one
// presigned PUT URL per part.
func huggingFaceLFSBatch(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
	oid string,
	size int64,
) (huggingFaceUploadPlan, error) {
	body := map[string]any{
		"operation": "upload",
		"transfers": []string{"basic", "multipart"},
		"hash_algo": "sha256",
		"ref":       map[string]string{"name": config.Revision},
		"objects":   []map[string]any{{"oid": oid, "size": size}},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return huggingFaceUploadPlan{}, err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, huggingFaceLFSBatchURL(config), bytes.NewReader(encoded),
	)
	if err != nil {
		return huggingFaceUploadPlan{}, err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Accept", huggingFaceLFSContentType)
	request.Header.Set("Content-Type", huggingFaceLFSContentType)

	response, err := httpClient.Do(request)
	if err != nil {
		return huggingFaceUploadPlan{}, err
	}
	defer response.Body.Close()
	if err := huggingFaceStatus(response, "LFS batch", config); err != nil {
		return huggingFaceUploadPlan{}, err
	}

	var payload struct {
		Objects []struct {
			OID     string `json:"oid"`
			Size    int64  `json:"size"`
			Error   *struct{ Message string } `json:"error"`
			Actions *struct {
				Upload *struct {
					Href   string            `json:"href"`
					Header map[string]string `json:"header"`
				} `json:"upload"`
				Verify *struct {
					Href string `json:"href"`
				} `json:"verify"`
			} `json:"actions"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return huggingFaceUploadPlan{}, err
	}
	if len(payload.Objects) == 0 {
		return huggingFaceUploadPlan{}, fmt.Errorf(
			"hugging face LFS batch returned a malformed response for %s", config.describe(),
		)
	}
	object := payload.Objects[0]
	if object.Error != nil {
		return huggingFaceUploadPlan{}, fmt.Errorf(
			"hugging face LFS batch rejected %s: %s", config.describe(), object.Error.Message,
		)
	}
	if object.Actions == nil || object.Actions.Upload == nil {
		// No actions means the Hub already stores this content; only the commit is left to do.
		return huggingFaceUploadPlan{OID: object.OID, Size: size}, nil
	}
	if object.Actions.Upload.Href == "" {
		return huggingFaceUploadPlan{}, fmt.Errorf(
			"hugging face LFS batch returned no upload href for %s", config.describe(),
		)
	}

	plan := huggingFaceUploadPlan{OID: object.OID, Size: size, UploadHref: object.Actions.Upload.Href}
	if object.Actions.Verify != nil {
		plan.VerifyHref = object.Actions.Verify.Href
	}

	header := object.Actions.Upload.Header
	if raw, ok := header["chunk_size"]; ok {
		chunkSize, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || chunkSize <= 0 {
			return huggingFaceUploadPlan{}, fmt.Errorf(
				"hugging face LFS batch returned a malformed chunk_size %q for %s", raw, config.describe(),
			)
		}
		plan.ChunkSize = chunkSize
	}

	partNumbers := make([]int, 0, len(header))
	partsByNumber := make(map[int]string, len(header))
	for key, value := range header {
		if !huggingFacePartKey.MatchString(key) {
			continue
		}
		number, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		partNumbers = append(partNumbers, number)
		partsByNumber[number] = value
	}
	sort.Ints(partNumbers)
	for _, number := range partNumbers {
		plan.PartURLs = append(plan.PartURLs, partsByNumber[number])
	}

	if plan.ChunkSize > 0 {
		expected := int((size + plan.ChunkSize - 1) / plan.ChunkSize)
		if len(plan.PartURLs) != expected {
			return huggingFaceUploadPlan{}, fmt.Errorf(
				"hugging face returned %d part URLs for %s, expected %d at chunk_size %d",
				len(plan.PartURLs), config.describe(), expected, plan.ChunkSize,
			)
		}
	}
	return plan, nil
}

// huggingFaceCompleteLFSUpload closes a multipart LFS upload with {oid, parts:[{partNumber, etag}]}.
func huggingFaceCompleteLFSUpload(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
	href string,
	oid string,
	etags []string,
) error {
	parts := make([]map[string]any, 0, len(etags))
	for index, etag := range etags {
		parts = append(parts, map[string]any{"partNumber": index + 1, "etag": etag})
	}
	encoded, err := json.Marshal(map[string]any{"oid": oid, "parts": parts})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, href, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Accept", huggingFaceLFSContentType)
	request.Header.Set("Content-Type", huggingFaceLFSContentType)
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return huggingFaceStatus(response, "LFS completion", config)
}

// huggingFaceVerifyLFSUpload runs the Hub's optional server-side check that the object landed intact.
func huggingFaceVerifyLFSUpload(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
	href string,
	oid string,
	size int64,
) error {
	return huggingFaceJSON(
		ctx, httpClient, href, config, "LFS verify",
		map[string]any{"oid": oid, "size": size}, nil,
	)
}

// huggingFaceCommit publishes the uploaded blob as a commit.
//
// The body is NDJSON: a header line then one lfsFile line.
func huggingFaceCommit(
	ctx context.Context,
	httpClient *http.Client,
	config huggingFaceConfig,
	oid string,
	size int64,
) error {
	summary := config.CommitMessage
	if summary == "" {
		summary = fmt.Sprintf("Upload %s with Beam", config.Path)
	}
	lines := []map[string]any{
		{"key": "header", "value": map[string]any{"summary": summary, "description": config.CommitDescription}},
		{"key": "lfsFile", "value": map[string]any{
			"path": config.Path, "algo": "sha256", "oid": oid, "size": size,
		}},
	}
	encoded := make([]string, 0, len(lines))
	for _, line := range lines {
		raw, err := json.Marshal(line)
		if err != nil {
			return err
		}
		encoded = append(encoded, string(raw))
	}

	endpoint := fmt.Sprintf(
		"%s/commit/%s", huggingFaceAPIBase(config), url.QueryEscape(config.Revision),
	)
	if config.CreatePR {
		endpoint += "?create_pr=1"
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint, strings.NewReader(strings.Join(encoded, "\n")),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/x-ndjson")
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return huggingFaceStatus(response, "commit", config)
}

// huggingFaceSourceSample reads the first bytes of a URL base64-encoded, for preupload.
func huggingFaceSourceSample(
	ctx context.Context,
	httpClient *http.Client,
	sourceURL string,
	length int,
) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Range", fmt.Sprintf("bytes=0-%d", length-1))
	response, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return "", fmt.Errorf("source sample read failed with status %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

// huggingFaceHashSource streams a URL once, returning the sha256 of the whole body and the
// hex MD5 of every part.
//
// The Hub will not issue upload URLs without the sha256, and the per-part MD5 is the ETag the
// completion payload has to quote.
func huggingFaceHashSource(
	ctx context.Context,
	httpClient *http.Client,
	sourceURL string,
	partSize int64,
	wholeSHA256 bool,
) (string, []string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return "", nil, err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return "", nil, fmt.Errorf("source hash read failed with status %d", response.StatusCode)
	}

	whole := sha256.New()
	part := md5.New()
	var partBytes int64
	partETags := make([]string, 0)
	buffer := make([]byte, 1<<20)

	for {
		read, readErr := response.Body.Read(buffer)
		if read > 0 {
			chunk := buffer[:read]
			if wholeSHA256 {
				whole.Write(chunk)
			}
			for partSize > 0 && len(chunk) > 0 {
				room := partSize - partBytes
				if int64(len(chunk)) < room {
					part.Write(chunk)
					partBytes += int64(len(chunk))
					break
				}
				part.Write(chunk[:room])
				partETags = append(partETags, hex.EncodeToString(part.Sum(nil)))
				part = md5.New()
				partBytes = 0
				chunk = chunk[room:]
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", nil, readErr
		}
	}
	if partSize > 0 && partBytes > 0 {
		partETags = append(partETags, hex.EncodeToString(part.Sum(nil)))
	}

	digest := ""
	if wholeSHA256 {
		digest = hex.EncodeToString(whole.Sum(nil))
	}
	return digest, partETags, nil
}

func huggingFaceJSON(
	ctx context.Context,
	httpClient *http.Client,
	endpoint string,
	config huggingFaceConfig,
	step string,
	body any,
	out any,
) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := huggingFaceStatus(response, step, config); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(out)
}

func huggingFaceStatus(response *http.Response, step string, config huggingFaceConfig) error {
	if response.StatusCode < 400 {
		return nil
	}
	detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
	if len(detail) > 0 {
		return fmt.Errorf(
			"hugging face %s failed with status %d for %s: %s",
			step, response.StatusCode, config.describe(), string(detail),
		)
	}
	return fmt.Errorf(
		"hugging face %s failed with status %d for %s", step, response.StatusCode, config.describe(),
	)
}

func huggingFaceSameHost(left string, right string) bool {
	leftURL, err := url.Parse(left)
	if err != nil {
		return true
	}
	// Parse resolves a relative Location against the request URL, the way a client would.
	rightURL, err := leftURL.Parse(right)
	if err != nil {
		return true
	}
	return leftURL.Host == rightURL.Host
}

func huggingFaceNormalizeETag(value string) string {
	return strings.Trim(strings.TrimPrefix(value, "W/"), `"`)
}

// huggingFaceTargetPath resolves the path an upload commits to, expanding a trailing-slash prefix.
func huggingFaceTargetPath(destinationPath string, sourceFilename string) (string, error) {
	trimmed := strings.TrimLeft(destinationPath, "/")
	if !strings.HasSuffix(trimmed, "/") {
		return trimmed, nil
	}
	if sourceFilename == "" {
		return "", fmt.Errorf(
			"hugging face path %s is a folder and the source has no filename", destinationPath,
		)
	}
	return trimmed + sourceFilename, nil
}

func huggingFaceMetadata(config huggingFaceConfig, etag string, commitHash string) map[string]any {
	metadata := map[string]any{
		"driver":    "huggingface",
		"repo_id":   config.RepoID,
		"repo_type": config.RepoType,
		"revision":  config.Revision,
		"path":      config.Path,
		"endpoint":  config.endpoint(),
	}
	// For an LFS blob the Hub's linked ETag is the object's sha256.
	if etag != "" {
		metadata["sha256"] = etag
	}
	if commitHash != "" {
		metadata["commit_hash"] = commitHash
	}
	return metadata
}

// huggingFaceUploadState is one Hugging Face LFS upload, held from prepare until the commit.
//
// The Hub issues upload URLs only for a known sha256 and dictates the part size, so the plan is
// built around what the LFS batch hands back rather than the other way round.
type huggingFaceUploadState struct {
	Config        huggingFaceConfig
	DestinationID string
	SourceID      string
	OID           string
	Size          int64
	ChunkSize     int64
	PartURLs      []string
	UploadHref    string
	VerifyHref    string

	// partETags resolves alongside the transfer; it is only read at commit time.
	partETags    []string
	partETagsErr error
	partETagsDone chan struct{}
}

// planHuggingFaceUploads negotiates every Hugging Face destination before the plan exists.
//
// The Hub will not issue upload URLs without the object sha256, and it chooses the part size
// itself, so this runs first and the plan is then requested at that size.
func (client *Client) planHuggingFaceUploads(
	ctx context.Context,
	sources []ProviderSource,
	preparedSources []PreparedHTTPSource,
	destinations []ProviderDestination,
	preparedDestinations []PreparedDestination,
) ([]*huggingFaceUploadState, int64, error) {
	targets := make([]int, 0)
	for index, destination := range destinations {
		if _, ok := destination.(HuggingFaceProviderDestination); ok {
			targets = append(targets, index)
		}
	}
	if len(targets) == 0 {
		return nil, 0, nil
	}
	if len(preparedSources) != 1 {
		return nil, 0, fmt.Errorf(
			"a huggingface destination requires exactly one source: the Hub dictates the part size "+
				"and the plan carries a single chunk size, but %d sources were given", len(preparedSources),
		)
	}
	preparedSource := preparedSources[0]

	// For an LFS source the Hub already published the sha256 as the linked ETag.
	publishedSHA256 := ""
	if _, ok := sources[0].(HuggingFaceProviderSource); ok {
		if value, ok := preparedSource.Metadata["sha256"].(string); ok {
			publishedSHA256 = value
		}
	}

	states := make([]*huggingFaceUploadState, 0, len(targets))
	var chunkSize int64

	for _, index := range targets {
		destination := destinations[index].(HuggingFaceProviderDestination)
		if destination.RepoType == "bucket" {
			return nil, 0, fmt.Errorf(
				"%s is a Hugging Face bucket, which Beam cannot write to. Buckets expose no LFS "+
					"batch endpoint; their only upload path is the Hub's Xet CAS client, which "+
					"cannot be expressed as presigned URLs for Beam's workers. Buckets do work as "+
					"a transfer source. Use `hf sync` to write to a bucket", destination.RepoID,
			)
		}
		config := huggingFaceDestinationConfig(destination)
		path, err := huggingFaceTargetPath(config.Path, preparedSource.Filename)
		if err != nil {
			return nil, 0, err
		}
		config.Path = path

		oid := publishedSHA256
		if oid == "" {
			if !destination.AllowSourceRehash {
				return nil, 0, fmt.Errorf(
					"uploading to %s needs the source sha256, which the Hub requires before it issues "+
						"upload URLs. The SDK must read the source once to compute it; set "+
						"AllowSourceRehash to opt in", config.describe(),
				)
			}
			digest, _, err := huggingFaceHashSource(ctx, client.httpClient, preparedSource.URL, 0, true)
			if err != nil {
				return nil, 0, err
			}
			oid = digest
		}

		sample, err := huggingFaceSourceSample(ctx, client.httpClient, preparedSource.URL, 512)
		if err != nil {
			return nil, 0, err
		}
		uploadMode, shouldIgnore, err := huggingFacePreupload(
			ctx, client.httpClient, config, preparedSource.Size, sample,
		)
		if err != nil {
			return nil, 0, err
		}
		if shouldIgnore {
			return nil, 0, fmt.Errorf("%s is excluded by the repo's .gitignore", config.describe())
		}
		if uploadMode != "lfs" {
			return nil, 0, fmt.Errorf(
				"%s would be committed as a regular git blob, not an LFS blob. Beam uploads through "+
					"the LFS protocol only; add the path to .gitattributes as LFS", config.describe(),
			)
		}

		plan, err := huggingFaceLFSBatch(ctx, client.httpClient, config, oid, preparedSource.Size)
		if err != nil {
			return nil, 0, err
		}
		if plan.ChunkSize > 0 {
			if chunkSize > 0 && chunkSize != plan.ChunkSize {
				return nil, 0, fmt.Errorf(
					"huggingface destinations disagree on part size (%d vs %d); the plan carries a "+
						"single chunk size", chunkSize, plan.ChunkSize,
				)
			}
			chunkSize = plan.ChunkSize
		}

		state := &huggingFaceUploadState{
			Config:        config,
			DestinationID: preparedDestinations[index].DestinationID,
			SourceID:      preparedSource.SourceID,
			OID:           oid,
			Size:          preparedSource.Size,
			ChunkSize:     plan.ChunkSize,
			PartURLs:      plan.PartURLs,
			UploadHref:    plan.UploadHref,
			VerifyHref:    plan.VerifyHref,
		}
		if plan.ChunkSize > 0 {
			// Runs alongside the transfer; the ETags are only needed at completion time.
			state.partETagsDone = make(chan struct{})
			go func(state *huggingFaceUploadState, sourceURL string) {
				defer close(state.partETagsDone)
				_, etags, err := huggingFaceHashSource(
					context.WithoutCancel(ctx), client.httpClient, sourceURL, state.ChunkSize, false,
				)
				state.partETags, state.partETagsErr = etags, err
			}(state, preparedSource.URL)
		}
		states = append(states, state)
	}

	return states, chunkSize, nil
}

// assertHuggingFacePlan fails before any byte moves if BeamCore did not adopt the Hub's part layout.
func assertHuggingFacePlan(prepared *TransferPrepareResponse, states []*huggingFaceUploadState) error {
	for _, state := range states {
		var planDestination *CompactTransferPlanDestination
		for index := range prepared.PlanDescriptor.Destinations {
			if prepared.PlanDescriptor.Destinations[index].DestinationID == state.DestinationID {
				planDestination = &prepared.PlanDescriptor.Destinations[index]
				break
			}
		}
		var planSource *CompactTransferPlanSource
		for index := range prepared.PlanDescriptor.Sources {
			if prepared.PlanDescriptor.Sources[index].SourceID == state.SourceID {
				planSource = &prepared.PlanDescriptor.Sources[index]
				break
			}
		}
		if planDestination == nil || planSource == nil {
			return fmt.Errorf(
				"BeamCore plan is missing the huggingface coordinate %s:%s", state.SourceID, state.DestinationID,
			)
		}

		if key := planDestination.FinalObjectKeys[state.SourceID]; key != state.Config.Path {
			return fmt.Errorf(
				"BeamCore planned %s but the Hub upload was negotiated for %s", key, state.Config.Path,
			)
		}

		if state.ChunkSize == 0 {
			if planSource.ChunkCount != 1 {
				return fmt.Errorf(
					"%s was issued a single-part upload, but the plan has %d chunks",
					state.Config.describe(), planSource.ChunkCount,
				)
			}
			continue
		}
		if int64(prepared.PlanDescriptor.ChunkSize) != state.ChunkSize {
			return fmt.Errorf(
				"the Hub requires %d-byte parts for %s, but BeamCore planned %d-byte chunks",
				state.ChunkSize, state.Config.describe(), prepared.PlanDescriptor.ChunkSize,
			)
		}
		if planSource.ChunkCount != len(state.PartURLs) {
			return fmt.Errorf(
				"the Hub issued %d part URLs for %s, but the plan has %d chunks",
				len(state.PartURLs), state.Config.describe(), planSource.ChunkCount,
			)
		}
	}
	return nil
}

// FinalizeHuggingFaceUploads closes every Hugging Face upload for a transfer: it completes the
// LFS multipart, verifies it, and commits the blob so the file appears in the repo.
//
// Call it once the transfer is complete. It never sits in the transfer's progression path.
func (client *Client) FinalizeHuggingFaceUploads(ctx context.Context, transferID string) error {
	client.huggingFaceMu.Lock()
	states := client.huggingFaceUploads[transferID]
	delete(client.huggingFaceUploads, transferID)
	client.huggingFaceMu.Unlock()

	for _, state := range states {
		if state.UploadHref != "" && state.ChunkSize > 0 {
			<-state.partETagsDone
			if state.partETagsErr != nil {
				return state.partETagsErr
			}
			if len(state.partETags) != len(state.PartURLs) {
				return fmt.Errorf(
					"computed %d part ETags for %s, expected %d",
					len(state.partETags), state.Config.describe(), len(state.PartURLs),
				)
			}
			if err := huggingFaceCompleteLFSUpload(
				ctx, client.httpClient, state.Config, state.UploadHref, state.OID, state.partETags,
			); err != nil {
				return err
			}
		}
		if state.VerifyHref != "" {
			if err := huggingFaceVerifyLFSUpload(
				ctx, client.httpClient, state.Config, state.VerifyHref, state.OID, state.Size,
			); err != nil {
				return err
			}
		}
		if err := huggingFaceCommit(ctx, client.httpClient, state.Config, state.OID, state.Size); err != nil {
			return err
		}
	}
	return nil
}
