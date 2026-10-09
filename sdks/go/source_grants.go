package beamnetworksdk

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// One immutable grant per source chunk in a signing generation. Each new stream
// or recovery request owns its cache, so credentials, conditions and expiry
// cannot leak across generations. Completed normal chunks release it promptly.
type sourceChunkGrant struct {
	url       string
	headers   map[string]string
	expiresAt time.Time
}
type sourceChunkFuture struct {
	once  sync.Once
	grant *sourceChunkGrant
	err   error
}

func signSourceChunk(ctx context.Context, httpClient *http.Client, source ProviderSource, chunk ChunkSigningPlanItem, ifMatch string, versionID string, expiresIn time.Duration) (*sourceChunkGrant, error) {
	defer performancePhase(ctx, "sdk.source_signing")()
	expiresAt := time.Now().UTC().Add(expiresIn)
	url, headers, err := signSourceRoute(ctx, httpClient, source, chunk, ifMatch, versionID, expiresIn)
	if err != nil {
		return nil, err
	}
	return &sourceChunkGrant{url: url, headers: headers, expiresAt: boundedGrantExpiry(url, expiresAt)}, nil
}
func plannedSourcePin(descriptor CompactTransferPlanDescriptor, sourceID string) (string, string) {
	for _, planned := range descriptor.Sources {
		if planned.SourceID == sourceID {
			etag, _ := planned.Metadata["etag"].(string)
			versionID, _ := planned.Metadata["version_id"].(string)
			return etag, versionID
		}
	}
	return "", ""
}
func (future *sourceChunkFuture) get(ctx context.Context, session *providerTransferSession, chunk ChunkSigningPlanItem) (*sourceChunkGrant, error) {
	defer performancePhase(ctx, "sdk.source_grant_wait")()
	created := false
	future.once.Do(func() {
		created = true
		ifMatch, versionID := plannedSourcePin(session.prepared.PlanDescriptor, chunk.SourceID)
		future.grant, future.err = signSourceChunk(ctx, session.client.httpClient, session.source(chunk.SourceID), chunk, ifMatch, versionID, session.expiresIn)
	})
	if p := performanceFromContext(ctx); p != nil && future.err == nil {
		if created {
			p.sourceCreated(chunk.ChunkIndex)
		} else {
			p.mu.Lock()
			p.counters.SourceReuses++
			p.mu.Unlock()
		}
	}
	return future.grant, future.err
}

func boundedGrantExpiry(signedURL string, upperBound time.Time) time.Time {
	parsed, err := url.Parse(signedURL)
	if err != nil {
		return upperBound
	}
	query := parsed.Query()
	for _, prefix := range []string{"X-Amz", "X-Goog"} {
		started, e1 := time.Parse("20060102T150405Z", query.Get(prefix+"-Date"))
		duration, e2 := strconv.ParseInt(query.Get(prefix+"-Expires"), 10, 64)
		if e1 == nil && e2 == nil && duration > 0 && duration <= 604800 {
			expires := started.Add(time.Duration(duration) * time.Second)
			if expires.Before(upperBound) {
				upperBound = expires
			}
		}
	}
	if epoch, err := strconv.ParseInt(query.Get("Expires"), 10, 64); err == nil && epoch > 0 {
		if expires := time.Unix(epoch, 0); expires.Before(upperBound) {
			upperBound = expires
		}
	}
	return upperBound
}
