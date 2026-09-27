package beamnetworksdk

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// integrityAuditSigner signs and submits the grants for one audit challenge.
type integrityAuditSigner func(ctx context.Context, challenge *IntegrityAuditChallenge) error

// integritySubmission coalesces concurrent submissions for one audit id. A
// successful submission is kept so later polls do not resubmit; a failed one
// is dropped so the next poll retries.
type integritySubmission struct {
	transferID  string
	fingerprint [32]byte
	expiresAt   time.Time
	done        chan struct{}
	err         error
}

// SignedReadRange is a short-lived, read-only range grant. Replay Headers
// unchanged: conditions such as If-Match are part of the signature.
type SignedReadRange struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// transferSigners is one owner's route recovery and integrity signers for a
// transfer. Its pointer identity is the owner token: an owner stops only the
// signers it registered, never a replacement owner's.
type transferSigners struct {
	stop      func()
	integrity integrityAuditSigner
}

// registerTransferSigners installs an owner's signers, replacing (and
// stopping) any previous owner's.
func (client *Client) registerTransferSigners(transferID string, signers *transferSigners) {
	client.signerMu.Lock()
	if client.recoverySigners == nil {
		client.recoverySigners = map[string]*transferSigners{}
	}
	previous := client.recoverySigners[transferID]
	client.recoverySigners[transferID] = signers
	client.signerMu.Unlock()
	if previous != nil && previous != signers && previous.stop != nil {
		previous.stop()
	}
}

// stopRecoverySigner stops whichever route recovery and integrity signers the
// transfer has. Terminal status and explicit cancellation use it.
func (client *Client) stopRecoverySigner(transferID string) {
	client.stopTransferSigners(transferID, nil)
}

// stopOwnedRecoverySigner stops the owner's signers only while they are still
// the transfer's current signers.
func (client *Client) stopOwnedRecoverySigner(transferID string, signers *transferSigners) {
	if signers != nil {
		client.stopTransferSigners(transferID, signers)
	}
}

func (client *Client) stopTransferSigners(transferID string, expected *transferSigners) {
	client.signerMu.Lock()
	current := client.recoverySigners[transferID]
	if expected != nil && current != expected {
		client.signerMu.Unlock()
		return
	}
	delete(client.recoverySigners, transferID)
	for auditID, submission := range client.integritySubmissions {
		if submission.transferID == transferID {
			delete(client.integritySubmissions, auditID)
		}
	}
	client.signerMu.Unlock()
	if current != nil && current.stop != nil {
		current.stop()
	}
}

func (client *Client) stopAllRecoverySigners() {
	client.signerMu.Lock()
	transferIDs := make([]string, 0, len(client.recoverySigners))
	for transferID := range client.recoverySigners {
		transferIDs = append(transferIDs, transferID)
	}
	client.signerMu.Unlock()
	for _, transferID := range transferIDs {
		client.stopRecoverySigner(transferID)
	}
}

// submitIntegrityAuditGrantsIfPresent submits grants for a status challenge
// once per audit id; concurrent callers share one submission.
func (client *Client) submitIntegrityAuditGrantsIfPresent(ctx context.Context, challenge *IntegrityAuditChallenge) error {
	if challenge == nil {
		return nil
	}
	client.signerMu.Lock()
	var signer integrityAuditSigner
	if signers := client.recoverySigners[challenge.TransferID]; signers != nil {
		signer = signers.integrity
	}
	if signer == nil {
		client.signerMu.Unlock()
		return errors.New("integrity audit signer unavailable")
	}
	encoded, _ := json.Marshal(challenge)
	fingerprint := sha256.Sum256(encoded)
	if existing := client.integritySubmissions[challenge.AuditID]; existing != nil && time.Now().Before(existing.expiresAt) {
		if existing.fingerprint != fingerprint {
			client.signerMu.Unlock()
			return errors.New("conflicting integrity challenge")
		}
		client.signerMu.Unlock()
		select {
		case <-existing.done:
			return existing.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if client.integritySubmissions == nil {
		client.integritySubmissions = map[string]*integritySubmission{}
	}
	for key, value := range client.integritySubmissions {
		if time.Now().After(value.expiresAt) {
			delete(client.integritySubmissions, key)
		}
	}
	if len(client.integritySubmissions) >= 1024 {
		client.signerMu.Unlock()
		return errors.New("integrity signer capacity unavailable")
	}
	submission := &integritySubmission{transferID: challenge.TransferID, fingerprint: fingerprint, expiresAt: time.Now().Add(2 * time.Second), done: make(chan struct{})}
	client.integritySubmissions[challenge.AuditID] = submission
	client.signerMu.Unlock()

	submission.err = signer(ctx, challenge)
	if submission.err != nil {
		client.signerMu.Lock()
		if client.integritySubmissions[challenge.AuditID] == submission {
			delete(client.integritySubmissions, challenge.AuditID)
		}
		client.signerMu.Unlock()
	}
	close(submission.done)
	return submission.err
}

// submitProviderIntegrityAuditGrants signs exact read-only source and
// final-destination ranges for every challenged chunk and submits them.
func (client *Client) buildProviderIntegrityAuditGrants(ctx context.Context, session *providerTransferSession, challenge *IntegrityAuditChallenge) (map[string]any, error) {
	ctx = withProviderClients(ctx, session.clients)
	prepared := session.prepared
	if challenge.TransferID != prepared.TransferID {
		return nil, errors.New("integrity audit challenge transfer mismatch")
	}
	submittedAt := time.Now().UTC().Format(time.RFC3339Nano)
	chunks, err := mapOrderedWithConcurrency(ctx, challenge.Chunks, min(client.routeSigningConcurrency, max(1, len(challenge.Chunks))), func(ctx context.Context, chunk IntegrityAuditChallengeChunk) (map[string]any, error) {
		planChunk, ok := materializePlanChunk(prepared.PlanDescriptor, prepared.TransferID, chunk.SourceID, chunk.RouteChunkIndex)
		if !ok {
			return nil, fmt.Errorf("plan chunk is outside source range: %s:%d", chunk.SourceID, chunk.RouteChunkIndex)
		}
		if chunk.SourceOffset < planChunk.SourceOffset || chunk.SourceOffset+chunk.RangeLength > planChunk.SourceOffset+planChunk.ChunkSize {
			return nil, errors.New("integrity audit source range is outside the planned chunk")
		}
		var target *ChunkDestinationSigningTarget
		for index := range planChunk.Destinations {
			candidate := &planChunk.Destinations[index]
			if deliveryIndex, _ := exactIntegerValue(candidate.Metadata["delivery_index"]); candidate.DestinationID == chunk.DestinationID && deliveryIndex == chunk.DeliveryIndex {
				target = candidate
				break
			}
		}
		if target == nil || target.Metadata["final_object_key"] != chunk.FinalObjectKey {
			return nil, errors.New("integrity audit destination coordinate mismatch")
		}
		if chunk.DestinationOffset != chunk.SourceOffset {
			return nil, errors.New("integrity audit destination range mismatch")
		}
		source := session.source(chunk.SourceID)
		destination := session.destination(chunk.DestinationID)
		if source == nil || destination == nil {
			return nil, errors.New("integrity audit provider configuration is unavailable")
		}
		var sourceETag, sourceVersionID string
		for _, planned := range prepared.PlanDescriptor.Sources {
			if planned.SourceID == chunk.SourceID {
				sourceETag, _ = planned.Metadata["etag"].(string)
				sourceVersionID, _ = planned.Metadata["version_id"].(string)
				break
			}
		}
		grantExpiresAt := time.Now().UTC().Add(session.expiresIn)
		sourceGrant, err := signSourceReadRange(ctx, client.httpClient, source, sourceETag, sourceVersionID, chunk.SourceOffset, chunk.RangeLength, session.expiresIn)
		if err != nil {
			return nil, err
		}
		destinationETag := ""
		if chunk.FinalObjectETag != nil {
			destinationETag = *chunk.FinalObjectETag
		}
		destinationGrant, err := signDestinationReadRange(ctx, client.httpClient, destination, chunk.FinalObjectKey, destinationETag, chunk.DestinationOffset, chunk.RangeLength, session.expiresIn)
		if err != nil {
			return nil, err
		}
		// Orchestrator and worker identities stay out of the grant.
		return map[string]any{
			"challenge_id": chunk.ChallengeID, "task_id": chunk.TaskID, "attempt_id": chunk.AttemptID,
			"source_id": chunk.SourceID, "destination_id": chunk.DestinationID,
			"route_chunk_index": chunk.RouteChunkIndex, "delivery_index": chunk.DeliveryIndex,
			"source_offset": chunk.SourceOffset, "destination_offset": chunk.DestinationOffset,
			"range_length": chunk.RangeLength, "final_object_key": chunk.FinalObjectKey,
			"final_object_etag": chunk.FinalObjectETag,
			"source":            map[string]any{"url": sourceGrant.URL, "headers": sourceGrant.Headers, "expires_at": boundedGrantExpiry(sourceGrant.URL, grantExpiresAt).Format(time.RFC3339Nano)},
			"destination":       map[string]any{"url": destinationGrant.URL, "headers": destinationGrant.Headers, "expires_at": boundedGrantExpiry(destinationGrant.URL, grantExpiresAt).Format(time.RFC3339Nano)},
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"transfer_id": challenge.TransferID, "audit_id": challenge.AuditID,
		"submitted_at": submittedAt, "chunks": chunks,
	}, nil
}

// signSourceReadRange signs a read-only source range. ifMatch and versionID
// pin an S3-compatible source; other providers reject them rather than drop them.
func signSourceReadRange(ctx context.Context, httpClient *http.Client, source ProviderSource, ifMatch string, versionID string, offset int64, length int64, expiresIn time.Duration) (SignedReadRange, error) {
	rangeHeader := rangeHeaderForRange(offset, length)
	settings, s3Compatible, err := s3SettingsFor(source)
	if err != nil {
		return SignedReadRange{}, err
	}
	if s3Compatible {
		input := &s3.GetObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(settings.key), Range: aws.String(rangeHeader)}
		headers := map[string]string{"Range": rangeHeader}
		if ifMatch != "" {
			input.IfMatch = aws.String(ifMatch)
			headers["If-Match"] = ifMatch
		}
		if versionID != "" {
			input.VersionId = aws.String(versionID)
		}
		result, err := s3.NewPresignClient(s3ClientForContext(ctx, settings)).PresignGetObject(ctx, input, func(options *s3.PresignOptions) { options.Expires = expiresIn })
		if err != nil {
			return SignedReadRange{}, err
		}
		return SignedReadRange{URL: result.URL, Headers: headers}, nil
	}
	if ifMatch != "" || versionID != "" {
		return SignedReadRange{}, errors.New("conditional source ranges require S3-compatible storage")
	}
	url, headers, err := signSourceRoute(ctx, httpClient, source, ChunkSigningPlanItem{SourceOffset: offset, ChunkSize: length}, expiresIn)
	if err != nil {
		return SignedReadRange{}, err
	}
	return SignedReadRange{URL: url, Headers: headers}, nil
}

// signDestinationReadRange signs a read-only range of a final destination
// object; ifMatch pins it on S3-compatible storage.
func signDestinationReadRange(ctx context.Context, httpClient *http.Client, destination ProviderDestination, objectKey string, ifMatch string, offset int64, length int64, expiresIn time.Duration) (SignedReadRange, error) {
	rangeHeader := rangeHeaderForRange(offset, length)
	headers := map[string]string{"Range": rangeHeader}
	settings, s3Compatible, err := s3SettingsFor(destination)
	if err != nil {
		return SignedReadRange{}, err
	}
	if s3Compatible {
		input := &s3.GetObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(objectKey), Range: aws.String(rangeHeader)}
		if ifMatch != "" {
			input.IfMatch = aws.String(ifMatch)
			headers["If-Match"] = ifMatch
		}
		result, err := s3.NewPresignClient(s3ClientForContext(ctx, settings)).PresignGetObject(ctx, input, func(options *s3.PresignOptions) { options.Expires = expiresIn })
		if err != nil {
			return SignedReadRange{}, err
		}
		return SignedReadRange{URL: result.URL, Headers: headers}, nil
	}
	if ifMatch != "" {
		return SignedReadRange{}, errors.New("conditional destination ranges require S3-compatible storage")
	}
	switch typed := destination.(type) {
	case HippiusProviderDestination:
		signedURL, err := hippiusPresign(ctx, httpClient, defaultString(typed.BaseURL, "https://api.hippius.com"), typed.APIToken, typed.Bucket, objectKey, "get", expiresIn)
		if err != nil {
			return SignedReadRange{}, err
		}
		return SignedReadRange{URL: signedURL, Headers: headers}, nil
	case HuggingFaceProviderDestination:
		// Read-back resolves the committed file. Before the commit lands the bytes
		// exist only as uncommitted LFS parts, which the Hub does not expose.
		metadata, err := huggingFaceFileMetadataFor(ctx, httpClientOrDefault(httpClient), huggingFaceSourceConfig(HuggingFaceProviderSource{
			RepoID: typed.RepoID, Path: objectKey, RepoType: typed.RepoType, Revision: typed.Revision, Token: typed.Token, Endpoint: typed.Endpoint,
		}))
		if err != nil {
			return SignedReadRange{}, err
		}
		return SignedReadRange{URL: metadata.URL, Headers: headers}, nil
	default:
		return SignedReadRange{}, fmt.Errorf("destination range signing unsupported for %T", destination)
	}
}

// mapOrderedWithConcurrency maps values with bounded concurrency, preserving
// order, and stops scheduling after the first error.
func mapOrderedWithConcurrency[T any, R any](ctx context.Context, values []T, concurrency int, mapValue func(context.Context, T) (R, error)) ([]R, error) {
	results := make([]R, len(values))
	if len(values) == 0 {
		return results, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		next     int
		firstErr error
		wait     sync.WaitGroup
	)
	for worker := 0; worker < min(max(1, concurrency), len(values)); worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				mu.Lock()
				index := next
				next++
				stopped := firstErr != nil
				mu.Unlock()
				if stopped || index >= len(values) {
					return
				}
				result, err := mapValue(ctx, values[index])
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					mu.Unlock()
					return
				}
				results[index] = result
			}
		}()
	}
	wait.Wait()
	return results, firstErr
}
