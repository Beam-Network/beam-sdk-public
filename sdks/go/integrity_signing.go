package beamnetworksdk

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type integrityGrantEntry struct {
	fingerprint [32]byte
	expiresAt   time.Time
	done        chan struct{}
	payload     map[string]any
	err         error
}

func (client *Client) providerIntegritySigner(session *providerTransferSession) func(context.Context, *IntegrityAuditChallenge) (map[string]any, error) {
	var mu sync.Mutex
	entries := map[string]*integrityGrantEntry{}
	return func(ctx context.Context, challenge *IntegrityAuditChallenge) (map[string]any, error) {
		if err := ownershipErr(session.ownership); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(challenge)
		if err != nil {
			return nil, err
		}
		fingerprint := sha256.Sum256(encoded)
		mu.Lock()
		entry := entries[challenge.AuditID]
		if entry != nil && entry.fingerprint != fingerprint {
			mu.Unlock()
			return nil, errors.New("conflicting integrity challenge")
		}
		if entry == nil || time.Now().After(entry.expiresAt) {
			for key, value := range entries {
				if time.Now().After(value.expiresAt) {
					delete(entries, key)
				}
			}
			if len(entries) >= 1024 {
				mu.Unlock()
				return nil, errors.New("integrity signer capacity unavailable")
			}
			entry = &integrityGrantEntry{fingerprint: fingerprint, expiresAt: time.Now().Add(session.expiresIn), done: make(chan struct{})}
			entries[challenge.AuditID] = entry
			go func(entry *integrityGrantEntry) {
				signCtx, cancelOwner := withOwnership(context.Background(), session.ownership)
				defer cancelOwner()
				signCtx, cancel := context.WithTimeout(signCtx, 30*time.Second)
				defer cancel()
				entry.payload, entry.err = client.buildProviderIntegrityAuditGrants(signCtx, session, challenge)
				mu.Lock()
				if chunks, ok := entry.payload["chunks"].([]map[string]any); ok {
					for _, chunk := range chunks {
						for _, side := range []string{"source", "destination"} {
							grant, _ := chunk[side].(map[string]any)
							value, _ := grant["expires_at"].(string)
							if expiry, err := time.Parse(time.RFC3339Nano, value); err == nil && expiry.Before(entry.expiresAt) {
								entry.expiresAt = expiry
							}
						}
					}
				}
				mu.Unlock()
				if entry.err != nil {
					mu.Lock()
					if entries[challenge.AuditID] == entry {
						delete(entries, challenge.AuditID)
					}
					mu.Unlock()
				}
				close(entry.done)
			}(entry)
		}
		mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-entry.done:
			return entry.payload, entry.err
		}
	}
}
