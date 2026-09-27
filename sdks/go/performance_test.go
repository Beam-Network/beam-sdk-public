package beamnetworksdk

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGrantExpiryCannotBeExtendedByReuse(t *testing.T) {
	upper := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	actual := upper.Add(-time.Hour)
	url := "https://example.invalid/read?X-Amz-Date=20260926T100000Z&X-Amz-Expires=3600"
	if got := boundedGrantExpiry(url, upper); !got.Equal(actual) {
		t.Fatalf("expiry = %s", got)
	}
	if got := boundedGrantExpiry(url, actual.Add(-time.Minute)); !got.Equal(actual.Add(-time.Minute)) {
		t.Fatal("extended a shorter grant")
	}
}

func TestSourceGrantSharedAcrossConcurrentDestinations(t *testing.T) {
	session := &providerTransferSession{client: &Client{}, expiresIn: time.Hour, sourcesByID: map[string]ProviderSource{"src": nil}}
	future := &sourceChunkFuture{}
	chunk := ChunkSigningPlanItem{SourceID: "src", SourceOffset: 8, ChunkSize: 4, SourceURL: "https://example.invalid/source"}
	grants := make([]*sourceChunkGrant, 32)
	var wait sync.WaitGroup
	for i := range grants {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			var err error
			grants[i], err = future.get(context.Background(), session, chunk)
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wait.Wait()
	for _, grant := range grants {
		if grant != grants[0] || grant.headers["Range"] != "bytes=8-11" {
			t.Fatal("source grant not shared or range changed")
		}
	}
	next, err := (&sourceChunkFuture{}).get(context.Background(), session, chunk)
	if err != nil || next == grants[0] {
		t.Fatal("new generation reused previous grant")
	}
}

func TestSourceRenewalHistoryBoundAndLegacyShape(t *testing.T) {
	history := &sourceSignatureHistory{}
	initial := newSDKPerformance()
	initial.sourceHistory = history
	initial.sourceCreated(0)
	initial.sourceCreated(131071)
	if n := initial.snapshot(true).Counters.SourceRenewals; n == nil || *n != 0 {
		t.Fatal("initial signatures counted as renewals")
	}
	if initial.snapshot().Counters.SourceRenewals != nil {
		t.Fatal("v1 changed")
	}
	replay := newSDKPerformance()
	replay.sourceHistory = history
	replay.sourceCreated(131071)
	replay.sourceCreated(1)
	replay.observeDuration("sdk.producer_wait", 3*time.Millisecond)
	if n := replay.snapshot(true).Counters.SourceRenewals; n == nil || *n != 1 {
		t.Fatal("renewal not counted")
	}
	if replay.snapshot(true).Counters.SourceSignatures != 2 {
		t.Fatal("creation count changed")
	}
	replay.sourceCreated(131072)
	if replay.snapshot(true).Counters.SourceRenewals != nil {
		t.Fatal("incomplete history represented as complete")
	}
	resumed := newSDKPerformance()
	resumed.sourceHistory = &sourceSignatureHistory{incomplete: true}
	resumed.sourceCreated(0)
	if resumed.snapshot(true).Counters.SourceRenewals != nil {
		t.Fatal("takeover history represented as known")
	}
}
