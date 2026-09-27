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
