package beamnetworksdk

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sync"
	"time"
)

type providerClientsKey struct{}
type providerClients struct {
	mu      sync.Mutex
	values  map[[32]byte]*s3.Client
	retired bool
}

func newProviderClients() *providerClients {
	return &providerClients{values: make(map[[32]byte]*s3.Client)}
}
func withProviderClients(ctx context.Context, clients *providerClients) context.Context {
	return context.WithValue(ctx, providerClientsKey{}, clients)
}
func (p *providerClients) retire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	clear(p.values)
	p.retired = true
}
func cachedProviderClient(ctx context.Context, identity []string, create func() *s3.Client) *s3.Client {
	rawCreate := create
	create = func() *s3.Client {
		start := time.Now()
		client := rawCreate()
		if metrics := performanceFromContext(ctx); metrics != nil {
			metrics.observe("sdk.provider_client_setup", start)
			metrics.increment("provider_clients_created")
		}
		return client
	}
	p, ok := ctx.Value(providerClientsKey{}).(*providerClients)
	if !ok || p == nil {
		return create()
	}
	encoded, _ := json.Marshal(identity)
	key := sha256.Sum256(encoded)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return create()
	}
	if client := p.values[key]; client != nil {
		if metrics := performanceFromContext(ctx); metrics != nil {
			metrics.increment("provider_clients_reused")
		}
		return client
	}
	client := create()
	p.values[key] = client
	return client
}
