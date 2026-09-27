package beamnetworksdk

import (
	"sync"
	"sync/atomic"
	"time"
)

type SDKPerformanceMeasurement struct {
	Name   string  `json:"name"`
	Count  uint64  `json:"count"`
	WorkMs float64 `json:"work_ms"`
	MaxMs  float64 `json:"max_ms"`
}
type SDKPerformanceSummary struct {
	SchemaVersion string                      `json:"schema_version"`
	Measurements  []SDKPerformanceMeasurement `json:"measurements"`
	Counters      PerformanceCounters         `json:"counters"`
}
type sdkPerformance struct {
	mu       sync.Mutex
	started  time.Time
	values   map[string]SDKPerformanceMeasurement
	counters PerformanceCounters
}

func newSDKPerformance() *sdkPerformance {
	return &sdkPerformance{started: time.Now(), values: make(map[string]SDKPerformanceMeasurement)}
}
func (p *sdkPerformance) observe(name string, start time.Time) {
	p.observeDuration(name, time.Since(start))
}
func (p *sdkPerformance) observeDuration(name string, duration time.Duration) {
	elapsed := float64(duration) / float64(time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	value := p.values[name]
	value.Name = name
	value.Count++
	value.WorkMs += elapsed
	if elapsed > value.MaxMs {
		value.MaxMs = elapsed
	}
	p.values[name] = value
}
func (p *sdkPerformance) source(reuses int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counters.SourceSignatures++
	if reuses > 0 {
		p.counters.SourceReuses += uint64(reuses)
	}
}
func (p *sdkPerformance) batch() { p.mu.Lock(); defer p.mu.Unlock(); p.counters.RouteBatches++ }
func (p *sdkPerformance) snapshot() SDKPerformanceSummary {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := SDKPerformanceSummary{SchemaVersion: "sdk-performance/v1", Measurements: []SDKPerformanceMeasurement{}, Counters: p.counters}
	for _, name := range []string{"sdk.discovery", "sdk.multipart_create", "sdk.signing", "sdk.batch_ack", "sdk.buffer_wait", "sdk.preparation"} {
		if value, ok := p.values[name]; ok {
			result.Measurements = append(result.Measurements, value)
		}
	}
	return result
}

// WithDiagnostics delivers at most one pending callback; slow callbacks drop later detail.
// The callback is never awaited by transfer execution and receives no storage grants.
func WithDiagnostics(callback func(SDKPerformanceSummary)) Option {
	return func(client *Client) {
		busy := new(atomic.Bool)
		client.onDiagnostics = func(summary SDKPerformanceSummary) {
			if callback == nil || !busy.CompareAndSwap(false, true) {
				return
			}
			go func() { defer busy.Store(false); defer func() { _ = recover() }(); callback(summary) }()
		}
	}
}
