package beamnetworksdk

import (
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type SDKPerformanceMeasurement struct {
	Name      string   `json:"name"`
	Count     uint64   `json:"count"`
	WorkMs    float64  `json:"work_ms"`
	MaxMs     float64  `json:"max_ms"`
	Histogram []uint64 `json:"histogram,omitempty"`
}
type SDKPerformanceSummary struct {
	SchemaVersion string                      `json:"schema_version"`
	Measurements  []SDKPerformanceMeasurement `json:"measurements"`
	Counters      PerformanceCounters         `json:"counters"`
	Gauges        map[string]float64          `json:"gauges,omitempty"`
	Milestones    map[string]float64          `json:"milestones,omitempty"`
	Unmeasured    []string                    `json:"unmeasured,omitempty"`
	DetailDropped *uint64                     `json:"detail_dropped,omitempty"`
}

// A 16 KiB bitmap bounds per-transfer renewal history without retaining grants.
type sourceSignatureHistory struct {
	mu         sync.Mutex
	bits       [16384]byte
	incomplete bool
}

func (h *sourceSignatureHistory) created(index int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if index < 0 || index >= len(h.bits)*8 {
		h.incomplete = true
		return false
	}
	mask := byte(1 << (index & 7))
	renewed := h.bits[index>>3]&mask != 0
	h.bits[index>>3] |= mask
	return renewed
}
func (h *sourceSignatureHistory) complete() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.incomplete
}

type sdkPerformance struct {
	sourceHistory  *sourceSignatureHistory
	sourceRenewals uint64
	mu             sync.Mutex
	started        time.Time
	values         map[string]SDKPerformanceMeasurement
	counters       PerformanceCounters
	gauges         map[string]float64
	milestones     map[string]float64
	dropped        uint64
}

var performanceNames = []string{"sdk.discovery", "sdk.multipart_create", "sdk.signing", "sdk.batch_ack", "sdk.buffer_wait", "sdk.preparation", "sdk.source_signing", "sdk.destination_signing", "sdk.source_grant_wait", "sdk.multipart_ready_wait", "sdk.multipart_provider", "sdk.multipart_queue", "sdk.multipart_callback", "sdk.manifest_ack", "sdk.provider_client_setup", "sdk.metadata_request", "sdk.route_assembly", "sdk.batch_encode", "sdk.batch_checksum", "sdk.batch_queue", "sdk.signing_queue", "sdk.process_cpu", "sdk.event_loop_delay", "sdk.producer_wait"}
var performanceBounds = []float64{.1, .5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 120000}
var performanceGauges = map[string]bool{"signing_configured_limit": true, "multipart_configured_limit": true, "signing_limit": true, "multipart_limit": true, "signing_active_peak": true, "signing_pending_peak": true, "multipart_active_peak": true, "buffered_batches_peak": true, "batch_routes_max": true, "batch_bytes_max": true, "concurrency_changes": true, "source_waiters": true, "provider_clients_created": true, "provider_clients_reused": true}
var performanceMilestones = map[string]bool{"first_manifest_ms": true, "first_route_ms": true, "first_batch_ms": true, "final_flush_ms": true, "prepared_ms": true}

type performanceContextKey struct{}

func performanceFromContext(ctx context.Context) *sdkPerformance {
	p, _ := ctx.Value(performanceContextKey{}).(*sdkPerformance)
	return p
}
func performancePhase(ctx context.Context, name string) func() {
	p := performanceFromContext(ctx)
	start := time.Now()
	return func() {
		if p != nil {
			p.observe(name, start)
		}
	}
}
func (p *sdkPerformance) gauge(name string, value float64) {
	if !performanceGauges[name] || os.Getenv("BEAM_SDK_PERFORMANCE_DETAILS") == "false" || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return
	}
	value = math.Min(value, 1000000000)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gauges == nil {
		p.gauges = map[string]float64{}
	}
	if value > p.gauges[name] {
		p.gauges[name] = value
	}
}
func (p *sdkPerformance) increment(name string) {
	if !performanceGauges[name] || os.Getenv("BEAM_SDK_PERFORMANCE_DETAILS") == "false" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gauges == nil {
		p.gauges = map[string]float64{}
	}
	p.gauges[name]++
}
func (p *sdkPerformance) mark(name string) {
	if !performanceMilestones[name] || os.Getenv("BEAM_SDK_PERFORMANCE_DETAILS") == "false" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.milestones == nil {
		p.milestones = map[string]float64{}
	}
	if _, ok := p.milestones[name]; !ok {
		p.milestones[name] = float64(time.Since(p.started)) / float64(time.Millisecond)
	}
}

func newSDKPerformance() *sdkPerformance {
	return &sdkPerformance{started: time.Now(), values: make(map[string]SDKPerformanceMeasurement)}
}
func (p *sdkPerformance) observe(name string, start time.Time) {
	p.observeDuration(name, time.Since(start))
}
func (p *sdkPerformance) observeDuration(name string, duration time.Duration) {
	valid := false
	for i, candidate := range performanceNames {
		if candidate == name && (i < 6 || os.Getenv("BEAM_SDK_PERFORMANCE_DETAILS") != "false") {
			valid = true
			break
		}
	}
	if !valid || duration < 0 || duration > 24*time.Hour {
		return
	}
	elapsed := float64(duration) / float64(time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	value := p.values[name]
	value.Name = name
	if value.Count >= 1000000 || value.WorkMs+elapsed > 86400000 {
		p.dropped++
		return
	}
	if value.Histogram == nil {
		value.Histogram = make([]uint64, 18)
	}
	bin := 17
	for i, bound := range performanceBounds {
		if elapsed <= bound {
			bin = i
			break
		}
	}
	value.Histogram[bin]++
	value.Count++
	value.WorkMs += elapsed
	if elapsed > value.MaxMs {
		value.MaxMs = elapsed
	}
	p.values[name] = value
}
func (p *sdkPerformance) sourceCreated(index int) {
	renewed := p.sourceHistory != nil && p.sourceHistory.created(index)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counters.SourceSignatures++
	if renewed {
		p.sourceRenewals++
	}
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
func (p *sdkPerformance) snapshot(version ...bool) SDKPerformanceSummary {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := SDKPerformanceSummary{SchemaVersion: "sdk-performance/v1", Measurements: []SDKPerformanceMeasurement{}, Counters: p.counters}
	v2 := len(version) > 0 && version[0]
	if v2 {
		result.SchemaVersion = "sdk-performance/v2"
		if p.sourceHistory != nil && p.sourceHistory.complete() {
			n := p.sourceRenewals
			result.Counters.SourceRenewals = &n
		}
		result.Gauges = map[string]float64{}
		result.Milestones = map[string]float64{}
		dropped := p.dropped
		result.DetailDropped = &dropped
		for k, v := range p.gauges {
			result.Gauges[k] = v
		}
		for k, v := range p.milestones {
			result.Milestones[k] = v
		}
	}
	for i, name := range performanceNames {
		if i >= 6 && !v2 {
			continue
		}
		if value, ok := p.values[name]; ok {
			if v2 {
				value.Histogram = append([]uint64(nil), value.Histogram...)
			} else {
				value.Histogram = nil
			}
			result.Measurements = append(result.Measurements, value)
		} else if v2 && i >= 6 {
			result.Unmeasured = append(result.Unmeasured, name)
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
