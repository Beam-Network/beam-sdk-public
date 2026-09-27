package beamnetworksdk

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPerformanceV2AndLegacyWireShape(t *testing.T) {
	p := newSDKPerformance()
	p.observeDuration("sdk.discovery", 7*time.Millisecond)
	p.observeDuration("sdk.multipart_provider", 25*time.Millisecond)
	p.observeDuration("sdk.multipart_provider", 250*time.Millisecond)
	p.observeDuration("signed-url:private", time.Second)
	p.gauge("signing_limit", 64)
	p.mark("prepared_ms")
	current := p.snapshot(true)
	if current.SchemaVersion != "sdk-performance/v2" {
		t.Fatal(current)
	}
	for _, v := range current.Measurements {
		if v.Name == "sdk.multipart_provider" {
			var count uint64
			for _, n := range v.Histogram {
				count += n
			}
			if count != 2 || len(v.Histogram) != 18 || v.WorkMs != 275 {
				t.Fatal(v)
			}
		}
	}
	data, _ := json.Marshal(current)
	if strings.Contains(string(data), "private") {
		t.Fatal("unbounded name escaped")
	}
	legacy := p.snapshot()
	data, _ = json.Marshal(legacy)
	if strings.Contains(string(data), "histogram") || strings.Contains(string(data), "gauges") || len(legacy.Measurements) != 1 {
		t.Fatal(string(data))
	}
}
