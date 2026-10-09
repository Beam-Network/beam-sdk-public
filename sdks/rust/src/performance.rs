use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkPerformanceMeasurement {
    pub name: String,
    pub count: u64,
    pub work_ms: f64,
    pub max_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub histogram: Option<Vec<u64>>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SdkPerformanceCounters {
    pub source_signatures: u64,
    pub source_reuses: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_renewals: Option<u64>,
    pub route_batches: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkPerformanceSummary {
    pub schema_version: String,
    pub measurements: Vec<SdkPerformanceMeasurement>,
    pub counters: SdkPerformanceCounters,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gauges: Option<BTreeMap<String, f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub milestones: Option<BTreeMap<String, f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmeasured: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_dropped: Option<u64>,
}
pub(crate) struct SourceSignatureHistory {
    bits: [u8; 16384],
    complete: bool,
}
impl SourceSignatureHistory {
    pub fn new(complete: bool) -> Self {
        Self {
            bits: [0; 16384],
            complete,
        }
    }
    fn created(&mut self, index: u64) -> bool {
        if index >= (self.bits.len() * 8) as u64 {
            self.complete = false;
            return false;
        }
        let byte = (index >> 3) as usize;
        let mask = 1u8 << (index & 7);
        let renewed = self.bits[byte] & mask != 0;
        self.bits[byte] |= mask;
        renewed
    }
}
pub(crate) struct Collector {
    pub source_history: Option<Arc<Mutex<SourceSignatureHistory>>>,
    source_renewals: u64,
    pub started: Instant,
    values: BTreeMap<String, SdkPerformanceMeasurement>,
    counters: SdkPerformanceCounters,
    gauges: BTreeMap<String, f64>,
    milestones: BTreeMap<String, f64>,
    dropped: u64,
}
impl Collector {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            source_history: None,
            source_renewals: 0,
            values: BTreeMap::new(),
            counters: SdkPerformanceCounters::default(),
            gauges: BTreeMap::new(),
            milestones: BTreeMap::new(),
            dropped: 0,
        }
    }
    pub fn observe(&mut self, name: &str, started: Instant) {
        self.observe_duration(name, started.elapsed());
    }
    pub fn observe_duration(&mut self, name: &str, duration: std::time::Duration) {
        let Some(index) = NAMES.iter().position(|value| *value == name) else {
            return;
        };
        if index >= 6 && std::env::var("BEAM_SDK_PERFORMANCE_DETAILS").as_deref() == Ok("false") {
            return;
        }
        let ms = duration.as_secs_f64() * 1000.0;
        if ms > 86400000.0 {
            return;
        }
        let value = self
            .values
            .entry(name.to_owned())
            .or_insert(SdkPerformanceMeasurement {
                name: name.to_owned(),
                count: 0,
                work_ms: 0.0,
                max_ms: 0.0,
                histogram: Some(vec![0; 18]),
            });
        if value.count >= 1000000 || value.work_ms + ms > 86400000.0 {
            self.dropped += 1;
            return;
        }
        let bin = BOUNDS.iter().position(|bound| ms <= *bound).unwrap_or(17);
        value.histogram.as_mut().unwrap()[bin] += 1;
        value.count += 1;
        value.work_ms += ms;
        value.max_ms = value.max_ms.max(ms);
    }
    pub fn source_used(&mut self, created: bool, index: u64) {
        if created {
            self.counters.source_signatures += 1;
            if self
                .source_history
                .as_ref()
                .is_some_and(|h| h.lock().unwrap().created(index))
            {
                self.source_renewals += 1;
            }
        } else {
            self.counters.source_reuses += 1;
        }
    }
    pub fn seed_discovery(&mut self, other: &Self) {
        for name in ["provider_clients_created", "provider_clients_reused"] {
            if let Some(value) = other.gauges.get(name) {
                self.gauge(name, *value);
            }
        }
        for name in ["sdk.metadata_request", "sdk.provider_client_setup"] {
            if let Some(value) = other.values.get(name) {
                self.values.insert(name.into(), value.clone());
            }
        }
    }
    pub fn batch(&mut self) {
        self.counters.route_batches += 1;
    }
    pub fn gauge(&mut self, name: &str, value: f64) {
        if !GAUGES.contains(&name)
            || !value.is_finite()
            || value < 0.0
            || std::env::var("BEAM_SDK_PERFORMANCE_DETAILS").as_deref() == Ok("false")
        {
            return;
        }
        let value = value.min(1_000_000_000.0);
        let old = self.gauges.entry(name.into()).or_default();
        *old = old.max(value);
    }
    pub fn increment(&mut self, name: &str) {
        let value = self.gauges.get(name).copied().unwrap_or(0.0) + 1.0;
        self.gauge(name, value);
    }
    pub fn mark(&mut self, name: &str) {
        if !MILESTONES.contains(&name)
            || std::env::var("BEAM_SDK_PERFORMANCE_DETAILS").as_deref() == Ok("false")
        {
            return;
        }
        self.milestones
            .entry(name.into())
            .or_insert(self.started.elapsed().as_secs_f64() * 1000.0);
    }
    pub fn snapshot(&self) -> SdkPerformanceSummary {
        self.snapshot_for(false)
    }
    pub fn snapshot_for(&self, v2: bool) -> SdkPerformanceSummary {
        let mut counters = self.counters.clone();
        counters.source_renewals = (v2
            && self
                .source_history
                .as_ref()
                .is_some_and(|h| h.lock().unwrap().complete))
        .then_some(self.source_renewals);
        SdkPerformanceSummary {
            schema_version: if v2 {
                "sdk-performance/v2"
            } else {
                "sdk-performance/v1"
            }
            .into(),
            measurements: self
                .values
                .values()
                .filter(|v| v2 || NAMES[..6].contains(&v.name.as_str()))
                .cloned()
                .map(|mut v| {
                    if !v2 {
                        v.histogram = None;
                    }
                    v
                })
                .collect(),
            counters,
            gauges: v2.then(|| self.gauges.clone()),
            milestones: v2.then(|| self.milestones.clone()),
            unmeasured: v2.then(|| {
                NAMES[6..]
                    .iter()
                    .filter(|n| !self.values.contains_key(**n))
                    .map(|n| n.to_string())
                    .collect()
            }),
            detail_dropped: v2.then_some(self.dropped),
        }
    }
}
type Callback = Arc<dyn Fn(SdkPerformanceSummary) + Send + Sync>;
#[derive(Clone, Default)]
pub(crate) struct Diagnostics {
    callback: Arc<Mutex<Option<Callback>>>,
    busy: Arc<AtomicBool>,
}
impl Diagnostics {
    pub fn set(&self, callback: Callback) {
        if let Ok(mut guard) = self.callback.lock() {
            *guard = Some(callback);
        }
    }
    pub fn emit(&self, summary: SdkPerformanceSummary) {
        let callback = self.callback.lock().ok().and_then(|guard| guard.clone());
        let Some(callback) = callback else {
            return;
        };
        if self.busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let busy = self.busy.clone();
        tokio::task::spawn_blocking(move || {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(summary)));
            busy.store(false, Ordering::Release);
        });
    }
}

const GAUGES: &[&str] = &[
    "signing_configured_limit",
    "multipart_configured_limit",
    "signing_limit",
    "multipart_limit",
    "signing_active_peak",
    "signing_pending_peak",
    "multipart_active_peak",
    "buffered_batches_peak",
    "batch_routes_max",
    "batch_bytes_max",
    "concurrency_changes",
    "source_waiters",
    "provider_clients_created",
    "provider_clients_reused",
];
const MILESTONES: &[&str] = &[
    "first_manifest_ms",
    "first_route_ms",
    "first_batch_ms",
    "final_flush_ms",
    "prepared_ms",
];
const NAMES: &[&str] = &[
    "sdk.discovery",
    "sdk.multipart_create",
    "sdk.signing",
    "sdk.batch_ack",
    "sdk.buffer_wait",
    "sdk.preparation",
    "sdk.source_signing",
    "sdk.destination_signing",
    "sdk.source_grant_wait",
    "sdk.multipart_ready_wait",
    "sdk.multipart_provider",
    "sdk.multipart_queue",
    "sdk.multipart_callback",
    "sdk.manifest_ack",
    "sdk.provider_client_setup",
    "sdk.metadata_request",
    "sdk.route_assembly",
    "sdk.batch_encode",
    "sdk.batch_checksum",
    "sdk.batch_queue",
    "sdk.signing_queue",
    "sdk.process_cpu",
    "sdk.event_loop_delay",
    "sdk.producer_wait",
];
const BOUNDS: &[f64] = &[
    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10000.0, 30000.0, 120000.0,
];

tokio::task_local! { pub(crate) static CURRENT: Arc<Mutex<Collector>>; }
pub(crate) struct Phase {
    metrics: Option<Arc<Mutex<Collector>>>,
    started: Instant,
    name: &'static str,
}
pub(crate) fn phase(name: &'static str) -> Phase {
    Phase {
        metrics: CURRENT.try_with(Clone::clone).ok(),
        started: Instant::now(),
        name,
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            if let Ok(mut metrics) = metrics.lock() {
                metrics.observe(self.name, self.started);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renewal_history_is_bounded_and_legacy_reports_stay_unchanged() {
        let history = Arc::new(Mutex::new(SourceSignatureHistory::new(true)));
        let mut initial = Collector::new();
        initial.source_history = Some(history.clone());
        initial.source_used(true, 0);
        initial.source_used(true, 131071);
        assert_eq!(initial.snapshot_for(true).counters.source_renewals, Some(0));
        assert_eq!(initial.snapshot().counters.source_renewals, None);
        let mut replay = Collector::new();
        replay.source_history = Some(history);
        replay.source_used(true, 131071);
        replay.source_used(true, 1);
        replay.observe_duration("sdk.producer_wait", std::time::Duration::from_millis(3));
        assert_eq!(replay.snapshot_for(true).counters.source_renewals, Some(1));
        assert_eq!(replay.snapshot_for(true).counters.source_signatures, 2);
        replay.source_used(true, 131072);
        assert_eq!(replay.snapshot_for(true).counters.source_renewals, None);
        let mut resumed = Collector::new();
        resumed.source_history = Some(Arc::new(Mutex::new(SourceSignatureHistory::new(false))));
        resumed.source_used(true, 0);
        assert_eq!(resumed.snapshot_for(true).counters.source_renewals, None);
    }
    #[test]
    fn bounded_details_preserve_the_legacy_contract() {
        let mut metrics = Collector::new();
        metrics.observe_duration("sdk.source_signing", std::time::Duration::from_millis(7));
        metrics.observe_duration("sdk.signing", std::time::Duration::from_millis(9));
        metrics.gauge("signed_url", 1.0);
        metrics.gauge("signing_limit", 16.0);
        metrics.mark("first_batch_ms");
        let old = metrics.snapshot();
        assert_eq!(old.measurements.len(), 1);
        assert!(old.measurements[0].histogram.is_none());
        assert!(old.gauges.is_none());
        let current = metrics.snapshot_for(true);
        assert_eq!(current.schema_version, "sdk-performance/v2");
        assert_eq!(current.gauges.unwrap().len(), 1);
        assert!(current
            .unmeasured
            .unwrap()
            .contains(&"sdk.event_loop_delay".into()));
        assert!(current.measurements.iter().all(|value| value
            .histogram
            .as_ref()
            .unwrap()
            .iter()
            .sum::<u64>()
            == value.count));
    }
}
