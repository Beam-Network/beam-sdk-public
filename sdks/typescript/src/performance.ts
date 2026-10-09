import { AsyncLocalStorage } from "node:async_hooks";
import type { SdkPerformanceSummary } from './models.js';

const NAMES = ["sdk.discovery", "sdk.multipart_create", "sdk.signing", "sdk.batch_ack", "sdk.buffer_wait", "sdk.preparation", "sdk.source_signing", "sdk.destination_signing", "sdk.source_grant_wait", "sdk.multipart_ready_wait", "sdk.multipart_provider", "sdk.multipart_queue", "sdk.multipart_callback", "sdk.manifest_ack", "sdk.provider_client_setup", "sdk.metadata_request", "sdk.route_assembly", "sdk.batch_encode", "sdk.batch_checksum", "sdk.batch_queue", "sdk.signing_queue", "sdk.process_cpu", "sdk.event_loop_delay", "sdk.producer_wait"] as const;
const BOUNDS = [0.1, .5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 120000];
const GAUGES = new Set(["signing_configured_limit", "multipart_configured_limit", "signing_limit", "multipart_limit", "signing_active_peak", "signing_pending_peak", "multipart_active_peak", "buffered_batches_peak", "batch_routes_max", "batch_bytes_max", "concurrency_changes", "source_waiters", "provider_clients_created", "provider_clients_reused"]);
const MILESTONES = new Set(["first_manifest_ms", "first_route_ms", "first_batch_ms", "final_flush_ms", "prepared_ms"]);
const context = new AsyncLocalStorage<SdkPerformanceCollector>();
let activeDelaySamplers = 0;
export const currentSdkPerformance = () => context.getStore();
export const measureSdkPhase = <T>(name: string, work: () => Promise<T>): Promise<T> => context.getStore()?.measure(name, work) ?? work();

/** Bounded identities only: no URLs, credentials or grant bodies. */
export class SourceSignatureHistory {
  private readonly bits = new Uint8Array(16_384);
  constructor(public complete = true) {}
  created(index: number): boolean {
    if (!Number.isSafeInteger(index) || index < 0 || index >= this.bits.length * 8) { this.complete = false; return false; }
    const byte = index >>> 3, mask = 1 << (index & 7);
    const renewed = (this.bits[byte]! & mask) !== 0;
    this.bits[byte]! |= mask;
    return renewed;
  }
}
export class SdkPerformanceCollector {
  constructor(private readonly sourceHistory?: SourceSignatureHistory) {}
  private sourceRenewals = 0;
  sourceCreated(index: number): void {
    this.counters.source_signatures++;
    if (this.sourceHistory?.created(index)) this.sourceRenewals++;
  }
  private readonly started = performance.now();
  private readonly details = process.env.BEAM_SDK_PERFORMANCE_DETAILS !== 'false';
  private readonly cpu = process.cpuUsage();
  private finished = false;
  private delayTimer?: ReturnType<typeof setInterval>;
  private readonly measurements = new Map<string, { count: number; work_ms: number; max_ms: number; histogram: number[] }>();
  private readonly gauges: Record<string, number> = {};
  private readonly milestones: Record<string, number> = {};
  private dropped = 0;
  readonly counters = { source_signatures: 0, source_reuses: 0, route_batches: 0 };
  observe(name: string, milliseconds: number): void {
    const index = (NAMES as readonly string[]).indexOf(name);
    if (index < 0 || (!this.details && index >= 6) || !Number.isFinite(milliseconds) || milliseconds < 0 || milliseconds > 86_400_000) return;
    const value = this.measurements.get(name) ?? { count: 0, work_ms: 0, max_ms: 0, histogram: Array(18).fill(0) };
    if (value.count >= 1_000_000 || value.work_ms + milliseconds > 86_400_000) { this.dropped++; return; }
    value.count++; value.work_ms += milliseconds; value.max_ms = Math.max(value.max_ms, milliseconds);
    const bin = BOUNDS.findIndex(bound => milliseconds <= bound);
    value.histogram[bin < 0 ? 17 : bin]!++;
    this.measurements.set(name, value);
  }
  gauge(name: string, value: number): void {
    if (this.details && GAUGES.has(name) && Number.isFinite(value) && value >= 0) this.gauges[name] = Math.min(1_000_000_000, Math.max(this.gauges[name] ?? 0, value));
  }
  increment(name: string): void { this.gauge(name, (this.gauges[name] ?? 0) + 1); }
  seedDiscovery(other: SdkPerformanceCollector): void {
    for (const name of ["sdk.discovery", "sdk.metadata_request", "sdk.provider_client_setup"]) {
      const value = other.measurements.get(name); if (value) this.measurements.set(name, { ...value, histogram: [...value.histogram] });
    }
    for (const name of ["provider_clients_created", "provider_clients_reused"]) if (other.gauges[name] !== undefined) this.gauges[name] = other.gauges[name]!;
  }
  mark(name: string): void {
    if (this.details && MILESTONES.has(name) && this.milestones[name] === undefined) this.milestones[name] = performance.now() - this.started;
  }
  async measure<T>(name: string, work: () => Promise<T>): Promise<T> {
    const start = performance.now();
    try { return await context.run(this, work); } finally { this.observe(name, performance.now() - start); }
  }
  startDelaySampling(): void {
    if (!this.details || this.delayTimer || this.finished || activeDelaySamplers >= 16) return;
    activeDelaySamplers++;
    let last = performance.now();
    this.delayTimer = setInterval(() => {
      const now = performance.now(); this.observe('sdk.event_loop_delay', Math.max(0, now - last - 100)); last = now;
    }, 100);
    this.delayTimer.unref();
  }
  finish(): void {
    if (this.finished) return; this.finished = true;
    if (this.delayTimer) { clearInterval(this.delayTimer); this.delayTimer = undefined; activeDelaySamplers--; }
    const used = process.cpuUsage(this.cpu); this.observe('sdk.process_cpu', (used.user + used.system) / 1000);
    this.mark('prepared_ms');
  }
  snapshot(v2 = true): SdkPerformanceSummary {
    const measurements = [...this.measurements].filter(([name]) => v2 || (NAMES.slice(0, 6) as readonly string[]).includes(name))
      .map(([name, value]) => ({ name, count: value.count, work_ms: value.work_ms, max_ms: value.max_ms, ...(v2 ? { histogram: [...value.histogram] } : {}) }));
    return { schema_version: v2 ? 'sdk-performance/v2' : 'sdk-performance/v1', measurements, counters: { ...this.counters, ...(v2 && this.sourceHistory?.complete ? { source_renewals: this.sourceRenewals } : {}) },
      ...(v2 ? { gauges: { ...this.gauges }, milestones: { ...this.milestones }, unmeasured: NAMES.slice(6).filter(name => !this.measurements.has(name)), detail_dropped: this.dropped } : {}) };
  }
}
