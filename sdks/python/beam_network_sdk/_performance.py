"""Bounded local work durations. These overlap and are not an elapsed-time sum."""

from __future__ import annotations

import asyncio
import contextvars
import math
import os
import threading
import time
from typing import Any

BASE = ("discovery", "multipart_create", "signing", "batch_ack", "buffer_wait", "preparation")
DETAIL = (
    "source_signing",
    "destination_signing",
    "source_grant_wait",
    "multipart_ready_wait",
    "multipart_provider",
    "multipart_queue",
    "multipart_callback",
    "manifest_ack",
    "provider_client_setup",
    "metadata_request",
    "route_assembly",
    "batch_encode",
    "batch_checksum",
    "batch_queue",
    "signing_queue",
    "process_cpu",
    "event_loop_delay",
    "producer_wait",
)
BOUNDS = (0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 120000)
GAUGES = {
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
}
MILESTONES = {
    "first_manifest_ms",
    "first_route_ms",
    "first_batch_ms",
    "final_flush_ms",
    "prepared_ms",
}
current: contextvars.ContextVar[PerformanceCollector | None] = contextvars.ContextVar(
    "beam_performance", default=None
)


class SourceSignatureHistory:
    def __init__(self, complete: bool = True) -> None:
        self.complete = complete
        self.bits = bytearray(16_384)

    def created(self, index: int) -> bool:
        if index < 0 or index >= len(self.bits) * 8:
            self.complete = False
            return False
        byte, mask = index >> 3, 1 << (index & 7)
        renewed = bool(self.bits[byte] & mask)
        self.bits[byte] |= mask
        return renewed


class PerformanceCollector:
    _samplers = 0

    def __init__(self) -> None:
        self.started = time.monotonic()
        self.measurements: dict[str, dict[str, Any]] = {}
        self.counters = {"source_signatures": 0, "source_reuses": 0, "route_batches": 0}
        self.source_history: SourceSignatureHistory | None = None
        self.source_renewals = 0
        self.cpu_started = time.process_time()
        self.finished = False
        self.details = os.getenv("BEAM_SDK_PERFORMANCE_DETAILS", "true") != "false"
        self.gauges: dict[str, float] = {}
        self.milestones: dict[str, float] = {}
        self.dropped = 0
        self.lock = threading.RLock()
        self.active_signers = 0
        self.delay_timer: asyncio.TimerHandle | None = None

    def source_created(self, index: int) -> None:
        self.counters["source_signatures"] += 1
        if self.source_history is not None and self.source_history.created(index):
            self.source_renewals += 1

    def observe(self, name: str, started: float) -> None:
        self.observe_duration(name, time.monotonic() - started)

    def observe_duration(self, name: str, seconds: float) -> None:
        if name not in {"sdk." + item for item in BASE + (DETAIL if self.details else ())}:
            return
        ms = seconds * 1000
        if not math.isfinite(ms) or not 0 <= ms <= 86400000:
            return
        with self.lock:
            value = self.measurements.setdefault(
                name,
                {"name": name, "count": 0, "work_ms": 0.0, "max_ms": 0.0, "histogram": [0] * 18},
            )
            if value["count"] >= 1000000 or value["work_ms"] + ms > 86400000:
                self.dropped += 1
                return
            value["count"] += 1
            value["work_ms"] += ms
            value["max_ms"] = max(value["max_ms"], ms)
            value["histogram"][next((i for i, bound in enumerate(BOUNDS) if ms <= bound), 17)] += 1

    def gauge(self, name: str, value: float) -> None:
        if self.details and name in GAUGES and math.isfinite(value) and value >= 0:
            with self.lock:
                self.gauges[name] = min(1000000000, max(self.gauges.get(name, 0), value))

    def increment(self, name: str) -> None:
        with self.lock:
            self.gauge(name, self.gauges.get(name, 0) + 1)

    def signer_active(self, delta: int) -> None:
        with self.lock:
            self.active_signers += delta
            self.gauge("signing_active_peak", self.active_signers)

    def seed_discovery(self, other: PerformanceCollector) -> None:
        with other.lock:
            for name in ("sdk.metadata_request", "sdk.provider_client_setup"):
                if name in other.measurements:
                    self.measurements[name] = {
                        **other.measurements[name],
                        "histogram": list(other.measurements[name]["histogram"]),
                    }
            for name in ("provider_clients_created", "provider_clients_reused"):
                if name in other.gauges:
                    self.gauges[name] = other.gauges[name]

    def mark(self, name: str) -> None:
        if self.details and name in MILESTONES:
            self.milestones.setdefault(name, max(0, (time.monotonic() - self.started) * 1000))

    def start(self) -> None:
        if not self.details or self.delay_timer is not None or PerformanceCollector._samplers >= 16:
            return
        loop = asyncio.get_running_loop()
        PerformanceCollector._samplers += 1
        expected = loop.time() + 0.1

        def sample() -> None:
            nonlocal expected
            self.observe_duration("sdk.event_loop_delay", max(0, loop.time() - expected))
            expected = loop.time() + 0.1
            self.delay_timer = loop.call_later(0.1, sample)

        self.delay_timer = loop.call_later(0.1, sample)

    def finish(self) -> None:
        if not self.finished:
            self.finished = True
            if self.delay_timer is not None:
                self.delay_timer.cancel()
                self.delay_timer = None
                PerformanceCollector._samplers -= 1
            self.observe_duration("sdk.process_cpu", time.process_time() - self.cpu_started)
            self.mark("prepared_ms")

    def snapshot(self, v2: bool = False) -> dict[str, Any]:
        with self.lock:
            measurements = []
            for name, value in self.measurements.items():
                if v2 or name.removeprefix("sdk.") in BASE:
                    measurements.append(
                        {
                            k: list(v) if isinstance(v, list) else v
                            for k, v in value.items()
                            if v2 or k != "histogram"
                        }
                    )
            result: dict[str, Any] = {
                "schema_version": "sdk-performance/v2" if v2 else "sdk-performance/v1",
                "measurements": measurements,
                "counters": dict(self.counters),
            }
            if v2:
                if self.source_history is not None and self.source_history.complete:
                    result["counters"]["source_renewals"] = self.source_renewals
                result.update(
                    gauges=dict(self.gauges),
                    milestones=dict(self.milestones),
                    unmeasured=["sdk." + n for n in DETAIL if "sdk." + n not in self.measurements],
                    detail_dropped=self.dropped,
                )
            return result
