from beam_network_sdk.provider_signing import bounded_grant_expiry
import unittest
from beam_network_sdk._performance import PerformanceCollector, SourceSignatureHistory


class PerformanceV2Tests(unittest.TestCase):
    def test_bounded_v2_detail_and_legacy_wire_shape(self):
        metrics = PerformanceCollector()
        metrics.observe_duration("sdk.discovery", .007)
        metrics.observe_duration("sdk.multipart_provider", .025)
        metrics.observe_duration("sdk.multipart_provider", .25)
        metrics.observe_duration("signed-url:private", 1)
        metrics.gauge("signing_limit", 64)
        metrics.gauge("object_key", 1)
        metrics.finish()
        current = metrics.snapshot(True)
        provider = next(v for v in current["measurements"] if v["name"] == "sdk.multipart_provider")
        self.assertEqual(len(provider["histogram"]), 18)
        self.assertEqual(sum(provider["histogram"]), 2)
        self.assertEqual(provider["work_ms"], 275)
        self.assertEqual(current["gauges"], {"signing_limit": 64})
        self.assertNotIn("private", str(current))
        legacy = metrics.snapshot()
        self.assertEqual(set(legacy), {"schema_version", "measurements", "counters"})
        self.assertEqual(legacy["schema_version"], "sdk-performance/v1")
        self.assertEqual(legacy["measurements"], [{"name":"sdk.discovery", "count":1, "work_ms":7, "max_ms":7}])


    def test_grant_reuse_never_extends_provider_expiry(self):
        url = "https://example.invalid/read?X-Amz-Date=20260926T100000Z&X-Amz-Expires=3600"
        assert bounded_grant_expiry(url, "2026-09-26T12:00:00Z") == "2026-09-26T11:00:00.000Z"
        assert bounded_grant_expiry(url, "2026-09-26T10:30:00Z") == "2026-09-26T10:30:00.000Z"


    def test_renewal_history_is_bounded_and_excludes_legacy_reports(self):
        history = SourceSignatureHistory()
        initial = PerformanceCollector()
        initial.source_history = history
        initial.source_created(0)
        initial.source_created(131071)
        assert initial.snapshot(True)["counters"]["source_renewals"] == 0
        assert "source_renewals" not in initial.snapshot()["counters"]
        replay = PerformanceCollector()
        replay.source_history = history
        replay.source_created(131071)
        replay.source_created(1)
        replay.observe_duration("sdk.producer_wait", .003)
        assert replay.snapshot(True)["counters"]["source_renewals"] == 1
        assert replay.snapshot(True)["counters"]["source_signatures"] == 2
        replay.source_created(131072)
        assert "source_renewals" not in replay.snapshot(True)["counters"]
        resumed = PerformanceCollector()
        resumed.source_history = SourceSignatureHistory(False)
        resumed.source_created(0)
        assert "source_renewals" not in resumed.snapshot(True)["counters"]
