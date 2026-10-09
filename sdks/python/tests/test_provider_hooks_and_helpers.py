"""Provider hooks, the ownership fence, S3-compatible configs, and hybrid helpers."""

from __future__ import annotations

import asyncio
import threading
import unittest
from typing import Any
from urllib.parse import parse_qs, urlsplit

from pydantic import ValidationError
from test_provider_transfer_flow import (
    TRANSFER_ID,
    OrderedFakeControl,
    ProviderHarness,
    multi_destination_descriptor,
    r2_destination,
    r2_source,
)

from beam_network_sdk import _client as client_module
from beam_network_sdk import provider_signing
from beam_network_sdk.cancellation import BeamCancellationToken
from beam_network_sdk.exceptions import BeamCancelledError, BeamProviderTransferError
from beam_network_sdk.models import (
    HippiusProviderDestination,
    ProviderMultipartGroupIdentity,
    R2ProviderSource,
    S3CompatibleProviderDestination,
    S3CompatibleProviderSource,
    S3ProviderSource,
)


class Hooks:
    def __init__(self, control: OrderedFakeControl) -> None:
        self.control = control
        self.order: list[str] = []
        self.identities: list[ProviderMultipartGroupIdentity] = []

    def seen(self, message_type: str) -> int:
        return sum(1 for call in self.control.calls if call["message_type"] == message_type)

    def before_prepare(self) -> None:
        self.order.append(f"before_prepare(prepares={self.seen('transfer.prepare')})")

    async def prepared(self, prepared: Any) -> None:
        self.order.append(f"prepared(begins={self.seen('transfer.route_stream.begin')})")

    async def group_ready(self, identity: ProviderMultipartGroupIdentity) -> None:
        manifests = self.seen("transfer.route_stream.manifest")
        self.order.append(f"group_ready(manifests={manifests})")
        self.identities.append(identity)


class ProviderHookTests(unittest.IsolatedAsyncioTestCase):
    def sdk(self, **options: Any) -> tuple[client_module.BeamSDK, OrderedFakeControl]:
        sdk = client_module.BeamSDK(api_key="b1m_py", nats_url="nats://127.0.0.1:4222", **options)
        control = OrderedFakeControl()
        sdk._control = control  # type: ignore[assignment]
        sdk.transfers._control = control  # type: ignore[assignment]
        return sdk, control

    async def test_hooks_run_in_order_and_expose_only_durable_identity(self) -> None:
        with ProviderHarness(self) as provider:
            provider.create_delay = 0.02
            sdk, control = self.sdk()
            control.prepare_plan_descriptor = multi_destination_descriptor(5)
            hooks = Hooks(control)
            destinations = [
                r2_destination().model_copy(update={"key": f"out-{index}.bin"})
                for index in range(5)
            ]
            prepared = await sdk.transfers.create_transfer(
                sources=[r2_source()],
                destinations=destinations,
                on_before_transfer_prepare=hooks.before_prepare,
                on_prepared=hooks.prepared,
                on_multipart_group_ready=hooks.group_ready,
            )
        self.assertTrue(prepared.success)
        self.assertEqual(hooks.order[0], "before_prepare(prepares=0)")
        self.assertEqual(hooks.order[1], "prepared(begins=0)")
        self.assertEqual(len(hooks.identities), 5)
        self.assertEqual(provider.max_active_creates, 2, "multipart control is bounded")
        for identity in hooks.identities:
            serialized = identity.model_dump_json()
            for secret in ("cleanup-access", "cleanup-secret", "http://", "https://"):
                self.assertNotIn(secret, serialized)
            self.assertEqual(identity.transfer_id, prepared.transfer_id)
            self.assertEqual(identity.expected_part_count, 1)
        self.assertEqual(
            sorted(identity.upload_id for identity in hooks.identities),
            sorted(f"upload-{index}" for index in range(1, 6)),
        )
        begin = next(c for c in control.calls if c["message_type"] == "transfer.route_stream.begin")
        self.assertTrue(begin["payload"]["auto_distribute"], "create_transfer distributes")

    async def test_group_ready_failure_aborts_that_upload_and_fails_closed(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()

            def reject(_identity: ProviderMultipartGroupIdentity) -> None:
                raise RuntimeError("could not persist the upload identity")

            with self.assertRaises(BeamProviderTransferError) as raised:
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                    on_multipart_group_ready=reject,
                )
        self.assertTrue(raised.exception.transfer_cancelled)
        self.assertEqual([item["upload_id"] for item in provider.aborted], ["upload-1"])
        self.assertEqual(
            [c for c in control.calls if c["message_type"] == "transfer.route_stream.manifest"],
            [],
        )
        self.assertEqual(control.recovery_leases, {})

    async def test_on_prepared_failure_keeps_the_recovery_lease(self) -> None:
        with ProviderHarness(self):
            sdk, control = self.sdk()

            def fail(_prepared: Any) -> None:
                raise RuntimeError("caller bookkeeping failed")

            with self.assertRaisesRegex(RuntimeError, "bookkeeping"):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                    transfer_id=TRANSFER_ID,
                    on_prepared=fail,
                )
        self.assertIn(TRANSFER_ID, control.recovery_leases)
        self.assertEqual(control.continued_recovery, [TRANSFER_ID])
        self.assertNotIn("transfer.route_stream.begin", [c["message_type"] for c in control.calls])

    async def test_throw_if_cancelled_stops_the_foreground_and_continues_recovery(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            polls: list[str | None] = []

            def throw_if_cancelled(transfer_id: str | None = None) -> None:
                polls.append(transfer_id)
                if len(polls) > 4:
                    raise KeyboardInterruptLike()

            with self.assertRaises(KeyboardInterruptLike):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                    transfer_id=TRANSFER_ID,
                    throw_if_cancelled=throw_if_cancelled,
                )
        self.assertEqual(polls[:4], [None, None, None, TRANSFER_ID])
        self.assertIn(TRANSFER_ID, control.recovery_leases)
        self.assertEqual(control.continued_recovery, [TRANSFER_ID])
        self.assertNotIn("transfer.cancel", [c["message_type"] for c in control.calls])
        self.assertEqual(provider.aborted, [])

    async def test_asyncio_cancellation_continues_recovery(self) -> None:
        with ProviderHarness(self) as provider:
            provider.sign_delay = 5
            sdk, control = self.sdk()
            task = asyncio.create_task(
                sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
                )
            )
            while provider.active_signs == 0:
                await asyncio.sleep(0.001)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertIn(TRANSFER_ID, control.recovery_leases)
        self.assertEqual(control.continued_recovery, [TRANSFER_ID])
        self.assertEqual(provider.aborted, [])

    async def test_ownership_fence_stops_recovery_without_cancelling_or_aborting(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            ownership = BeamCancellationToken()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()],
                destinations=[r2_destination()],
                transfer_id=TRANSFER_ID,
                ownership=ownership,
            )
            signer = control.recovery_signers[TRANSFER_ID]
            replay = control.recovery_leases[TRANSFER_ID]["replay_routes"]
            # Cancelled from another thread, as a supervisor process might.
            thread = threading.Thread(target=ownership.cancel, args=("owner replaced",))
            thread.start()
            thread.join()
            await asyncio.sleep(0)
            self.assertEqual(control.recovery_leases, {})
            self.assertEqual(control.recovery_signers, {})
            self.assertNotIn(TRANSFER_ID, sdk.transfers._integrity_context)
            with self.assertRaisesRegex(BeamCancelledError, "owner replaced"):
                await signer({"route_generation_id": "later", "chunks": []})
            with self.assertRaisesRegex(BeamCancelledError, "owner replaced"):
                await replay("44444444-4444-4444-8444-444444444444")
        self.assertNotIn("transfer.cancel", [c["message_type"] for c in control.calls])
        self.assertEqual(provider.aborted, [])

    async def test_ownership_lost_mid_stream_never_aborts_a_replacement_owners_upload(
        self,
    ) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            ownership = BeamCancellationToken()
            provider.on_create = lambda: ownership.cancel(RuntimeError("owner replaced"))
            with self.assertRaisesRegex(RuntimeError, "owner replaced"):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                    transfer_id=TRANSFER_ID,
                    ownership=ownership,
                )
            await asyncio.sleep(0)
        self.assertEqual(len(provider.created), 1)
        self.assertEqual(provider.aborted, [])
        self.assertNotIn("transfer.cancel", [c["message_type"] for c in control.calls])
        self.assertEqual(control.recovery_leases, {})

    async def test_an_already_cancelled_owner_does_nothing(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            ownership = BeamCancellationToken()
            ownership.cancel()
            with self.assertRaises(BeamCancelledError):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()], ownership=ownership
                )
        self.assertEqual(control.calls, [])
        self.assertEqual(provider.prepared_sources, 0)


class KeyboardInterruptLike(Exception):
    """A caller-defined cancellation error raised from throw_if_cancelled."""


class S3CompatibleConfigTests(unittest.TestCase):
    def test_configs_validate_like_the_typescript_factory(self) -> None:
        config = S3CompatibleProviderSource(
            provider=" Wasabi ",
            bucket="b",
            key="k",
            endpoint_url="https://s3.us-east-1.wasabisys.com",
            access_key_id="ak",
            secret_access_key="sk",
        )
        self.assertEqual(config.provider, "wasabi")
        self.assertEqual(config.driver, "s3-compatible")
        with self.assertRaisesRegex(ValidationError, "minio config requires endpoint_url"):
            S3CompatibleProviderDestination(
                provider="minio", bucket="b", key="k", access_key_id="ak", secret_access_key="sk"
            )
        with self.assertRaisesRegex(ValidationError, "account_id or endpoint_url"):
            S3CompatibleProviderSource(
                provider="r2", bucket="b", key="k", access_key_id="ak", secret_access_key="sk"
            )
        S3CompatibleProviderSource(
            provider="s3", bucket="b", key="k", access_key_id="ak", secret_access_key="sk"
        )

    def test_endpoint_region_and_path_style_resolution_is_provider_aware(self) -> None:
        r2 = R2ProviderSource(
            bucket="bucket", key="file.bin", account_id="account123", access_key_id="ak",
            secret_access_key="sk",
        )
        self.assertEqual(
            provider_signing.s3_compatible_endpoint(r2),
            "https://account123.r2.cloudflarestorage.com",
        )
        self.assertEqual(provider_signing.s3_compatible_region(r2), "auto")
        self.assertTrue(provider_signing.s3_compatible_force_path_style(r2))
        custom = S3CompatibleProviderSource(
            provider="wasabi",
            bucket="bucket",
            key="file.bin",
            endpoint_url="https://s3.us-east-1.wasabisys.com",
            access_key_id="ak",
            secret_access_key="sk",
        )
        self.assertEqual(
            provider_signing.s3_compatible_endpoint(custom), "https://s3.us-east-1.wasabisys.com"
        )
        self.assertEqual(provider_signing.s3_compatible_region(custom), "us-east-1")
        self.assertTrue(provider_signing.s3_compatible_force_path_style(custom))
        self.assertFalse(
            provider_signing.s3_compatible_force_path_style(
                custom.model_copy(update={"force_path_style": False})
            )
        )
        s3 = S3ProviderSource(bucket="bucket", key="file.bin", access_key_id="ak", secret_access_key="sk")
        self.assertIsNone(provider_signing.s3_compatible_endpoint(s3))
        self.assertEqual(provider_signing.s3_compatible_region(s3), "us-east-1")
        self.assertIsNone(provider_signing.s3_compatible_force_path_style(s3))

    def test_custom_provider_signs_path_style_urls_and_reports_its_name(self) -> None:
        provider_signing._s3_client_cache.clear()
        destination = S3CompatibleProviderDestination(
            provider="minio",
            bucket="archive",
            key="file.bin",
            endpoint_url="https://storage.example.test",
            access_key_id="ak",
            secret_access_key="sk",
        )
        prepared = provider_signing.prepare_provider_destination(destination)
        self.assertEqual(prepared.provider, "minio")
        self.assertEqual(prepared.logical_prefix, "file.bin")
        self.assertEqual(prepared.metadata["driver"], "s3-compatible")
        self.assertEqual(prepared.metadata["endpoint_url"], "https://storage.example.test")
        url = provider_signing.sign_destination_url(
            destination, object_key="file.bin", expires_in=60
        )
        self.assertTrue(url.startswith("https://storage.example.test/archive/file.bin?"))


class HybridHelperTests(unittest.TestCase):
    config = S3CompatibleProviderDestination(
        provider="r2",
        bucket="bucket",
        key="file.bin",
        region="us-east-1",
        endpoint_url="https://storage.example.test",
        force_path_style=True,
        access_key_id="fixture-key",
        secret_access_key="fixture-secret",
    )

    def test_frozen_source_ranges_and_checksum_bound_uploads_without_data_reads(self) -> None:
        provider_signing._s3_client_cache.clear()
        source = S3CompatibleProviderSource(
            provider="r2",
            bucket="bucket",
            key="file.bin",
            region="us-east-1",
            endpoint_url="https://storage.example.test",
            force_path_style=True,
            access_key_id="fixture-key",
            secret_access_key="fixture-secret",
        )
        grant = provider_signing.sign_source_read_range(
            source, offset=10, length=20, expires_in=60, if_match='"frozen-etag"', version_id="v1"
        )
        query = parse_qs(urlsplit(grant["url"]).query)
        self.assertEqual(grant["headers"]["Range"], "bytes=10-29")
        self.assertEqual(grant["headers"]["If-Match"], '"frozen-etag"')
        self.assertEqual(query["versionId"], ["v1"])
        self.assertIn("if-match", query["X-Amz-SignedHeaders"][0])
        url = provider_signing.sign_destination_url(
            self.config,
            object_key="final.bin",
            upload_id="upload-1",
            part_number=1,
            expires_in=60,
            content_md5="1B2M2Y8AsgTpgAmY7PhCfg==",
        )
        parts = urlsplit(url)
        query = parse_qs(parts.query)
        self.assertEqual(parts.path, "/bucket/final.bin")
        self.assertEqual(query["uploadId"], ["upload-1"])
        self.assertEqual(query["partNumber"], ["1"])
        self.assertIn("content-md5", query["X-Amz-SignedHeaders"][0])

    def test_checksum_bound_uploads_require_s3_compatible_storage(self) -> None:
        hippius = HippiusProviderDestination(bucket="b", key="k", api_token="t")
        with self.assertRaisesRegex(ValueError, "checksum-bound uploads"):
            provider_signing.sign_destination_url(
                hippius, object_key="k", content_md5="1B2M2Y8AsgTpgAmY7PhCfg=="
            )

    def test_parts_paginate_and_complete_without_reading_payloads(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class Client:
            def list_parts(self, **kwargs: Any) -> dict[str, Any]:
                calls.append(("list_parts", kwargs))
                if "PartNumberMarker" not in kwargs:
                    return {
                        "IsTruncated": True,
                        "NextPartNumberMarker": 1,
                        "Parts": [{"PartNumber": 1, "ETag": "a", "Size": 6}],
                    }
                return {"IsTruncated": False, "Parts": [{"PartNumber": 2, "ETag": "b", "Size": 6}]}

            def complete_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
                calls.append(("complete", kwargs))
                return {"ETag": "final", "VersionId": "v2"}

            def head_object(self, **kwargs: Any) -> dict[str, Any]:
                calls.append(("head", kwargs))
                return {
                    "ContentLength": 12,
                    "ETag": '"final"',
                    "Metadata": {"beam-room-operation-id": "operation"},
                }

        client = Client()

        def factory(*_args: Any, **_kwargs: Any) -> Client:
            return client

        parts = provider_signing.list_multipart_parts(
            self.config, object_key="file", upload_id="upload", client_factory=factory
        )
        self.assertEqual(
            [(p.part_number, p.etag, p.size) for p in parts], [(1, "a", 6), (2, "b", 6)]
        )
        completed = provider_signing.complete_multipart_upload(
            self.config,
            object_key="file",
            upload_id="upload",
            parts=list(reversed(parts)),
            client_factory=factory,
        )
        self.assertEqual((completed.etag, completed.version_id), ("final", "v2"))
        self.assertEqual(
            calls[2][1]["MultipartUpload"],
            {"Parts": [{"PartNumber": 1, "ETag": "a"}, {"PartNumber": 2, "ETag": "b"}]},
        )
        head = provider_signing.inspect_destination_object(
            self.config, object_key="file", client_factory=factory
        )
        self.assertEqual(head.size, 12)
        self.assertEqual(head.metadata["beam-room-operation-id"], "operation")
        self.assertEqual([name for name, _ in calls], ["list_parts", "list_parts", "complete", "head"])

    def test_invalid_pagination_is_rejected(self) -> None:
        class Client:
            def list_parts(self, **_kwargs: Any) -> dict[str, Any]:
                return {"IsTruncated": True, "NextPartNumberMarker": None, "Parts": []}

        with self.assertRaisesRegex(ValueError, "invalid multipart pagination"):
            provider_signing.list_multipart_parts(
                self.config, object_key="f", upload_id="u", client_factory=lambda *a, **k: Client()
            )

    def test_cancellation_stops_provider_metadata_operations(self) -> None:
        token = BeamCancellationToken()
        token.cancel("stop")
        called: list[str] = []

        def factory(*_args: Any, **_kwargs: Any) -> Any:
            called.append("client")
            raise AssertionError("no provider call may start after cancellation")

        operations = [
            lambda: provider_signing.create_multipart_upload(
                destination=self.config,
                object_key="file",
                metadata={},
                cancellation=token,
                client_factory=factory,
            ),
            lambda: provider_signing.list_multipart_parts(
                self.config,
                object_key="file",
                upload_id="u",
                cancellation=token,
                client_factory=factory,
            ),
            lambda: provider_signing.complete_multipart_upload(
                self.config,
                object_key="file",
                upload_id="u",
                parts=[{"part_number": 1, "etag": "part"}],
                cancellation=token,
                client_factory=factory,
            ),
            lambda: provider_signing.inspect_destination_object(
                self.config, object_key="file", cancellation=token, client_factory=factory
            ),
            lambda: provider_signing.abort_multipart_upload(
                destination=self.config,
                object_key="file",
                upload_id="u",
                cancellation=token,
                client_factory=factory,
            ),
        ]
        for operation in operations:
            with self.assertRaisesRegex(BeamCancelledError, "stop"):
                operation()
        self.assertEqual(called, [])

    def test_plan_sources_carry_metadata_but_no_signed_url(self) -> None:
        class Client:
            def head_object(self, **_kwargs: Any) -> dict[str, Any]:
                return {"ContentLength": 99, "ETag": '"e"'}

            def generate_presigned_url(self, *_args: Any, **_kwargs: Any) -> str:
                raise AssertionError("planning must not sign a read URL")

        planned = provider_signing.prepare_provider_source_for_plan(
            S3ProviderSource(bucket="b", key="k", access_key_id="ak", secret_access_key="sk"),
            index=3,
            client_factory=lambda *a, **k: Client(),
        )
        self.assertEqual(planned.source_id, "src_3")
        self.assertIsNone(planned.url)
        self.assertEqual(planned.size, 99)
        self.assertEqual(planned.metadata["etag"], '"e"')


if __name__ == "__main__":
    unittest.main()
