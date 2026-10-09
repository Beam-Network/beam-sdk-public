"""Provider-transfer lifecycle behaviour, mirroring the TypeScript functional tests."""

from __future__ import annotations

import asyncio
import threading
import time
import unittest
from contextlib import ExitStack
from typing import Any
from unittest.mock import patch

import msgpack

from beam_network_sdk import _client as client_module
from beam_network_sdk._nats_control import (
    TransferClientControl,
    build_connection_options,
)
from beam_network_sdk.exceptions import (
    BeamAPIError,
    BeamProviderTransferError,
    BeamRouteRecoveryPendingError,
)
from beam_network_sdk.models import (
    ChunkSigningPlanItem,
    HippiusProviderDestination,
    PreparedDestination,
    PreparedHttpSource,
    R2ProviderDestination,
    R2ProviderSource,
    SignedChunkRoute,
)
from test_functional_client import FakeTransferControl, fake_plan_descriptor

TRANSFER_ID = "11111111-1111-4111-8111-111111111111"


def r2_source() -> R2ProviderSource:
    return R2ProviderSource(
        bucket="source-bucket",
        key="source.bin",
        endpoint_url="https://r2.example",
        access_key_id="access",
        secret_access_key="secret",
    )


def r2_destination() -> R2ProviderDestination:
    return R2ProviderDestination(
        bucket="dest-bucket",
        key="dest.bin",
        endpoint_url="https://r2.example",
        access_key_id="cleanup-access",
        secret_access_key="cleanup-secret",
    )


def multi_destination_descriptor(count: int) -> dict[str, Any]:
    descriptor = fake_plan_descriptor()
    template = descriptor["destinations"][0]
    descriptor["destinations"] = [
        {
            **template,
            "destination_id": f"dst_{index}",
            "destination_index": index,
            "final_object_keys": {"src_0": f"out-{index}.bin"},
        }
        for index in range(count)
    ]
    descriptor["delivery_route_count"] = count
    return descriptor


def multi_chunk_descriptor(chunk_count: int, *, provider: str = "r2") -> dict[str, Any]:
    descriptor = fake_plan_descriptor(chunk_size=1024)
    descriptor["sources"][0]["size"] = 1024 * chunk_count
    descriptor["sources"][0]["chunk_count"] = chunk_count
    descriptor["destinations"][0]["provider"] = provider
    descriptor["logical_chunk_count"] = chunk_count
    descriptor["delivery_route_count"] = chunk_count
    return descriptor


class ProviderHarness:
    """Patches provider I/O in the client module and records what the SDK asked for."""

    def __init__(self, test: unittest.TestCase) -> None:
        self.test = test
        self.created: list[dict[str, Any]] = []
        self.aborted: list[dict[str, Any]] = []
        self.abort_failures = 0
        self.prepared_sources = 0
        self.signed_routes: list[dict[str, Any]] = []
        self.sign_delay = 0.0
        self.active_signs = 0
        self.max_active_signs = 0
        self.events: list[str] = []
        self.lock = threading.Lock()
        self.create_delay = 0.0
        self.active_creates = 0
        self.max_active_creates = 0
        self.on_create: Any = None
        self._stack = ExitStack()

    def __enter__(self) -> ProviderHarness:
        def prepare_source(*_args: Any, **_kwargs: Any) -> PreparedHttpSource:
            self.prepared_sources += 1
            return PreparedHttpSource(
                source_id="src_0", url="https://source.example/file.bin", size=4096
            )

        def prepare_destination(*_args: Any, **kwargs: Any) -> PreparedDestination:
            return PreparedDestination(destination_id=f"dst_{kwargs.get('index', 0)}", provider="r2")

        def create_upload(**kwargs: Any) -> str:
            with self.lock:
                self.active_creates += 1
                self.max_active_creates = max(self.max_active_creates, self.active_creates)
            try:
                if self.create_delay:
                    time.sleep(self.create_delay)
                if self.on_create is not None:
                    self.on_create()
                with self.lock:
                    self.created.append(kwargs)
                    self.events.append("create")
                    return f"upload-{len(self.created)}"
            finally:
                with self.lock:
                    self.active_creates -= 1

        def abort_upload(**kwargs: Any) -> None:
            destination = kwargs["destination"]
            self.aborted.append(
                {
                    **kwargs,
                    "access_key_id": destination.access_key_id,
                    "secret": destination.secret_access_key.get_secret_value(),
                }
            )
            self.events.append("abort")
            if self.abort_failures:
                self.abort_failures -= 1
                raise RuntimeError("abort throttled https://r2.example/?X-Amz-Signature=secret")

        async def sign_route(**kwargs: Any) -> SignedChunkRoute:
            self.active_signs += 1
            self.max_active_signs = max(self.max_active_signs, self.active_signs)
            try:
                if self.sign_delay:
                    await asyncio.sleep(self.sign_delay)
                self.signed_routes.append(kwargs)
                chunk: ChunkSigningPlanItem = kwargs["chunk"]
                target = kwargs["target"]
                upload = kwargs.get("upload")
                metadata = dict(target.metadata)
                if upload is not None:
                    metadata["upload_id"] = upload.upload_id
                    metadata["multipart_group_id"] = upload.manifest.multipart_group_id
                return SignedChunkRoute(
                    source_id=chunk.source_id,
                    destination_id=target.destination_id,
                    chunk_index=chunk.chunk_index,
                    delivery_index=target.metadata.get("delivery_index", 0),
                    source_url=chunk.source_url,
                    dest_url=f"https://dest.example/part-{kwargs['part_number']}",
                    source_offset=chunk.source_offset,
                    chunk_size=chunk.chunk_size,
                    metadata=metadata,
                )
            finally:
                self.active_signs -= 1

        patches = {
            "prepare_provider_source": prepare_source,
            "prepare_provider_destination": prepare_destination,
            "create_multipart_upload": create_upload,
            "abort_multipart_upload": abort_upload,
            "sign_complete_multipart_upload": lambda **_k: "https://dest.example/complete",
            "sign_abort_multipart_upload": lambda **_k: "https://dest.example/abort",
            "sign_list_multipart_upload": lambda **_k: "https://dest.example/list",
            "sign_final_object_head": lambda **_k: "https://dest.example/final-head",
        }
        for name, value in patches.items():
            self._stack.enter_context(patch.object(client_module, name, value))
        self._stack.enter_context(
            patch.object(
                client_module.TransferManager, "_sign_provider_route", staticmethod(sign_route)
            )
        )
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stack.close()


class OrderedFakeControl(FakeTransferControl):
    """Fake control that records cancellation order and can fail with a chosen error."""

    def __init__(self, events: list[str] | None = None) -> None:
        super().__init__()
        self.events = events if events is not None else []
        self.fail_once_error: BaseException | None = None

    async def request(
        self,
        message_type: str,
        payload: dict,
        transfer_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        if message_type == "transfer.cancel":
            self.events.append("cancel")
        if self.fail_once_error is not None and self.fail_once_at == message_type:
            error = self.fail_once_error
            self.fail_once_error = None
            self.fail_once_at = None
            self.calls.append(
                {
                    "message_type": message_type,
                    "payload": payload,
                    "transfer_id": transfer_id,
                    "idempotency_key": idempotency_key,
                }
            )
            raise error
        return await super().request(message_type, payload, transfer_id, idempotency_key)


class ProviderTransferFlowTests(unittest.IsolatedAsyncioTestCase):
    def sdk(
        self, *, events: list[str] | None = None, **options: Any
    ) -> tuple[client_module.BeamSDK, OrderedFakeControl]:
        sdk = client_module.BeamSDK(api_key="b1m_py", nats_url="nats://127.0.0.1:4222", **options)
        control = OrderedFakeControl(events)
        sdk._control = control  # type: ignore[assignment]
        sdk.transfers._control = control  # type: ignore[assignment]
        return sdk, control

    def message_types(self, control: FakeTransferControl) -> list[str]:
        return [call["message_type"] for call in control.calls]

    async def test_non_recoverable_failure_keeps_scoped_authority_through_cleanup(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk(events=provider.events)
            control.fail_once_at = "transfer.route_stream.complete"
            control.fail_once_error = BeamAPIError(400, "route contract rejected")
            destination = r2_destination()
            with self.assertRaises(BeamProviderTransferError) as raised:
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()],
                    destinations=[destination],
                    transfer_id=TRANSFER_ID,
                )

        error = raised.exception
        self.assertEqual(error.transfer_id, TRANSFER_ID)
        self.assertTrue(error.transfer_cancelled)
        self.assertTrue(error.multipart_cleanup_complete)
        self.assertIsInstance(error.cause, BeamAPIError)
        self.assertEqual(error.errors, [error.cause])
        self.assertEqual(len(provider.aborted), 1)
        # The abort ran with the retained credentials, before the lease scrubbed them.
        self.assertEqual(provider.aborted[0]["access_key_id"], "cleanup-access")
        self.assertEqual(provider.aborted[0]["secret"], "cleanup-secret")
        self.assertEqual(provider.aborted[0]["upload_id"], "upload-1")
        self.assertEqual(provider.events, ["create", "cancel", "abort"])
        self.assertEqual(self.message_types(control).count("transfer.cancel"), 1)
        self.assertEqual(control.recovery_leases, {})
        self.assertEqual(control.recovery_signers, {})
        # The caller's config is untouched; only the SDK's retained copy was scrubbed.
        self.assertEqual(destination.secret_access_key.get_secret_value(), "cleanup-secret")

    async def test_abort_after_cancel_runs_once_once_routes_may_exist(self) -> None:
        with ProviderHarness(self) as provider:
            provider.abort_failures = 1
            sdk, control = self.sdk(events=provider.events)
            control.fail_once_at = "transfer.route_stream.complete"
            control.fail_once_error = BeamAPIError(400, "route contract rejected")
            with self.assertRaises(BeamProviderTransferError) as raised:
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()]
                )
        self.assertTrue(raised.exception.transfer_cancelled)
        self.assertFalse(raised.exception.multipart_cleanup_complete)
        self.assertEqual(provider.events, ["create", "cancel", "abort"])

    async def test_cleanup_failure_is_reported_without_signed_urls(self) -> None:
        with ProviderHarness(self) as provider:
            provider.abort_failures = 5
            sdk, control = self.sdk(events=provider.events)
            control.fail_once_at = "transfer.route_stream.complete"
            control.fail_once_error = BeamAPIError(400, "route contract rejected")
            with self.assertRaises(BeamProviderTransferError) as raised:
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()]
                )
        error = raised.exception
        self.assertTrue(error.transfer_cancelled)
        self.assertFalse(error.multipart_cleanup_complete)
        self.assertEqual(len(error.errors), 2)
        for item in (error, *error.errors[1:]):
            self.assertNotIn("X-Amz-Signature", str(item))
            self.assertNotIn("https://", str(item))

    async def test_stream_begin_failure_cancels_before_any_upload_exists(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk(events=provider.events)
            control.fail_once_at = "transfer.route_stream.begin"
            control.fail_once_error = BeamAPIError(400, "invalid stream")
            with self.assertRaises(BeamProviderTransferError):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()]
                )
        # Nothing was created before begin, so cleanup has nothing to abort.
        self.assertEqual(provider.events, ["cancel"])

    async def test_cancel_failure_is_sanitized_and_reported(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk(events=provider.events)
            control.fail_once_at = "transfer.route_stream.complete"
            control.fail_once_error = BeamAPIError(400, "https://private.example/?token=x")

            async def failing_cancel(transfer_id: str) -> Any:
                raise BeamAPIError(503, "https://private.example/?X-Amz-Signature=secret")

            with (
                patch.object(sdk.transfers, "_request_transfer_cancellation", failing_cancel),
                self.assertRaises(BeamProviderTransferError) as raised,
            ):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()]
                )
        error = raised.exception
        self.assertFalse(error.transfer_cancelled)
        self.assertTrue(error.multipart_cleanup_complete)
        self.assertIn("BeamAPIError:status=503", str(error.errors[1]))
        self.assertNotIn("private.example", str(error.errors[1]))
        self.assertEqual(len(provider.aborted), 1)

    async def test_route_replay_restreams_the_retained_plan(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            replay = control.recovery_leases[TRANSFER_ID]["replay_routes"]
            await replay("44444444-4444-4444-8444-444444444444")

        types = self.message_types(control)
        self.assertEqual(types.count("transfer.prepare"), 1)
        self.assertEqual(provider.prepared_sources, 1)
        self.assertEqual(len(provider.created), 1, "replay must reuse the existing upload")
        self.assertEqual(types.count("transfer.route_stream.complete"), 2)
        begins = [call for call in control.calls if call["message_type"] == "transfer.route_stream.begin"]
        self.assertEqual(
            begins[1]["payload"]["route_generation_id"], "44444444-4444-4444-8444-444444444444"
        )
        self.assertNotEqual(begins[0]["payload"]["stream_id"], begins[1]["payload"]["stream_id"])

    async def test_worker_source_reads_are_pinned_to_the_prepared_source(self) -> None:
        recovery_request = {
            "transfer_id": TRANSFER_ID,
            "route_generation_id": "recover-1",
            "chunks": [
                {
                    "source_id": "src_0",
                    "destination_id": "dst_0",
                    "chunk_index": 0,
                    "delivery_index": 0,
                    "source_offset": 0,
                    "chunk_size": 4096,
                    "logical_attempt_index": 1,
                    "attempt_slot": 0,
                    "part_number": 1,
                    "route_generation_id": "recover-1",
                    "multipart_group_id": f"{TRANSFER_ID}:dst_0:src_0:dest.bin",
                    "final_object_key": "dest.bin",
                    "upload_id": "upload-1",
                }
            ],
        }
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            control.prepare_plan_descriptor["sources"][0]["metadata"] = {
                "etag": '"source-etag"',
                "version_id": "source-version",
            }
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            await control.recovery_signers[TRANSFER_ID](recovery_request)

        self.assertEqual(len(provider.signed_routes), 2)
        for signed in provider.signed_routes:
            url, headers, _expires_at = signed["source_grant"]
            self.assertEqual(headers, {"Range": "bytes=0-4095", "If-Match": '"source-etag"'})
            self.assertIn("versionId=source-version", url)
            self.assertIn("X-Amz-SignedHeaders=host%3Bif-match%3Brange", url)

        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            await control.recovery_signers[TRANSFER_ID](recovery_request)

        self.assertEqual(len(provider.signed_routes), 2)
        for signed in provider.signed_routes:
            url, headers, _expires_at = signed["source_grant"]
            self.assertEqual(headers, {"Range": "bytes=0-4095"})
            self.assertNotIn("versionId=", url)
            self.assertIn("X-Amz-SignedHeaders=host%3Brange", url)

    async def test_recovery_signing_checks_the_attempt_slot_part_number(self) -> None:
        with ProviderHarness(self):
            sdk, control = self.sdk()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            signer = control.recovery_signers[TRANSFER_ID]

            def request(**overrides: Any) -> dict[str, Any]:
                chunk = {
                    "source_id": "src_0",
                    "destination_id": "dst_0",
                    "chunk_index": 0,
                    "delivery_index": 0,
                    "source_offset": 0,
                    "chunk_size": 4096,
                    "logical_attempt_index": 4,
                    "attempt_slot": 0,
                    "part_number": 1,
                    "route_generation_id": "recover-1",
                    "multipart_group_id": f"{TRANSFER_ID}:dst_0:src_0:dest.bin",
                    "final_object_key": "dest.bin",
                    "upload_id": "upload-1",
                    **overrides,
                }
                return {
                    "transfer_id": TRANSFER_ID,
                    "route_generation_id": "recover-1",
                    "chunks": [chunk],
                }

            # v7: one consecutive part per source chunk; the logical attempt index
            # never changes the part number and the only attempt slot is 0.
            reply = await signer(request())
            self.assertEqual(reply["chunk_routes"][0]["metadata"]["part_number"], 1)
            with self.assertRaisesRegex(ValueError, "slot mapping mismatch"):
                await signer(request(part_number=2))
            for slot in (1, 2):
                with self.assertRaisesRegex(ValueError, "attempt_slot must be 0"):
                    await signer(request(attempt_slot=slot, part_number=1))
            with self.assertRaisesRegex(ValueError, "multipart identity mismatch"):
                await signer(request(upload_id="upload-other"))

    async def test_recovery_signing_applies_staged_multipart_recovery(self) -> None:
        attempt_id = "22222222-2222-4222-8222-222222222222"
        group_id = f"{TRANSFER_ID}:dst_0:src_0:dest.bin"
        with ProviderHarness(self):
            sdk, control = self.sdk()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            reply = await control.recovery_signers[TRANSFER_ID](
                {
                    "transfer_id": TRANSFER_ID,
                    "route_generation_id": "recover-1",
                    "chunks": [
                        {
                            "source_id": "src_0",
                            "destination_id": "dst_0",
                            "chunk_index": 0,
                            "delivery_index": 0,
                            "source_offset": 0,
                            "chunk_size": 4096,
                            "logical_attempt_index": 2,
                            "attempt_slot": 0,
                            "part_number": 1,
                            "route_generation_id": "recover-1",
                            "multipart_group_id": group_id,
                            "final_object_key": "dest.bin",
                            "upload_id": "upload-1",
                            "recovery": {
                                "operation": "upload",
                                "mode": "staged",
                                "attempt_id": attempt_id,
                            },
                        }
                    ],
                }
            )
        route = reply["chunk_routes"][0]
        staging = route["metadata"]["recovery_staging"]
        encoded_group = group_id.replace(":", "%3A")
        self.assertEqual(
            staging["object_key"],
            f"dest.bin.beam-recovery/{TRANSFER_ID}/{encoded_group}/1/{attempt_id}",
        )
        self.assertEqual(route["metadata"]["upload_id"], "upload-1")
        self.assertIn("uploadId=upload-1", staging["copy_url"])
        self.assertIn(attempt_id, route["dest_url"])
        self.assertNotIn("uploadId", route["dest_url"])

    async def test_recovery_signing_skips_multipart_for_plain_put_destinations(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            control.prepare_plan_descriptor = fake_plan_descriptor()
            control.prepare_plan_descriptor["destinations"][0]["provider"] = "hippius"
            hippius = HippiusProviderDestination(bucket="b", key="dest.bin", api_token="t")
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[hippius], transfer_id=TRANSFER_ID
            )
            self.assertEqual(provider.created, [])
            reply = await control.recovery_signers[TRANSFER_ID](
                {
                    "transfer_id": TRANSFER_ID,
                    "route_generation_id": "recover-1",
                    "chunks": [
                        {
                            "source_id": "src_0",
                            "destination_id": "dst_0",
                            "chunk_index": 0,
                            "delivery_index": 0,
                            "source_offset": 0,
                            "chunk_size": 4096,
                            "logical_attempt_index": 1,
                            "attempt_slot": 0,
                            "part_number": 1,
                            "route_generation_id": "recover-1",
                            "multipart_group_id": "",
                            "final_object_key": "dest.bin",
                            "upload_id": "",
                        }
                    ],
                }
            )
        self.assertEqual(len(reply["chunk_routes"]), 1)
        self.assertIsNone(provider.signed_routes[-1]["upload"])

    async def test_recovery_signing_rebuilds_upload_controls_from_the_requested_upload(
        self,
    ) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            control.fail_once_at = "transfer.route_stream.begin"
            with self.assertRaises(BeamRouteRecoveryPendingError):
                await sdk.transfers.prepare_provider_transfer(
                    sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
                )
            self.assertEqual(control.continued_recovery, [TRANSFER_ID])
            reply = await control.recovery_signers[TRANSFER_ID](
                {
                    "transfer_id": TRANSFER_ID,
                    "route_generation_id": "recover-1",
                    "chunks": [
                        {
                            "source_id": "src_0",
                            "destination_id": "dst_0",
                            "chunk_index": 0,
                            "delivery_index": 0,
                            "source_offset": 0,
                            "chunk_size": 4096,
                            "logical_attempt_index": 0,
                            "attempt_slot": 0,
                            "part_number": 1,
                            "route_generation_id": "recover-1",
                            "multipart_group_id": f"{TRANSFER_ID}:dst_0:src_0:dest.bin",
                            "final_object_key": "dest.bin",
                            "upload_id": "upload-from-runtime",
                        }
                    ],
                }
            )
        self.assertEqual(provider.created, [])
        upload = provider.signed_routes[-1]["upload"]
        self.assertEqual(upload.upload_id, "upload-from-runtime")
        self.assertEqual(upload.manifest.max_part_number, 1)
        self.assertEqual(
            reply["chunk_routes"][0]["metadata"]["upload_id"], "upload-from-runtime"
        )

    async def test_recovery_signing_is_bounded_by_route_signing_concurrency(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk(route_signing_concurrency=2)
            control.prepare_plan_descriptor = multi_chunk_descriptor(8)
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            provider.max_active_signs = 0
            provider.sign_delay = 0.01
            chunks = [
                {
                    "source_id": "src_0",
                    "destination_id": "dst_0",
                    "chunk_index": index,
                    "delivery_index": index,
                    "source_offset": index * 1024,
                    "chunk_size": 1024,
                    "logical_attempt_index": 1,
                    "attempt_slot": 0,
                    "part_number": index + 1,
                    "route_generation_id": "recover-1",
                    "multipart_group_id": f"{TRANSFER_ID}:dst_0:src_0:dest.bin",
                    "final_object_key": "dest.bin",
                    "upload_id": "upload-1",
                }
                for index in range(8)
            ]
            reply = await control.recovery_signers[TRANSFER_ID](
                {"transfer_id": TRANSFER_ID, "route_generation_id": "recover-1", "chunks": chunks}
            )
        self.assertEqual(provider.max_active_signs, 2)
        self.assertEqual(
            [route["chunk_index"] for route in reply["chunk_routes"]], list(range(8))
        )

    async def test_cancel_releases_recovery_even_when_rejected(self) -> None:
        with ProviderHarness(self):
            sdk, control = self.sdk()
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            self.assertIn(TRANSFER_ID, sdk.transfers._integrity_context)

            async def rejected(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
                return {"success": False, "message": "already terminal"}

            with patch.object(control, "request", rejected):
                result = await sdk.transfers.cancel(TRANSFER_ID)
        self.assertFalse(result.success)
        self.assertEqual(control.recovery_leases, {})
        self.assertEqual(control.recovery_signers, {})
        self.assertNotIn(TRANSFER_ID, sdk.transfers._integrity_context)

    async def test_lifecycle_idempotency_keys_follow_the_transfer_id(self) -> None:
        sdk, control = self.sdk()
        created = await sdk.transfers.create(
            sources=[client_module.SourceConfig(type="http", url="https://source.example/a")],
            destinations=[client_module.DestConfig(type="http", url="https://dest.example/a")],
            total_size=10,
            idempotency_key="studio-step",
        )
        prepared = await sdk.transfers.prepare(
            sources=[PreparedHttpSource(source_id="src_0", url="https://s.example", size=4096)],
            destinations=[PreparedDestination(destination_id="dst_0", provider="http")],
            idempotency_key="studio-step",
        )
        self.assertEqual(created.transfer_id, prepared.transfer_id)
        create_call, prepare_call = control.calls
        self.assertEqual(create_call["idempotency_key"], f"transfer:{created.transfer_id}:create")
        self.assertEqual(
            prepare_call["idempotency_key"], f"transfer:{prepared.transfer_id}:prepare"
        )
        self.assertEqual(
            prepare_call["payload"]["route_generation_id"],
            client_module._route_generation_id_for_prepare_idempotency_key(
                f"transfer:{prepared.transfer_id}:prepare"
            ),
        )


class NatsControlParityTests(unittest.IsolatedAsyncioTestCase):
    def test_tls_urls_use_tls_handshake_first_with_the_gateway_hostname(self) -> None:
        options = build_connection_options(
            nats_url="tls://orch-gateway.b1m.ai:4222", api_key="b1m_key", key_prefix="b1m_key"
        )
        self.assertTrue(options["tls_handshake_first"])
        self.assertEqual(options["tls_hostname"], "orch-gateway.b1m.ai")
        self.assertTrue(options["tls"].check_hostname)
        self.assertEqual(options["servers"], ["tls://orch-gateway.b1m.ai:4222"])
        plaintext = build_connection_options(
            nats_url="nats://127.0.0.1:4222", api_key="b1m_key", key_prefix="b1m_key"
        )
        self.assertNotIn("tls", plaintext)
        self.assertNotIn("tls_handshake_first", plaintext)

    async def test_connection_passes_tls_handshake_first_to_nats(self) -> None:
        control = TransferClientControl(
            api_key="b1m_key", nats_url="tls://orch-gateway.b1m.ai:4222"
        )
        captured: dict[str, Any] = {}

        class Connection:
            is_closed = False

        async def connect(**kwargs: Any) -> Connection:
            captured.update(kwargs)
            return Connection()

        with patch("beam_network_sdk._nats_control.nats.connect", connect):
            await control._connection()
        self.assertTrue(captured["tls_handshake_first"])
        self.assertEqual(captured["tls_hostname"], "orch-gateway.b1m.ai")

    def test_route_splitting_targets_4_mib_and_honors_encoded_guards(self) -> None:
        def route(index: int, size: int) -> dict[str, Any]:
            return {
                "source_id": "src_0",
                "destination_id": "dst_0",
                "chunk_index": index,
                "delivery_index": index,
                "source_url": f"https://storage.example/{'x' * size}",
                "dest_url": "https://storage.example/destination",
                "source_offset": index * 1024,
                "chunk_size": 1024,
            }

        def control(max_payload_bytes: int | None = None) -> TransferClientControl:
            options: dict[str, Any] = {}
            if max_payload_bytes is not None:
                options["max_payload_bytes"] = max_payload_bytes
            return TransferClientControl(
                api_key="beam_test", nats_url="nats://127.0.0.1:4222", **options
            )

        def split(target: TransferClientControl, routes: list[dict[str, Any]]) -> list[int]:
            return [
                len(batch)
                for batch in target.split_routes_for_payload(
                    "transfer.route_stream.batch", {}, routes
                )
            ]

        defaults = control()
        routes = [route(0, 4_500_000), route(1, 4_500_000)]
        self.assertEqual(split(defaults, routes), [1, 1])
        self.assertEqual(split(control(10 * 1024 * 1024), routes), [1, 1])
        self.assertEqual(split(defaults, [route(3, 4_190_000), route(4, 4_190_000)]), [1, 1])
        with self.assertRaisesRegex(ValueError, "above max_payload_bytes=8388608"):
            split(defaults, [route(2, 8 * 1024 * 1024)])
        self.assertEqual(split(defaults, [route(5, 6 * 1024 * 1024)]), [1])
        with self.assertRaisesRegex(ValueError, "above max_payload_bytes=3145728"):
            split(control(3 * 1024 * 1024), [route(6, 3 * 1024 * 1024)])
        small = [route(index, 10) for index in range(50)]
        self.assertEqual(split(defaults, small), [50])

    async def test_auth_token_refreshes_30_seconds_before_expiry(self) -> None:
        control = TransferClientControl(api_key="b1m_fake", nats_url="nats://localhost:4222")
        control._auth_token = "near-expiry"
        control._auth_exp = int(time.time()) + 20
        resolves = 0

        class Connection:
            async def request(self, subject: str, data: bytes, *, timeout: float) -> Any:
                nonlocal resolves
                resolves += 1
                return type("Reply", (), {"data": b'{"ok": true, "token": "fresh"}'})()

        async def connection(_control: TransferClientControl) -> Connection:
            return Connection()

        with (
            patch.object(TransferClientControl, "_connection", connection),
            patch(
                "beam_network_sdk._nats_control.decode_jwt_payload",
                return_value={"exp": int(time.time()) + 3600},
            ),
        ):
            self.assertEqual(await control._auth_token_value(), "fresh")
            self.assertEqual(await control._auth_token_value(), "fresh")
        self.assertEqual(resolves, 1)

    async def test_resume_replays_when_recovery_requires_route_replay(self) -> None:
        control = TransferClientControl(api_key="b1m_fake", nats_url="nats://localhost:4222")
        replayed: list[str] = []

        async def request(_control: TransferClientControl, *_args: Any, **_kwargs: Any) -> dict:
            return {"recovery": "route_replay_required"}

        async def replay(generation_id: str) -> None:
            replayed.append(generation_id)

        with patch.object(TransferClientControl, "request", request):
            control._recovery_leases[TRANSFER_ID] = client_module_lease(replay)
            await control._recover_transfer(control._recovery_leases[TRANSFER_ID])
        self.assertEqual(len(replayed), 1)
        control._recovery_leases.clear()
        await control.close()

    def test_route_recovery_reply_envelope_round_trips(self) -> None:
        control = TransferClientControl(api_key="b1m_fake", nats_url="nats://localhost:4222")
        reply = control._route_recovery_sign_reply(
            {"message_id": "m", "request_id": "r"}, transfer_id="t", ok=True, status=200
        )
        self.assertEqual(msgpack.unpackb(msgpack.packb(reply), raw=False)["message_id"], "m")


def client_module_lease(replay: Any) -> Any:
    from beam_network_sdk._nats_control import _RecoveryLease

    return _RecoveryLease(
        transfer_id=TRANSFER_ID,
        shard_id=0,
        plan_fingerprint="a" * 64,
        coordinate_checksum="sha256-xor-v1:1:" + "0" * 64,
        replay_routes=replay,
    )


if __name__ == "__main__":
    unittest.main()
