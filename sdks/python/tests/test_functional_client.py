from __future__ import annotations

import asyncio
import inspect
import unittest
from unittest.mock import patch

from beam_network_sdk import BeamRouteRecoveryPendingError, BeamSDK
from beam_network_sdk import _client as client_module
from beam_network_sdk._multipart_limits import multipart_part_number
from beam_network_sdk._nats_control import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    TransferClientControl,
    _RecoveryLease,
    is_recoverable_route_stream_error,
)
from beam_network_sdk.exceptions import BeamAPIError
from beam_network_sdk.models import (
    DestConfig,
    PreparedDestination,
    PreparedHttpSource,
    R2ProviderDestination,
    R2ProviderSource,
    SignedChunkRoute,
    SourceConfig,
)


class SDKDefaultsTests(unittest.TestCase):
    def test_route_message_default_and_recovery_error_are_public(self) -> None:
        self.assertEqual(DEFAULT_MAX_PAYLOAD_BYTES, 8 * 1024 * 1024)
        cause = BeamAPIError(409, "route_generation_mismatch")
        error = BeamRouteRecoveryPendingError("transfer-1", cause)
        self.assertEqual(error.transfer_id, "transfer-1")
        self.assertIs(error.cause, cause)

    def test_multipart_upload_route_requires_group_identity(self) -> None:
        route = SignedChunkRoute(
            source_id="src",
            destination_id="dst",
            chunk_index=0,
            source_url="https://source.example/file.bin",
            dest_url="https://dest.example/part-1",
            source_offset=0,
            chunk_size=512,
            metadata={
                "upload_id": "upload",
                "final_object_key": "file.bin",
                "part_number": 1,
            },
        )
        with self.assertRaisesRegex(ValueError, "missing multipart_group_id"):
            client_module._validate_signed_route_manifest_contract("transfer", [route], [])


def fake_plan_descriptor(*, chunk_size: int = 4096, final_object_key: str = "dest.bin") -> dict:
    return {
        "version": "compact-transfer-plan/v1",
        "plan_nonce": "testplan",
        "chunk_size": chunk_size,
        "sources": [
            {
                "source_id": "src_0",
                "type": "http",
                "url": "https://source.example/file.bin",
                "size": chunk_size,
                "metadata": {},
                "global_chunk_start": 0,
                "chunk_count": 1,
            }
        ],
        "destinations": [
            {
                "destination_id": "dst_0",
                "provider": "r2",
                "mode": "object_chunks",
                "metadata": {},
                "destination_index": 0,
                "final_object_keys": {"src_0": final_object_key},
            }
        ],
        "logical_chunk_count": 1,
        "delivery_route_count": 1,
        "multipart_attempt_slots": 3,
        "formulas": {
            "source_offset": "source_chunk_index * chunk_size",
            "delivery_index": "chunk_index * destination_count + destination_index",
            "part_number": "source_chunk_index * 3 + attempt_slot + 1",
            "route_generation_id": "initial-{chunk_index}-{destination_id}",
        },
    }


async def _manual_recovery_result(routes: list, manifests: list, expires_at: str | None) -> tuple:
    return routes, manifests, expires_at


class FakeTransferControl:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.prepare_plan_descriptor = fake_plan_descriptor()
        self.recovery_leases: dict[str, dict] = {}
        self.recovery_signers: dict[str, object] = {}
        self.recovery_present_at_route_begin = False
        self.continued_recovery: list[str] = []
        self.fail_once_at: str | None = None

    async def request(
        self,
        message_type: str,
        payload: dict,
        transfer_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        self.calls.append(
            {
                "message_type": message_type,
                "payload": payload,
                "transfer_id": transfer_id,
                "idempotency_key": idempotency_key,
            }
        )
        if message_type == "transfer.route_stream.begin":
            self.recovery_present_at_route_begin = payload["transfer_id"] in self.recovery_leases
        if self.fail_once_at == message_type:
            self.fail_once_at = None
            raise ConnectionError("connection closed during route streaming")
        if message_type == "transfer.create":
            return {
                "success": True,
                "transfer_id": payload["transfer_id"],
                "total_chunks": 2,
                "total_sources": len(payload["sources"]),
                "total_destinations": len(payload["destinations"]),
            }
        if message_type == "transfer.plan":
            return {
                "success": True,
                "chunk_size": 4096,
                "total_size": 4096,
                "total_sources": 1,
                "total_destinations": 1,
                "logical_chunks": 1,
                "total_chunks": 1,
                "signed_url_flow": "signed_url",
                "plan_fingerprint": "a" * 64,
                "coordinate_checksum": "sha256-xor-v1:1:" + "0" * 64,
                "plan_descriptor": self.prepare_plan_descriptor,
            }
        if message_type == "transfer.prepare":
            return {
                "success": True,
                "transfer_id": payload["transfer_id"],
                "transfer_key": "tk_py",
                "chunk_size": 4096,
                "total_size": 4096,
                "total_sources": 1,
                "total_destinations": 1,
                "logical_chunks": 1,
                "total_chunks": 1,
                "signed_url_flow": "signed_url",
                "plan_fingerprint": "a" * 64,
                "coordinate_checksum": "sha256-xor-v1:1:" + "0" * 64,
                "route_generation_id": payload["route_generation_id"],
                "plan_descriptor": self.prepare_plan_descriptor,
            }
        if message_type == "transfer.route_stream.complete":
            return {"success": True, "transfer_id": payload["transfer_id"], "total_routes": 2}
        if message_type == "transfer.distribute":
            return {
                "success": True,
                "transfer_id": payload["transfer_id"],
                "orchestrators_assigned": 1,
            }
        if message_type == "transfer.status":
            return {
                "transfer_id": payload["transfer_id"],
                "status": "completed",
                "error_message": None,
                "runtime": True,
                "source_bytes_total": 4096,
                "delivery_bytes_total": 4096,
                "delivery_bytes_completed": 4096,
                "delivery_tasks_total": 1,
                "delivery_tasks_completed": 1,
                "destinations_total": 1,
                "destinations_completed": 1,
                "destination_progress": [
                    {
                        "destination_id": "dst_0",
                        "delivery_bytes_total": 4096,
                        "delivery_bytes_completed": 4096,
                        "delivery_tasks_total": 1,
                        "delivery_tasks_completed": 1,
                        "completed": True,
                    }
                ],
                "destination_groups": {
                    "totalGroups": 0,
                    "completedGroups": 0,
                    "pendingGroups": 0,
                    "totalDestinations": 0,
                    "completedDestinations": 0,
                },
                "started_at": "2026-06-22T22:40:20.000Z",
                "completed_at": "2026-06-22T22:41:20.000Z",
            }
        if message_type == "transfer.cancel":
            return {"success": True, "message": "cancelled"}
        return {"success": True}

    def split_routes_for_payload(
        self, _message_type: str, _base_payload: dict, routes: list
    ) -> list[list]:
        return [routes]

    def register_recovery_lease(self, **kwargs: object) -> None:
        self.recovery_leases[str(kwargs["transfer_id"])] = kwargs

    async def serve_route_recovery_signer(self, transfer_id: str, handler: object) -> None:
        self.recovery_signers[transfer_id] = handler

    def release_recovery_lease(self, transfer_id: str) -> None:
        self.recovery_leases.pop(transfer_id, None)
        self.recovery_signers.pop(transfer_id, None)

    def continue_recovery_lease(self, transfer_id: str) -> None:
        self.continued_recovery.append(transfer_id)

    async def close(self) -> None:
        return None


class BeamSDKNatsLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_epoch_recovery_coalesces_and_clears_secrets(self) -> None:
        control = TransferClientControl(
            api_key="b1m_recovery",
            nats_url="nats://127.0.0.1:4222",
            environment="dev",
        )
        resume_calls = 0

        async def request(
            _control: TransferClientControl, message_type: str, *_args: object, **_kwargs: object
        ) -> dict:
            nonlocal resume_calls
            self.assertEqual(message_type, "transfer.resume")
            resume_calls += 1
            return {"recovery": "route_replay_required", "route_replay_required": True}

        request_patch = patch.object(TransferClientControl, "request", request)
        request_patch.start()
        self.addCleanup(request_patch.stop)
        first_replay_gate = asyncio.Event()
        replay_count = 0
        active_replays = 0
        max_active_replays = 0
        replay_generations: list[str] = []
        disposed = 0

        async def replay_routes(generation_id: str) -> None:
            nonlocal replay_count, active_replays, max_active_replays
            replay_generations.append(generation_id)
            replay_count += 1
            active_replays += 1
            max_active_replays = max(max_active_replays, active_replays)
            if replay_count == 1:
                await first_replay_gate.wait()
            active_replays -= 1

        def dispose() -> None:
            nonlocal disposed
            disposed += 1

        transfer_id = "11111111-1111-4111-8111-111111111111"
        control._recovery_leases[transfer_id] = _RecoveryLease(
            transfer_id=transfer_id,
            shard_id=0,
            plan_fingerprint="a" * 64,
            coordinate_checksum="sha256-xor-v1:1:" + "0" * 64,
            replay_routes=replay_routes,
            dispose=dispose,
        )
        control._observe_runtime_epochs(
            0, {"runtime_epoch": "runtime-a", "transport_epoch": "transport-a"}
        )
        control._observe_runtime_epochs(
            0, {"runtime_epoch": "runtime-b", "transport_epoch": "transport-b"}
        )
        while replay_count < 1:
            await asyncio.sleep(0)
        control._observe_runtime_epochs(
            0, {"runtime_epoch": "runtime-c", "transport_epoch": "transport-c"}
        )
        first_replay_gate.set()
        for _ in range(100):
            if replay_count == 2:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(replay_count, 2)
        self.assertEqual(resume_calls, 2)
        self.assertEqual(max_active_replays, 1)
        self.assertEqual(len(set(replay_generations)), 2)
        self.assertIn(
            "max_reconnect_attempts=-1", inspect.getsource(TransferClientControl._connection)
        )
        control.release_recovery_lease(transfer_id)
        self.assertEqual(disposed, 1)
        await control.close()

    def test_multipart_parts_use_source_local_chunk_numbering(self) -> None:
        self.assertEqual(multipart_part_number(0), 1)
        self.assertEqual(multipart_part_number(0, 1), 2)
        self.assertEqual(multipart_part_number(0, 2), 3)
        self.assertEqual(multipart_part_number(1), 4)
        self.assertEqual(multipart_part_number(3_332, 2), 9_999)
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            multipart_part_number(-1)
        with self.assertRaisesRegex(ValueError, "less than 3333"):
            multipart_part_number(3_333)

    def test_runtime_state_loss_keeps_route_recovery_lease(self) -> None:
        self.assertTrue(is_recoverable_route_stream_error(BeamAPIError(404, "transfer_not_active")))
        self.assertTrue(
            is_recoverable_route_stream_error(BeamAPIError(409, "route_generation_mismatch"))
        )
        self.assertFalse(is_recoverable_route_stream_error(BeamAPIError(400, "invalid_route")))

    def sdk_with_fake_control(self) -> tuple[BeamSDK, FakeTransferControl]:
        sdk = BeamSDK(api_key="b1m_py", nats_url="nats://127.0.0.1:4222")
        fake = FakeTransferControl()
        sdk._control = fake
        sdk.transfers._control = fake
        return sdk, fake

    async def test_create_distribute_status_and_cancel_use_nats_control(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        created = await sdk.transfers.create(
            sources=[SourceConfig(type="http", url="https://source.example/file.bin")],
            destinations=[DestConfig(type="http", url="https://dest.example/file.bin")],
            total_size=10_485_760,
            test_mode=True,
        )
        self.assertTrue(created.success)
        self.assertRegex(created.transfer_id, r"^[0-9a-f-]{36}$")

        distributed = await sdk.transfers.distribute(created.transfer_id)
        self.assertEqual(distributed.orchestrators_assigned, 1)

        status = await sdk.transfers.wait_complete(
            created.transfer_id, timeout=0.1, poll_interval=0.01
        )
        self.assertEqual(status.status, "completed")
        self.assertEqual(status.destination_groups["completedGroups"], 0)

        cancelled = await sdk.transfers.cancel(created.transfer_id)
        self.assertTrue(cancelled.success)

        self.assertEqual(
            [call["message_type"] for call in control.calls],
            ["transfer.create", "transfer.distribute", "transfer.status", "transfer.cancel"],
        )
        self.assertNotIn("chunk_size", control.calls[0]["payload"])
        self.assertTrue(control.calls[0]["payload"]["test_mode"])

    async def test_create_passes_explicit_chunk_size_hint(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        created = await sdk.transfers.create(
            sources=[SourceConfig(type="http", url="https://source.example/file.bin")],
            destinations=[DestConfig(type="http", url="https://dest.example/file.bin")],
            total_size=10_485_760,
            chunk_size=8192,
        )
        self.assertTrue(created.success)
        self.assertEqual(control.calls[0]["payload"]["chunk_size"], 8192)

    async def test_idempotent_prepare_reuses_route_generation(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        request = {
            "sources": [
                PreparedHttpSource(
                    source_id="src_0",
                    url="https://source.example/file.bin",
                    size=4096,
                )
            ],
            "destinations": [
                PreparedDestination(
                    destination_id="dst_0",
                    provider="http",
                    mode="http_chunks",
                    logical_prefix="out/file.bin",
                )
            ],
            "idempotency_key": "studio-step-retry",
        }

        first = await sdk.transfers.prepare(**request)
        second = await sdk.transfers.prepare(**request)
        prepares = [call for call in control.calls if call["message_type"] == "transfer.prepare"]

        self.assertEqual(first.transfer_id, second.transfer_id)
        self.assertEqual(len(prepares), 2)
        self.assertEqual(
            prepares[0]["payload"]["transfer_id"], prepares[1]["payload"]["transfer_id"]
        )
        self.assertEqual(
            prepares[0]["payload"]["route_generation_id"],
            prepares[1]["payload"]["route_generation_id"],
        )
        self.assertEqual(prepares[0]["idempotency_key"], prepares[1]["idempotency_key"])

    async def test_plan_prepare_and_attach_signed_urls_use_chunked_nats_control(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        source = PreparedHttpSource(
            source_id="src_0", url="https://source.example/file.bin", size=4096
        )
        destination = PreparedDestination(
            destination_id="dst_0",
            provider="http",
            mode="http_chunks",
            logical_prefix="imports/file.bin",
        )

        planned = await sdk.transfers.plan(sources=[source], destinations=[destination])
        self.assertTrue(planned.success)

        prepared = await sdk.transfers.prepare(sources=[source], destinations=[destination])
        self.assertEqual(prepared.transfer_key, "tk_py")

        attached = await sdk.transfers.attach_signed_urls(
            prepared.transfer_id,
            chunk_routes=[
                SignedChunkRoute(
                    source_id="src_0",
                    destination_id="dst_0",
                    chunk_index=0,
                    source_url="https://source.example/file.bin",
                    dest_url="https://dest.example/file.bin.part0",
                    source_offset=0,
                    chunk_size=2048,
                    metadata={
                        "multipart_group_id": "group-0",
                        "upload_id": "upload-0",
                        "final_object_key": "file.bin",
                        "bucket": "dest-bucket",
                        "part_number": 1,
                        "delivery_index": 0,
                    },
                ),
                SignedChunkRoute(
                    source_id="src_0",
                    destination_id="dst_0",
                    chunk_index=1,
                    source_url="https://source.example/file.bin",
                    dest_url="https://dest.example/file.bin.part1",
                    source_offset=2048,
                    chunk_size=2048,
                    metadata={
                        "multipart_group_id": "group-0",
                        "upload_id": "upload-0",
                        "final_object_key": "file.bin",
                        "bucket": "dest-bucket",
                        "part_number": 4,
                        "delivery_index": 1,
                    },
                ),
            ],
            multipart_group_manifest=[
                {
                    "multipart_group_id": "group-0",
                    "source_id": "src_0",
                    "destination_id": "dst_0",
                    "final_object_key": "file.bin",
                    "upload_id": "upload-0",
                    "expected_object_size": 4096,
                    "expected_part_count": 2,
                    "max_part_number": 6,
                    "complete_url": "https://dest.example/complete",
                    "abort_url": "https://dest.example/abort",
                    "list_page_urls": ["https://dest.example/list"],
                    "final_head_url": "https://dest.example/final-head",
                    "final_object_metadata": {
                        "beam-transfer-id": prepared.transfer_id,
                    },
                    "urls_expires_at": "2026-07-16T00:00:00.000Z",
                }
            ],
            transfer_key=prepared.transfer_key,
            route_generation_id=prepared.route_generation_id,
            plan_fingerprint=prepared.plan_fingerprint,
            coordinate_checksum=prepared.coordinate_checksum,
            recovery_factory=lambda _generation_id: _manual_recovery_result(
                [], [], "2026-07-16T00:00:00.000Z"
            ),
        )
        self.assertTrue(attached.success)
        self.assertTrue(control.recovery_present_at_route_begin)

        self.assertEqual(
            [call["message_type"] for call in control.calls],
            [
                "transfer.plan",
                "transfer.prepare",
                "transfer.route_stream.begin",
                "transfer.route_stream.manifest",
                "transfer.route_stream.batch",
                "transfer.route_stream.complete",
            ],
        )
        self.assertEqual(control.calls[2]["payload"]["total_routes"], 2)
        self.assertEqual(control.calls[2]["payload"]["route_contract_version"], "signed_url")
        self.assertNotIn("multipart_group_manifest", control.calls[2]["payload"])
        self.assertEqual(control.calls[3]["payload"]["groups"][0]["max_part_number"], 6)
        self.assertEqual(control.calls[4]["payload"]["batch_index"], 0)
        self.assertEqual(control.calls[4]["payload"]["route_count"], 2)
        route_batch = control.calls[4]["payload"]["route_batch"]
        self.assertNotIn("multipart_groups", route_batch)
        self.assertEqual(
            [route.get("multipart_group_id") for route in route_batch["routes"]],
            ["group-0", "group-0"],
        )
        self.assertEqual(
            [route.get("metadata") for route in route_batch["routes"]],
            [
                {"part_number": 1},
                {"part_number": 4},
            ],
        )
        self.assertRegex(control.calls[5]["payload"]["route_keys_checksum"], r"^sha256-xor-v1:2:")

    async def test_route_stream_uses_maximally_filled_2048_route_logical_batches(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        routes = [
            SignedChunkRoute(
                source_id="src_0",
                destination_id="dst_0",
                chunk_index=index,
                source_url=f"https://source.example/file.bin?chunk={index}",
                dest_url=f"https://dest.example/file.bin.part{index}",
                source_offset=index * 512,
                chunk_size=512,
            )
            for index in range(5_120)
        ]

        await sdk.transfers.attach_signed_urls(
            "11111111-1111-4111-8111-111111111111",
            chunk_routes=routes,
            multipart_group_manifest=[],
            transfer_key="tk_py",
            route_generation_id="11111111-1111-4111-8111-111111111112",
            plan_fingerprint="a" * 64,
            coordinate_checksum="sha256-xor-v1:5120:" + "0" * 64,
            recovery_factory=lambda _generation_id: _manual_recovery_result(routes, [], None),
        )

        batches = [
            call for call in control.calls if call["message_type"] == "transfer.route_stream.batch"
        ]
        self.assertEqual([call["payload"]["batch_index"] for call in batches], [0, 1, 2])
        self.assertEqual(
            [call["payload"]["route_count"] for call in batches], [2_048, 2_048, 1_024]
        )
        self.assertEqual(control.calls[-1]["payload"]["expected_batches"], 3)

    async def test_route_stream_interruptions_retain_recovery_before_begin_and_after_batch(
        self,
    ) -> None:
        transfer_id = "11111111-1111-4111-8111-111111111111"
        routes = [
            SignedChunkRoute(
                source_id="src_0",
                destination_id="dst_0",
                chunk_index=0,
                delivery_index=0,
                source_url="https://source.example/file.bin",
                dest_url="https://dest.example/file.bin.part0",
                source_offset=0,
                chunk_size=512,
            )
        ]
        for boundary in ("transfer.route_stream.begin", "transfer.route_stream.batch"):
            with self.subTest(boundary=boundary):
                sdk, control = self.sdk_with_fake_control()
                control.fail_once_at = boundary
                with self.assertRaises(BeamRouteRecoveryPendingError) as raised:
                    await sdk.transfers.attach_signed_urls(
                        transfer_id,
                        chunk_routes=routes,
                        multipart_group_manifest=[],
                        route_generation_id="22222222-2222-4222-8222-222222222222",
                        plan_fingerprint="a" * 64,
                        coordinate_checksum="sha256-xor-v1:1:" + "0" * 64,
                        recovery_factory=lambda _generation_id: _manual_recovery_result(
                            routes, [], None
                        ),
                    )
                self.assertEqual(raised.exception.transfer_id, transfer_id)
                self.assertIsInstance(raised.exception.cause, ConnectionError)
                self.assertIn("connection closed", str(raised.exception.cause))
                self.assertTrue(control.recovery_present_at_route_begin)
                self.assertEqual(control.continued_recovery, [transfer_id])
                self.assertIn(transfer_id, control.recovery_leases)

    async def test_prepare_provider_transfer_accepts_caller_transfer_id(self) -> None:
        sdk, control = self.sdk_with_fake_control()
        transfer_id = "11111111-1111-4111-8111-111111111111"
        source = R2ProviderSource(
            bucket="source-bucket",
            key="source.bin",
            endpoint_url="https://r2.example",
            access_key_id="access",
            secret_access_key="secret",
        )
        destination = R2ProviderDestination(
            bucket="dest-bucket",
            key="dest.bin",
            endpoint_url="https://r2.example",
            access_key_id="access",
            secret_access_key="secret",
        )
        original_prepare_source = client_module.prepare_provider_source
        original_prepare_destination = client_module.prepare_provider_destination
        original_create_multipart = client_module.create_multipart_upload
        original_sign_complete = client_module.sign_complete_multipart_upload
        original_sign_abort = client_module.sign_abort_multipart_upload
        original_sign_list = client_module.sign_list_multipart_upload
        original_sign_final_head = client_module.sign_final_object_head
        original_sign_provider_route = client_module.TransferManager._sign_provider_route

        async def fake_sign_provider_route(**kwargs):
            chunk = kwargs["chunk"]
            target = kwargs["target"]
            return SignedChunkRoute(
                source_id=chunk.source_id,
                destination_id=target.destination_id,
                chunk_index=chunk.chunk_index,
                delivery_index=target.metadata.get("delivery_index", 0),
                source_url=chunk.source_url,
                dest_url="https://dest.example/part-1",
                source_offset=chunk.source_offset,
                chunk_size=chunk.chunk_size,
                metadata=dict(target.metadata),
            )

        client_module.prepare_provider_source = lambda *_args, **_kwargs: PreparedHttpSource(
            source_id="src_0",
            url="https://source.example/file.bin",
            size=4096,
        )
        client_module.prepare_provider_destination = lambda *_args, **_kwargs: PreparedDestination(
            destination_id="dst_0",
            provider="http",
            mode="http_chunks",
            logical_prefix="imports/file.bin",
        )
        client_module.create_multipart_upload = lambda **_kwargs: "upload-test"
        client_module.sign_complete_multipart_upload = lambda **_kwargs: (
            "https://dest.example/complete"
        )
        client_module.sign_abort_multipart_upload = lambda **_kwargs: "https://dest.example/abort"
        client_module.sign_list_multipart_upload = lambda **_kwargs: "https://dest.example/list"
        client_module.sign_final_object_head = lambda **_kwargs: "https://dest.example/final-head"
        client_module.TransferManager._sign_provider_route = staticmethod(fake_sign_provider_route)
        try:
            prepared = await sdk.transfers.prepare_provider_transfer(
                sources=[source],
                destinations=[destination],
                transfer_id=transfer_id,
            )
            recovery_signer = control.recovery_signers[transfer_id]
            self.assertTrue(callable(recovery_signer))
            recovery_generation = "33333333-3333-4333-8333-333333333333"
            recovery = await recovery_signer(
                {
                    "transfer_id": transfer_id,
                    "route_generation_id": recovery_generation,
                    "chunks": [
                        {
                            "source_id": "src_0",
                            "destination_id": "dst_0",
                            "chunk_index": 0,
                            "delivery_index": 0,
                            "source_offset": 0,
                            "chunk_size": 4096,
                            "logical_attempt_index": 2,
                            "attempt_slot": 2,
                            "part_number": 3,
                            "route_generation_id": recovery_generation,
                            "multipart_group_id": f"{transfer_id}:dst_0:src_0:dest.bin",
                            "final_object_key": "dest.bin",
                            "upload_id": "upload-test",
                        }
                    ],
                }
            )
        finally:
            client_module.prepare_provider_source = original_prepare_source
            client_module.prepare_provider_destination = original_prepare_destination
            client_module.create_multipart_upload = original_create_multipart
            client_module.sign_complete_multipart_upload = original_sign_complete
            client_module.sign_abort_multipart_upload = original_sign_abort
            client_module.sign_list_multipart_upload = original_sign_list
            client_module.sign_final_object_head = original_sign_final_head
            client_module.TransferManager._sign_provider_route = original_sign_provider_route

        self.assertEqual(prepared.transfer_id, transfer_id)
        prepare_call = next(
            call for call in control.calls if call["message_type"] == "transfer.prepare"
        )
        self.assertEqual(prepare_call["payload"]["transfer_id"], transfer_id)
        self.assertEqual(prepare_call["transfer_id"], transfer_id)
        stream_begin_call = next(
            call for call in control.calls if call["message_type"] == "transfer.route_stream.begin"
        )
        self.assertEqual(stream_begin_call["payload"]["transfer_id"], transfer_id)
        self.assertTrue(control.recovery_present_at_route_begin)
        self.assertEqual(recovery["route_generation_id"], recovery_generation)
        self.assertEqual(recovery["chunk_routes"][0]["metadata"]["part_number"], 3)
        self.assertEqual(recovery["chunk_routes"][0]["metadata"]["attempt_slot"], 2)

    async def test_rejects_http_lifecycle_endpoint(self) -> None:
        with self.assertRaisesRegex(ValueError, "nats_url must use nats:// or tls://"):
            BeamSDK(api_key="b1m_py", nats_url="http://beamcore.test")


if __name__ == "__main__":
    unittest.main()
