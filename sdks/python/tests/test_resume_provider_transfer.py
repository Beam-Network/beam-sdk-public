"""resume_provider_transfer, mirroring the TypeScript resumeProviderTransfer test."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any
from unittest.mock import patch

from test_provider_transfer_flow import (
    TRANSFER_ID,
    OrderedFakeControl,
    ProviderHarness,
    r2_destination,
    r2_source,
)

from beam_network_sdk import _client as client_module
from beam_network_sdk.cancellation import BeamCancellationToken
from beam_network_sdk.exceptions import BeamCancelledError, BeamProviderTransferError
from beam_network_sdk.models import ProviderMultipartGroupIdentity

GROUP_ID = f"{TRANSFER_ID}:dst_0:src_0:dest.bin"


def identity(**overrides: Any) -> dict[str, Any]:
    return {
        "transfer_id": TRANSFER_ID,
        "multipart_group_id": GROUP_ID,
        "source_id": "src_0",
        "destination_id": "dst_0",
        "object_key": "dest.bin",
        "upload_id": "upload-existing",
        "expected_object_size": 4096,
        "expected_part_count": 1,
        "expires_at": "2026-09-25T00:00:00.000Z",
        **overrides,
    }


class ResumeProviderTransferTests(unittest.IsolatedAsyncioTestCase):
    def sdk(
        self, control: OrderedFakeControl | None = None, events: list[str] | None = None
    ) -> tuple[client_module.BeamSDK, OrderedFakeControl]:
        sdk = client_module.BeamSDK(
            api_key="b1m_py", nats_url="nats://127.0.0.1:4222", route_signing_concurrency=4
        )
        control = control or OrderedFakeControl(events)
        sdk._control = control  # type: ignore[assignment]
        sdk.transfers._control = control  # type: ignore[assignment]
        return sdk, control

    def calls(self, control: OrderedFakeControl, message_type: str) -> list[dict[str, Any]]:
        return [call for call in control.calls if call["message_type"] == message_type]

    async def test_resume_reuses_uploads_replays_routes_and_fences_replaced_owners(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            ownership = BeamCancellationToken()
            begins_at_prepared: list[int] = []

            def on_prepared(_prepared: Any) -> None:
                begins_at_prepared.append(len(self.calls(control, "transfer.route_stream.begin")))

            prepared = await sdk.transfers.resume_provider_transfer(
                transfer_id=TRANSFER_ID,
                multipart_groups=[ProviderMultipartGroupIdentity(**identity())],
                sources=[r2_source()],
                destinations=[r2_destination()],
                ownership=ownership,
                on_prepared=on_prepared,
            )
            self.assertEqual(prepared.transfer_id, TRANSFER_ID)
            self.assertEqual(begins_at_prepared, [0])
            self.assertEqual(provider.prepared_sources, 1)
            self.assertEqual(provider.created, [])
            self.assertIn(TRANSFER_ID, control.recovery_signers)
            self.assertIn(TRANSFER_ID, control.recovery_leases)
            manifest = self.calls(control, "transfer.route_stream.manifest")[0]
            self.assertEqual(manifest["payload"]["groups"][0]["upload_id"], "upload-existing")

            second, _ = self.sdk(control)
            await second.transfers.resume_provider_transfer(
                transfer_id=TRANSFER_ID,
                multipart_groups=[identity()],
                sources=[r2_source()],
                destinations=[r2_destination()],
                ownership=ownership,
            )
            prepares = self.calls(control, "transfer.prepare")
            self.assertEqual(len(prepares), 2)
            for call in prepares:
                self.assertRegex(
                    call["idempotency_key"], rf"^transfer:{TRANSFER_ID}:prepare:resume:[0-9a-f-]{{36}}$"
                )
                self.assertEqual(call["payload"]["transfer_id"], TRANSFER_ID)
            self.assertNotEqual(prepares[0]["idempotency_key"], prepares[1]["idempotency_key"])
            self.assertNotEqual(
                prepares[0]["payload"]["route_generation_id"],
                prepares[1]["payload"]["route_generation_id"],
            )

            signer = control.recovery_signers[TRANSFER_ID]
            reply = await signer(
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
                            "multipart_group_id": GROUP_ID,
                            "final_object_key": "dest.bin",
                            "upload_id": "upload-existing",
                        }
                    ],
                }
            )
            route = reply["chunk_routes"][0]
            self.assertEqual(route["metadata"]["upload_id"], "upload-existing")
            self.assertEqual(route["metadata"]["multipart_group_id"], GROUP_ID)
            self.assertEqual(route["metadata"]["part_number"], 1)
            self.assertEqual(len(self.calls(control, "transfer.route_stream.complete")), 2)

            lease = control.recovery_leases[TRANSFER_ID]
            await lease["replay_routes"]("33333333-3333-4333-8333-333333333333")
            self.assertEqual(provider.created, [], "route replay must reuse the existing upload")
            self.assertEqual(len(self.calls(control, "transfer.route_stream.complete")), 3)

            for invalid in (
                [],
                [identity(), identity()],
                [identity(object_key="different")],
                [identity(upload_id="")],
                [identity(expected_part_count=2)],
                [identity(transfer_id="22222222-2222-4222-8222-222222222222")],
            ):
                with (
                    self.subTest(invalid=invalid),
                    self.assertRaisesRegex(ValueError, "provider_multipart_recovery"),
                ):
                    await second.transfers.resume_provider_transfer(
                        transfer_id=TRANSFER_ID,
                        multipart_groups=invalid,
                        sources=[r2_source()],
                        destinations=[r2_destination()],
                    )
            self.assertEqual(provider.created, [], "invalid state must never create an upload")

            ownership.cancel(RuntimeError("owner replaced"))
            await asyncio.sleep(0)
            with self.assertRaisesRegex(RuntimeError, "owner replaced"):
                await signer({"route_generation_id": "later", "chunks": []})
            with self.assertRaisesRegex(RuntimeError, "owner replaced"):
                await lease["replay_routes"]("44444444-4444-4444-8444-444444444444")
        self.assertEqual(self.calls(control, "transfer.cancel"), [])
        self.assertEqual(provider.aborted, [])

    async def test_resume_rejects_a_different_transfer(self) -> None:
        with ProviderHarness(self):
            sdk, control = self.sdk()
            original = control.request

            async def request(message_type: str, payload: dict, *args: Any, **kwargs: Any) -> dict:
                result = await original(message_type, payload, *args, **kwargs)
                if message_type == "transfer.prepare":
                    result = {**result, "transfer_id": "22222222-2222-4222-8222-222222222222"}
                return result

            with (
                patch.object(control, "request", request),
                self.assertRaisesRegex(ValueError, "resumed provider transfer id mismatch"),
            ):
                await sdk.transfers.resume_provider_transfer(
                    transfer_id=TRANSFER_ID,
                    multipart_groups=[identity()],
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                )
        self.assertEqual(control.recovery_leases, {})

    async def test_resume_rejects_non_list_identities(self) -> None:
        sdk, _ = self.sdk()
        with self.assertRaisesRegex(ValueError, "provider_multipart_recovery_identity_required"):
            await sdk.transfers.resume_provider_transfer(
                transfer_id=TRANSFER_ID,
                multipart_groups=None,  # type: ignore[arg-type]
                sources=[r2_source()],
                destinations=[r2_destination()],
            )

    async def test_uploads_are_aborted_before_cancel_when_the_stream_never_began(self) -> None:
        with ProviderHarness(self) as provider:
            provider.abort_failures = 1
            sdk, control = self.sdk(events=provider.events)

            class BrokenStream:
                def __init__(self, **_kwargs: Any) -> None:
                    raise ValueError("route stream could not be built")

            with (
                patch.object(client_module, "_RouteStreamSender", BrokenStream),
                self.assertRaises(BeamProviderTransferError) as raised,
            ):
                await sdk.transfers.resume_provider_transfer(
                    transfer_id=TRANSFER_ID,
                    multipart_groups=[identity()],
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                )
        # Abort first (no route can write a part yet); the failed abort is retried after cancel.
        self.assertEqual(provider.events, ["abort", "cancel", "abort"])
        self.assertTrue(raised.exception.transfer_cancelled)
        self.assertTrue(raised.exception.multipart_cleanup_complete)
        self.assertEqual(provider.aborted[-1]["upload_id"], "upload-existing")
        self.assertEqual(control.recovery_leases, {})

    async def test_ownership_lost_before_resume_touches_nothing(self) -> None:
        with ProviderHarness(self) as provider:
            sdk, control = self.sdk()
            ownership = BeamCancellationToken()
            ownership.cancel()
            with self.assertRaises(BeamCancelledError):
                await sdk.transfers.resume_provider_transfer(
                    transfer_id=TRANSFER_ID,
                    multipart_groups=[identity()],
                    sources=[r2_source()],
                    destinations=[r2_destination()],
                    ownership=ownership,
                )
        self.assertEqual(control.calls, [])
        self.assertEqual(provider.aborted, [])


if __name__ == "__main__":
    unittest.main()
