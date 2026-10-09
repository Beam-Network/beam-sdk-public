"""Model parity and integrity-audit diagnostics, mirroring the TypeScript SDK."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any
from unittest.mock import patch

from pydantic import ValidationError
from test_functional_client import FakeTransferControl, fake_plan_descriptor
from test_provider_transfer_flow import TRANSFER_ID, ProviderHarness, r2_destination, r2_source

from beam_network_sdk import _client as client_module
from beam_network_sdk.models import (
    IntegrityAuditChallenge,
    MultipartGroupManifest,
    ProviderMultipartGroupIdentity,
    TransferCreateResponse,
    TransferPlanResponse,
    TransferPrepareResponse,
    TransferStatusInfo,
)


def status_payload(**overrides: Any) -> dict[str, Any]:
    return {
        "transfer_id": TRANSFER_ID,
        "status": "in_progress",
        "source_bytes_total": 4096,
        "delivery_bytes_total": 4096,
        "delivery_bytes_completed": 0,
        "delivery_tasks_total": 1,
        "delivery_tasks_completed": 0,
        "destinations_total": 1,
        "destination_progress": [],
        **overrides,
    }


def challenge(audit_id: str = "audit-1", chunks: list[dict[str, Any]] | None = None) -> dict:
    return {"transfer_id": TRANSFER_ID, "audit_id": audit_id, "chunks": chunks or []}


def manifest(**overrides: Any) -> dict[str, Any]:
    return {
        "multipart_group_id": "group-0",
        "source_id": "src_0",
        "destination_id": "dst_0",
        "final_object_key": "file.bin",
        "upload_id": "upload-0",
        "expected_object_size": 4096,
        "expected_part_count": 2,
        "max_part_number": 2,
        "complete_url": "https://dest.example/complete",
        "abort_url": "https://dest.example/abort",
        "list_page_urls": ["https://dest.example/list"],
        "final_head_url": "https://dest.example/head",
        "final_object_metadata": {"beam-transfer-id": TRANSFER_ID},
        "urls_expires_at": "2026-07-16T00:00:00.000Z",
        **overrides,
    }


class ModelParityTests(unittest.TestCase):
    def test_failed_prepare_and_plan_responses_parse_without_a_plan(self) -> None:
        prepared = TransferPrepareResponse.model_validate(
            {"success": False, "error": "quota_exceeded", "message": "quota exceeded"}
        )
        self.assertFalse(prepared.success)
        self.assertIsNone(prepared.plan_descriptor)
        planned = TransferPlanResponse.model_validate({"success": False, "error": "invalid"})
        self.assertIsNone(planned.plan_descriptor)
        with self.assertRaisesRegex(ValidationError, "requires plan_descriptor"):
            TransferPrepareResponse.model_validate({"success": True, "transfer_id": "t"})
        with self.assertRaisesRegex(ValidationError, "requires"):
            TransferPlanResponse.model_validate({"success": True})

    def test_successful_prepare_still_requires_its_plan_identity(self) -> None:
        payload = {
            "success": True,
            "transfer_id": "t",
            "plan_descriptor": fake_plan_descriptor(),
            "signed_url_flow": "signed_url",
            "plan_fingerprint": "a" * 64,
            "coordinate_checksum": "c",
        }
        with self.assertRaisesRegex(ValidationError, "route_generation_id"):
            TransferPrepareResponse.model_validate(payload)
        prepared = TransferPrepareResponse.model_validate({**payload, "route_generation_id": "g"})
        self.assertIsNotNone(prepared.plan_descriptor)

    def test_create_response_carries_the_transfer_key(self) -> None:
        created = TransferCreateResponse.model_validate(
            {"success": True, "transfer_id": "t", "transfer_key": "tk_1"}
        )
        self.assertEqual(created.transfer_key, "tk_1")

    def test_manifest_metadata_is_exactly_the_transfer_identity(self) -> None:
        MultipartGroupManifest.model_validate(manifest())
        with self.assertRaisesRegex(ValidationError, "exactly beam-transfer-id"):
            MultipartGroupManifest.model_validate(
                manifest(
                    final_object_metadata={
                        "beam-transfer-id": TRANSFER_ID,
                        "beam-multipart-group-id": "group-0",
                    }
                )
            )
        with self.assertRaises(ValidationError):
            MultipartGroupManifest.model_validate(
                manifest(expected_part_count=10_001, max_part_number=10_001)
            )

    def test_status_models_name_optional_groups_and_typed_challenge(self) -> None:
        status = TransferStatusInfo.model_validate(
            status_payload(
                name="nightly",
                integrity_audit_challenge={
                    **challenge(),
                    "requested_at": "2026-09-01T00:00:00.000Z",
                    "range_bytes": 65536,
                    "future_field": True,
                },
            )
        )
        self.assertEqual(status.name, "nightly")
        self.assertIsNone(status.destination_groups)
        self.assertIsNone(status.integrity_audit_submission_error)
        audit = status.integrity_audit_challenge
        assert audit is not None
        self.assertIsInstance(audit, IntegrityAuditChallenge)
        self.assertEqual(audit.range_bytes, 65536)
        # Mapping access keeps callers of the former dict field working.
        self.assertEqual(audit["audit_id"], "audit-1")
        self.assertEqual(audit.get("missing", "fallback"), "fallback")

    def test_multipart_group_identity_is_credential_free(self) -> None:
        identity = ProviderMultipartGroupIdentity(
            transfer_id="t",
            multipart_group_id="t:dst_0:src_0:out.bin",
            source_id="src_0",
            destination_id="dst_0",
            object_key="out.bin",
            upload_id="upload-1",
            expected_object_size=1024,
            expected_part_count=1,
            expires_at="2026-09-01T00:00:00.000Z",
        )
        self.assertEqual(
            set(identity.model_dump()),
            {
                "transfer_id",
                "multipart_group_id",
                "source_id",
                "destination_id",
                "object_key",
                "upload_id",
                "expected_object_size",
                "expected_part_count",
                "expires_at",
            },
        )


class IntegrityAuditTests(unittest.IsolatedAsyncioTestCase):
    def sdk(self) -> tuple[client_module.BeamSDK, FakeTransferControl]:
        sdk = client_module.BeamSDK(api_key="b1m_py", nats_url="nats://127.0.0.1:4222")
        control = FakeTransferControl()
        sdk._control = control  # type: ignore[assignment]
        sdk.transfers._control = control  # type: ignore[assignment]
        return sdk, control

    async def test_grant_failures_remain_visible_without_signed_urls(self) -> None:
        sdk, control = self.sdk()

        async def request(message_type: str, *_args: Any, **_kwargs: Any) -> dict:
            return status_payload(integrity_audit_challenge=challenge())

        with patch.object(control, "request", request):
            status = await sdk.transfers.status(TRANSFER_ID)
            self.assertEqual(
                status.integrity_audit_submission_error, "integrity audit signer unavailable"
            )
            sdk.transfers._integrity_context[TRANSFER_ID] = ()  # type: ignore[assignment]
            failures = [
                RuntimeError("failed to sign https://private.example/?X-Amz-Signature=secret"),
                ValueError("range " + "x" * 300),
            ]

            async def failing_submit(_challenge: Any) -> None:
                raise failures.pop(0)

            with patch.object(sdk.transfers, "_submit_integrity_audit_grants", failing_submit):
                first = await sdk.transfers.status(TRANSFER_ID)
                second = await sdk.transfers.status(TRANSFER_ID)
        self.assertEqual(first.integrity_audit_submission_error, "RuntimeError")
        self.assertEqual(second.integrity_audit_submission_error, ("range " + "x" * 300)[:200])

    async def test_concurrent_status_polls_share_one_submission_per_audit(self) -> None:
        sdk, control = self.sdk()
        sdk.transfers._integrity_context[TRANSFER_ID] = ()  # type: ignore[assignment]
        submissions = 0
        release = asyncio.Event()

        async def request(message_type: str, *_args: Any, **_kwargs: Any) -> dict:
            return status_payload(integrity_audit_challenge=challenge())

        async def submit(_challenge: Any) -> None:
            nonlocal submissions
            submissions += 1
            await release.wait()

        with (
            patch.object(control, "request", request),
            patch.object(sdk.transfers, "_submit_integrity_audit_grants", submit),
        ):
            polls = [asyncio.create_task(sdk.transfers.status(TRANSFER_ID)) for _ in range(3)]
            await asyncio.sleep(0.01)
            release.set()
            results = await asyncio.gather(*polls)
            await sdk.transfers.status(TRANSFER_ID)
        self.assertEqual(submissions, 1)
        self.assertTrue(all(r.integrity_audit_submission_error is None for r in results))

    async def test_grants_pin_the_source_etag_and_strip_orchestrator_identity(self) -> None:
        with ProviderHarness(self):
            sdk, control = self.sdk()
            control.prepare_plan_descriptor = fake_plan_descriptor()
            control.prepare_plan_descriptor["sources"][0]["metadata"] = {
                "etag": '"etag-1"',
                "version_id": "v1",
            }
            await sdk.transfers.prepare_provider_transfer(
                sources=[r2_source()], destinations=[r2_destination()], transfer_id=TRANSFER_ID
            )
            signed: list[tuple[str, dict[str, Any]]] = []

            def sign_source(**kwargs: Any) -> dict[str, Any]:
                signed.append(("source", kwargs))
                return {"url": "https://s", "headers": {}, "expires_at": "2099-01-01T00:00:00Z"}

            def sign_destination(**kwargs: Any) -> dict[str, Any]:
                signed.append(("destination", kwargs))
                return {"url": "https://d", "headers": {}, "expires_at": "2099-01-01T00:00:00Z"}

            chunk = {
                "challenge_id": "c1",
                "task_id": "task",
                "attempt_id": None,
                "orchestrator_id": "orch",
                "orchestrator_hotkey": "hotkey",
                "worker_id": "worker",
                "source_id": "src_0",
                "destination_id": "dst_0",
                "route_chunk_index": 0,
                "delivery_index": 0,
                "source_offset": 0,
                "destination_offset": 0,
                "range_length": 64,
                "final_object_key": "dest.bin",
                "final_object_etag": '"final"',
            }
            original_request = control.request

            async def request(message_type: str, payload: dict, *args: Any, **kwargs: Any) -> dict:
                if message_type == "transfer.status":
                    return status_payload(integrity_audit_challenge=challenge(chunks=[chunk]))
                return await original_request(message_type, payload, *args, **kwargs)

            with (
                patch.object(client_module, "sign_source_read_range", sign_source),
                patch.object(client_module, "sign_destination_read_range", sign_destination),
                patch.object(control, "request", request),
            ):
                status = await sdk.transfers.status(TRANSFER_ID)
        self.assertIsNone(status.integrity_audit_submission_error)
        source_call = dict(signed)["source"]
        self.assertEqual(source_call["if_match"], '"etag-1"')
        self.assertEqual(source_call["version_id"], "v1")
        self.assertEqual(dict(signed)["destination"]["if_match"], '"final"')
        grants = next(
            call for call in control.calls if call["message_type"] == "transfer.integrity_audit_grants"
        )
        self.assertEqual(
            grants["idempotency_key"], f"transfer:{TRANSFER_ID}:integrity-audit:audit-1:{grants['payload']['submitted_at']}"
        )
        grant_chunk = grants["payload"]["chunks"][0]
        for key in ("orchestrator_id", "orchestrator_hotkey", "worker_id"):
            self.assertNotIn(key, grant_chunk)
        self.assertIsNone(grant_chunk["attempt_id"])
        self.assertEqual(grant_chunk["source"]["url"], "https://s")


if __name__ == "__main__":
    unittest.main()
