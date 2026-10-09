"""BeamSDK options, public exports, errors, and S3 client reuse."""

from __future__ import annotations

import inspect
import os
import unittest
from typing import Any
from unittest.mock import patch

import beam_network_sdk
from beam_network_sdk import provider_signing
from beam_network_sdk.cli import cli
from beam_network_sdk.exceptions import BeamAPIError, BeamProviderTransferError
from beam_network_sdk.models import R2ProviderDestination, S3ProviderSource


class BeamSDKOptionTests(unittest.TestCase):
    def test_options_reach_the_lifecycle_transport(self) -> None:
        sdk = beam_network_sdk.BeamSDK(
            api_key="b1m_options",
            nats_url="nats://127.0.0.1:4222",
            max_payload_bytes=1_048_576,
            multipart_control_concurrency=4,
            transfer_client_subject_prefix=".custom.prefix.",
            transfer_runtime_shard_count=3,
        )
        self.assertEqual(sdk._control.max_payload_bytes, 1_048_576)
        self.assertEqual(sdk._control.subject_prefix, "custom.prefix")
        self.assertEqual(sdk._control.shard_count, 3)
        self.assertEqual(sdk.transfers._multipart_control_concurrency, 4)
        self.assertTrue(
            sdk._control._request_subject("transfer.status", 2).startswith("custom.prefix.prod.")
        )

    def test_defaults_match_the_typescript_sdk(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TRANSFER_RUNTIME_SHARD_COUNT", None)
            sdk = beam_network_sdk.BeamSDK(api_key="b1m_options", nats_url="nats://127.0.0.1:4222")
        self.assertEqual(beam_network_sdk.BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY, 2)
        self.assertEqual(sdk.transfers._multipart_control_concurrency, 2)
        self.assertEqual(sdk.transfers._route_signing_concurrency, 64)
        self.assertEqual(sdk._control.max_payload_bytes, 8 * 1024 * 1024)
        self.assertEqual(sdk._control.subject_prefix, "beam.transfer.client")
        self.assertEqual(sdk._control.shard_count, 1)

    def test_shard_count_falls_back_to_the_environment(self) -> None:
        with patch.dict(os.environ, {"TRANSFER_RUNTIME_SHARD_COUNT": "5"}):
            sdk = beam_network_sdk.BeamSDK(api_key="b1m_options", nats_url="nats://127.0.0.1:4222")
        self.assertEqual(sdk._control.shard_count, 5)

    def test_invalid_options_are_rejected(self) -> None:
        cases: list[tuple[dict[str, Any], str]] = [
            ({"route_signing_concurrency": 0}, "route_signing_concurrency"),
            ({"multipart_control_concurrency": 0}, "multipart_control_concurrency"),
            ({"multipart_control_concurrency": 1.5}, "multipart_control_concurrency"),
            ({"max_payload_bytes": -1}, "max_payload_bytes"),
            ({"transfer_runtime_shard_count": 0}, "transfer_runtime_shard_count"),
            ({"transfer_client_subject_prefix": "..."}, "transfer_client_subject_prefix"),
        ]
        for options, message in cases:
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, message):
                beam_network_sdk.BeamSDK(
                    api_key="b1m_options", nats_url="nats://127.0.0.1:4222", **options
                )


class ChunkSizeTests(unittest.TestCase):
    """Beam chooses the chunk size, so no client surface takes one."""

    def test_no_transfer_method_model_or_cli_command_takes_a_chunk_size(self) -> None:
        transfers = beam_network_sdk.BeamSDK(
            api_key="b1m_options", nats_url="nats://127.0.0.1:4222"
        ).transfers
        for name in (
            "create",
            "create_and_distribute",
            "plan",
            "prepare",
            "plan_provider_transfer",
            "prepare_provider_transfer",
            "resume_provider_transfer",
            "create_transfer",
        ):
            with self.subTest(method=name):
                parameters = inspect.signature(getattr(transfers, name)).parameters
                self.assertNotIn("chunk_size", parameters)
                self.assertNotIn("provider_part_size", parameters)
        self.assertNotIn("chunk_size", beam_network_sdk.TransferCreateRequest.model_fields)
        for name, command in cli.commands.items():
            with self.subTest(command=name):
                options = [option for param in command.params for option in param.opts]
                self.assertNotIn("--chunk-size", options)


class PublicExportTests(unittest.TestCase):
    def test_typescript_names_are_exported(self) -> None:
        for name in (
            "MULTIPART_MAX_PART_NUMBER",
            "MULTIPART_ATTEMPT_SLOT_COUNT",
            "MULTIPART_ATTEMPT_SLOTS",
            "MULTIPART_MAX_SOURCE_CHUNKS",
            "multipart_part_number",
            "BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY",
            "BEAM_DEFAULT_MAX_PAYLOAD_BYTES",
            "BeamProviderTransferError",
            "HuggingFaceProviderSource",
            "HuggingFaceProviderDestination",
            "ProviderMultipartGroupIdentity",
            "IntegrityAuditChallenge",
            "prepare_provider_source",
            "sign_destination_route",
            "sign_source_read_range",
            "create_multipart_upload",
            "abort_multipart_upload",
        ):
            with self.subTest(name=name):
                self.assertIn(name, beam_network_sdk.__all__)
                self.assertTrue(hasattr(beam_network_sdk, name))
        self.assertEqual(beam_network_sdk.MULTIPART_MAX_SOURCE_CHUNKS, 10_000)
        self.assertEqual(beam_network_sdk.MULTIPART_ATTEMPT_SLOT_COUNT, 1)
        self.assertEqual(beam_network_sdk.MULTIPART_ATTEMPT_SLOTS, 1)
        self.assertEqual(beam_network_sdk.multipart_part_number(1), 2)
        with self.assertRaisesRegex(ValueError, "attempt_slot must be 0"):
            beam_network_sdk.multipart_part_number(1, 2)


class ProviderTransferErrorTests(unittest.TestCase):
    def test_errors_retain_nested_causes_and_cleanup_outcomes(self) -> None:
        provider_error = BeamAPIError(503, "R2 request throttled")
        cleanup_error = RuntimeError("failed to abort 1 multipart upload(s)")
        error = BeamProviderTransferError(
            "transfer-1", provider_error, cleanup_error=cleanup_error
        )
        self.assertEqual(error.transfer_id, "transfer-1")
        self.assertTrue(error.transfer_cancelled)
        self.assertFalse(error.multipart_cleanup_complete)
        self.assertEqual(error.errors, [provider_error, cleanup_error])
        self.assertIs(error.cause, provider_error)
        self.assertIsInstance(error, beam_network_sdk.BeamTransferError)
        self.assertEqual(
            str(error),
            "provider transfer failed for transfer-1 "
            "(transfer_cancelled=true, multipart_cleanup_complete=false)",
        )


class S3ClientCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_signing._s3_client_cache.clear()
        self.addCleanup(provider_signing._s3_client_cache.clear)

    def test_default_clients_are_reused_per_configuration_with_five_attempts(self) -> None:
        created: list[dict[str, Any]] = []

        def fake_boto3_client(service: str, **kwargs: Any) -> object:
            created.append(kwargs)
            return object()

        source = S3ProviderSource(bucket="b", key="k", access_key_id="ak", secret_access_key="sk")
        other = S3ProviderSource(bucket="b2", key="k2", access_key_id="ak", secret_access_key="sk")
        rotated = S3ProviderSource(bucket="b", key="k", access_key_id="ak", secret_access_key="sk2")
        r2 = R2ProviderDestination(
            bucket="b", key="k", account_id="acct", access_key_id="ak", secret_access_key="sk"
        )
        with patch("boto3.client", fake_boto3_client):
            first = provider_signing._s3_client(source, None)
            self.assertIs(provider_signing._s3_client(other, None), first)
            self.assertIsNot(provider_signing._s3_client(rotated, None), first)
            self.assertIsNot(provider_signing._s3_client(r2, None), first)
        self.assertEqual(len(created), 3)
        self.assertEqual(created[0]["config"].retries, {"total_max_attempts": 5, "mode": "standard"})
        self.assertEqual(created[2]["endpoint_url"], "https://acct.r2.cloudflarestorage.com")

    def test_explicit_client_factories_are_not_cached(self) -> None:
        calls = 0

        def factory(*_args: Any, **_kwargs: Any) -> object:
            nonlocal calls
            calls += 1
            return object()

        source = S3ProviderSource(bucket="b", key="k", access_key_id="ak", secret_access_key="sk")
        provider_signing._s3_client(source, factory)
        provider_signing._s3_client(source, factory)
        self.assertEqual(calls, 2)
        self.assertEqual(provider_signing._s3_client_cache, {})


if __name__ == "__main__":
    unittest.main()
