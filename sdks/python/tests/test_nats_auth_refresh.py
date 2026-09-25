from __future__ import annotations

import json
import time
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import msgpack

from beam_network_sdk._nats_control import TransferClientControl
from beam_network_sdk.exceptions import BeamAPIError


class FakeNatsConnection:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.envelopes: list[dict[str, Any]] = []
        self.auth_resolves = 0

    async def request(self, subject: str, data: bytes, *, timeout: float) -> SimpleNamespace:
        if subject.endswith(".resolve"):
            self.auth_resolves += 1
            return SimpleNamespace(data=json.dumps({"ok": True, "token": "fresh-token"}).encode())
        self.envelopes.append(msgpack.unpackb(data, raw=False))
        return SimpleNamespace(data=msgpack.packb(self.responses.pop(0), use_bin_type=True))


class ExpiredNatsTokenTests(unittest.IsolatedAsyncioTestCase):
    async def _request_with_responses(
        self, responses: list[dict[str, Any]]
    ) -> tuple[FakeNatsConnection, dict[str, Any] | BeamAPIError]:
        control = TransferClientControl(api_key="b1m_fake", nats_url="nats://localhost:4222")
        control._auth_token = "stale-token"
        control._auth_exp = int(time.time()) + 3600
        fake = FakeNatsConnection(responses)

        async def connection(_control: TransferClientControl) -> FakeNatsConnection:
            return fake

        with (
            patch.object(TransferClientControl, "_connection", connection),
            patch(
                "beam_network_sdk._nats_control.decode_jwt_payload",
                return_value={"exp": int(time.time()) + 3600},
            ),
        ):
            try:
                result: dict[str, Any] | BeamAPIError = await control.request(
                    "transfer.create",
                    {"transfer_id": "transfer-1"},
                    transfer_id="transfer-1",
                    idempotency_key="stable-key",
                )
            except BeamAPIError as error:
                result = error
        return fake, result

    async def test_expired_token_refreshes_and_retries_same_request(self) -> None:
        fake, result = await self._request_with_responses(
            [
                {"ok": False, "status": 401, "error": {"code": "auth_token_expired"}},
                {"ok": True, "payload": {"success": True}},
            ]
        )
        self.assertEqual(result, {"success": True})
        self.assertEqual(fake.auth_resolves, 1)
        self.assertEqual([envelope["auth_token"] for envelope in fake.envelopes], ["stale-token", "fresh-token"])
        self.assertEqual(fake.envelopes[0]["request_id"], fake.envelopes[1]["request_id"])
        self.assertEqual(fake.envelopes[0]["message_id"], fake.envelopes[1]["message_id"])

    async def test_other_authorization_failure_is_terminal(self) -> None:
        fake, result = await self._request_with_responses(
            [{"ok": False, "status": 401, "error": {"code": "invalid_auth_token"}}]
        )
        if not isinstance(result, BeamAPIError):
            self.fail("expected authorization failure")
        self.assertEqual(result.status_code, 401)
        self.assertEqual(fake.auth_resolves, 0)
        self.assertEqual(len(fake.envelopes), 1)

    async def test_expired_token_retries_only_once(self) -> None:
        fake, result = await self._request_with_responses(
            [
                {"ok": False, "status": 401, "error": {"code": "auth_token_expired"}},
                {"ok": False, "status": 401, "error": {"code": "auth_token_expired"}},
            ]
        )
        if not isinstance(result, BeamAPIError):
            self.fail("expected authorization failure")
        self.assertEqual(result.status_code, 401)
        self.assertEqual(fake.auth_resolves, 1)
        self.assertEqual(len(fake.envelopes), 2)


if __name__ == "__main__":
    unittest.main()
