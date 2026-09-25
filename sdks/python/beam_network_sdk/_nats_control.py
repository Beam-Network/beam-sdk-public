"""NATS request/reply transport for BEAM SDK lifecycle control."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import msgpack
import nats

from beam_network_sdk.exceptions import BeamAPIError
from beam_network_sdk.models import TransferTerminalEvent

SCHEMA_VERSION = "transfer-client-control/v6"
BEAM_DEV_NATS_URL = "nats://127.0.0.1:4222"
BEAM_PROD_NATS_URL = "tls://orch-gateway.b1m.ai:4222"
DEFAULT_SUBJECT_PREFIX = "beam.transfer.client"
# NATS enforces max_payload per message. This SDK guard splits signed-route
# control messages before the broker rejects them; transfer bytes never use NATS.
DEFAULT_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024


def _key_prefix(api_key: str) -> str:
    return api_key[:12]


class TransferTerminalSignalWaiter:
    """Persistent owned terminal subscription for one transfer."""

    def __init__(
        self,
        subscription: Any,
        transfer_id: str,
        on_close: Any,
    ) -> None:
        self._subscription = subscription
        self._transfer_id = transfer_id
        self._on_close = on_close
        self._closed = False
        self._wait_lock = asyncio.Lock()

    async def wait(self, timeout: float) -> TransferTerminalEvent | None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        async with self._wait_lock:
            if self._closed:
                return None
            try:
                message = await self._subscription.next_msg(timeout=timeout)
            except nats.errors.TimeoutError:
                return None
            except nats.errors.BadSubscriptionError:
                if self._closed:
                    return None
                raise
            event = TransferTerminalEvent.model_validate(msgpack.unpackb(message.data, raw=False))
            if event.transfer_id != self._transfer_id:
                raise ValueError("terminal event transfer identity does not match subscription")
            return event

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._on_close(self)
        await self._subscription.unsubscribe()


@dataclass(slots=True)
class _RecoveryLease:
    transfer_id: str
    shard_id: int
    plan_fingerprint: str
    coordinate_checksum: str
    replay_routes: Callable[[str], Awaitable[None]]
    dispose: Callable[[], None] | None = None


@dataclass(slots=True)
class TransferClientControl:
    api_key: str
    nats_url: str
    environment: str = "prod"
    subject_prefix: str = DEFAULT_SUBJECT_PREFIX
    shard_count: int = 1
    timeout: float = 30.0
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
    key_prefix: str = field(init=False)
    _nc: Any | None = field(init=False, default=None)
    _auth_token: str | None = field(init=False, default=None)
    _auth_exp: int = field(init=False, default=0)
    _connection_lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _auth_lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _closed: bool = field(init=False, default=False)
    _terminal_waiters: set[TransferTerminalSignalWaiter] = field(init=False, default_factory=set)
    _runtime_epochs: dict[int, tuple[str, str]] = field(init=False, default_factory=dict)
    _hello_tasks: dict[int, asyncio.Task[None]] = field(init=False, default_factory=dict)
    _recovery_leases: dict[str, _RecoveryLease] = field(init=False, default_factory=dict)
    _recovery_tasks: dict[str, asyncio.Task[None]] = field(init=False, default_factory=dict)
    _recovery_requested_epochs: dict[str, tuple[str, str]] = field(init=False, default_factory=dict)
    _route_recovery_signers: dict[str, Any] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.shard_count < 1:
            raise ValueError("shard_count must be positive")
        if self.nats_url.startswith(("http://", "https://", "ws://", "wss://")):
            raise ValueError("nats_url must use nats:// or tls:// for Python lifecycle transport")
        self.key_prefix = _key_prefix(self.api_key)
        self.subject_prefix = self.subject_prefix.strip(".")

    async def close(self) -> None:
        async with self._connection_lock:
            self._closed = True
            nc = self._nc
            self._nc = None
        waiters = list(self._terminal_waiters)
        hello_tasks = list(self._hello_tasks.values())
        recovery_tasks = list(self._recovery_tasks.values())
        recovery_signers = list(self._route_recovery_signers.values())
        self._hello_tasks.clear()
        self._recovery_tasks.clear()
        self._route_recovery_signers.clear()
        for task in (*hello_tasks, *recovery_tasks):
            task.cancel()
        for lease in self._recovery_leases.values():
            if lease.dispose is not None:
                lease.dispose()
        self._recovery_leases.clear()
        self._recovery_requested_epochs.clear()
        if waiters:
            await asyncio.gather(*(waiter.close() for waiter in waiters), return_exceptions=True)
        if recovery_signers:
            await asyncio.gather(
                *(subscription.unsubscribe() for subscription in recovery_signers),
                return_exceptions=True,
            )
        if hello_tasks or recovery_tasks:
            await asyncio.gather(*hello_tasks, *recovery_tasks, return_exceptions=True)
        if nc is not None and not nc.is_closed:
            await nc.drain()

    async def serve_route_recovery_signer(
        self,
        transfer_id: str,
        handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        previous = self._route_recovery_signers.pop(transfer_id, None)
        if previous is not None:
            await previous.unsubscribe()
        nc = await self._connection()
        subject = self._route_recovery_sign_subject(transfer_id)

        async def respond(message: Any) -> None:
            request: dict[str, Any] | None = None
            try:
                decoded = msgpack.unpackb(message.data, raw=False)
                if not isinstance(decoded, dict):
                    raise ValueError("route recovery request envelope must be an object")
                request = decoded
                payload = request.get("payload")
                if (
                    request.get("schema_version") != SCHEMA_VERSION
                    or request.get("environment") != self.environment
                    or request.get("key_prefix") != self.key_prefix
                    or request.get("transfer_id") != transfer_id
                    or request.get("message_type") != "transfer.route_recovery.sign"
                    or request.get("producer") != "transfer-runtime"
                    or not isinstance(payload, dict)
                    or payload.get("transfer_id") != transfer_id
                    or not isinstance(payload.get("route_generation_id"), str)
                    or not payload["route_generation_id"]
                    or not isinstance(payload.get("chunks"), list)
                    or not payload["chunks"]
                ):
                    raise ValueError("route recovery request envelope mismatch")
                signed = await handler(payload)
                response = self._route_recovery_sign_reply(
                    request,
                    transfer_id=transfer_id,
                    ok=True,
                    status=200,
                    payload=signed,
                )
            except Exception as exc:
                response = self._route_recovery_sign_reply(
                    request or {},
                    transfer_id=transfer_id,
                    ok=False,
                    status=500,
                    error={
                        "code": "route_recovery_sign_failed",
                        "message": str(exc),
                    },
                )
            await message.respond(msgpack.packb(response, use_bin_type=True))

        subscription = await nc.subscribe(subject, cb=respond)
        await nc.flush()
        if self._closed:
            await subscription.unsubscribe()
            raise RuntimeError("NATS lifecycle control is closed")
        self._route_recovery_signers[transfer_id] = subscription

    async def open_terminal_signal_waiter(
        self,
        transfer_id: str,
    ) -> TransferTerminalSignalWaiter:
        nc = await self._connection()
        subscription = await nc.subscribe(self._terminal_subject(transfer_id))
        await nc.flush()
        if self._closed:
            await subscription.unsubscribe()
            raise RuntimeError("NATS lifecycle control is closed")
        waiter = TransferTerminalSignalWaiter(
            subscription,
            transfer_id,
            self._terminal_waiters.discard,
        )
        self._terminal_waiters.add(waiter)
        if self._closed:
            await waiter.close()
            raise RuntimeError("NATS lifecycle control is closed")
        return waiter

    async def request(
        self,
        message_type: str,
        payload: dict[str, Any],
        transfer_id: str | None = None,
        *,
        idempotency_key: str | None = None,
        shard_id_override: int | None = None,
    ) -> dict[str, Any]:
        shard_id = (
            shard_id_override
            if shard_id_override is not None
            else transfer_shard_id(transfer_id, self.shard_count)
            if transfer_id
            else 0
        )
        request_id = _lifecycle_request_id(message_type, idempotency_key)
        envelope = {
            "message_id": f"{SCHEMA_VERSION}:{self.environment}:{self.key_prefix}:{message_type}:{request_id}",
            "schema_version": SCHEMA_VERSION,
            "environment": self.environment,
            "key_prefix": self.key_prefix,
            "shard_id": shard_id,
            "message_type": message_type,
            "request_id": request_id,
            "occurred_at": iso_now(),
            "producer": "sdk",
            "payload": payload,
        }
        subject = self._request_subject(message_type, shard_id)
        last_error: Exception | None = None
        transient_retries = 0
        expired_token_retried = False
        while True:
            token = await self._auth_token_value()
            envelope["auth_token"] = token
            data = msgpack.packb(envelope, use_bin_type=True)
            if len(data) > self.max_payload_bytes:
                raise ValueError(
                    f"NATS lifecycle request is {len(data)} bytes, above max_payload_bytes={self.max_payload_bytes}"
                )
            try:
                nc = await self._connection()
                reply = await nc.request(subject, data, timeout=self.timeout)
                decoded = msgpack.unpackb(reply.data, raw=False)
                self._observe_runtime_epochs(shard_id, decoded)
                if decoded.get("ok"):
                    payload_out = decoded.get("payload")
                    return payload_out if isinstance(payload_out, dict) else {}
                error = decoded.get("error") or {}
                detail = error.get("message") or error.get("code") or "unknown error"
                status = int(decoded.get("status") or 500)
                if (
                    status == 401
                    and error.get("code") == "auth_token_expired"
                    and not expired_token_retried
                ):
                    expired_token_retried = True
                    async with self._auth_lock:
                        if self._auth_token == token:
                            self._auth_token = None
                            self._auth_exp = 0
                    continue
                api_error = BeamAPIError(status, str(detail), url=subject, body=error)
                if status not in {408, 425, 429} and status < 500:
                    raise api_error
                last_error = api_error
            except Exception as exc:
                if isinstance(exc, BeamAPIError):
                    if exc.status_code not in {408, 425, 429} and exc.status_code < 500:
                        raise
                elif not _is_retryable_lifecycle_error(exc):
                    raise
                last_error = exc
                if self._nc is not None and self._nc.is_closed:
                    self._nc = None
            if transient_retries == 2:
                break
            delay = (0.15, 0.5)[transient_retries] * random.uniform(0.8, 1.2)
            transient_retries += 1
            await asyncio.sleep(delay)
        assert last_error is not None
        raise last_error

    def register_recovery_lease(
        self,
        *,
        transfer_id: str,
        plan_fingerprint: str,
        coordinate_checksum: str,
        replay_routes: Callable[[str], Awaitable[None]],
        dispose: Callable[[], None] | None = None,
    ) -> None:
        shard_id = transfer_shard_id(transfer_id, self.shard_count)
        replaced = self._recovery_leases.get(transfer_id)
        if replaced is not None and replaced.dispose is not None:
            replaced.dispose()
        self._recovery_leases[transfer_id] = _RecoveryLease(
            transfer_id=transfer_id,
            shard_id=shard_id,
            plan_fingerprint=plan_fingerprint,
            coordinate_checksum=coordinate_checksum,
            replay_routes=replay_routes,
            dispose=dispose,
        )
        if shard_id not in self._hello_tasks or self._hello_tasks[shard_id].done():
            self._hello_tasks[shard_id] = asyncio.create_task(self._monitor_runtime(shard_id))

    def release_recovery_lease(self, transfer_id: str) -> None:
        lease = self._recovery_leases.pop(transfer_id, None)
        self._recovery_requested_epochs.pop(transfer_id, None)
        signer = self._route_recovery_signers.pop(transfer_id, None)
        if signer is not None:
            asyncio.create_task(signer.unsubscribe())
        if lease is not None and lease.dispose is not None:
            lease.dispose()
        if lease is not None and not any(
            candidate.shard_id == lease.shard_id for candidate in self._recovery_leases.values()
        ):
            monitor = self._hello_tasks.pop(lease.shard_id, None)
            if monitor is not None:
                monitor.cancel()

    def continue_recovery_lease(self, transfer_id: str) -> None:
        """Continue a retained transfer after its foreground operation stops."""
        lease = self._recovery_leases.get(transfer_id)
        if lease is None:
            return
        requested_epoch = self._runtime_epochs.get(lease.shard_id)
        if requested_epoch is None:
            requested_epoch = ("foreground", str(uuid.uuid4()))
        self._recovery_requested_epochs[transfer_id] = requested_epoch
        self._schedule_recovery(lease)

    def _observe_runtime_epochs(self, shard_id: int, envelope: dict[str, Any]) -> None:
        runtime_epoch = envelope.get("runtime_epoch")
        transport_epoch = envelope.get("transport_epoch")
        if not isinstance(runtime_epoch, str) or not isinstance(transport_epoch, str):
            return
        previous = self._runtime_epochs.get(shard_id)
        current = (runtime_epoch, transport_epoch)
        self._runtime_epochs[shard_id] = current
        if previous is None or previous == current:
            return
        if previous[0] != runtime_epoch:
            self._auth_token = None
            self._auth_exp = 0
        for lease in tuple(self._recovery_leases.values()):
            if lease.shard_id == shard_id:
                self._recovery_requested_epochs[lease.transfer_id] = current
                self._schedule_recovery(lease)

    def _schedule_recovery(self, lease: _RecoveryLease) -> None:
        current = self._recovery_tasks.get(lease.transfer_id)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(self._recover_transfer(lease))
        self._recovery_tasks[lease.transfer_id] = task

        def finished(done: asyncio.Task[None]) -> None:
            if self._recovery_tasks.get(lease.transfer_id) is done:
                self._recovery_tasks.pop(lease.transfer_id, None)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)

    async def _recover_transfer(self, lease: _RecoveryLease) -> None:
        attempt = 0
        while not self._closed and self._recovery_leases.get(lease.transfer_id) is lease:
            requested_epoch = self._recovery_requested_epochs.get(lease.transfer_id)
            route_generation_id = str(uuid.uuid4())
            try:
                result = await self.request(
                    "transfer.resume",
                    {
                        "transfer_id": lease.transfer_id,
                        "plan_fingerprint": lease.plan_fingerprint,
                        "coordinate_checksum": lease.coordinate_checksum,
                        "route_generation_id": route_generation_id,
                    },
                    transfer_id=lease.transfer_id,
                    idempotency_key=f"transfer:{lease.transfer_id}:resume:{route_generation_id}",
                )
                if result.get("recovery") == "terminal":
                    self.release_recovery_lease(lease.transfer_id)
                    return
                if result.get("route_replay_required") is True:
                    await lease.replay_routes(route_generation_id)
                if self._recovery_requested_epochs.get(lease.transfer_id) != requested_epoch:
                    attempt = 0
                    continue
                current_task = asyncio.current_task()
                if self._recovery_tasks.get(lease.transfer_id) is current_task:
                    self._recovery_tasks.pop(lease.transfer_id, None)
                return
            except asyncio.CancelledError:
                raise
            except BeamAPIError as exc:
                if exc.status_code not in {408, 425, 429} and exc.status_code < 500:
                    self.release_recovery_lease(lease.transfer_id)
                    return
                attempt += 1
                await asyncio.sleep(
                    min(30.0, 0.5 * (2 ** min(attempt, 6))) * random.uniform(0.8, 1.2)
                )
            except Exception as exc:
                if not is_retryable_lifecycle_error(exc):
                    self.release_recovery_lease(lease.transfer_id)
                    return
                attempt += 1
                await asyncio.sleep(
                    min(30.0, 0.5 * (2 ** min(attempt, 6))) * random.uniform(0.8, 1.2)
                )

    async def _monitor_runtime(self, shard_id: int) -> None:
        try:
            while not self._closed and any(
                lease.shard_id == shard_id for lease in self._recovery_leases.values()
            ):
                try:
                    await self.request(
                        "runtime.hello",
                        {},
                        idempotency_key=f"runtime-hello:{shard_id}",
                        shard_id_override=shard_id,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            return
        finally:
            current = asyncio.current_task()
            if self._hello_tasks.get(shard_id) is current:
                self._hello_tasks.pop(shard_id, None)

    def split_routes_for_payload(
        self, message_type: str, base_payload: dict[str, Any], routes: list[Any]
    ) -> list[list[Any]]:
        def encoded_size(candidate: list[Any]) -> int:
            envelope = {
                "schema_version": SCHEMA_VERSION,
                "environment": self.environment,
                "key_prefix": self.key_prefix,
                "shard_id": 0,
                "message_type": message_type,
                "request_id": "00000000-0000-4000-8000-000000000000",
                "auth_token": "x" * 512,
                "occurred_at": iso_now(),
                "producer": "sdk",
                "payload": {**base_payload, "route_batch": compact_signed_routes(candidate)},
            }
            return len(msgpack.packb(envelope, use_bin_type=True))

        chunks: list[list[Any]] = []
        offset = 0
        while offset < len(routes):
            low, high, accepted = 1, len(routes) - offset, 0
            while low <= high:
                count = (low + high) // 2
                if encoded_size(routes[offset : offset + count]) <= self.max_payload_bytes:
                    accepted = count
                    low = count + 1
                else:
                    high = count - 1
            if accepted == 0:
                size = encoded_size(routes[offset : offset + 1])
                raise ValueError(
                    f"single signed route is {size} bytes, above max_payload_bytes={self.max_payload_bytes}"
                )
            chunks.append(routes[offset : offset + accepted])
            offset += accepted
        return chunks

    async def _connection(self) -> Any:
        if self._closed:
            raise RuntimeError("NATS lifecycle control is closed")
        if self._nc and not self._nc.is_closed:
            return self._nc
        async with self._connection_lock:
            if self._closed:
                raise RuntimeError("NATS lifecycle control is closed")
            if self._nc and not self._nc.is_closed:
                return self._nc
            self._nc = await nats.connect(
                servers=[self.nats_url],
                user=self.key_prefix,
                password=self.api_key,
                name=f"beam-python-sdk-{self.key_prefix}",
                allow_reconnect=True,
                max_reconnect_attempts=-1,
                reconnect_time_wait=1,
            )
            return self._nc

    async def _auth_token_value(self) -> str:
        now = int(time.time())
        if self._auth_token and self._auth_exp - 5 > now:
            return self._auth_token
        async with self._auth_lock:
            now = int(time.time())
            if self._auth_token and self._auth_exp - 5 > now:
                return self._auth_token
            reply: Any | None = None
            last_error: Exception | None = None
            for attempt in range(3):
                try:
                    nc = await self._connection()
                    reply = await nc.request(self._auth_subject(), b"{}", timeout=self.timeout)
                    break
                except Exception as exc:
                    if not _is_retryable_lifecycle_error(exc):
                        raise
                    last_error = exc
                    if attempt == 2:
                        raise
                    delay = (0.15, 0.5)[attempt] * random.uniform(0.8, 1.2)
                    await asyncio.sleep(delay)
            if reply is None:
                assert last_error is not None
                raise last_error
            parsed = json.loads(reply.data.decode("utf-8"))
            if not parsed.get("ok") or not parsed.get("token"):
                raise BeamAPIError(
                    401,
                    f"NATS auth resolve failed: {parsed.get('error', 'unknown_error')}",
                    url=self._auth_subject(),
                    body=parsed,
                )
            token = str(parsed["token"])
            claims = decode_jwt_payload(token)
            self._auth_token = token
            self._auth_exp = int(claims.get("exp") or 0)
            return token

    def _auth_subject(self) -> str:
        return f"{self.subject_prefix}.{self.environment}.auth.{self.key_prefix}.resolve"

    def _request_subject(self, message_type: str, shard_id: int) -> str:
        return f"{self.subject_prefix}.{self.environment}.sdk.{self.key_prefix}.shard.{shard_id}.{message_type.replace('.', '_')}"

    def _terminal_subject(self, transfer_id: str) -> str:
        return f"{self.subject_prefix}.{self.environment}.events.{self.key_prefix}.{transfer_id}.terminal"

    def _route_recovery_sign_subject(self, transfer_id: str) -> str:
        return (
            f"{self.subject_prefix}.{self.environment}.sdk.{self.key_prefix}."
            f"transfer.{transfer_id}.route_recovery_sign"
        )

    def _route_recovery_sign_reply(
        self,
        request: dict[str, Any],
        *,
        transfer_id: str,
        ok: bool,
        status: int,
        payload: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "message_id": str(request.get("message_id") or "unknown"),
            "schema_version": SCHEMA_VERSION,
            "environment": self.environment,
            "key_prefix": self.key_prefix,
            "transfer_id": transfer_id,
            "message_type": "transfer.route_recovery.sign",
            "request_id": str(request.get("request_id") or "unknown"),
            "occurred_at": iso_now(),
            "producer": "sdk",
            "ok": ok,
            "status": status,
            **({"payload": payload} if payload is not None else {}),
            **({"error": error} if error is not None else {}),
        }


def transfer_shard_id(transfer_id: str, shard_count: int) -> int:
    h = 2166136261
    for ch in transfer_id:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return h % shard_count


def decode_jwt_payload(jwt: str) -> dict[str, Any]:
    parts = jwt.split(".")
    if len(parts) < 2:
        raise ValueError("invalid JWT")
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(padded.encode()).decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("invalid JWT payload")
    return {str(key): value for key, value in decoded.items()}


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"


def _lifecycle_request_id(message_type: str, idempotency_key: str | None) -> str:
    if not idempotency_key or not idempotency_key.strip():
        return str(uuid.uuid4())
    digest = bytearray(
        hashlib.sha256(f"beam:{message_type}:{idempotency_key.strip()}".encode()).digest()[:16]
    )
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


_ROUTE_ATTEMPT_METADATA_KEYS = {
    "part_number",
    "logical_attempt_index",
    "attempt_slot",
    "route_generation_id",
}


def compact_signed_routes(routes: list[Any]) -> dict[str, Any]:
    source_chunks: list[dict[str, Any]] = []
    source_refs: dict[str, int] = {}
    compact_routes: list[dict[str, Any]] = []
    for route_index, raw_route in enumerate(routes):
        route = (
            raw_route.model_dump(exclude_none=True)
            if hasattr(raw_route, "model_dump")
            else dict(raw_route)
        )
        source_key = json.dumps(
            [
                route["source_id"],
                route["chunk_index"],
                route["source_url"],
                route["source_offset"],
                route["chunk_size"],
                route.get("expires_at"),
                route.get("headers"),
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
        source_ref = source_refs.get(source_key)
        if source_ref is None:
            source_ref = len(source_chunks)
            source_refs[source_key] = source_ref
            source_chunks.append(
                _compact_dict(
                    {
                        "source_ref": source_ref,
                        "source_id": route["source_id"],
                        "chunk_index": route["chunk_index"],
                        "source_url": route["source_url"],
                        "source_offset": route["source_offset"],
                        "chunk_size": route["chunk_size"],
                        "expires_at": route.get("expires_at"),
                        "headers": route.get("headers"),
                    }
                )
            )
        metadata = dict(route.get("metadata") or {})
        delivery_index = route.get("delivery_index", metadata.get("delivery_index", route_index))
        if not isinstance(delivery_index, int) or delivery_index < 0:
            raise ValueError("signed route delivery_index must be non-negative")
        for key in (
            "source_id",
            "destination_id",
            "chunk_index",
            "route_chunk_index",
            "delivery_index",
        ):
            metadata.pop(key, None)
        group_id = str(metadata.get("multipart_group_id") or "") or None
        if group_id:
            metadata = {
                key: value for key, value in metadata.items() if key in _ROUTE_ATTEMPT_METADATA_KEYS
            }
        compact_routes.append(
            _compact_dict(
                {
                    "source_ref": source_ref,
                    "destination_id": route["destination_id"],
                    "delivery_index": delivery_index,
                    "dest_url": route["dest_url"],
                    "expires_at": route.get("expires_at"),
                    "dest_headers": route.get("dest_headers"),
                    "multipart_group_id": group_id,
                    "metadata": metadata or None,
                }
            )
        )
    return {"source_chunks": source_chunks, "routes": compact_routes}


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def is_retryable_lifecycle_error(error: Exception) -> bool:
    if isinstance(error, BeamAPIError):
        return error.status_code in {408, 425, 429} or error.status_code >= 500
    combined = f"{type(error).__module__}.{type(error).__name__} {error}".lower()
    return any(
        token in combined
        for token in (
            "timeout",
            "noresponders",
            "no responders",
            "connectionclosed",
            "connection closed",
            "disconnected",
            "econnreset",
            "econnrefused",
            "etimedout",
            "connecterror",
            "fetch failed",
            "socket",
            "network",
            "no servers",
        )
    )


def is_recoverable_route_stream_error(error: Exception) -> bool:
    """Keep an owned lease when a Runtime restart invalidates foreground state."""
    return is_retryable_lifecycle_error(error) or (
        isinstance(error, BeamAPIError) and error.status_code in {404, 409}
    )


_is_retryable_lifecycle_error = is_retryable_lifecycle_error
