"""Async BEAM transfer client."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from contextvars import copy_context
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, NoReturn, TypeVar
from weakref import WeakSet

from pydantic import BaseModel, SecretStr

from beam_network_sdk import huggingface as hf
from beam_network_sdk._diagnostics import opaque_correlation_token, safe_error_diagnostic
from beam_network_sdk._multipart_limits import multipart_part_number
from beam_network_sdk._nats_control import (
    BEAM_DEV_NATS_URL,
    BEAM_PROD_NATS_URL,
    TransferClientControl,
    TransferTerminalSignalWaiter,
    compact_signed_routes,
    is_recoverable_route_stream_error,
    iso_now,
)
from beam_network_sdk._performance import PerformanceCollector, SourceSignatureHistory
from beam_network_sdk._performance import current as current_performance
from beam_network_sdk._validation import validate_id as _validate_id
from beam_network_sdk.cancellation import BeamCancellationToken, raise_if_cancelled
from beam_network_sdk.exceptions import (
    BeamAPIError,
    BeamAuthError,
    BeamProviderTransferError,
    BeamRouteRecoveryPendingError,
    BeamTimeoutError,
    transfer_failed_error,
)
from beam_network_sdk.models import (
    AttachSignedUrlsResponse,
    CallbackConfig,
    ChunkDestinationSigningTarget,
    ChunkSigningPlanItem,
    CompactTransferPlanDescriptor,
    DestConfig,
    DistributeResponse,
    HippiusProviderDestination,
    HuggingFaceProviderDestination,
    HuggingFaceProviderSource,
    IntegrityAuditChallenge,
    IntegrityAuditChallengeChunk,
    MultipartGroupManifest,
    PlanningHttpSource,
    PreparedDestination,
    PreparedHttpSource,
    ProviderDestinationConfig,
    ProviderMultipartGroupIdentity,
    ProviderSourceConfig,
    SignedChunkRoute,
    SignedUrlFlow,
    SourceConfig,
    TransferCancelResponse,
    TransferCreateResponse,
    TransferPlanResponse,
    TransferPrepareResponse,
    TransferStatusInfo,
    TransferTerminalEvent,
)
from beam_network_sdk.provider_signing import (
    abort_multipart_upload,
    create_multipart_upload,
    expires_at_iso,
    prepare_provider_destination,
    prepare_provider_source,
    release_provider_clients,
    sign_abort_multipart_upload,
    sign_complete_multipart_upload,
    sign_destination_read_range,
    sign_destination_route,
    sign_final_object_head,
    sign_list_multipart_upload,
    sign_multipart_recovery,
    sign_source_read_range,
    source_chunk_grant,
)

logger = logging.getLogger("beam_network_sdk.client")

BEAM_DEV_URL = BEAM_DEV_NATS_URL
BEAM_PROD_URL = BEAM_PROD_NATS_URL


ROUTE_STREAM_BATCH_ROUTES = 1_024
BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY = 2

_T = TypeVar("_T")
_R = TypeVar("_R")


@dataclass
class _HuggingFaceUploadState:
    """One Hugging Face LFS upload, held from prepare until the transfer's commit.

    The Hub issues upload URLs only for a known sha256 and dictates the part size, so the
    plan is built around what the LFS batch hands back rather than the other way round.
    """

    destination: HuggingFaceProviderDestination
    destination_id: str
    source_id: str
    oid: str
    size: int
    chunk_size: int | None
    part_urls: list[str]
    upload_href: str | None
    verify_href: str | None
    #: Resolves alongside the transfer; awaited only at commit time.
    part_etags: asyncio.Future[list[str]] | None = None


def _huggingface_target_path(
    destination: HuggingFaceProviderDestination, source_filename: str | None
) -> str:
    """Resolve the path a Hugging Face upload commits to, expanding a trailing-slash prefix."""
    path = destination.path.lstrip("/")
    if not path.endswith("/"):
        return path
    if not source_filename:
        raise ValueError(
            f"huggingface path {destination.path} is a folder and the source has no filename"
        )
    return f"{path}{source_filename}"


def _huggingface_part_etags(url: str, part_size: int) -> list[str]:
    _, part_etags = hf.hash_source_stream(url, part_size=part_size, sha256=False)
    return part_etags


def _transfer_id_for_idempotency_key(idempotency_key: str | None) -> str:
    if not idempotency_key or not idempotency_key.strip():
        return str(uuid.uuid4())
    return _stable_uuid_from_identity(f"beam-transfer:{idempotency_key.strip()}")


def _route_generation_id_for_prepare_idempotency_key(idempotency_key: str) -> str:
    return _stable_uuid_from_identity(f"beam-route-generation:{idempotency_key.strip()}")


def _stable_uuid_from_identity(identity: str) -> str:
    digest = bytearray(hashlib.sha256(identity.encode()).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


class _RouteKeysChecksum:
    def __init__(self) -> None:
        self._bytes = bytearray(32)
        self._count = 0

    def add(self, route_key: str) -> None:
        digest = hashlib.sha256(route_key.encode("utf-8")).digest()
        for index, value in enumerate(digest):
            self._bytes[index] ^= value
        self._count += 1

    def value(self) -> str:
        return f"sha256-xor-v1:{self._count}:{self._bytes.hex()}"


def _route_key(route: dict[str, Any]) -> str:
    return f"{route['source_id']}:{route['destination_id']}:{route['chunk_index']}"


def _route_keys_checksum(routes: list[dict[str, Any]]) -> str:
    checksum = _RouteKeysChecksum()
    for route in routes:
        checksum.add(_route_key(route))
    return checksum.value()


def _signed_route_key(route: SignedChunkRoute) -> str:
    return f"{route.source_id}:{route.destination_id}:{route.chunk_index}"


def _route_coordinate_checksum(routes: list[SignedChunkRoute]) -> str:
    checksum = _RouteKeysChecksum()
    for route in routes:
        delivery_index = _route_delivery_index(route)
        checksum.add(
            f"{_signed_route_key(route)}:{delivery_index if delivery_index is not None else 'missing'}"
        )
    return checksum.value()


def _dict_route_coordinate_checksum(routes: list[dict[str, Any]]) -> str:
    checksum = _RouteKeysChecksum()
    for route in routes:
        delivery_index = _dict_route_delivery_index(route)
        checksum.add(
            f"{_route_key(route)}:{delivery_index if delivery_index is not None else 'missing'}"
        )
    return checksum.value()


def _route_delivery_index(route: SignedChunkRoute) -> int | None:
    metadata_index = route.metadata.get("delivery_index")
    if isinstance(metadata_index, int):
        return metadata_index
    return route.delivery_index


def _route_sort_key(route: SignedChunkRoute) -> tuple[int, str]:
    delivery_index = _route_delivery_index(route)
    if delivery_index is None:
        raise ValueError("route delivery_index is required")
    return delivery_index, _signed_route_key(route)


def _dict_route_delivery_index(route: dict[str, Any]) -> int | None:
    metadata = route.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("delivery_index"), int):
        return int(metadata["delivery_index"])
    delivery_index = route.get("delivery_index")
    return int(delivery_index) if isinstance(delivery_index, int) else None


def _stable_route_stream_id(
    *,
    transfer_id: str,
    plan_identity: str,
    total_routes: int,
    total_chunks: int,
    signed_url_flow: SignedUrlFlow,
) -> str:
    digest = bytearray(
        hashlib.sha256(
            (
                f"beam:route-stream:{signed_url_flow}:"
                f"{transfer_id}:{plan_identity}:{total_routes}:{total_chunks}"
            ).encode()
        ).digest()[:16]
    )
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


def _stable_manifest_batch_identity(groups: list[dict[str, Any]]) -> str:
    identities = sorted(
        f"{group['multipart_group_id']}:{group['source_id']}:{group['destination_id']}"
        for group in groups
    )
    return hashlib.sha256("\n".join(identities).encode("utf-8")).hexdigest()[:32]


def _count_distinct_route_chunks(routes: list[SignedChunkRoute]) -> int:
    return len({f"{route.source_id}:{route.chunk_index}" for route in routes})


def _iter_plan_chunks(
    descriptor: CompactTransferPlanDescriptor,
    transfer_id: str,
) -> Iterator[ChunkSigningPlanItem]:
    for source in descriptor.sources:
        for source_chunk_index in range(source.chunk_count):
            yield _materialize_plan_chunk(
                descriptor,
                transfer_id,
                source.source_id,
                source.global_chunk_start + source_chunk_index,
            )


def _materialize_plan_chunk(
    descriptor: CompactTransferPlanDescriptor,
    transfer_id: str,
    source_id: str,
    chunk_index: int,
) -> ChunkSigningPlanItem:
    source = next(
        (candidate for candidate in descriptor.sources if candidate.source_id == source_id), None
    )
    if source is None:
        raise ValueError(f"plan source not found: {source_id}")
    source_chunk_index = chunk_index - source.global_chunk_start
    if source_chunk_index < 0 or source_chunk_index >= source.chunk_count:
        raise ValueError(f"plan chunk outside source range: {source_id}:{chunk_index}")
    source_offset = source_chunk_index * descriptor.chunk_size
    chunk_size = min(descriptor.chunk_size, source.size - source_offset)
    destinations: list[ChunkDestinationSigningTarget] = []
    for destination in descriptor.destinations:
        final_object_key = destination.final_object_keys.get(source_id)
        if not final_object_key:
            raise ValueError(
                f"plan final object key missing: {source_id}:{destination.destination_id}"
            )
        delivery_index = chunk_index * len(descriptor.destinations) + destination.destination_index
        part_number = multipart_part_number(source_chunk_index)
        object_key = (
            f"{final_object_key}/{descriptor.plan_nonce}/chunk-{source_chunk_index:06d}"
            if destination.provider.lower() == "hippius"
            else final_object_key
        )
        destinations.append(
            ChunkDestinationSigningTarget(
                destination_id=destination.destination_id,
                provider=destination.provider,
                object_key=object_key,
                metadata={
                    **destination.metadata,
                    "final_object_key": final_object_key,
                    "part_number": part_number,
                    "logical_attempt_index": 0,
                    "attempt_slot": 0,
                    "route_generation_id": f"initial-{chunk_index}-{destination.destination_id}",
                    "delivery_index": delivery_index,
                },
            )
        )
    return ChunkSigningPlanItem(
        chunk_index=chunk_index,
        source_id=source_id,
        source_chunk_index=source_chunk_index,
        source_offset=source_offset,
        chunk_size=chunk_size,
        source_url=source.url,
        destinations=destinations,
    )


_PART_ROUTE_METADATA_KEYS = frozenset(
    {
        "part_number",
        "logical_attempt_index",
        "attempt_slot",
        "route_generation_id",
        "delivery_index",
        "etag_required",
    }
)


def _part_route_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    return {
        key: value for key, value in (metadata or {}).items() if key in _PART_ROUTE_METADATA_KEYS
    }


def _direct_put_route_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Route metadata for a plain-PUT destination; BeamCore rejects part_number there."""
    return {
        key: value for key, value in _part_route_metadata(metadata).items() if key != "part_number"
    }


@dataclass
class _MultipartUploadState:
    """A created multipart upload; ``manifest`` is set once its control URLs are signed."""

    destination: ProviderDestinationConfig
    object_key: str
    upload_id: str
    manifest: MultipartGroupManifest | None = None


def _multipart_group_state_key(
    transfer_id: str, destination_id: str, source_id: str, final_object_key: str
) -> str:
    return f"{transfer_id}:{destination_id}:{source_id}:{final_object_key}"


def _is_direct_put_destination(destination: ProviderDestinationConfig) -> bool:
    """Destinations that take one plain PUT per chunk instead of an S3 multipart upload."""
    return isinstance(destination, (HippiusProviderDestination, HuggingFaceProviderDestination))


def _positive_integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _multipart_max_part_number(chunk_count: int) -> int:
    return multipart_part_number(_positive_integer(chunk_count, "source chunk_count") - 1)


def _multipart_list_page_markers(max_part_number: int) -> list[int]:
    return list(range(0, max_part_number, 1_000))


def _validate_multipart_part_number(part_number: int, manifest: MultipartGroupManifest) -> None:
    if (
        not isinstance(part_number, int)
        or isinstance(part_number, bool)
        or part_number < 1
        or part_number > manifest.max_part_number
    ):
        raise ValueError(
            f"multipart part_number {part_number} is outside group "
            f"{manifest.multipart_group_id} range 1-{manifest.max_part_number}"
        )


def _validate_multipart_group_manifest(
    manifests: Sequence[MultipartGroupManifest], transfer_id: str
) -> None:
    group_ids: set[str] = set()
    for group in manifests:
        if group.multipart_group_id in group_ids:
            raise ValueError(
                f"multipart_group_id must be non-empty and unique: {group.multipart_group_id}"
            )
        group_ids.add(group.multipart_group_id)
        if group.final_object_metadata != {"beam-transfer-id": transfer_id}:
            raise ValueError(
                f"multipart group {group.multipart_group_id} has invalid final_object_metadata"
            )


def _plan_descriptor(prepared: TransferPrepareResponse) -> CompactTransferPlanDescriptor:
    descriptor = prepared.plan_descriptor
    if descriptor is None:
        raise ValueError("BeamCore returned no compact transfer plan")
    return descriptor


_SENSITIVE_ERROR_TEXT = re.compile(
    r"https?://|x-amz|password|secret|token|authorization|credential", re.IGNORECASE
)


def _integrity_audit_error_summary(error: BaseException) -> str:
    """Summarize a grant failure for status output without leaking URLs or secrets."""
    if not isinstance(error, Exception):
        return "unknown_error"
    message = str(error).strip()
    if not message or _SENSITIVE_ERROR_TEXT.search(message):
        return type(error).__name__ or "unknown_error"
    return message[:200]


def _multipart_group_identity(
    transfer_id: str, manifest: MultipartGroupManifest
) -> ProviderMultipartGroupIdentity:
    """Durable, credential-free identity of a created multipart upload."""
    return ProviderMultipartGroupIdentity(
        transfer_id=transfer_id,
        multipart_group_id=manifest.multipart_group_id,
        source_id=manifest.source_id,
        destination_id=manifest.destination_id,
        object_key=manifest.final_object_key,
        upload_id=manifest.upload_id,
        expected_object_size=manifest.expected_object_size,
        expected_part_count=manifest.expected_part_count,
        expires_at=manifest.urls_expires_at,
    )


def _restore_provider_multipart_identities(
    prepared: TransferPrepareResponse,
    destination_by_id: dict[str, ProviderDestinationConfig],
    identities: list[ProviderMultipartGroupIdentity],
    uploads: dict[str, _MultipartUploadState],
) -> None:
    """Adopt a previous owner's uploads; the set must match the plan's groups exactly."""
    descriptor = _plan_descriptor(prepared)
    expected: dict[str, tuple[ProviderDestinationConfig, str, Any, str]] = {}
    for source in descriptor.sources:
        for target in descriptor.destinations:
            destination = destination_by_id.get(target.destination_id)
            if destination is None:
                raise ValueError("provider_multipart_recovery_destination_invalid")
            if _is_direct_put_destination(destination):
                continue
            object_key = target.final_object_keys.get(source.source_id)
            if not object_key:
                raise ValueError("provider_multipart_recovery_object_invalid")
            expected[
                _multipart_group_state_key(
                    prepared.transfer_id, target.destination_id, source.source_id, object_key
                )
            ] = (destination, object_key, source, target.destination_id)
    if len(expected) != len(identities):
        raise ValueError("provider_multipart_recovery_incomplete")
    for identity in identities:
        group = expected.get(identity.multipart_group_id)
        if (
            group is None
            or identity.multipart_group_id in uploads
            or identity.transfer_id != prepared.transfer_id
            or identity.source_id != group[2].source_id
            or identity.destination_id != group[3]
            or identity.object_key != group[1]
            or identity.expected_object_size != group[2].size
            or identity.expected_part_count != group[2].chunk_count
            or not identity.upload_id.strip()
        ):
            raise ValueError("provider_multipart_recovery_identity_invalid")
        uploads[identity.multipart_group_id] = _MultipartUploadState(
            destination=group[0], object_key=group[1], upload_id=identity.upload_id
        )


def _consume_future_exception(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


def _create_multipart_group_waiters(
    prepared: TransferPrepareResponse,
    destination_by_id: dict[str, ProviderDestinationConfig],
) -> dict[str, asyncio.Future[_MultipartUploadState]]:
    loop = asyncio.get_running_loop()
    waiters: dict[str, asyncio.Future[_MultipartUploadState]] = {}
    for source in _plan_descriptor(prepared).sources:
        for destination in _plan_descriptor(prepared).destinations:
            config = destination_by_id.get(destination.destination_id)
            if config is None or _is_direct_put_destination(config):
                continue
            final_object_key = destination.final_object_keys.get(source.source_id)
            if not final_object_key:
                raise ValueError(
                    "plan final object key not found: "
                    f"{source.source_id}:{destination.destination_id}"
                )
            waiter: asyncio.Future[_MultipartUploadState] = loop.create_future()
            waiter.add_done_callback(_consume_future_exception)
            waiters[
                _multipart_group_state_key(
                    prepared.transfer_id,
                    destination.destination_id,
                    source.source_id,
                    final_object_key,
                )
            ] = waiter
    return waiters


async def _map_ordered_with_concurrency(
    values: Sequence[_T],
    concurrency: int,
    operation: Callable[[_T], Awaitable[_R]],
) -> list[_R]:
    """Map ``values`` with at most ``concurrency`` operations in flight, preserving order."""
    results: list[Any] = [None] * len(values)
    next_index = 0

    async def worker() -> None:
        nonlocal next_index
        while True:
            index = next_index
            next_index += 1
            if index >= len(values):
                return
            results[index] = await operation(values[index])

    await asyncio.gather(*(worker() for _ in range(min(max(1, concurrency), len(values)))))
    return results


_SAFE_ERROR_CODE = re.compile(r"[A-Za-z0-9_.-]{1,64}")


def _safe_error_code(error: BaseException) -> str:
    """Name an error by type/code and HTTP status only, never by its message."""
    code: Any = getattr(error, "code", None)
    status: Any = None
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        provider_error = response.get("Error")
        if not isinstance(code, str) and isinstance(provider_error, dict):
            code = provider_error.get("Code")
        response_metadata = response.get("ResponseMetadata")
        if isinstance(response_metadata, dict):
            status = response_metadata.get("HTTPStatusCode")
    name = (
        code if isinstance(code, str) and _SAFE_ERROR_CODE.fullmatch(code) else type(error).__name__
    )
    for candidate in (getattr(error, "status", None), getattr(error, "status_code", None), status):
        if (
            isinstance(candidate, int)
            and not isinstance(candidate, bool)
            and 100 <= candidate <= 599
        ):
            return f"{name}:status={candidate}"
    return name


async def _maybe_await(value: Any) -> None:
    if inspect.isawaitable(value):
        await value


_RECOVERY_SECRET_FIELDS = (
    "access_key_id",
    "secret_access_key",
    "session_token",
    "api_token",
    "token",
    "password",
    "account_key",
    "sas_token",
)


def _clear_recovery_secrets(configs: Iterable[BaseModel]) -> None:
    """Scrub credentials from the SDK's retained provider copies once recovery ends."""
    for config in configs:
        fields = type(config).model_fields
        for field_name in _RECOVERY_SECRET_FIELDS:
            if field_name not in fields:
                continue
            value = getattr(config, field_name)
            if value is None:
                continue
            setattr(config, field_name, SecretStr("") if isinstance(value, SecretStr) else "")


class _RouteStreamSender:
    def __init__(
        self,
        *,
        control: TransferClientControl,
        transfer_id: str,
        total_routes: int,
        total_chunks: int,
        auto_distribute: bool,
        urls_expires_at: str | None = None,
        plan_identity: str,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
    ) -> None:
        self._control = control
        self._transfer_id = transfer_id
        self._stream_id = _stable_route_stream_id(
            transfer_id=transfer_id,
            plan_identity=plan_identity,
            total_routes=total_routes,
            total_chunks=total_chunks,
            signed_url_flow=signed_url_flow,
        )
        self._signed_url_flow = signed_url_flow
        self._route_generation_id = route_generation_id or str(uuid.uuid4())
        self._total_routes = total_routes
        self._total_chunks = total_chunks
        self._auto_distribute = auto_distribute
        self._urls_expires_at = urls_expires_at
        self._checksum = _RouteKeysChecksum()
        self._batch: list[dict[str, Any]] = []
        self._seen_delivery_indices: set[int] = set()
        self._batch_index = 0
        self._route_count = 0
        self._send_tail: asyncio.Task[None] | None = None
        self._send_error: BaseException | None = None
        self.telemetry = PerformanceCollector()

    async def begin(self) -> None:
        self.telemetry.start()
        payload: dict[str, Any] = {
            "transfer_id": self._transfer_id,
            "stream_id": self._stream_id,
            "route_generation_id": self._route_generation_id,
            "total_routes": self._total_routes,
            "total_chunks": self._total_chunks,
            "route_contract_version": self._signed_url_flow,
            "signed_url_flow": self._signed_url_flow,
            "auto_distribute": self._auto_distribute,
        }
        if self._urls_expires_at:
            payload["urls_expires_at"] = self._urls_expires_at
        await self._control.request(
            "transfer.route_stream.begin",
            payload,
            transfer_id=self._transfer_id,
            idempotency_key=f"transfer:{self._transfer_id}:route-stream:{self._stream_id}:begin",
        )

    async def add_manifest_groups(
        self,
        groups: list[MultipartGroupManifest | dict[str, Any]],
    ) -> None:
        if not groups:
            return
        normalized = [MultipartGroupManifest.model_validate(group).model_dump() for group in groups]
        for group in normalized:
            if group["final_object_metadata"]["beam-transfer-id"] != self._transfer_id:
                raise ValueError(
                    f"multipart group {group['multipart_group_id']} transfer identity does not match"
                )

        async def attach(group: dict[str, Any]) -> None:
            identity = _stable_manifest_batch_identity([group])
            await self._control.request(
                "transfer.route_stream.manifest",
                {
                    "transfer_id": self._transfer_id,
                    "stream_id": self._stream_id,
                    "route_generation_id": self._route_generation_id,
                    "manifest_batch_id": identity,
                    "groups": [group],
                },
                transfer_id=self._transfer_id,
                idempotency_key=f"transfer:{self._transfer_id}:route-stream:{self._stream_id}:manifest:{identity}",
            )

        started = time.monotonic()
        await asyncio.gather(*(attach(group) for group in normalized))
        self.telemetry.observe("sdk.manifest_ack", started)
        self.telemetry.mark("first_manifest_ms")

    async def add_route(self, route: dict[str, Any]) -> None:
        self.telemetry.mark("first_route_ms")
        assembly_started = time.monotonic()
        route = dict(route)
        delivery_index = _dict_route_delivery_index(route)
        if delivery_index is None:
            raise ValueError("route delivery_index is required")
        if delivery_index < 0 or delivery_index >= self._total_routes:
            raise ValueError(
                f"route delivery_index {delivery_index} is outside the declared route stream"
            )
        if delivery_index in self._seen_delivery_indices:
            raise ValueError(f"duplicate route delivery_index {delivery_index}")
        self._seen_delivery_indices.add(delivery_index)
        self._route_count += 1
        route["delivery_index"] = delivery_index
        self._checksum.add(_route_key(route))
        self._batch.append(route)
        self.telemetry.observe("sdk.route_assembly", assembly_started)
        self.telemetry.gauge(
            "buffered_batches_peak",
            1 + int(self._send_tail is not None and not self._send_tail.done()),
        )
        if len(self._batch) >= ROUTE_STREAM_BATCH_ROUTES:
            await self._enqueue_flush()

    async def complete(self) -> dict[str, Any]:
        if (
            self._route_count != self._total_routes
            or len(self._seen_delivery_indices) != self._total_routes
        ):
            raise ValueError(
                "route stream has incomplete or duplicate delivery indices: "
                f"received {self._route_count} of {self._total_routes} routes"
            )
        self.telemetry.mark("final_flush_ms")
        await self._enqueue_flush()
        if self._send_tail is not None:
            await self._send_tail
        if self._send_error is not None:
            raise self._send_error
        self.telemetry.observe("sdk.preparation", self.telemetry.started)
        self.telemetry.finish()
        return await self._control.request(
            "transfer.route_stream.complete",
            {
                "transfer_id": self._transfer_id,
                "stream_id": self._stream_id,
                "route_generation_id": self._route_generation_id,
                "expected_batches": self._batch_index,
                "expected_routes": self._total_routes,
                "route_keys_checksum": self._checksum.value(),
                "sdk_performance": self.telemetry.snapshot(
                    getattr(self._control, "supports_performance_v2", lambda _: False)(
                        self._transfer_id
                    )
                ),
            },
            transfer_id=self._transfer_id,
            idempotency_key=f"transfer:{self._transfer_id}:route-stream:{self._stream_id}:complete",
        )

    async def abort(self) -> None:
        self.telemetry.finish()
        if self._send_error is None:
            self._send_error = RuntimeError("route_stream_aborted")
        if self._send_tail is not None:
            await asyncio.gather(self._send_tail, return_exceptions=True)

    async def _enqueue_flush(self) -> None:
        if not self._batch:
            return
        if self._send_tail is not None:
            started = time.monotonic()
            await self._send_tail
            self.telemetry.observe("sdk.buffer_wait", started)
            self._send_tail = None
        if self._send_error is not None:
            raise self._send_error
        routes = self._batch
        self._batch = []
        encode_started = time.monotonic()
        chunks = self._control.split_routes_for_payload(
            "transfer.route_stream.batch",
            {
                "transfer_id": self._transfer_id,
                "stream_id": self._stream_id,
                "route_generation_id": self._route_generation_id,
                "batch_id": f"{self._stream_id}:estimate",
                "batch_index": self._batch_index,
            },
            routes,
        )
        self.telemetry.observe("sdk.batch_encode", encode_started)
        scheduled: list[tuple[int, list[dict[str, Any]]]] = []
        queued_at = time.monotonic()
        for chunk in chunks:
            batch_index = self._batch_index
            self._batch_index += 1
            scheduled.append((batch_index, chunk))

        async def send_chunks() -> None:
            if self._send_error is not None:
                return
            try:
                for batch_index, chunk in scheduled:
                    if self._send_error is not None:
                        return
                    self.telemetry.observe("sdk.batch_queue", queued_at)
                    checksum_started = time.monotonic()
                    route_checksum = _route_keys_checksum(chunk)
                    coordinate_checksum = _dict_route_coordinate_checksum(chunk)
                    batch_id = f"{self._stream_id}:{batch_index}:{coordinate_checksum}"
                    self.telemetry.observe("sdk.batch_checksum", checksum_started)
                    self.telemetry.gauge("batch_routes_max", len(chunk))
                    self.telemetry.mark("first_batch_ms")
                    started = time.monotonic()
                    await self._control.request(
                        "transfer.route_stream.batch",
                        {
                            "transfer_id": self._transfer_id,
                            "stream_id": self._stream_id,
                            "route_generation_id": self._route_generation_id,
                            "batch_id": batch_id,
                            "batch_index": batch_index,
                            "route_batch": compact_signed_routes(chunk),
                            "route_count": len(chunk),
                            "route_keys_checksum": route_checksum,
                        },
                        transfer_id=self._transfer_id,
                        idempotency_key=(
                            f"transfer:{self._transfer_id}:route-stream:{self._stream_id}:"
                            f"batch:{batch_index}:{coordinate_checksum}"
                        ),
                    )
                    self.telemetry.observe("sdk.batch_ack", started)
                    self.telemetry.counters["route_batches"] += 1
            except BaseException as error:
                self._send_error = error

        self._send_tail = asyncio.create_task(send_chunks())


class TransferManager:
    """Transfer management and provider-aware transfer preparation."""

    def __init__(
        self,
        control: TransferClientControl,
        *,
        route_signing_concurrency: int | None = None,
        multipart_control_concurrency: int | None = None,
        on_diagnostics: Callable[[dict[str, Any]], Any] | None = None,
    ):
        self._control = control
        self._route_signing_concurrency = (
            64
            if route_signing_concurrency is None
            else _positive_integer(route_signing_concurrency, "route_signing_concurrency")
        )
        self._route_signing_concurrency_overridden = route_signing_concurrency is not None
        self._multipart_control_concurrency = _positive_integer(
            BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY
            if multipart_control_concurrency is None
            else multipart_control_concurrency,
            "multipart_control_concurrency",
        )
        self._route_signing_executor = ThreadPoolExecutor(
            max_workers=256,
            thread_name_prefix="beam-route-signing",
        )
        self._huggingface_uploads: dict[str, list[_HuggingFaceUploadState]] = {}
        self._integrity_context: dict[
            str,
            tuple[
                TransferPrepareResponse,
                dict[str, ProviderSourceConfig],
                dict[str, ProviderDestinationConfig],
                int,
            ],
        ] = {}
        self._integrity_audit_submissions: dict[str, asyncio.Future[None]] = {}
        self._integrity_grant_cache: dict[str, dict[str, Any]] = {}
        self._on_diagnostics = on_diagnostics
        self._diagnostic_pending = False
        self._closed = False

    async def _run_in_executor(self, function: Callable[..., _R], /, **kwargs: Any) -> _R:
        telemetry = current_performance.get()
        queued_at = time.monotonic()
        context = copy_context()
        phase = {
            "source_chunk_grant": "sdk.source_signing",
            "_sign_provider_route_sync": "sdk.destination_signing",
            "create_multipart_upload": "sdk.multipart_provider",
        }.get(getattr(function, "__name__", ""))

        def execute() -> _R:
            started = time.monotonic()
            if telemetry is not None:
                telemetry.observe_duration(
                    "sdk.multipart_queue"
                    if phase == "sdk.multipart_provider"
                    else "sdk.signing_queue",
                    started - queued_at,
                )
                if phase in {"sdk.source_signing", "sdk.destination_signing"}:
                    telemetry.signer_active(1)
            try:
                return function(**kwargs)
            finally:
                if telemetry is not None and phase:
                    telemetry.observe(phase, started)
                    if phase in {"sdk.source_signing", "sdk.destination_signing"}:
                        telemetry.signer_active(-1)

        return await asyncio.get_running_loop().run_in_executor(
            self._route_signing_executor, partial(context.run, execute)
        )

    def _stop_recovery_signer(
        self, transfer_id: str, *, prepared: TransferPrepareResponse | None = None
    ) -> None:
        """Stop route recovery signing and integrity grants for a transfer.

        With ``prepared``, only stop them if they still belong to that preparation, so a
        fenced-off owner never stops a replacement owner in the same client.
        """
        context = self._integrity_context.get(transfer_id)
        if prepared is not None and (context is None or context[0] is not prepared):
            return
        self._control.stop_route_recovery_signer(transfer_id)
        self._integrity_context.pop(transfer_id, None)
        for audit_id, entry in list(self._integrity_grant_cache.items()):
            if entry["transfer_id"] == transfer_id:
                self._integrity_grant_cache.pop(audit_id, None)
                self._integrity_audit_submissions.pop(audit_id, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(
            self._route_signing_executor.shutdown,
            wait=True,
            cancel_futures=True,
        )

    async def create(
        self,
        *,
        sources: list[SourceConfig],
        destinations: list[DestConfig],
        total_size: int,
        name: str | None = None,
        merkle_root: str | None = None,
        chunk_hashes: list[str] | None = None,
        callbacks: list[CallbackConfig] | None = None,
        progressive_mode: bool = False,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
    ) -> TransferCreateResponse:
        """Create a BeamCore transfer from provider-agnostic source/destination configs."""
        body: dict[str, Any] = {
            "transfer_id": _transfer_id_for_idempotency_key(idempotency_key),
            "sources": [
                source.model_dump(exclude_none=True) if hasattr(source, "model_dump") else source
                for source in sources
            ],
            "destinations": [
                destination.model_dump(exclude_none=True)
                if hasattr(destination, "model_dump")
                else destination
                for destination in destinations
            ],
            "total_size": total_size,
            "signed_url_flow": signed_url_flow,
        }
        if name:
            body["name"] = name
        if merkle_root:
            body["merkle_root"] = merkle_root
        if chunk_hashes:
            body["chunk_hashes"] = chunk_hashes
        if callbacks:
            body["callbacks"] = [
                callback.model_dump() if hasattr(callback, "model_dump") else callback
                for callback in callbacks
            ]
        if progressive_mode:
            body["progressive_mode"] = True
        data = await self._control.request(
            "transfer.create",
            body,
            transfer_id=str(body["transfer_id"]),
            idempotency_key=f"transfer:{body['transfer_id']}:create",
        )
        return TransferCreateResponse.model_validate(data)

    async def status(self, transfer_id: str) -> TransferStatusInfo:
        """Get transfer status, answering any pending integrity audit challenge.

        A failed grant submission does not fail the status call: the sanitized reason
        is reported as ``integrity_audit_submission_error`` and the next poll retries.
        """
        _validate_id(transfer_id, "transfer_id")
        data = await self._control.request(
            "transfer.status", {"transfer_id": transfer_id}, transfer_id=transfer_id
        )
        result = TransferStatusInfo.model_validate(data)
        if result.integrity_audit_challenge is not None:
            try:
                await self._submit_integrity_audit_grants_if_present(result)
            except Exception as exc:
                result.integrity_audit_submission_error = _integrity_audit_error_summary(exc)
                logger.warning(
                    "beam_integrity_audit_grants_failed transfer_id=%s diagnostic=%s",
                    transfer_id,
                    safe_error_diagnostic(exc, retryable=True),
                )
        if result.status in {"completed", "failed", "cancelled"}:
            self._control.release_recovery_lease(transfer_id)
            self._stop_recovery_signer(transfer_id)
        return result

    async def _submit_integrity_audit_grants_if_present(self, status: TransferStatusInfo) -> None:
        challenge = status.integrity_audit_challenge
        if challenge is None:
            return
        if challenge.transfer_id not in self._integrity_context:
            raise RuntimeError("integrity audit signer unavailable")
        # Concurrent status polls share one submission per audit; a failed one is retried.
        existing = self._integrity_audit_submissions.get(challenge.audit_id)
        entry = self._integrity_grant_cache.get(challenge.audit_id)
        fingerprint = hashlib.sha256(
            json.dumps(challenge.model_dump(), sort_keys=True).encode()
        ).hexdigest()
        if entry and entry["fingerprint"] != fingerprint:
            raise ValueError("conflicting integrity challenge")
        if existing is not None and (entry is None or entry["expires_at"] > time.time() + 5):
            await asyncio.shield(existing)
            return
        submission = asyncio.ensure_future(self._submit_integrity_audit_grants(challenge))
        submission.add_done_callback(_consume_future_exception)
        self._integrity_audit_submissions[challenge.audit_id] = submission
        try:
            await asyncio.shield(submission)
        except Exception:
            if self._integrity_audit_submissions.get(challenge.audit_id) is submission:
                self._integrity_audit_submissions.pop(challenge.audit_id, None)
            raise

    async def _signed_integrity_grants(
        self, value: IntegrityAuditChallenge | dict[str, Any]
    ) -> dict[str, Any]:
        challenge = IntegrityAuditChallenge.model_validate(value)
        fingerprint = hashlib.sha256(
            json.dumps(challenge.model_dump(), sort_keys=True).encode()
        ).hexdigest()
        prior = self._integrity_grant_cache.get(challenge.audit_id)
        if prior and prior["fingerprint"] != fingerprint:
            raise ValueError("conflicting integrity challenge")
        if prior and prior["expires_at"] > time.time() + 5:
            return await asyncio.shield(prior["task"])
        if (
            len(self._integrity_grant_cache) >= 1024
            and challenge.audit_id not in self._integrity_grant_cache
        ):
            raise RuntimeError("integrity signer cache busy")
        entry: dict[str, Any] = {
            "transfer_id": challenge.transfer_id,
            "fingerprint": fingerprint,
            "expires_at": float("inf"),
        }

        async def build_cached() -> dict[str, Any]:
            try:
                payload = await self._build_integrity_audit_grants(challenge)
                entry["expires_at"] = min(
                    datetime.fromisoformat(
                        chunk[side]["expires_at"].replace("Z", "+00:00")
                    ).timestamp()
                    for chunk in payload["chunks"]
                    for side in ("source", "destination")
                )
                return payload
            except BaseException:
                if self._integrity_grant_cache.get(challenge.audit_id) is entry:
                    self._integrity_grant_cache.pop(challenge.audit_id, None)
                raise

        task = asyncio.create_task(build_cached())
        task.add_done_callback(_consume_future_exception)
        entry["task"] = task
        self._integrity_grant_cache[challenge.audit_id] = entry
        return await asyncio.shield(task)

    async def _submit_integrity_audit_grants(self, challenge: IntegrityAuditChallenge) -> None:
        payload = await self._signed_integrity_grants(challenge)
        response = await self._control.request(
            "transfer.integrity_audit_grants",
            payload,
            transfer_id=challenge.transfer_id,
            idempotency_key=f"transfer:{challenge.transfer_id}:integrity-audit:{challenge.audit_id}:{payload['submitted_at']}",
        )
        if response.get("published") is not True:
            raise RuntimeError("integrity audit delivery unavailable")

    async def _build_integrity_audit_grants(
        self, challenge: IntegrityAuditChallenge
    ) -> dict[str, Any]:
        transfer_id = challenge.transfer_id
        audit_id = challenge.audit_id
        context = self._integrity_context.get(transfer_id)
        if context is None:
            raise RuntimeError("integrity audit signer unavailable")
        prepared, sources, destinations, expires_in = context
        if prepared.transfer_id != transfer_id:
            raise ValueError("integrity audit challenge transfer mismatch")
        descriptor = _plan_descriptor(prepared)
        submitted_at = iso_now()
        semaphore = asyncio.Semaphore(self._route_signing_concurrency)

        async def sign_chunk(chunk: IntegrityAuditChallengeChunk) -> dict[str, Any]:
            async with semaphore:
                plan_chunk = _materialize_plan_chunk(
                    descriptor, transfer_id, chunk.source_id, chunk.route_chunk_index
                )
                offset = chunk.source_offset
                length = chunk.range_length
                if (
                    length <= 0
                    or offset < plan_chunk.source_offset
                    or offset + length > plan_chunk.source_offset + plan_chunk.chunk_size
                ):
                    raise ValueError("integrity audit range outside planned chunk")
                target = next(
                    (
                        item
                        for item in plan_chunk.destinations
                        if item.destination_id == chunk.destination_id
                        and item.metadata.get("delivery_index") == chunk.delivery_index
                    ),
                    None,
                )
                if (
                    target is None
                    or target.metadata.get("final_object_key") != chunk.final_object_key
                ):
                    raise ValueError("integrity audit destination coordinate mismatch")
                if chunk.destination_offset != offset:
                    raise ValueError("integrity audit destination offset mismatch")
                source = sources.get(chunk.source_id)
                destination = destinations.get(chunk.destination_id)
                if source is None or destination is None:
                    raise ValueError("integrity audit provider configuration is unavailable")
                planned_source = next(
                    (item for item in descriptor.sources if item.source_id == chunk.source_id),
                    None,
                )
                metadata = planned_source.metadata if planned_source is not None else {}
                source_etag = metadata.get("etag")
                source_version_id = metadata.get("version_id")
                source_grant, destination_grant = await asyncio.gather(
                    self._run_in_executor(
                        sign_source_read_range,
                        source=source,
                        offset=offset,
                        length=length,
                        expires_in=expires_in,
                        if_match=source_etag if isinstance(source_etag, str) else None,
                        version_id=source_version_id
                        if isinstance(source_version_id, str)
                        else None,
                    ),
                    self._run_in_executor(
                        sign_destination_read_range,
                        destination=destination,
                        object_key=chunk.final_object_key,
                        offset=offset,
                        length=length,
                        expires_in=expires_in,
                        if_match=chunk.final_object_etag or None,
                    ),
                )
                grant_chunk = chunk.model_dump(
                    exclude_unset=True,
                    exclude={"orchestrator_id", "orchestrator_hotkey", "worker_id"},
                )
                return {**grant_chunk, "source": source_grant, "destination": destination_grant}

        chunks = await asyncio.gather(*(sign_chunk(chunk) for chunk in challenge.chunks))
        return {
            "transfer_id": transfer_id,
            "audit_id": audit_id,
            "submitted_at": submitted_at,
            "chunks": chunks,
        }

    def _report_diagnostics(self, summary: dict[str, Any]) -> None:
        if self._on_diagnostics is None or self._diagnostic_pending:
            return
        self._diagnostic_pending = True
        callback = self._on_diagnostics

        async def report() -> None:
            try:
                value = await asyncio.to_thread(callback, summary)
                if inspect.isawaitable(value):
                    await value
            except Exception:
                pass
            finally:
                self._diagnostic_pending = False

        asyncio.create_task(report())

    async def open_terminal_signal_waiter(
        self,
        transfer_id: str,
    ) -> TransferTerminalSignalWaiter:
        """Subscribe to the authenticated terminal signal before status reconciliation."""
        _validate_id(transfer_id, "transfer_id")
        return await self._control.open_terminal_signal_waiter(transfer_id)

    async def distribute(self, transfer_id: str) -> DistributeResponse:
        """Distribute a created/prepared transfer."""
        _validate_id(transfer_id, "transfer_id")
        data = await self._control.request(
            "transfer.distribute",
            {"transfer_id": transfer_id},
            transfer_id=transfer_id,
            idempotency_key=f"transfer:{transfer_id}:distribute",
        )
        return DistributeResponse.model_validate(data)

    async def _request_transfer_cancellation(self, transfer_id: str) -> TransferCancelResponse:
        _validate_id(transfer_id, "transfer_id")
        data = await self._control.request(
            "transfer.cancel",
            {"transfer_id": transfer_id},
            transfer_id=transfer_id,
            idempotency_key=f"transfer:{transfer_id}:cancel",
        )
        return TransferCancelResponse.model_validate(data)

    async def cancel(self, transfer_id: str) -> TransferCancelResponse:
        """Cancel an active transfer and stop this client's recovery for it."""
        result = await self._request_transfer_cancellation(transfer_id)
        self._control.release_recovery_lease(transfer_id)
        self._stop_recovery_signer(transfer_id)
        return result

    async def prepare(
        self,
        *,
        sources: list[PreparedHttpSource],
        destinations: list[PreparedDestination],
        transfer_id: str | None = None,
        name: str | None = None,
        urls_expires_at: str | None = None,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
    ) -> TransferPrepareResponse:
        """Prepare a signed-URL transfer and receive BeamCore's chunk plan."""
        return await self._prepare_with_request_key(
            sources=sources,
            destinations=destinations,
            transfer_id=transfer_id,
            name=name,
            urls_expires_at=urls_expires_at,
            idempotency_key=idempotency_key,
            signed_url_flow=signed_url_flow,
            route_generation_id=route_generation_id,
        )

    async def _prepare_with_request_key(
        self,
        *,
        sources: list[PreparedHttpSource],
        destinations: list[PreparedDestination],
        transfer_id: str | None = None,
        name: str | None = None,
        urls_expires_at: str | None = None,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
        request_key: str | None = None,
        provider_part_size: int | None = None,
    ) -> TransferPrepareResponse:
        effective_transfer_id = transfer_id or _transfer_id_for_idempotency_key(idempotency_key)
        _validate_id(effective_transfer_id, "transfer_id")
        prepare_idempotency_key = request_key or f"transfer:{effective_transfer_id}:prepare"
        body: dict[str, Any] = {
            "transfer_id": effective_transfer_id,
            "route_generation_id": route_generation_id
            or _route_generation_id_for_prepare_idempotency_key(prepare_idempotency_key),
            "sources": [source.model_dump(exclude_none=True) for source in sources],
            "destinations": [
                destination.model_dump(exclude_none=True) for destination in destinations
            ],
        }
        if name:
            body["name"] = name
        if provider_part_size is not None:
            # The part size a Hugging Face LFS destination dictates; never a caller preference.
            body["provider_part_size"] = provider_part_size
        if urls_expires_at:
            body["urls_expires_at"] = urls_expires_at
        body["signed_url_flow"] = signed_url_flow
        data = await self._control.request(
            "transfer.prepare",
            body,
            transfer_id=effective_transfer_id,
            idempotency_key=prepare_idempotency_key,
        )
        return TransferPrepareResponse.model_validate(data)

    async def plan(
        self,
        *,
        sources: Sequence[PreparedHttpSource | PlanningHttpSource],
        destinations: list[PreparedDestination],
        name: str | None = None,
        urls_expires_at: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
    ) -> TransferPlanResponse:
        """Plan a signed-URL transfer without creating BeamCore transfer state."""
        body: dict[str, Any] = {
            "sources": [source.model_dump(exclude_none=True) for source in sources],
            "destinations": [
                destination.model_dump(exclude_none=True) for destination in destinations
            ],
        }
        if name:
            body["name"] = name
        if urls_expires_at:
            body["urls_expires_at"] = urls_expires_at
        body["signed_url_flow"] = signed_url_flow
        data = await self._control.request("transfer.plan", body)
        return TransferPlanResponse.model_validate(data)

    async def attach_signed_urls(
        self,
        transfer_id: str,
        *,
        chunk_routes: list[SignedChunkRoute],
        multipart_group_manifest: list[MultipartGroupManifest | dict[str, Any]],
        transfer_key: str | None = None,
        urls_expires_at: str | None = None,
        auto_distribute: bool = True,
        route_generation_id: str,
        plan_fingerprint: str,
        coordinate_checksum: str,
        recovery_factory: Callable[
            [str],
            Awaitable[
                tuple[
                    list[SignedChunkRoute],
                    list[MultipartGroupManifest | dict[str, Any]],
                    str | None,
                ]
            ],
        ],
    ) -> AttachSignedUrlsResponse:
        """Stream per-chunk signed destination routes to a prepared transfer."""
        _validate_id(transfer_id, "transfer_id")
        del transfer_key

        async def stream_routes(
            routes: list[SignedChunkRoute],
            manifests: list[MultipartGroupManifest | dict[str, Any]],
            expires_at: str | None,
            generation_id: str,
        ) -> AttachSignedUrlsResponse:
            _validate_signed_route_manifest_contract(transfer_id, routes, manifests)
            destination_count = len({route.destination_id for route in routes})
            ordered_routes: list[SignedChunkRoute] = []
            for route in routes:
                if _route_delivery_index(route) is not None:
                    ordered_routes.append(route)
                elif destination_count == 1:
                    ordered_routes.append(
                        route.model_copy(update={"delivery_index": route.chunk_index})
                    )
                else:
                    raise ValueError(
                        "delivery_index is required when manually attaching routes for multiple destinations"
                    )
            ordered_routes.sort(key=_route_sort_key)
            sender = _RouteStreamSender(
                control=self._control,
                transfer_id=transfer_id,
                total_routes=len(ordered_routes),
                total_chunks=_count_distinct_route_chunks(ordered_routes),
                urls_expires_at=expires_at,
                auto_distribute=auto_distribute,
                plan_identity=f"{_route_coordinate_checksum(ordered_routes)}:{generation_id}",
                signed_url_flow="signed_url",
                route_generation_id=generation_id,
            )
            await sender.begin()
            await sender.add_manifest_groups(manifests)
            for route in ordered_routes:
                await sender.add_route(route.model_dump(exclude_none=True))
            return AttachSignedUrlsResponse.model_validate(await sender.complete())

        route_replay_lock = asyncio.Lock()
        await route_replay_lock.acquire()

        async def replay_routes(next_generation_id: str) -> None:
            async with route_replay_lock:
                routes, manifests, expires_at = await recovery_factory(next_generation_id)
                result = await stream_routes(routes, manifests, expires_at, next_generation_id)
                if not result.success:
                    raise BeamAPIError(status_code=500, detail=result.error or result.message)

        self._control.register_recovery_lease(
            transfer_id=transfer_id,
            plan_fingerprint=plan_fingerprint,
            coordinate_checksum=coordinate_checksum,
            replay_routes=replay_routes,
        )
        try:
            result = await stream_routes(
                chunk_routes,
                multipart_group_manifest,
                urls_expires_at,
                route_generation_id,
            )
            if not result.success:
                self._control.release_recovery_lease(transfer_id)
            return result
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                self._control.continue_recovery_lease(transfer_id)
                raise
            if isinstance(exc, Exception) and is_recoverable_route_stream_error(exc):
                self._control.continue_recovery_lease(transfer_id)
                raise BeamRouteRecoveryPendingError(transfer_id, exc) from exc
            self._control.release_recovery_lease(transfer_id)
            raise
        finally:
            route_replay_lock.release()

    async def plan_provider_transfer(
        self,
        *,
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        name: str | None = None,
        expires_in: int = 3600,
        signed_url_flow: SignedUrlFlow = "signed_url",
    ) -> TransferPlanResponse:
        """Plan a provider-backed transfer without creating transfer or upload state."""
        loop = asyncio.get_running_loop()
        prepared_sources = await asyncio.gather(
            *(
                loop.run_in_executor(
                    self._route_signing_executor,
                    partial(prepare_provider_source, source, index=index, expires_in=expires_in),
                )
                for index, source in enumerate(sources)
            )
        )
        prepared_destinations = [
            prepare_provider_destination(destination, index=index)
            for index, destination in enumerate(destinations)
        ]
        return await self.plan(
            sources=prepared_sources,
            destinations=prepared_destinations,
            name=name,
            signed_url_flow=signed_url_flow,
        )

    async def prepare_provider_transfer(
        self,
        *,
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        transfer_id: str | None = None,
        name: str | None = None,
        expires_in: int = 3600,
        distribute: bool = False,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
        on_before_transfer_prepare: Callable[[], Awaitable[None] | None] | None = None,
        on_prepared: Callable[[TransferPrepareResponse], Awaitable[None] | None] | None = None,
        on_multipart_group_ready: Callable[[ProviderMultipartGroupIdentity], Awaitable[None] | None]
        | None = None,
        throw_if_cancelled: Callable[..., Awaitable[None] | None] | None = None,
        ownership: BeamCancellationToken | None = None,
    ) -> TransferPrepareResponse:
        """Prepare, sign, and attach a provider-backed transfer.

        Provider credentials stay local to the SDK. BeamCore only receives
        short-lived HTTP source URLs and per-chunk destination routes.

        Hooks (sync or async), matching the TypeScript SDK:

        - ``on_before_transfer_prepare()`` runs after sources are resolved, before
          ``transfer.prepare``.
        - ``on_prepared(prepared)`` runs before any route is streamed. If it raises,
          the recovery lease keeps the prepared transfer going in the background.
        - ``on_multipart_group_ready(identity)`` runs after each multipart upload is
          created and before its routes stream. The identity has no credentials or
          URLs; persist it to call :meth:`resume_provider_transfer` later. If it
          raises, that upload is aborted and the transfer fails closed.
        - ``throw_if_cancelled(transfer_id=None)`` is polled while preparing and
          signing; raising stops the foreground call and leaves recovery running.
        - ``ownership`` is a :class:`BeamCancellationToken` fencing this owner.
          Cancelling it stops signing, route replay, and multipart creation and
          releases the recovery lease and signer. It does not cancel the transfer
          and never aborts uploads a replacement owner may use.

        ``asyncio`` cancellation of the calling task behaves like
        ``throw_if_cancelled``: the recovery lease continues the transfer.
        """
        if transfer_id:
            _validate_id(transfer_id, "transfer_id")
        return await self._execute_provider_transfer(
            sources=sources,
            destinations=destinations,
            transfer_id=transfer_id,
            name=name,
            expires_in=expires_in,
            distribute=distribute,
            idempotency_key=idempotency_key,
            signed_url_flow=signed_url_flow,
            route_generation_id=route_generation_id,
            on_before_transfer_prepare=on_before_transfer_prepare,
            on_prepared=on_prepared,
            on_multipart_group_ready=on_multipart_group_ready,
            throw_if_cancelled=throw_if_cancelled,
            ownership=ownership,
        )

    async def resume_provider_transfer(
        self,
        *,
        transfer_id: str,
        multipart_groups: Sequence[ProviderMultipartGroupIdentity | dict[str, Any]],
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        name: str | None = None,
        expires_in: int = 3600,
        distribute: bool = True,
        signed_url_flow: SignedUrlFlow = "signed_url",
        on_before_transfer_prepare: Callable[[], Awaitable[None] | None] | None = None,
        on_prepared: Callable[[TransferPrepareResponse], Awaitable[None] | None] | None = None,
        on_multipart_group_ready: Callable[[ProviderMultipartGroupIdentity], Awaitable[None] | None]
        | None = None,
        throw_if_cancelled: Callable[..., Awaitable[None] | None] | None = None,
        ownership: BeamCancellationToken | None = None,
    ) -> TransferPrepareResponse:
        """Take over an existing provider transfer in a new process (TS ``resumeProviderTransfer``).

        Re-prepares ``transfer_id`` under a fresh request key, verifies BeamCore returned
        the same transfer, and reuses the uploads in ``multipart_groups`` (the identities
        ``on_multipart_group_ready`` reported) instead of creating new ones. The set must
        cover every multipart group of the plan exactly once; otherwise a
        ``provider_multipart_recovery_*`` error is raised and nothing is created. Pair it
        with an ``ownership`` token so a replaced owner stops signing. ``distribute``
        defaults to ``True`` as in the TypeScript SDK.
        """
        _validate_id(transfer_id, "transfer_id")
        if not isinstance(multipart_groups, (list, tuple)):
            raise ValueError("provider_multipart_recovery_identity_required")
        identities = [
            group
            if isinstance(group, ProviderMultipartGroupIdentity)
            else ProviderMultipartGroupIdentity.model_validate(group)
            for group in multipart_groups
        ]
        return await self._execute_provider_transfer(
            sources=sources,
            destinations=destinations,
            transfer_id=transfer_id,
            name=name,
            expires_in=expires_in,
            distribute=distribute,
            idempotency_key=None,
            signed_url_flow=signed_url_flow,
            route_generation_id=None,
            on_before_transfer_prepare=on_before_transfer_prepare,
            on_prepared=on_prepared,
            on_multipart_group_ready=on_multipart_group_ready,
            throw_if_cancelled=throw_if_cancelled,
            ownership=ownership,
            resume_multipart_groups=identities,
        )

    async def create_transfer(
        self,
        *,
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        transfer_id: str | None = None,
        name: str | None = None,
        expires_in: int = 3600,
        distribute: bool = True,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
        on_before_transfer_prepare: Callable[[], Awaitable[None] | None] | None = None,
        on_prepared: Callable[[TransferPrepareResponse], Awaitable[None] | None] | None = None,
        on_multipart_group_ready: Callable[[ProviderMultipartGroupIdentity], Awaitable[None] | None]
        | None = None,
        throw_if_cancelled: Callable[..., Awaitable[None] | None] | None = None,
        ownership: BeamCancellationToken | None = None,
    ) -> TransferPrepareResponse:
        """TypeScript ``createTransfer``: :meth:`prepare_provider_transfer` that distributes.

        Unlike ``prepare_provider_transfer`` (whose default is kept for compatibility),
        ``distribute`` defaults to ``True`` here, as in the TypeScript SDK.
        """
        return await self.prepare_provider_transfer(
            sources=sources,
            destinations=destinations,
            transfer_id=transfer_id,
            name=name,
            expires_in=expires_in,
            distribute=distribute,
            idempotency_key=idempotency_key,
            signed_url_flow=signed_url_flow,
            route_generation_id=route_generation_id,
            on_before_transfer_prepare=on_before_transfer_prepare,
            on_prepared=on_prepared,
            on_multipart_group_ready=on_multipart_group_ready,
            throw_if_cancelled=throw_if_cancelled,
            ownership=ownership,
        )

    async def _execute_provider_transfer(
        self,
        *,
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        transfer_id: str | None,
        name: str | None,
        expires_in: int,
        distribute: bool,
        idempotency_key: str | None,
        signed_url_flow: SignedUrlFlow,
        route_generation_id: str | None,
        on_before_transfer_prepare: Callable[[], Awaitable[None] | None] | None = None,
        on_prepared: Callable[[TransferPrepareResponse], Awaitable[None] | None] | None = None,
        on_multipart_group_ready: Callable[[ProviderMultipartGroupIdentity], Awaitable[None] | None]
        | None = None,
        throw_if_cancelled: Callable[..., Awaitable[None] | None] | None = None,
        ownership: BeamCancellationToken | None = None,
        resume_multipart_groups: list[ProviderMultipartGroupIdentity] | None = None,
    ) -> TransferPrepareResponse:
        def assert_ownership() -> None:
            if ownership is not None:
                ownership.raise_if_cancelled()

        async def check_foreground_cancelled(*args: str) -> None:
            if throw_if_cancelled is not None:
                await _maybe_await(throw_if_cancelled(*args))

        assert_ownership()
        # The SDK owns deep copies of every provider config: route signing, recovery
        # signing, route replay, and multipart cleanup all use them, and releasing the
        # recovery lease scrubs their credentials. Callers' objects are never mutated.
        preparation_started = time.monotonic()
        discovery_metrics = PerformanceCollector()

        def discover(source: ProviderSourceConfig, index: int) -> PreparedHttpSource:
            token = current_performance.set(discovery_metrics)
            try:
                return prepare_provider_source(source, index=index, expires_in=expires_in)
            finally:
                current_performance.reset(token)

        retained_sources = [source.model_copy(deep=True) for source in sources]
        retained_destinations = [destination.model_copy(deep=True) for destination in destinations]
        prepared_destinations = [
            prepare_provider_destination(destination, index=index)
            for index, destination in enumerate(retained_destinations)
        ]
        destination_by_id: dict[str, ProviderDestinationConfig] = {
            prepared.destination_id: destination
            for prepared, destination in zip(
                prepared_destinations, retained_destinations, strict=True
            )
        }
        await check_foreground_cancelled()
        loop = asyncio.get_running_loop()
        prepared_sources = list(
            await asyncio.gather(
                *(
                    loop.run_in_executor(
                        self._route_signing_executor,
                        partial(discover, source, index=index),
                    )
                    for index, source in enumerate(retained_sources)
                )
            )
        )
        await check_foreground_cancelled()
        huggingface_states, provider_part_size = await self._plan_huggingface_uploads(
            sources=retained_sources,
            prepared_sources=prepared_sources,
            destinations=retained_destinations,
            prepared_destinations=prepared_destinations,
        )
        discovery_seconds = time.monotonic() - preparation_started
        await check_foreground_cancelled()
        if on_before_transfer_prepare is not None:
            await _maybe_await(on_before_transfer_prepare())
        assert_ownership()
        prepared = await self._prepare_with_request_key(
            sources=prepared_sources,
            destinations=prepared_destinations,
            transfer_id=transfer_id,
            name=name,
            idempotency_key=idempotency_key,
            signed_url_flow=signed_url_flow,
            route_generation_id=route_generation_id,
            # A resumed owner prepares under a fresh key, so it gets a new route generation.
            request_key=(
                f"transfer:{transfer_id}:prepare:resume:{uuid.uuid4()}"
                if resume_multipart_groups is not None
                else None
            ),
            provider_part_size=provider_part_size,
        )
        if resume_multipart_groups is not None and prepared.transfer_id != transfer_id:
            raise ValueError("resumed provider transfer id mismatch")
        assert_ownership()
        if not prepared.success:
            return prepared
        descriptor = _plan_descriptor(prepared)
        prepared_transfer_id = prepared.transfer_id

        if huggingface_states:
            self._assert_huggingface_plan(prepared, huggingface_states)
            self._huggingface_uploads[prepared_transfer_id] = huggingface_states
        huggingface_by_destination = {state.destination_id: state for state in huggingface_states}
        source_by_id: dict[str, ProviderSourceConfig] = {
            prepared_source.source_id: source
            for prepared_source, source in zip(prepared_sources, retained_sources, strict=True)
        }

        # Uploads this owner created (cleanup authority) and fully signed groups (recovery).
        multipart_uploads: dict[str, _MultipartUploadState] = {}
        if resume_multipart_groups is not None:
            _restore_provider_multipart_identities(
                prepared, destination_by_id, resume_multipart_groups, multipart_uploads
            )
        recovery_multipart_uploads: dict[str, _MultipartUploadState] = {}
        initial_throw_if_cancelled = throw_if_cancelled
        route_stream_lock = asyncio.Lock()
        await route_stream_lock.acquire()

        source_signature_history = SourceSignatureHistory(resume_multipart_groups is None)

        async def stream_prepared_routes(generation_id: str, recovery_replay: bool) -> None:
            foreground_cancelled = False
            telemetry_token = None
            assert_ownership()

            async def check_cancelled() -> None:
                nonlocal foreground_cancelled
                try:
                    assert_ownership()
                    if not recovery_replay and initial_throw_if_cancelled is not None:
                        await _maybe_await(initial_throw_if_cancelled(prepared_transfer_id))
                except BaseException:
                    foreground_cancelled = True
                    raise

            pending_routes: set[asyncio.Task[SignedChunkRoute]] = set()
            signing_concurrency = self._route_signing_concurrency
            signed_in_window = 0
            signing_window_started_at = time.monotonic()
            route_stream: _RouteStreamSender | None = None
            route_stream_begin_attempted = False
            manifest_task: asyncio.Task[list[MultipartGroupManifest]] | None = None
            group_waiters: dict[str, asyncio.Future[_MultipartUploadState]] = {}
            used_source_grants: WeakSet[asyncio.Task[tuple[str, dict[str, str], str]]] = WeakSet()
            try:
                stream = _RouteStreamSender(
                    control=self._control,
                    transfer_id=prepared_transfer_id,
                    total_routes=descriptor.delivery_route_count,
                    total_chunks=descriptor.logical_chunk_count,
                    auto_distribute=distribute,
                    plan_identity=f"{descriptor.plan_nonce}:{generation_id}",
                    signed_url_flow=signed_url_flow,
                    route_generation_id=generation_id,
                    urls_expires_at=expires_at_iso(expires_in),
                )
                route_stream = stream
                stream.telemetry.seed_discovery(discovery_metrics)
                stream.telemetry.source_history = source_signature_history
                telemetry_token = current_performance.set(stream.telemetry)
                stream.telemetry.gauge("signing_configured_limit", self._route_signing_concurrency)
                stream.telemetry.gauge(
                    "multipart_configured_limit", self._multipart_control_concurrency
                )
                stream.telemetry.gauge("signing_limit", signing_concurrency)
                stream.telemetry.gauge("multipart_limit", self._multipart_control_concurrency)
                if not recovery_replay:
                    stream.telemetry.started = preparation_started
                    stream.telemetry.observe_duration("sdk.discovery", discovery_seconds)
                route_stream_begin_attempted = True
                await stream.begin()
                group_waiters = _create_multipart_group_waiters(prepared, destination_by_id)

                async def on_group_ready(state: _MultipartUploadState) -> None:
                    manifest = state.manifest
                    if manifest is None:
                        raise ValueError("multipart group is missing its manifest")
                    assert_ownership()
                    recovery_multipart_uploads[manifest.multipart_group_id] = state
                    _validate_multipart_group_manifest([manifest], prepared_transfer_id)
                    callback_started = time.monotonic()
                    if on_multipart_group_ready is not None:
                        await _maybe_await(
                            on_multipart_group_ready(
                                _multipart_group_identity(prepared_transfer_id, manifest)
                            )
                        )
                    stream.telemetry.observe("sdk.multipart_callback", callback_started)
                    await stream.add_manifest_groups([manifest])
                    waiter = group_waiters.get(manifest.multipart_group_id)
                    if waiter is not None and not waiter.done():
                        waiter.set_result(state)

                def on_group_failed(group_id: str, error: BaseException) -> None:
                    waiter = group_waiters.get(group_id)
                    if waiter is None or waiter.done():
                        return
                    if isinstance(error, asyncio.CancelledError):
                        waiter.cancel()
                    else:
                        waiter.set_exception(error)

                manifest_task = asyncio.create_task(
                    self._create_multipart_group_manifest(
                        prepared=prepared,
                        destination_by_id=destination_by_id,
                        multipart_uploads=multipart_uploads,
                        expires_in=expires_in,
                        on_group_ready=on_group_ready,
                        on_group_failed=on_group_failed,
                        ownership=ownership,
                        telemetry=stream.telemetry,
                    )
                )

                async def stream_completed_routes() -> None:
                    nonlocal signing_concurrency, signed_in_window, signing_window_started_at
                    producer_started = time.monotonic()
                    done, _ = await asyncio.wait(
                        pending_routes, return_when=asyncio.FIRST_COMPLETED
                    )
                    stream.telemetry.observe("sdk.producer_wait", producer_started)
                    pending_routes.difference_update(done)
                    routes = [task.result() for task in done]
                    assert_ownership()
                    for route in routes:
                        await stream.add_route(route.model_dump(exclude_none=True))
                        signed_in_window += 1
                        if signed_in_window == ROUTE_STREAM_BATCH_ROUTES:
                            elapsed = time.monotonic() - signing_window_started_at
                            if (
                                not self._route_signing_concurrency_overridden
                                and elapsed > 4.0
                                and signing_concurrency < 256
                            ):
                                signing_concurrency = min(256, signing_concurrency * 2)
                                stream.telemetry.increment("concurrency_changes")
                                stream.telemetry.gauge("signing_limit", signing_concurrency)
                            signed_in_window = 0
                            signing_window_started_at = time.monotonic()

                async def sign_planned_route(
                    chunk: ChunkSigningPlanItem,
                    target: ChunkDestinationSigningTarget,
                    grant_task: asyncio.Task[tuple[str, dict[str, str], str]],
                ) -> SignedChunkRoute:
                    await check_cancelled()
                    destination = destination_by_id.get(target.destination_id)
                    if destination is None:
                        raise BeamAPIError(
                            status_code=500,
                            detail=f"BeamCore returned unknown destination_id: {target.destination_id}",
                        )
                    source = source_by_id.get(chunk.source_id)
                    if source is None:
                        raise BeamAPIError(
                            status_code=500,
                            detail=f"BeamCore returned unknown source_id: {chunk.source_id}",
                        )
                    metadata = target.metadata or {}
                    planned_key = metadata.get("final_object_key")
                    final_object_key = (
                        planned_key
                        if isinstance(planned_key, str) and planned_key
                        else target.object_key
                    )
                    if not final_object_key:
                        raise ValueError("destination signing target is missing object_key")
                    upload: _MultipartUploadState | None = None
                    if not _is_direct_put_destination(destination):
                        waiter = group_waiters.get(
                            _multipart_group_state_key(
                                prepared_transfer_id,
                                target.destination_id,
                                chunk.source_id,
                                final_object_key,
                            )
                        )
                        wait_started = time.monotonic()
                        upload = await asyncio.shield(waiter) if waiter is not None else None
                        stream.telemetry.observe("sdk.multipart_ready_wait", wait_started)
                        if upload is None:
                            raise ValueError(
                                "multipart group manifest is missing for "
                                f"{chunk.source_id}:{target.destination_id}"
                            )
                    planned_part_number = metadata.get("part_number")
                    part_number = (
                        planned_part_number
                        if isinstance(planned_part_number, int)
                        and not isinstance(planned_part_number, bool)
                        else multipart_part_number(chunk.source_chunk_index)
                    )
                    wait_started = time.monotonic()
                    grant = await asyncio.shield(grant_task)
                    if grant_task in used_source_grants:
                        stream.telemetry.counters["source_reuses"] += 1
                    else:
                        used_source_grants.add(grant_task)
                        stream.telemetry.source_created(chunk.chunk_index)
                    stream.telemetry.observe("sdk.source_grant_wait", wait_started)
                    started = time.monotonic()
                    route = await self._sign_provider_route(
                        source_grant=grant,
                        chunk=chunk,
                        target=target,
                        source=source,
                        destination=destination,
                        expires_in=expires_in,
                        upload=upload,
                        final_object_key=final_object_key,
                        part_number=part_number,
                        transfer_id=prepared_transfer_id,
                        signed_url_flow=signed_url_flow,
                        huggingface=huggingface_by_destination.get(target.destination_id),
                    )
                    stream.telemetry.observe("sdk.signing", started)
                    return route

                for chunk in _iter_plan_chunks(descriptor, prepared_transfer_id):
                    await check_cancelled()
                    grant_task = asyncio.create_task(
                        self._run_in_executor(
                            source_chunk_grant,
                            chunk=chunk,
                            source=source_by_id.get(chunk.source_id),
                            expires_in=expires_in,
                        )
                    )
                    grant_task.add_done_callback(_consume_future_exception)
                    for target in chunk.destinations:
                        pending_routes.add(
                            asyncio.create_task(sign_planned_route(chunk, target, grant_task))
                        )
                        stream.telemetry.gauge("signing_pending_peak", len(pending_routes))
                        if len(pending_routes) >= signing_concurrency:
                            await stream_completed_routes()
                while pending_routes:
                    await stream_completed_routes()
                manifests = await manifest_task
                _validate_multipart_group_manifest(manifests, prepared_transfer_id)
                assert_ownership()
                attached = AttachSignedUrlsResponse.model_validate(await stream.complete())
                self._report_diagnostics(stream.telemetry.snapshot())
                logger.info(
                    "beam_provider_signed_routes_streamed transfer_id=%s route_count=%s multipart_upload_count=%s",
                    prepared_transfer_id,
                    descriptor.delivery_route_count,
                    len(manifests),
                )
                if not attached.success:
                    raise BeamAPIError(
                        status_code=500,
                        detail=attached.error or attached.message or "route stream failed",
                    )
            except BaseException as exc:
                if ownership is not None and ownership.cancelled:
                    # A replacement owner may be streaming now: stop this owner's
                    # recovery without cancelling the transfer or aborting uploads.
                    self._stop_recovery_signer(prepared_transfer_id, prepared=prepared)
                    self._control.release_recovery_lease(
                        prepared_transfer_id, replay_routes=replay_routes
                    )
                    for task in pending_routes:
                        task.cancel()
                    if pending_routes:
                        await asyncio.gather(*pending_routes, return_exceptions=True)
                    if manifest_task is not None:
                        await asyncio.gather(manifest_task, return_exceptions=True)
                    reason = ownership.reason
                    if reason is None or reason is exc or isinstance(exc, asyncio.CancelledError):
                        raise
                    raise reason from exc
                if route_stream is not None:
                    await route_stream.abort()
                for task in pending_routes:
                    task.cancel()
                if pending_routes:
                    await asyncio.gather(*pending_routes, return_exceptions=True)
                if manifest_task is not None:
                    # Never cancel multipart creation mid-flight: a created upload must be
                    # recorded so that cleanup can abort it.
                    await asyncio.gather(manifest_task, return_exceptions=True)
                if recovery_replay:
                    raise
                if foreground_cancelled or isinstance(exc, asyncio.CancelledError):
                    # The foreground task stopped; the owned lease keeps the transfer going.
                    self._control.continue_recovery_lease(prepared_transfer_id)
                    raise
                if isinstance(exc, Exception) and is_recoverable_route_stream_error(exc):
                    self._control.continue_recovery_lease(prepared_transfer_id)
                    raise BeamRouteRecoveryPendingError(prepared_transfer_id, exc) from exc
                try:
                    await self._cancel_and_abort_provider_failure(
                        prepared_transfer_id,
                        exc,
                        multipart_uploads,
                        abort_before_cancel=not route_stream_begin_attempted,
                    )
                finally:
                    # The retained configs own the credentials multipart cleanup uses, so
                    # the lease (whose disposal scrubs them) is released only afterwards.
                    self._control.release_recovery_lease(prepared_transfer_id)
                    self._stop_recovery_signer(prepared_transfer_id)

            finally:
                if telemetry_token is not None:
                    current_performance.reset(telemetry_token)

        async def replay_routes(next_generation_id: str) -> None:
            async with route_stream_lock:
                await stream_prepared_routes(next_generation_id, True)

        remove_ownership_callback: Callable[[], None] | None = None

        def dispose_recovery_inputs() -> None:
            if remove_ownership_callback is not None:
                remove_ownership_callback()
            release_provider_clients([*retained_sources, *retained_destinations])
            _clear_recovery_secrets([*retained_sources, *retained_destinations])

        def stop_owned_recovery() -> None:
            self._stop_recovery_signer(prepared_transfer_id, prepared=prepared)
            self._control.release_recovery_lease(prepared_transfer_id, replay_routes=replay_routes)

        def on_ownership_lost() -> None:
            # The token may be cancelled from any thread; recovery state lives on the loop.
            try:
                running_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            if running_loop is loop:
                stop_owned_recovery()
            elif not loop.is_closed():
                loop.call_soon_threadsafe(stop_owned_recovery)

        try:
            await self._start_provider_route_recovery_signer(
                prepared=prepared,
                sources_by_id=source_by_id,
                destinations_by_id=destination_by_id,
                multipart_uploads=recovery_multipart_uploads,
                huggingface_by_destination=huggingface_by_destination,
                expires_in=expires_in,
                ownership=ownership,
            )
            self._control.register_recovery_lease(
                transfer_id=prepared_transfer_id,
                plan_fingerprint=prepared.plan_fingerprint,
                coordinate_checksum=prepared.coordinate_checksum,
                replay_routes=replay_routes,
                dispose=dispose_recovery_inputs,
            )
            if ownership is not None:
                remove_ownership_callback = ownership.add_callback(on_ownership_lost)
            try:
                assert_ownership()
                if on_prepared is not None:
                    await _maybe_await(on_prepared(prepared))
            except BaseException:
                self._control.continue_recovery_lease(prepared_transfer_id)
                raise
            try:
                if throw_if_cancelled is not None:
                    await _maybe_await(throw_if_cancelled(prepared_transfer_id))
            except BaseException:
                self._control.continue_recovery_lease(prepared_transfer_id)
                raise
            await stream_prepared_routes(prepared.route_generation_id, False)
            initial_throw_if_cancelled = None
        finally:
            route_stream_lock.release()
        return prepared

    async def _create_multipart_group_manifest(
        self,
        *,
        prepared: TransferPrepareResponse,
        destination_by_id: dict[str, ProviderDestinationConfig],
        multipart_uploads: dict[str, _MultipartUploadState],
        expires_in: int,
        on_group_ready: Callable[[_MultipartUploadState], Awaitable[None]],
        on_group_failed: Callable[[str, BaseException], None],
        ownership: BeamCancellationToken | None = None,
        telemetry: PerformanceCollector | None = None,
    ) -> list[MultipartGroupManifest]:
        """Create or reuse each multipart upload, bounded by multipart_control_concurrency."""
        descriptor = _plan_descriptor(prepared)
        groups = [
            (source, destination)
            for source in descriptor.sources
            for destination in descriptor.destinations
        ]

        queued_at = time.monotonic()
        active_multipart = 0

        async def create_group(
            group: tuple[Any, Any],
        ) -> tuple[_MultipartUploadState | None, BaseException | None]:
            nonlocal active_multipart
            active_multipart += 1
            if telemetry is not None:
                telemetry.observe("sdk.multipart_queue", queued_at)
                telemetry.gauge("multipart_active_peak", active_multipart)
            source, destination_plan = group
            group_id: str | None = None
            try:
                destination = destination_by_id.get(destination_plan.destination_id)
                if destination is None:
                    raise ValueError(
                        f"BeamCore returned unknown destination_id: {destination_plan.destination_id}"
                    )
                if _is_direct_put_destination(destination):
                    return None, None
                final_object_key = destination_plan.final_object_keys.get(source.source_id)
                if not final_object_key:
                    raise ValueError(
                        "plan final object key not found: "
                        f"{source.source_id}:{destination_plan.destination_id}"
                    )
                group_id = _multipart_group_state_key(
                    prepared.transfer_id,
                    destination_plan.destination_id,
                    source.source_id,
                    final_object_key,
                )
                raise_if_cancelled(ownership)
                retained = multipart_uploads.get(group_id)
                if retained is not None:
                    upload_id = retained.upload_id
                else:
                    create_started = time.monotonic()
                    upload_id = await self._run_in_executor(
                        create_multipart_upload,
                        destination=destination,
                        object_key=final_object_key,
                        metadata={"beam-transfer-id": prepared.transfer_id},
                    )
                    if telemetry is not None:
                        telemetry.observe("sdk.multipart_create", create_started)
                    multipart_uploads[group_id] = _MultipartUploadState(
                        destination=destination,
                        object_key=final_object_key,
                        upload_id=upload_id,
                    )
                created_upload = retained is None
                try:
                    manifest = await self._run_in_executor(
                        self._sign_multipart_group_manifest_sync,
                        destination=destination,
                        transfer_id=prepared.transfer_id,
                        multipart_group_id=group_id,
                        source_id=source.source_id,
                        destination_id=destination_plan.destination_id,
                        final_object_key=final_object_key,
                        upload_id=upload_id,
                        expected_object_size=source.size,
                        chunk_count=source.chunk_count,
                        expires_in=expires_in,
                    )
                    state = _MultipartUploadState(
                        destination=destination,
                        object_key=final_object_key,
                        upload_id=upload_id,
                        manifest=manifest,
                    )
                    multipart_uploads[group_id] = state
                    await on_group_ready(state)
                    return state, None
                except BaseException as error:
                    # After losing ownership the upload may belong to a replacement owner.
                    if created_upload and not (ownership is not None and ownership.cancelled):
                        try:
                            await self._run_in_executor(
                                abort_multipart_upload,
                                destination=destination,
                                object_key=final_object_key,
                                upload_id=upload_id,
                            )
                            multipart_uploads.pop(group_id, None)
                        except Exception as abort_error:
                            raise RuntimeError(
                                f"multipart group setup and cleanup failed for {group_id} "
                                f"({_safe_error_code(error)}; {_safe_error_code(abort_error)})"
                            ) from error
                    raise
            except asyncio.CancelledError as error:
                if group_id is not None:
                    on_group_failed(group_id, error)
                raise
            except Exception as error:
                if group_id is not None:
                    on_group_failed(group_id, error)
                return None, error
            finally:
                active_multipart -= 1

        results = await _map_ordered_with_concurrency(
            groups, self._multipart_control_concurrency, create_group
        )
        failure = next((error for _, error in results if error is not None), None)
        if failure is not None:
            raise failure
        return [state.manifest for state, _ in results if state is not None and state.manifest]

    @staticmethod
    def _sign_multipart_group_manifest_sync(
        *,
        destination: ProviderDestinationConfig,
        transfer_id: str,
        multipart_group_id: str,
        source_id: str,
        destination_id: str,
        final_object_key: str,
        upload_id: str,
        expected_object_size: int,
        chunk_count: int,
        expires_in: int,
    ) -> MultipartGroupManifest:
        max_part_number = _multipart_max_part_number(chunk_count)
        return MultipartGroupManifest(
            multipart_group_id=multipart_group_id,
            source_id=source_id,
            destination_id=destination_id,
            final_object_key=final_object_key,
            upload_id=upload_id,
            expected_object_size=expected_object_size,
            expected_part_count=chunk_count,
            max_part_number=max_part_number,
            complete_url=sign_complete_multipart_upload(
                destination=destination,
                object_key=final_object_key,
                upload_id=upload_id,
                expires_in=expires_in,
            ),
            abort_url=sign_abort_multipart_upload(
                destination=destination,
                object_key=final_object_key,
                upload_id=upload_id,
                expires_in=expires_in,
            ),
            list_page_urls=[
                sign_list_multipart_upload(
                    destination=destination,
                    object_key=final_object_key,
                    upload_id=upload_id,
                    expires_in=expires_in,
                    max_parts=1_000,
                    part_number_marker=marker or None,
                )
                for marker in _multipart_list_page_markers(max_part_number)
            ],
            final_head_url=sign_final_object_head(
                destination=destination,
                object_key=final_object_key,
                expires_in=expires_in,
            ),
            final_object_metadata={"beam-transfer-id": transfer_id},
            urls_expires_at=expires_at_iso(expires_in),
        )

    async def _start_provider_route_recovery_signer(
        self,
        *,
        prepared: TransferPrepareResponse,
        sources_by_id: dict[str, ProviderSourceConfig],
        destinations_by_id: dict[str, ProviderDestinationConfig],
        multipart_uploads: dict[str, _MultipartUploadState],
        huggingface_by_destination: dict[str, _HuggingFaceUploadState],
        expires_in: int,
        ownership: BeamCancellationToken | None = None,
    ) -> None:
        transfer_id = prepared.transfer_id
        descriptor = _plan_descriptor(prepared)
        self._stop_recovery_signer(transfer_id)

        async def sign_recovery_routes(payload: dict[str, Any]) -> dict[str, Any]:
            raise_if_cancelled(ownership)
            route_generation_id = payload.get("route_generation_id")
            if not isinstance(route_generation_id, str) or not route_generation_id:
                raise ValueError("route recovery generation is required")
            requested_chunks = payload.get("chunks")
            if not isinstance(requested_chunks, list) or not requested_chunks:
                raise ValueError("route recovery chunks are required")

            source_grants: dict[tuple[str, int], asyncio.Task[tuple[str, dict[str, str], str]]] = {}

            async def sign_requested_route(requested: Any) -> SignedChunkRoute:
                raise_if_cancelled(ownership)
                if not isinstance(requested, dict):
                    raise ValueError("route recovery chunk must be an object")

                def requested_int(key: str) -> int:
                    value = requested.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        raise ValueError(f"route recovery {key} must be a non-negative integer")
                    return value

                if requested.get("route_generation_id") != route_generation_id:
                    raise ValueError("route recovery chunk generation mismatch")
                source_id = str(requested.get("source_id") or "")
                destination_id = str(requested.get("destination_id") or "")
                chunk_index = requested_int("chunk_index")
                delivery_index = requested_int("delivery_index")
                logical_attempt_index = requested_int("logical_attempt_index")
                attempt_slot = requested_int("attempt_slot")
                part_number = requested_int("part_number")
                if attempt_slot != 0:
                    raise ValueError("route recovery attempt_slot must be 0")
                chunk = _materialize_plan_chunk(descriptor, transfer_id, source_id, chunk_index)
                if part_number != multipart_part_number(chunk.source_chunk_index, attempt_slot):
                    raise ValueError("route recovery multipart slot mapping mismatch")
                if (
                    requested_int("source_offset") != chunk.source_offset
                    or requested_int("chunk_size") != chunk.chunk_size
                ):
                    raise ValueError("route recovery source coordinate mismatch")
                target = next(
                    (
                        candidate
                        for candidate in chunk.destinations
                        if candidate.destination_id == destination_id
                        and candidate.metadata.get("delivery_index") == delivery_index
                    ),
                    None,
                )
                if target is None:
                    raise ValueError("route recovery destination coordinate mismatch")
                source = sources_by_id.get(source_id)
                destination = destinations_by_id.get(destination_id)
                if source is None or destination is None:
                    raise ValueError("route recovery provider configuration is unavailable")
                multipart_group_id = str(requested.get("multipart_group_id") or "")
                final_object_key = str(requested.get("final_object_key") or "")
                upload_id = str(requested.get("upload_id") or "")
                destination_metadata = requested.get("destination_metadata")
                if destination_metadata is not None and not isinstance(
                    destination_metadata,
                    dict,
                ):
                    raise ValueError("route recovery destination_metadata must be an object")
                upload = (
                    None
                    if _is_direct_put_destination(destination)
                    else await self._recovery_multipart_upload_state(
                        prepared=prepared,
                        destination=destination,
                        source_id=source_id,
                        destination_id=destination_id,
                        final_object_key=final_object_key,
                        upload_id=upload_id,
                        multipart_group_id=multipart_group_id,
                        multipart_uploads=multipart_uploads,
                        expires_in=expires_in,
                    )
                )
                recovery_target = target.model_copy(
                    update={
                        "object_key": final_object_key,
                        "metadata": {
                            **target.metadata,
                            **(destination_metadata or {}),
                            "transfer_id": transfer_id,
                            "final_object_key": final_object_key,
                            "upload_id": upload_id,
                            "multipart_group_id": multipart_group_id,
                            "delivery_index": delivery_index,
                            "part_number": part_number,
                            "logical_attempt_index": logical_attempt_index,
                            "attempt_slot": attempt_slot,
                            "route_generation_id": route_generation_id,
                        },
                    }
                )
                source_key = (chunk.source_id, chunk.chunk_index)
                grant_task = source_grants.get(source_key)
                if grant_task is None:
                    grant_task = asyncio.create_task(
                        self._run_in_executor(
                            source_chunk_grant, chunk=chunk, source=source, expires_in=expires_in
                        )
                    )
                    grant_task.add_done_callback(_consume_future_exception)
                    source_grants[source_key] = grant_task
                route = await self._sign_provider_route(
                    source_grant=await asyncio.shield(grant_task),
                    chunk=chunk,
                    target=recovery_target,
                    source=source,
                    destination=destination,
                    expires_in=expires_in,
                    upload=upload,
                    final_object_key=final_object_key,
                    part_number=part_number,
                    transfer_id=transfer_id,
                    signed_url_flow="signed_url",
                    huggingface=huggingface_by_destination.get(destination_id),
                )
                raise_if_cancelled(ownership)
                # v7 multipart recovery: renew direct controls, or sign a staged
                # recovery object plus UploadPartCopy, exactly as the TypeScript SDK.
                return await self._run_in_executor(
                    sign_multipart_recovery,
                    destination=destination,
                    transfer_id=transfer_id,
                    requested=requested,
                    route=route,
                    expires_in=expires_in,
                )

            routes = await _map_ordered_with_concurrency(
                requested_chunks, self._route_signing_concurrency, sign_requested_route
            )
            return {
                "transfer_id": transfer_id,
                "route_generation_id": route_generation_id,
                "signed_at": iso_now(),
                "chunk_routes": [route.model_dump(exclude_none=True) for route in routes],
            }

        await self._control.serve_route_recovery_signer(transfer_id, sign_recovery_routes)
        raise_if_cancelled(ownership)
        self._integrity_context[transfer_id] = (
            prepared,
            sources_by_id,
            destinations_by_id,
            expires_in,
        )

        async def sign_integrity(challenge: dict[str, Any]) -> dict[str, Any]:
            context = self._integrity_context.get(transfer_id)
            if context is None or context[0] is not prepared:
                raise RuntimeError("integrity signer ownership replaced")
            return await self._signed_integrity_grants(challenge)

        # Optional request/reply signing can retry through normal status reconciliation.
        with suppress(Exception):
            await self._control.serve_integrity_signer(transfer_id, sign_integrity)

    async def _recovery_multipart_upload_state(
        self,
        *,
        prepared: TransferPrepareResponse,
        destination: ProviderDestinationConfig,
        source_id: str,
        destination_id: str,
        final_object_key: str,
        upload_id: str,
        multipart_group_id: str,
        multipart_uploads: dict[str, _MultipartUploadState],
        expires_in: int,
    ) -> _MultipartUploadState:
        """Reuse a signed multipart group, or rebuild its controls from the requested upload id."""
        existing = multipart_uploads.get(multipart_group_id)
        if existing is not None:
            manifest = existing.manifest
            if (
                manifest is None
                or existing.upload_id != upload_id
                or existing.object_key != final_object_key
                or manifest.source_id != source_id
                or manifest.destination_id != destination_id
            ):
                raise ValueError("route recovery multipart identity mismatch")
            return existing
        if not upload_id:
            raise ValueError("route recovery multipart upload id is required")
        source = next(
            (
                candidate
                for candidate in _plan_descriptor(prepared).sources
                if candidate.source_id == source_id
            ),
            None,
        )
        if source is None:
            raise ValueError("route recovery source plan is unavailable")
        expected_group_id = _multipart_group_state_key(
            prepared.transfer_id, destination_id, source_id, final_object_key
        )
        if multipart_group_id != expected_group_id:
            raise ValueError("route recovery multipart group identity mismatch")
        manifest = await self._run_in_executor(
            self._sign_multipart_group_manifest_sync,
            destination=destination,
            transfer_id=prepared.transfer_id,
            multipart_group_id=multipart_group_id,
            source_id=source_id,
            destination_id=destination_id,
            final_object_key=final_object_key,
            upload_id=upload_id,
            expected_object_size=source.size,
            chunk_count=source.chunk_count,
            expires_in=expires_in,
        )
        _validate_multipart_group_manifest([manifest], prepared.transfer_id)
        state = _MultipartUploadState(
            destination=destination,
            object_key=final_object_key,
            upload_id=upload_id,
            manifest=manifest,
        )
        multipart_uploads[multipart_group_id] = state
        return state

    async def _plan_huggingface_uploads(
        self,
        *,
        sources: list[ProviderSourceConfig],
        prepared_sources: list[PreparedHttpSource],
        destinations: list[ProviderDestinationConfig],
        prepared_destinations: list[PreparedDestination],
    ) -> tuple[list[_HuggingFaceUploadState], int | None]:
        """Negotiate every Hugging Face destination before the plan exists.

        The Hub will not issue upload URLs without the object's sha256, and it chooses the
        part size itself, so this runs first and the prepare request then carries that size
        as ``provider_part_size``.
        """
        targets = [
            (index, destination)
            for index, destination in enumerate(destinations)
            if isinstance(destination, HuggingFaceProviderDestination)
        ]
        if not targets:
            return [], None

        if len(prepared_sources) != 1:
            raise ValueError(
                "a huggingface destination requires exactly one source: the Hub dictates the "
                "part size and the plan carries a single chunk size, but "
                f"{len(prepared_sources)} sources were given"
            )
        prepared_source = prepared_sources[0]
        source_config = sources[0]

        # For an LFS source the Hub already published the sha256 as the linked ETag.
        published_sha256 = (
            prepared_source.metadata.get("sha256")
            if isinstance(source_config, HuggingFaceProviderSource)
            else None
        )

        loop = asyncio.get_running_loop()
        states: list[_HuggingFaceUploadState] = []
        provider_part_size: int | None = None

        for index, destination in targets:
            if destination.repo_type == "bucket":
                raise ValueError(
                    f"{destination.repo_id} is a Hugging Face bucket, which Beam cannot write to. "
                    "Buckets expose no LFS batch endpoint; their only upload path is the Hub's Xet "
                    "CAS client, which cannot be expressed as presigned URLs for Beam's workers. "
                    "Buckets do work as a transfer source. Use `hf sync` to write to a bucket."
                )
            path = _huggingface_target_path(destination, prepared_source.filename)
            config = destination.model_copy(update={"path": path})

            oid = published_sha256
            if not oid:
                if not destination.allow_source_rehash:
                    raise ValueError(
                        f"uploading to {hf.describe(config)} needs the source sha256, which the "
                        "Hub requires before it issues upload URLs. The SDK must read the source "
                        "once to compute it; set allow_source_rehash=True to opt in."
                    )
                hashed, _ = await loop.run_in_executor(
                    self._route_signing_executor,
                    partial(hf.hash_source_stream, prepared_source.url),
                )
                oid = str(hashed)

            sample = await loop.run_in_executor(
                self._route_signing_executor,
                partial(hf.read_source_sample, prepared_source.url),
            )
            upload_mode, should_ignore = await loop.run_in_executor(
                self._route_signing_executor,
                partial(hf.preupload, config, prepared_source.size, sample),
            )
            if should_ignore:
                raise ValueError(f"{hf.describe(config)} is excluded by the repo's .gitignore")
            if upload_mode != "lfs":
                raise ValueError(
                    f"{hf.describe(config)} would be committed as a regular git blob, not an LFS "
                    "blob. Beam uploads through the LFS protocol only; add the path to "
                    ".gitattributes as LFS."
                )

            plan = await loop.run_in_executor(
                self._route_signing_executor,
                partial(hf.lfs_batch, config, oid, prepared_source.size),
            )

            if plan.chunk_size is not None:
                if provider_part_size is not None and provider_part_size != plan.chunk_size:
                    raise ValueError(
                        f"huggingface destinations disagree on part size ({provider_part_size} vs "
                        f"{plan.chunk_size}); the plan carries a single chunk size"
                    )
                provider_part_size = plan.chunk_size

            state = _HuggingFaceUploadState(
                destination=config,
                destination_id=prepared_destinations[index].destination_id,
                source_id=prepared_source.source_id,
                oid=oid,
                size=prepared_source.size,
                chunk_size=plan.chunk_size,
                part_urls=plan.part_urls,
                upload_href=plan.upload_href,
                verify_href=plan.verify_href,
            )
            if plan.chunk_size is not None:
                # Runs alongside the transfer; the ETags are only needed at completion time.
                state.part_etags = asyncio.ensure_future(
                    loop.run_in_executor(
                        self._route_signing_executor,
                        partial(
                            _huggingface_part_etags,
                            prepared_source.url,
                            plan.chunk_size,
                        ),
                    )
                )
            states.append(state)

        return states, provider_part_size

    @staticmethod
    def _assert_huggingface_plan(
        prepared: TransferPrepareResponse, states: list[_HuggingFaceUploadState]
    ) -> None:
        """Fail before any byte moves if BeamCore did not adopt the Hub's part layout."""
        for state in states:
            plan_destination = next(
                (
                    candidate
                    for candidate in _plan_descriptor(prepared).destinations
                    if candidate.destination_id == state.destination_id
                ),
                None,
            )
            plan_source = next(
                (
                    candidate
                    for candidate in _plan_descriptor(prepared).sources
                    if candidate.source_id == state.source_id
                ),
                None,
            )
            if plan_destination is None or plan_source is None:
                raise ValueError(
                    "BeamCore plan is missing the huggingface coordinate "
                    f"{state.source_id}:{state.destination_id}"
                )

            final_object_key = plan_destination.final_object_keys.get(state.source_id)
            if final_object_key != state.destination.path:
                raise ValueError(
                    f"BeamCore planned {final_object_key} but the Hub upload was negotiated "
                    f"for {state.destination.path}"
                )

            if state.chunk_size is None:
                if plan_source.chunk_count != 1:
                    raise ValueError(
                        f"{hf.describe(state.destination)} was issued a single-part upload, but "
                        f"the plan has {plan_source.chunk_count} chunks"
                    )
                continue

            if _plan_descriptor(prepared).chunk_size != state.chunk_size:
                raise ValueError(
                    f"the Hub requires {state.chunk_size}-byte parts for "
                    f"{hf.describe(state.destination)}, but BeamCore planned "
                    f"{_plan_descriptor(prepared).chunk_size}-byte chunks"
                )
            if plan_source.chunk_count != len(state.part_urls):
                raise ValueError(
                    f"the Hub issued {len(state.part_urls)} part URLs for "
                    f"{hf.describe(state.destination)}, but the plan has "
                    f"{plan_source.chunk_count} chunks"
                )

    async def finalize_huggingface_uploads(self, transfer_id: str) -> None:
        """Close every Hugging Face upload for a transfer.

        Completes the LFS multipart, verifies it, and commits the blob so the file appears in
        the repo. Runs only after the transfer is complete, so it never sits in the transfer's
        progression path.
        """
        states = self._huggingface_uploads.pop(transfer_id, None)
        if not states:
            return

        loop = asyncio.get_running_loop()
        for state in states:
            if state.upload_href is not None and state.chunk_size is not None:
                etags = await state.part_etags if state.part_etags is not None else []
                if len(etags) != len(state.part_urls):
                    raise ValueError(
                        f"computed {len(etags)} part ETags for {hf.describe(state.destination)}, "
                        f"expected {len(state.part_urls)}"
                    )
                await loop.run_in_executor(
                    self._route_signing_executor,
                    partial(
                        hf.complete_lfs_upload,
                        state.destination,
                        state.upload_href,
                        state.oid,
                        etags,
                    ),
                )

            if state.verify_href is not None:
                await loop.run_in_executor(
                    self._route_signing_executor,
                    partial(
                        hf.verify_lfs_upload,
                        state.destination,
                        state.verify_href,
                        state.oid,
                        state.size,
                    ),
                )

            await loop.run_in_executor(
                self._route_signing_executor,
                partial(hf.commit, state.destination, state.oid, state.size),
            )

    async def _sign_provider_route(
        self,
        **kwargs: Any,
    ) -> SignedChunkRoute:
        return await self._run_in_executor(self._sign_provider_route_sync, **kwargs)

    @staticmethod
    def _sign_provider_route_sync(
        *,
        chunk: ChunkSigningPlanItem,
        target: ChunkDestinationSigningTarget,
        source: ProviderSourceConfig,
        destination: ProviderDestinationConfig,
        expires_in: int,
        upload: _MultipartUploadState | None,
        final_object_key: str,
        part_number: int,
        transfer_id: str,
        signed_url_flow: SignedUrlFlow,
        huggingface: _HuggingFaceUploadState | None = None,
        source_grant: tuple[str, dict[str, str], str] | None = None,
    ) -> SignedChunkRoute:
        del transfer_id
        if isinstance(destination, HuggingFaceProviderDestination):
            if huggingface is None:
                raise ValueError(f"huggingface upload state is missing for {target.destination_id}")
            # The Hub presigns its own part targets; chunk N carries the URL for part N + 1.
            dest_url = (
                huggingface.upload_href
                if huggingface.chunk_size is None
                else (
                    huggingface.part_urls[chunk.source_chunk_index]
                    if chunk.source_chunk_index < len(huggingface.part_urls)
                    else None
                )
            )
            if not dest_url:
                raise ValueError(
                    f"the Hub issued no upload URL for chunk {chunk.source_chunk_index} of "
                    f"{hf.describe(destination)}"
                )
            return sign_destination_route(
                source_grant=source_grant,
                chunk=chunk,
                target=target.model_copy(
                    update={
                        "object_key": final_object_key,
                        "metadata": _direct_put_route_metadata(target.metadata),
                    }
                ),
                source=source,
                destination=destination,
                expires_in=expires_in,
                dest_url=dest_url,
            )

        if isinstance(destination, HippiusProviderDestination):
            return sign_destination_route(
                source_grant=source_grant,
                chunk=chunk,
                target=target.model_copy(
                    update={"metadata": _part_route_metadata(target.metadata)}
                ),
                source=source,
                destination=destination,
                expires_in=expires_in,
            )

        manifest = upload.manifest if upload is not None else None
        if upload is None or manifest is None:
            raise ValueError(f"{signed_url_flow} route is missing its multipart upload")
        _validate_multipart_part_number(part_number, manifest)
        return sign_destination_route(
            source_grant=source_grant,
            chunk=chunk,
            target=target.model_copy(
                update={
                    "object_key": final_object_key,
                    "metadata": _part_route_metadata(target.metadata),
                }
            ),
            source=source,
            destination=destination,
            expires_in=expires_in,
            part_number=part_number,
            multipart_group_id=manifest.multipart_group_id,
            upload_id=upload.upload_id,
            final_object_key=final_object_key,
            complete_url=manifest.complete_url,
            abort_url=manifest.abort_url,
            list_page_url=manifest.list_page_urls[(part_number - 1) // 1_000],
            final_head_url=manifest.final_head_url,
            expected_object_size=manifest.expected_object_size,
            expected_part_count=manifest.expected_part_count,
            max_part_number=manifest.max_part_number,
            final_object_metadata=dict(manifest.final_object_metadata),
        )

    async def create_and_distribute(
        self,
        *,
        sources: list[SourceConfig],
        destinations: list[DestConfig],
        total_size: int,
        name: str | None = None,
        merkle_root: str | None = None,
        chunk_hashes: list[str] | None = None,
        callbacks: list[CallbackConfig] | None = None,
        progressive_mode: bool = False,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
    ) -> TransferCreateResponse:
        """Create a transfer and immediately distribute it."""
        result = await self.create(
            sources=sources,
            destinations=destinations,
            total_size=total_size,
            name=name,
            merkle_root=merkle_root,
            chunk_hashes=chunk_hashes,
            callbacks=callbacks,
            progressive_mode=progressive_mode,
            idempotency_key=idempotency_key,
            signed_url_flow=signed_url_flow,
        )
        if result.success:
            await self.distribute(result.transfer_id)
        return result

    async def wait_complete(
        self,
        transfer_id: str,
        *,
        timeout: float = 300.0,
        poll_interval: float = 15.0,
        max_poll_interval: float = 30.0,
    ) -> TransferStatusInfo:
        """Wait terminal-first and reconcile status until completion, failure, cancellation, or timeout.

        A failed transfer raises :class:`BeamTransferFailedError`, or its
        :class:`BeamStorageAccessError` subclass when storage refused Beam's requests.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if poll_interval <= 0 or max_poll_interval < poll_interval:
            raise ValueError(
                "poll intervals must be positive and max_poll_interval must be at least poll_interval"
            )
        loop = asyncio.get_running_loop()
        start = loop.time()
        current_poll_interval = poll_interval
        waiter: TransferTerminalSignalWaiter | None = None
        try:
            waiter = await self.open_terminal_signal_waiter(transfer_id)
        except Exception as exc:
            logger.warning(
                "beam_terminal_signal_unavailable transfer_id=%s diagnostic=%s",
                transfer_id,
                safe_error_diagnostic(exc, retryable=True),
            )
        try:
            while True:
                status = await self.status(transfer_id)
                if status.is_complete:
                    # The parts have landed; publish them as a Hub commit before reporting success.
                    await self.finalize_huggingface_uploads(transfer_id)
                    return status
                if status.status == "failed":
                    raise transfer_failed_error(transfer_id, status.error_message)
                if status.status == "cancelled":
                    raise BeamAPIError(status_code=409, detail="Transfer cancelled")
                elapsed = loop.time() - start
                if elapsed >= timeout:
                    raise BeamTimeoutError(
                        f"Transfer {transfer_id} did not complete within {timeout}s "
                        f"(last status: {status.status})"
                    )
                wait_seconds = min(
                    timeout - elapsed,
                    current_poll_interval * (0.85 + 0.3 * (uuid.uuid4().int / (1 << 128))),
                )
                event: TransferTerminalEvent | None = None
                if waiter is not None:
                    wait_started = loop.time()
                    try:
                        event = await waiter.wait(max(0.001, wait_seconds))
                    except Exception as exc:
                        logger.warning(
                            "beam_terminal_signal_failed transfer_id=%s diagnostic=%s",
                            transfer_id,
                            safe_error_diagnostic(exc, retryable=True),
                        )
                        with suppress(Exception):
                            await waiter.close()
                        waiter = None
                        remaining_wait = max(0.0, wait_seconds - (loop.time() - wait_started))
                        if remaining_wait > 0:
                            await asyncio.sleep(remaining_wait)
                else:
                    await asyncio.sleep(max(0.001, wait_seconds))
                if event is not None:
                    current_poll_interval = poll_interval
                else:
                    current_poll_interval = min(max_poll_interval, current_poll_interval * 1.5)
        finally:
            if waiter is not None:
                try:
                    await waiter.close()
                except Exception as exc:
                    logger.warning(
                        "beam_terminal_signal_close_failed transfer_id=%s diagnostic=%s",
                        transfer_id,
                        safe_error_diagnostic(exc, retryable=False),
                    )

    async def _cancel_after_provider_failure(self, transfer_id: str, cause: BaseException) -> None:
        """Cancel without releasing the lease: cleanup still needs its retained credentials."""
        try:
            result = await self._request_transfer_cancellation(transfer_id)
            if not result.success:
                raise RuntimeError(
                    result.message or f"Beam rejected cancellation for {transfer_id}"
                )
        except Exception as cancel_exc:
            raise RuntimeError(
                f"provider transfer failed ({_safe_error_code(cause)}) and transfer "
                f"cancellation failed ({_safe_error_code(cancel_exc)})"
            ) from cancel_exc

    async def _cancel_and_abort_provider_failure(
        self,
        transfer_id: str,
        cause: BaseException,
        multipart_uploads: dict[str, _MultipartUploadState],
        *,
        abort_before_cancel: bool,
    ) -> NoReturn:
        """Cancel the transfer and abort created uploads, then raise the aggregated failure.

        When the route stream never began, BeamCore holds no route that could write a
        part, so uploads are aborted first. Otherwise the transfer is cancelled first so
        that no worker completes a part after its upload is aborted; a failed abort is
        retried once after cancellation.
        """
        cancel_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        if abort_before_cancel:
            try:
                await self._abort_created_uploads(multipart_uploads)
            except Exception as exc:
                cleanup_error = exc
        try:
            await self._cancel_after_provider_failure(transfer_id, cause)
        except Exception as exc:
            cancel_error = exc
        if not abort_before_cancel or cleanup_error is not None:
            try:
                await self._abort_created_uploads(multipart_uploads)
                cleanup_error = None
            except Exception as exc:
                cleanup_error = exc
        raise BeamProviderTransferError(
            transfer_id,
            cause,
            cancel_error=cancel_error,
            cleanup_error=cleanup_error,
        ) from cause

    async def _abort_created_uploads(
        self, multipart_uploads: dict[str, _MultipartUploadState]
    ) -> None:
        """Abort created uploads, bounded by multipart_control_concurrency.

        Aborted uploads are removed, so a retry only touches the ones that failed.
        """
        candidates = [
            (group_id, upload)
            for group_id, upload in list(multipart_uploads.items())
            if upload.upload_id and not isinstance(upload.destination, HippiusProviderDestination)
        ]

        async def abort_upload(
            candidate: tuple[str, _MultipartUploadState],
        ) -> BaseException | None:
            group_id, upload = candidate
            logger.warning(
                "beam_provider_multipart_abort operation=abort_multipart correlation_token=%s",
                opaque_correlation_token("abort_multipart", upload.upload_id, upload.object_key),
            )
            try:
                await self._run_in_executor(
                    abort_multipart_upload,
                    destination=upload.destination,
                    object_key=upload.object_key,
                    upload_id=upload.upload_id,
                )
            except Exception as error:
                return error
            multipart_uploads.pop(group_id, None)
            return None

        results = await _map_ordered_with_concurrency(
            candidates,
            min(self._multipart_control_concurrency, max(1, len(candidates))),
            abort_upload,
        )
        failures = [result for result in results if result is not None]
        if failures:
            diagnostics = [safe_error_diagnostic(failure, retryable=True) for failure in failures]
            raise RuntimeError(
                f"failed to abort {len(failures)} multipart upload(s); "
                f"correlation_tokens={','.join(item['correlation_token'] for item in diagnostics)}"
            )


def _validate_signed_route_manifest_contract(
    transfer_id: str,
    routes: list[SignedChunkRoute],
    manifest: list[MultipartGroupManifest | dict[str, Any]],
) -> None:
    validated_manifest = [MultipartGroupManifest.model_validate(item) for item in manifest]
    groups: dict[str, MultipartGroupManifest] = {}
    for group in validated_manifest:
        if group.multipart_group_id in groups:
            raise ValueError(f"duplicate multipart_group_id: {group.multipart_group_id}")
        if group.final_object_metadata["beam-transfer-id"] != transfer_id:
            raise ValueError(
                f"multipart group {group.multipart_group_id} transfer identity does not match"
            )
        groups[group.multipart_group_id] = group
    for route in routes:
        group_id = route.metadata.get("multipart_group_id")
        upload_id = route.metadata.get("upload_id")
        if (
            isinstance(upload_id, str)
            and upload_id
            and (not isinstance(group_id, str) or not group_id)
        ):
            raise ValueError(
                "signed multipart route "
                f"{route.source_id}:{route.destination_id}:{route.chunk_index} "
                "is missing multipart_group_id"
            )
        if not isinstance(group_id, str) or not group_id:
            continue
        route_group = groups.get(group_id)
        if route_group is None:
            raise ValueError(f"signed route references unknown multipart group {group_id}")
        if (
            route.source_id != route_group.source_id
            or route.destination_id != route_group.destination_id
        ):
            raise ValueError(f"signed route identity does not match multipart group {group_id}")
        if (
            upload_id != route_group.upload_id
            or route.metadata.get("final_object_key") != route_group.final_object_key
        ):
            raise ValueError(f"signed route controls do not match multipart group {group_id}")
        part_number = route.metadata.get("part_number")
        max_part_number = route_group.max_part_number
        if (
            not isinstance(part_number, int)
            or isinstance(part_number, bool)
            or not 1 <= part_number <= max_part_number
        ):
            raise ValueError(
                f"multipart part_number {part_number!r} is outside group {group_id} range 1-{max_part_number}"
            )


class BeamSDK:
    """NATS client facade for BEAM transfer creation and management.

    Options mirror the TypeScript ``BeamClientOptions``: ``max_payload_bytes``
    guards encoded lifecycle messages, ``multipart_control_concurrency`` bounds
    concurrent multipart create/abort calls (default
    :data:`BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY`), and
    ``transfer_client_subject_prefix`` / ``transfer_runtime_shard_count`` select the
    NATS subjects. The shard count falls back to ``TRANSFER_RUNTIME_SHARD_COUNT``.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        nats_url: str | None = None,
        environment: str | None = None,
        timeout: float = 30.0,
        route_signing_concurrency: int | None = None,
        multipart_control_concurrency: int | None = None,
        max_payload_bytes: int | None = None,
        transfer_client_subject_prefix: str | None = None,
        transfer_runtime_shard_count: int | None = None,
        on_diagnostics: Callable[[dict[str, Any]], Any] | None = None,
    ):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if route_signing_concurrency is not None:
            _positive_integer(route_signing_concurrency, "route_signing_concurrency")
        if multipart_control_concurrency is not None:
            _positive_integer(multipart_control_concurrency, "multipart_control_concurrency")
        if max_payload_bytes is not None:
            _positive_integer(max_payload_bytes, "max_payload_bytes")
        if transfer_runtime_shard_count is None:
            configured_shards = os.getenv("TRANSFER_RUNTIME_SHARD_COUNT", "1")
            try:
                transfer_runtime_shard_count = int(configured_shards)
            except ValueError as exc:
                raise ValueError("TRANSFER_RUNTIME_SHARD_COUNT must be a positive integer") from exc
        _positive_integer(transfer_runtime_shard_count, "transfer_runtime_shard_count")
        if transfer_client_subject_prefix is not None and not (
            isinstance(transfer_client_subject_prefix, str)
            and transfer_client_subject_prefix.strip(".").strip()
        ):
            raise ValueError("transfer_client_subject_prefix must be a non-empty string")

        resolved_api_key = api_key or os.getenv("BEAM_API_KEY")
        if not resolved_api_key:
            raise BeamAuthError("provide api_key or set BEAM_API_KEY")

        resolved_environment = environment or os.getenv("BEAM_ENV") or "prod"
        resolved_url = nats_url or os.getenv("BEAM_NATS_URL")
        if resolved_url is None:
            resolved_url = BEAM_PROD_URL if resolved_environment == "prod" else BEAM_DEV_URL
        self.nats_url = resolved_url.rstrip("/")
        control_options: dict[str, Any] = {}
        if max_payload_bytes is not None:
            control_options["max_payload_bytes"] = max_payload_bytes
        if transfer_client_subject_prefix is not None:
            control_options["subject_prefix"] = transfer_client_subject_prefix
        self._control = TransferClientControl(
            api_key=resolved_api_key,
            nats_url=self.nats_url,
            environment=resolved_environment,
            shard_count=transfer_runtime_shard_count,
            timeout=timeout,
            **control_options,
        )
        self.transfers = TransferManager(
            self._control,
            route_signing_concurrency=route_signing_concurrency,
            multipart_control_concurrency=multipart_control_concurrency,
            on_diagnostics=on_diagnostics,
        )

    async def close(self) -> None:
        """Close underlying NATS resources."""
        try:
            await self.transfers.close()
        finally:
            await self._control.close()

    async def __aenter__(self) -> BeamSDK:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
