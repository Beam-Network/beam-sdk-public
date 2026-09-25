"""Async BEAM transfer client."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from typing import Any

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
from beam_network_sdk._validation import validate_id as _validate_id
from beam_network_sdk.exceptions import (
    BeamAPIError,
    BeamAuthError,
    BeamRouteRecoveryPendingError,
    BeamTimeoutError,
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
    MultipartGroupManifest,
    PreparedDestination,
    PreparedHttpSource,
    ProviderDestinationConfig,
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
    sign_abort_multipart_upload,
    sign_complete_multipart_upload,
    sign_destination_route,
    sign_final_object_head,
    sign_list_multipart_upload,
)

logger = logging.getLogger("beam_network_sdk.client")

BEAM_DEV_URL = BEAM_DEV_NATS_URL
BEAM_PROD_URL = BEAM_PROD_NATS_URL


ROUTE_STREAM_BATCH_ROUTES = 2_048


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

    async def begin(self) -> None:
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

        await asyncio.gather(*(attach(group) for group in normalized))

    async def add_route(self, route: dict[str, Any]) -> None:
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
        await self._enqueue_flush()
        if self._send_tail is not None:
            await self._send_tail
        if self._send_error is not None:
            raise self._send_error
        return await self._control.request(
            "transfer.route_stream.complete",
            {
                "transfer_id": self._transfer_id,
                "stream_id": self._stream_id,
                "route_generation_id": self._route_generation_id,
                "expected_batches": self._batch_index,
                "expected_routes": self._total_routes,
                "route_keys_checksum": self._checksum.value(),
            },
            transfer_id=self._transfer_id,
            idempotency_key=f"transfer:{self._transfer_id}:route-stream:{self._stream_id}:complete",
        )

    async def abort(self) -> None:
        if self._send_error is None:
            self._send_error = RuntimeError("route_stream_aborted")
        if self._send_tail is not None:
            await asyncio.gather(self._send_tail, return_exceptions=True)

    async def _enqueue_flush(self) -> None:
        if not self._batch:
            return
        if self._send_tail is not None:
            await self._send_tail
            self._send_tail = None
        if self._send_error is not None:
            raise self._send_error
        routes = self._batch
        self._batch = []
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
        scheduled: list[tuple[int, list[dict[str, Any]]]] = []
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
                    route_checksum = _route_keys_checksum(chunk)
                    coordinate_checksum = _dict_route_coordinate_checksum(chunk)
                    batch_id = f"{self._stream_id}:{batch_index}:{coordinate_checksum}"
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
            except BaseException as error:
                self._send_error = error

        self._send_tail = asyncio.create_task(send_chunks())


class TransferManager:
    """Transfer management and provider-aware transfer preparation."""

    def __init__(
        self, control: TransferClientControl, *, route_signing_concurrency: int | None = None
    ):
        resolved_concurrency = route_signing_concurrency or 64
        if resolved_concurrency < 1:
            raise ValueError("route_signing_concurrency must be positive")
        self._control = control
        self._route_signing_concurrency = resolved_concurrency
        self._route_signing_concurrency_overridden = route_signing_concurrency is not None
        self._route_signing_executor = ThreadPoolExecutor(
            max_workers=256,
            thread_name_prefix="beam-route-signing",
        )
        self._huggingface_uploads: dict[str, list[_HuggingFaceUploadState]] = {}
        self._closed = False

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
        chunk_size: int | None = None,
        name: str | None = None,
        merkle_root: str | None = None,
        chunk_hashes: list[str] | None = None,
        callbacks: list[CallbackConfig] | None = None,
        test_mode: bool = False,
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
        if chunk_size:
            body["chunk_size"] = chunk_size
        if test_mode:
            body["test_mode"] = True
        if progressive_mode:
            body["progressive_mode"] = True
        data = await self._control.request(
            "transfer.create",
            body,
            transfer_id=str(body["transfer_id"]),
            idempotency_key=idempotency_key or f"transfer:{body['transfer_id']}:create",
        )
        return TransferCreateResponse.model_validate(data)

    async def status(self, transfer_id: str) -> TransferStatusInfo:
        """Get transfer status."""
        _validate_id(transfer_id, "transfer_id")
        data = await self._control.request(
            "transfer.status", {"transfer_id": transfer_id}, transfer_id=transfer_id
        )
        result = TransferStatusInfo.model_validate(data)
        if result.status in {"completed", "failed", "cancelled"}:
            self._control.release_recovery_lease(transfer_id)
        return result

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

    async def cancel(self, transfer_id: str) -> TransferCancelResponse:
        """Cancel an active transfer."""
        _validate_id(transfer_id, "transfer_id")
        data = await self._control.request(
            "transfer.cancel",
            {"transfer_id": transfer_id},
            transfer_id=transfer_id,
            idempotency_key=f"transfer:{transfer_id}:cancel",
        )
        result = TransferCancelResponse.model_validate(data)
        if result.success:
            self._control.release_recovery_lease(transfer_id)
        return result

    async def prepare(
        self,
        *,
        sources: list[PreparedHttpSource],
        destinations: list[PreparedDestination],
        transfer_id: str | None = None,
        name: str | None = None,
        test_mode: bool = False,
        chunk_size: int | None = None,
        urls_expires_at: str | None = None,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
    ) -> TransferPrepareResponse:
        """Prepare a signed-URL transfer and receive BeamCore's chunk plan."""
        if transfer_id:
            _validate_id(transfer_id, "transfer_id")
        effective_transfer_id = transfer_id or _transfer_id_for_idempotency_key(idempotency_key)
        prepare_idempotency_key = idempotency_key or f"transfer:{effective_transfer_id}:prepare"
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
        if test_mode:
            body["test_mode"] = True
        if chunk_size:
            body["chunk_size"] = chunk_size
        if urls_expires_at:
            body["urls_expires_at"] = urls_expires_at
        body["signed_url_flow"] = signed_url_flow
        data = await self._control.request(
            "transfer.prepare",
            body,
            transfer_id=str(body["transfer_id"]),
            idempotency_key=prepare_idempotency_key,
        )
        return TransferPrepareResponse.model_validate(data)

    async def plan(
        self,
        *,
        sources: list[PreparedHttpSource],
        destinations: list[PreparedDestination],
        name: str | None = None,
        test_mode: bool = False,
        chunk_size: int | None = None,
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
        if test_mode:
            body["test_mode"] = True
        if chunk_size:
            body["chunk_size"] = chunk_size
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
        test_mode: bool = False,
        chunk_size: int | None = None,
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
            test_mode=test_mode,
            chunk_size=chunk_size,
            signed_url_flow=signed_url_flow,
        )

    async def prepare_provider_transfer(
        self,
        *,
        sources: list[ProviderSourceConfig],
        destinations: list[ProviderDestinationConfig],
        transfer_id: str | None = None,
        name: str | None = None,
        test_mode: bool = False,
        chunk_size: int | None = None,
        expires_in: int = 3600,
        distribute: bool = False,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
        route_generation_id: str | None = None,
        _retained_multipart_uploads: dict[str, dict[str, Any]] | None = None,
        _recovery_prepared: TransferPrepareResponse | None = None,
        _recovery_replay: bool = False,
        _register_recovery_lease: bool = True,
    ) -> TransferPrepareResponse:
        """Prepare, sign, and attach a provider-backed transfer.

        Provider credentials stay local to the SDK. BeamCore only receives
        short-lived HTTP source URLs and per-chunk destination routes.
        """
        if transfer_id:
            _validate_id(transfer_id, "transfer_id")
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
        huggingface_states, huggingface_chunk_size = await self._plan_huggingface_uploads(
            sources=sources,
            prepared_sources=list(prepared_sources),
            destinations=destinations,
            prepared_destinations=prepared_destinations,
        )
        if huggingface_chunk_size is not None:
            chunk_size = huggingface_chunk_size
        if _recovery_prepared is not None:
            prepare_result = _recovery_prepared.model_copy(
                deep=True,
                update={"route_generation_id": route_generation_id},
            )
        else:
            prepare_result = await self.prepare(
                sources=prepared_sources,
                destinations=prepared_destinations,
                transfer_id=transfer_id,
                name=name,
                test_mode=test_mode,
                chunk_size=chunk_size,
                idempotency_key=idempotency_key,
                signed_url_flow=signed_url_flow,
                route_generation_id=route_generation_id,
            )
        if not prepare_result.success:
            return prepare_result

        if huggingface_states:
            self._assert_huggingface_plan(prepare_result, huggingface_states)
            self._huggingface_uploads[prepare_result.transfer_id] = huggingface_states
        huggingface_by_destination = {state.destination_id: state for state in huggingface_states}

        destination_by_id = {
            prepared.destination_id: original
            for prepared, original in zip(prepared_destinations, destinations, strict=True)
        }
        source_by_id = {
            prepared.source_id: original
            for prepared, original in zip(prepared_sources, sources, strict=True)
        }
        route_stream = _RouteStreamSender(
            control=self._control,
            transfer_id=prepare_result.transfer_id,
            total_routes=prepare_result.plan_descriptor.delivery_route_count,
            total_chunks=prepare_result.plan_descriptor.logical_chunk_count,
            auto_distribute=distribute,
            plan_identity=(
                f"{prepare_result.plan_descriptor.plan_nonce}:{prepare_result.route_generation_id}"
            ),
            signed_url_flow=signed_url_flow,
            route_generation_id=prepare_result.route_generation_id,
            urls_expires_at=expires_at_iso(expires_in),
        )
        multipart_upload_tasks: dict[str, asyncio.Task[dict[str, Any]]] = {}
        multipart_uploads = (
            _retained_multipart_uploads if _retained_multipart_uploads is not None else {}
        )
        route_replay_lock = asyncio.Lock()
        lease_registered = False
        if _register_recovery_lease:
            await route_replay_lock.acquire()
            recovery_sources = [source.model_copy(deep=True) for source in sources]
            recovery_destinations = [
                destination.model_copy(deep=True) for destination in destinations
            ]
            recovery_source_by_id = {
                prepared.source_id: source
                for prepared, source in zip(
                    prepared_sources,
                    recovery_sources,
                    strict=True,
                )
            }
            recovery_destination_by_id = {
                prepared.destination_id: destination
                for prepared, destination in zip(
                    prepared_destinations,
                    recovery_destinations,
                    strict=True,
                )
            }

            async def replay_provider_routes(next_generation_id: str) -> None:
                async with route_replay_lock:
                    await self.prepare_provider_transfer(
                        sources=recovery_sources,
                        destinations=recovery_destinations,
                        transfer_id=prepare_result.transfer_id,
                        name=name,
                        test_mode=test_mode,
                        chunk_size=chunk_size,
                        expires_in=expires_in,
                        distribute=distribute,
                        idempotency_key=idempotency_key,
                        signed_url_flow=signed_url_flow,
                        route_generation_id=next_generation_id,
                        _retained_multipart_uploads=multipart_uploads,
                        _recovery_prepared=prepare_result,
                        _recovery_replay=True,
                        _register_recovery_lease=False,
                    )

            def dispose_recovery_inputs() -> None:
                recovery_sources.clear()
                recovery_destinations.clear()
                recovery_source_by_id.clear()
                recovery_destination_by_id.clear()
                multipart_uploads.clear()

            async def sign_recovery_routes(payload: dict[str, Any]) -> dict[str, Any]:
                route_generation_id = payload.get("route_generation_id")
                if not isinstance(route_generation_id, str) or not route_generation_id:
                    raise ValueError("route recovery generation is required")
                requested_chunks = payload.get("chunks")
                if not isinstance(requested_chunks, list) or not requested_chunks:
                    raise ValueError("route recovery chunks are required")

                async def sign_requested_route(requested: Any) -> SignedChunkRoute:
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
                    if attempt_slot not in {0, 1, 2}:
                        raise ValueError("route recovery attempt_slot must be 0, 1, or 2")
                    chunk = _materialize_plan_chunk(
                        prepare_result.plan_descriptor,
                        prepare_result.transfer_id,
                        source_id,
                        chunk_index,
                    )
                    if (
                        part_number
                        != multipart_part_number(
                            chunk.source_chunk_index,
                            logical_attempt_index,
                        )
                        or attempt_slot != logical_attempt_index % 3
                    ):
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
                    source = recovery_source_by_id.get(source_id)
                    destination = recovery_destination_by_id.get(destination_id)
                    if source is None or destination is None:
                        raise ValueError("route recovery provider configuration is unavailable")
                    multipart_group_id = str(requested.get("multipart_group_id") or "")
                    final_object_key = str(requested.get("final_object_key") or "")
                    upload_id = str(requested.get("upload_id") or "")
                    upload = multipart_uploads.get(multipart_group_id)
                    if upload is None:
                        raise ValueError(
                            f"route recovery multipart upload is unavailable: {multipart_group_id}"
                        )
                    manifest = upload.get("manifest") or {}
                    if (
                        str(upload.get("upload_id") or "") != upload_id
                        or str(upload.get("object_key") or "") != final_object_key
                        or str(manifest.get("source_id") or "") != source_id
                        or str(manifest.get("destination_id") or "") != destination_id
                        or str(manifest.get("multipart_group_id") or "") != multipart_group_id
                    ):
                        raise ValueError("route recovery multipart identity mismatch")
                    destination_metadata = requested.get("destination_metadata")
                    if destination_metadata is not None and not isinstance(
                        destination_metadata,
                        dict,
                    ):
                        raise ValueError("route recovery destination_metadata must be an object")
                    recovery_target = target.model_copy(
                        update={
                            "object_key": final_object_key,
                            "metadata": {
                                **target.metadata,
                                **(destination_metadata or {}),
                                "transfer_id": prepare_result.transfer_id,
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
                    return await self._sign_provider_route(
                        chunk=chunk,
                        target=recovery_target,
                        source=source,
                        destination=destination,
                        expires_in=expires_in,
                        upload=upload,
                        final_object_key=final_object_key,
                        part_number=part_number,
                        transfer_id=prepare_result.transfer_id,
                        signed_url_flow="signed_url",
                        huggingface=huggingface_by_destination.get(recovery_target.destination_id),
                    )

                routes = await asyncio.gather(
                    *(sign_requested_route(requested) for requested in requested_chunks)
                )
                return {
                    "transfer_id": prepare_result.transfer_id,
                    "route_generation_id": route_generation_id,
                    "signed_at": iso_now(),
                    "chunk_routes": [route.model_dump(exclude_none=True) for route in routes],
                }

            self._control.register_recovery_lease(
                transfer_id=prepare_result.transfer_id,
                plan_fingerprint=prepare_result.plan_fingerprint,
                coordinate_checksum=prepare_result.coordinate_checksum,
                replay_routes=replay_provider_routes,
                dispose=dispose_recovery_inputs,
            )
            try:
                await self._control.serve_route_recovery_signer(
                    prepare_result.transfer_id,
                    sign_recovery_routes,
                )
            except Exception:
                self._control.release_recovery_lease(prepare_result.transfer_id)
                raise
            lease_registered = True
        try:
            await route_stream.begin()
            multipart_upload_tasks = self._start_multipart_group_manifest(
                prepared=prepare_result,
                destination_by_id=destination_by_id,
                expires_in=expires_in,
                route_stream=route_stream,
                created_uploads=multipart_uploads,
                signed_url_flow=signed_url_flow,
            )
            pending_routes: set[asyncio.Task[SignedChunkRoute]] = set()
            signing_concurrency = self._route_signing_concurrency
            signed_in_window = 0
            signing_window_started_at = time.monotonic()

            async def stream_completed_routes() -> None:
                nonlocal signing_concurrency, signed_in_window, signing_window_started_at
                done, pending = await asyncio.wait(
                    pending_routes,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                pending_routes.clear()
                pending_routes.update(pending)
                results = await asyncio.gather(*done, return_exceptions=True)
                failure = next(
                    (result for result in results if isinstance(result, BaseException)),
                    None,
                )
                if failure is not None:
                    raise failure
                for result in results:
                    if isinstance(result, BaseException):
                        continue
                    await route_stream.add_route(result.model_dump(exclude_none=True))
                    signed_in_window += 1
                    if signed_in_window == ROUTE_STREAM_BATCH_ROUTES:
                        elapsed = time.monotonic() - signing_window_started_at
                        if (
                            not self._route_signing_concurrency_overridden
                            and elapsed > 4.0
                            and signing_concurrency < 256
                        ):
                            signing_concurrency = min(256, signing_concurrency * 2)
                        signed_in_window = 0
                        signing_window_started_at = time.monotonic()

            async def sign_planned_route(
                chunk: ChunkSigningPlanItem,
                target: ChunkDestinationSigningTarget,
            ) -> SignedChunkRoute:
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
                final_object_key = (
                    target.metadata.get("final_object_key")
                    if isinstance(target.metadata, dict)
                    else None
                ) or target.object_key
                part_number = (
                    target.metadata.get("part_number")
                    if isinstance(target.metadata, dict)
                    else None
                )
                multipart_key = (
                    f"{prepare_result.transfer_id}:{target.destination_id}:"
                    f"{chunk.source_id}:{final_object_key}"
                )
                upload_task = multipart_upload_tasks.get(multipart_key)
                upload = await asyncio.shield(upload_task) if upload_task is not None else None
                if upload is None and not isinstance(
                    destination, (HippiusProviderDestination, HuggingFaceProviderDestination)
                ):
                    raise ValueError(f"multipart group manifest missing for {multipart_key}")

                return await self._sign_provider_route(
                    chunk=chunk,
                    target=target,
                    source=source,
                    destination=destination,
                    expires_in=expires_in,
                    upload=upload,
                    final_object_key=str(final_object_key),
                    part_number=int(part_number or multipart_part_number(chunk.source_chunk_index)),
                    transfer_id=prepare_result.transfer_id,
                    signed_url_flow=signed_url_flow,
                    huggingface=huggingface_by_destination.get(target.destination_id),
                )

            for chunk in _iter_plan_chunks(
                prepare_result.plan_descriptor,
                prepare_result.transfer_id,
            ):
                for target in chunk.destinations:
                    pending_routes.add(asyncio.create_task(sign_planned_route(chunk, target)))
                    if len(pending_routes) >= signing_concurrency:
                        await stream_completed_routes()
            while pending_routes:
                await stream_completed_routes()
            if multipart_upload_tasks:
                manifest_results = await asyncio.gather(*multipart_upload_tasks.values())
                multipart_uploads.update(
                    {
                        str(state["manifest"]["multipart_group_id"]): state
                        for state in manifest_results
                    }
                )
            attach_result = AttachSignedUrlsResponse.model_validate(await route_stream.complete())
            logger.info(
                "beam_provider_signed_routes_streamed transfer_id=%s route_count=%s multipart_upload_count=%s",
                prepare_result.transfer_id,
                prepare_result.plan_descriptor.delivery_route_count,
                len(multipart_uploads),
            )
            if not attach_result.success:
                raise BeamAPIError(
                    status_code=500, detail=attach_result.error or attach_result.message
                )
        except BaseException as exc:
            if "pending_routes" in locals():
                for task in pending_routes:
                    task.cancel()
                await asyncio.gather(*pending_routes, return_exceptions=True)
            await route_stream.abort()
            if multipart_upload_tasks:
                await asyncio.gather(
                    *multipart_upload_tasks.values(),
                    return_exceptions=True,
                )
            if not _recovery_replay:
                cancelled = isinstance(exc, asyncio.CancelledError)
                recoverable = isinstance(exc, Exception) and is_recoverable_route_stream_error(exc)
                if (cancelled or recoverable) and lease_registered:
                    self._control.continue_recovery_lease(prepare_result.transfer_id)
                    if recoverable:
                        raise BeamRouteRecoveryPendingError(
                            prepare_result.transfer_id,
                            exc,
                        ) from exc
                else:
                    await self._cancel_and_abort_provider_failure(
                        prepare_result.transfer_id,
                        exc,
                        multipart_uploads,
                    )
                    if lease_registered:
                        self._control.release_recovery_lease(prepare_result.transfer_id)
            raise
        finally:
            if lease_registered:
                route_replay_lock.release()
        return prepare_result

    def _start_multipart_group_manifest(
        self,
        *,
        prepared: TransferPrepareResponse,
        destination_by_id: dict[str, ProviderDestinationConfig],
        expires_in: int,
        route_stream: _RouteStreamSender,
        created_uploads: dict[str, dict[str, Any]],
        signed_url_flow: SignedUrlFlow,
    ) -> dict[str, asyncio.Task[dict[str, Any]]]:
        semaphore = asyncio.Semaphore(self._route_signing_concurrency)

        async def create_group(
            source: Any,
            destination_plan: Any,
            destination: ProviderDestinationConfig,
            final_object_key: str,
            retained_state: dict[str, Any] | None,
        ) -> dict[str, Any]:
            async with semaphore:
                state = await asyncio.get_running_loop().run_in_executor(
                    self._route_signing_executor,
                    partial(
                        self._create_multipart_group_sync,
                        prepared=prepared,
                        source=source,
                        destination_plan=destination_plan,
                        destination=destination,
                        final_object_key=str(final_object_key),
                        expires_in=expires_in,
                        signed_url_flow=signed_url_flow,
                        retained_state=retained_state,
                    ),
                )
            created_uploads[str(state["manifest"]["multipart_group_id"])] = state
            await route_stream.add_manifest_groups([state["manifest"]])
            return state

        tasks: dict[str, asyncio.Task[dict[str, Any]]] = {}
        for source in prepared.plan_descriptor.sources:
            for destination_plan in prepared.plan_descriptor.destinations:
                destination = destination_by_id.get(destination_plan.destination_id)
                if destination is None:
                    raise ValueError(f"unknown destination_id: {destination_plan.destination_id}")
                if isinstance(
                    destination, (HippiusProviderDestination, HuggingFaceProviderDestination)
                ):
                    continue
                final_object_key = destination_plan.final_object_keys.get(source.source_id)
                if not final_object_key:
                    raise ValueError(
                        f"missing final object key for {source.source_id}:{destination_plan.destination_id}"
                    )
                group_id = (
                    f"{prepared.transfer_id}:{destination_plan.destination_id}:"
                    f"{source.source_id}:{final_object_key}"
                )
                retained_state = created_uploads.get(group_id)
                tasks[group_id] = asyncio.create_task(
                    create_group(
                        source,
                        destination_plan,
                        destination,
                        str(final_object_key),
                        retained_state,
                    )
                )
        return tasks

    @staticmethod
    def _create_multipart_group_sync(
        *,
        prepared: TransferPrepareResponse,
        source: Any,
        destination_plan: Any,
        destination: ProviderDestinationConfig,
        final_object_key: str,
        expires_in: int,
        signed_url_flow: SignedUrlFlow,
        retained_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        group_id = (
            f"{prepared.transfer_id}:{destination_plan.destination_id}:"
            f"{source.source_id}:{final_object_key}"
        )
        final_metadata = {
            "beam-transfer-id": prepared.transfer_id,
        }
        upload_id = (
            str(retained_state["upload_id"])
            if retained_state is not None
            else create_multipart_upload(
                destination=destination,
                object_key=final_object_key,
                metadata=final_metadata,
            )
        )
        created_upload = retained_state is None
        try:
            urls_expires_at = expires_at_iso(expires_in)
            max_part_number = multipart_part_number(source.chunk_count - 1, 2)
            list_page_urls = [
                sign_list_multipart_upload(
                    destination=destination,
                    object_key=final_object_key,
                    upload_id=upload_id,
                    expires_in=expires_in,
                    max_parts=1_000,
                    part_number_marker=marker or None,
                )
                for marker in range(0, max_part_number, 1_000)
            ]
            manifest = {
                "multipart_group_id": group_id,
                "source_id": source.source_id,
                "destination_id": destination_plan.destination_id,
                "final_object_key": final_object_key,
                "upload_id": upload_id,
                "expected_object_size": source.size,
                "expected_part_count": source.chunk_count,
                "max_part_number": max_part_number,
                "complete_url": sign_complete_multipart_upload(
                    destination=destination,
                    object_key=final_object_key,
                    upload_id=upload_id,
                    expires_in=expires_in,
                ),
                "abort_url": sign_abort_multipart_upload(
                    destination=destination,
                    object_key=final_object_key,
                    upload_id=upload_id,
                    expires_in=expires_in,
                ),
                "list_page_urls": list_page_urls,
                "final_head_url": sign_final_object_head(
                    destination=destination,
                    object_key=final_object_key,
                    expires_in=expires_in,
                ),
                "final_object_metadata": final_metadata,
                "urls_expires_at": urls_expires_at,
            }
            return {
                "destination": destination,
                "object_key": final_object_key,
                "upload_id": upload_id,
                "manifest": manifest,
            }
        except Exception as exc:
            if created_upload:
                try:
                    abort_multipart_upload(
                        destination=destination,
                        object_key=final_object_key,
                        upload_id=upload_id,
                    )
                except Exception as cleanup_exc:
                    raise RuntimeError(
                        f"multipart group setup failed ({exc}) and cleanup failed ({cleanup_exc})"
                    ) from exc
            raise

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
        part size itself, so this runs first and the plan is then requested at that size.
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
        chunk_size: int | None = None

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
                if chunk_size is not None and chunk_size != plan.chunk_size:
                    raise ValueError(
                        f"huggingface destinations disagree on part size ({chunk_size} vs "
                        f"{plan.chunk_size}); the plan carries a single chunk size"
                    )
                chunk_size = plan.chunk_size

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

        return states, chunk_size

    @staticmethod
    def _assert_huggingface_plan(
        prepared: TransferPrepareResponse, states: list[_HuggingFaceUploadState]
    ) -> None:
        """Fail before any byte moves if BeamCore did not adopt the Hub's part layout."""
        for state in states:
            plan_destination = next(
                (
                    candidate
                    for candidate in prepared.plan_descriptor.destinations
                    if candidate.destination_id == state.destination_id
                ),
                None,
            )
            plan_source = next(
                (
                    candidate
                    for candidate in prepared.plan_descriptor.sources
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

            if prepared.plan_descriptor.chunk_size != state.chunk_size:
                raise ValueError(
                    f"the Hub requires {state.chunk_size}-byte parts for "
                    f"{hf.describe(state.destination)}, but BeamCore planned "
                    f"{prepared.plan_descriptor.chunk_size}-byte chunks"
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
        return await asyncio.get_running_loop().run_in_executor(
            self._route_signing_executor,
            partial(self._sign_provider_route_sync, **kwargs),
        )

    @staticmethod
    def _sign_provider_route_sync(
        *,
        chunk: Any,
        target: Any,
        source: ProviderSourceConfig,
        destination: ProviderDestinationConfig,
        expires_in: int,
        upload: dict[str, Any] | None,
        final_object_key: str,
        part_number: int,
        transfer_id: str,
        signed_url_flow: SignedUrlFlow,
        huggingface: _HuggingFaceUploadState | None = None,
    ) -> SignedChunkRoute:
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
                chunk=chunk,
                target=target.model_copy(
                    update={
                        "object_key": final_object_key,
                        # BeamCore rejects part_number on a destination it does not treat as
                        # an S3 multipart target.
                        "metadata": {
                            key: value
                            for key, value in (target.metadata or {}).items()
                            if key != "part_number"
                        },
                    }
                ),
                source=source,
                destination=destination,
                expires_in=expires_in,
                transfer_id=transfer_id,
                dest_url=dest_url,
            )

        if isinstance(destination, HippiusProviderDestination):
            return sign_destination_route(
                chunk=chunk,
                target=target,
                source=source,
                destination=destination,
                expires_in=expires_in,
                transfer_id=transfer_id,
            )

        if not target.object_key:
            raise ValueError("destination signing target is missing object_key")
        if upload is None:
            raise ValueError("multipart group manifest is required for provider destinations")
        max_part_number = int(upload["manifest"]["max_part_number"])
        if part_number < 1 or part_number > max_part_number:
            raise ValueError(
                f"multipart part_number {part_number} is outside group "
                f"{upload['manifest']['multipart_group_id']} range 1-{max_part_number}"
            )

        list_page_index = (part_number - 1) // 1_000
        manifest = upload["manifest"]
        return sign_destination_route(
            chunk=chunk,
            target=target.model_copy(update={"object_key": final_object_key}),
            source=source,
            destination=destination,
            expires_in=expires_in,
            part_number=part_number,
            transfer_id=transfer_id,
            multipart_group_id=str(manifest["multipart_group_id"]),
            upload_id=str(upload["upload_id"]),
            final_object_key=final_object_key,
            complete_url=str(manifest["complete_url"]),
            abort_url=str(manifest["abort_url"]),
            list_page_url=str(manifest["list_page_urls"][list_page_index]),
            final_head_url=str(manifest["final_head_url"]),
            expected_object_size=int(manifest["expected_object_size"]),
            expected_part_count=int(manifest["expected_part_count"]),
            max_part_number=int(manifest["max_part_number"]),
            final_object_metadata=dict(manifest["final_object_metadata"]),
        )

    async def create_and_distribute(
        self,
        *,
        sources: list[SourceConfig],
        destinations: list[DestConfig],
        total_size: int,
        chunk_size: int | None = None,
        name: str | None = None,
        merkle_root: str | None = None,
        chunk_hashes: list[str] | None = None,
        callbacks: list[CallbackConfig] | None = None,
        test_mode: bool = False,
        progressive_mode: bool = False,
        idempotency_key: str | None = None,
        signed_url_flow: SignedUrlFlow = "signed_url",
    ) -> TransferCreateResponse:
        """Create a transfer and immediately distribute it."""
        result = await self.create(
            sources=sources,
            destinations=destinations,
            total_size=total_size,
            chunk_size=chunk_size,
            name=name,
            merkle_root=merkle_root,
            chunk_hashes=chunk_hashes,
            callbacks=callbacks,
            test_mode=test_mode,
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
        """Wait terminal-first and reconcile status until completion, failure, cancellation, or timeout."""
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
                    raise BeamAPIError(
                        status_code=500,
                        detail=f"Transfer failed: {status.error_message}",
                    )
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
        try:
            result = await self.cancel(transfer_id)
            if not result.success:
                raise RuntimeError(
                    result.message or f"Beam rejected cancellation for {transfer_id}"
                )
        except BaseException as cancel_exc:
            raise RuntimeError(
                f"provider transfer failed ({cause}) and transfer cancellation failed ({cancel_exc})"
            ) from cause

    async def _cancel_and_abort_provider_failure(
        self,
        transfer_id: str,
        cause: BaseException,
        multipart_uploads: dict[str, dict[str, Any]],
    ) -> None:
        cancel_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        try:
            await self._cancel_after_provider_failure(transfer_id, cause)
        except BaseException as exc:
            cancel_error = exc
        try:
            await self._abort_created_uploads(multipart_uploads)
        except BaseException as exc:
            cleanup_error = exc
        if cancel_error is not None and cleanup_error is not None:
            raise RuntimeError(
                f"provider transfer failed ({cause}), transfer cancellation failed "
                f"({cancel_error}), and multipart cleanup failed ({cleanup_error})"
            ) from cause
        if cancel_error is not None:
            raise cancel_error
        if cleanup_error is not None:
            raise RuntimeError(
                f"provider transfer failed ({cause}) and multipart cleanup failed ({cleanup_error})"
            ) from cause

    async def _abort_created_uploads(self, multipart_uploads: dict[str, dict[str, Any]]) -> None:
        loop = asyncio.get_running_loop()

        def abort_upload(upload: dict[str, Any]) -> None:
            logger.warning(
                "beam_provider_multipart_abort operation=abort_multipart correlation_token=%s",
                opaque_correlation_token(
                    "abort_multipart",
                    upload.get("upload_id"),
                    upload.get("object_key"),
                ),
            )
            abort_multipart_upload(
                destination=upload["destination"],
                object_key=str(upload["object_key"]),
                upload_id=str(upload["upload_id"]),
            )

        results = await asyncio.gather(
            *(
                loop.run_in_executor(self._route_signing_executor, partial(abort_upload, upload))
                for upload in multipart_uploads.values()
                if upload.get("upload_id")
            ),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
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
    """NATS client facade for BEAM transfer creation and management."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        nats_url: str | None = None,
        environment: str | None = None,
        timeout: float = 30.0,
        route_signing_concurrency: int | None = None,
    ):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if route_signing_concurrency is not None and route_signing_concurrency < 1:
            raise ValueError("route_signing_concurrency must be positive")

        resolved_api_key = api_key or os.getenv("BEAM_API_KEY")
        if not resolved_api_key:
            raise BeamAuthError("provide api_key or set BEAM_API_KEY")

        resolved_environment = environment or os.getenv("BEAM_ENV") or "prod"
        resolved_url = nats_url or os.getenv("BEAM_NATS_URL")
        if resolved_url is None:
            resolved_url = BEAM_PROD_URL if resolved_environment == "prod" else BEAM_DEV_URL
        self.nats_url = resolved_url.rstrip("/")
        self._control = TransferClientControl(
            api_key=resolved_api_key,
            nats_url=self.nats_url,
            environment=resolved_environment,
            shard_count=int(os.getenv("TRANSFER_RUNTIME_SHARD_COUNT", "1")),
            timeout=timeout,
        )
        self.transfers = TransferManager(
            self._control,
            route_signing_concurrency=route_signing_concurrency,
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
