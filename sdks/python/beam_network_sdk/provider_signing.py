"""Provider adapters for SDK-side signed URL transfer preparation."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from urllib.parse import parse_qs, quote, urlsplit
from uuid import UUID

from beam_network_sdk import huggingface as hf
from beam_network_sdk._performance import current as current_performance
from beam_network_sdk.cancellation import BeamCancellationToken, raise_if_cancelled
from beam_network_sdk.models import (
    AzureProviderDestination,
    AzureProviderSource,
    ChunkDestinationSigningTarget,
    ChunkSigningPlanItem,
    CompletedMultipartUpload,
    DestinationObjectInfo,
    GCSProviderDestination,
    GCSProviderSource,
    HippiusProviderDestination,
    HippiusProviderSource,
    HuggingFaceProviderDestination,
    HuggingFaceProviderSource,
    MultipartPart,
    PlanningHttpSource,
    PreparedDestination,
    PreparedHttpSource,
    ProviderDestinationConfig,
    ProviderSourceConfig,
    R2ProviderDestination,
    R2ProviderSource,
    S3CompatibleProviderDestination,
    S3CompatibleProviderSource,
    S3ProviderDestination,
    S3ProviderSource,
    SignedChunkRoute,
)

ClientFactory = Callable[..., Any]

S3CompatibleSource = S3ProviderSource | R2ProviderSource | S3CompatibleProviderSource
S3CompatibleDestination = (
    S3ProviderDestination | R2ProviderDestination | S3CompatibleProviderDestination
)
AnyS3CompatibleConfig = S3CompatibleSource | S3CompatibleDestination
_S3_COMPATIBLE_TYPES: tuple[type, ...] = (
    S3ProviderSource,
    R2ProviderSource,
    S3CompatibleProviderSource,
    S3ProviderDestination,
    R2ProviderDestination,
    S3CompatibleProviderDestination,
)


class _FallbackS3V4Config:
    signature_version = "s3v4"


#: Total attempts per S3 API call, matching the TypeScript SDK's ``maxAttempts``.
S3_CLIENT_MAX_ATTEMPTS = 5
_S3_CLIENT_CACHE_SIZE = 128
_s3_client_cache: OrderedDict[tuple[Any, ...], Any] = OrderedDict()
_s3_client_cache_lock = threading.Lock()


def _s3v4_config(force_path_style: bool | None = None) -> Any:
    try:
        from botocore.config import Config  # type: ignore
    except ImportError:
        return _FallbackS3V4Config()
    options: dict[str, Any] = {
        "signature_version": "s3v4",
        "retries": {"total_max_attempts": S3_CLIENT_MAX_ATTEMPTS, "mode": "standard"},
    }
    if force_path_style is not None:
        options["s3"] = {"addressing_style": "path" if force_path_style else "virtual"}
    return Config(**options)


def expires_at_iso(expires_in: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_millis(value: datetime) -> str:
    """Render a timestamp the way JavaScript's ``Date#toISOString`` does."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def _require_boto3_client_factory(client_factory: ClientFactory | None = None) -> ClientFactory:
    if client_factory is not None:
        return client_factory
    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for S3/R2 signing. Install beam-network-sdk[s3] or beam-network-sdk[r2]."
        ) from exc
    return cast(ClientFactory, boto3.client)


def _has_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def is_s3_compatible_provider(config: object) -> bool:
    """Return whether a provider config is signed through the S3 API."""
    return isinstance(config, _S3_COMPATIBLE_TYPES)


def s3_compatible_endpoint(config: AnyS3CompatibleConfig) -> str | None:
    """Resolve the S3 API endpoint for a config; ``None`` means AWS's regional default."""
    endpoint_url = getattr(config, "endpoint_url", None)
    if _has_text(endpoint_url):
        return str(endpoint_url)
    provider = str(config.provider)
    if provider == "r2":
        account_id = getattr(config, "account_id", None)
        if _has_text(account_id):
            return f"https://{account_id}.r2.cloudflarestorage.com"
        raise ValueError("r2 config requires account_id or endpoint_url.")
    if provider != "s3":
        raise ValueError(f"{provider} config requires endpoint_url.")
    return None


def s3_compatible_region(config: AnyS3CompatibleConfig) -> str:
    """Resolve the signing region: explicit region, ``auto`` for R2, else ``us-east-1``."""
    region = getattr(config, "region", None)
    if _has_text(region):
        return str(region)
    return "auto" if config.provider == "r2" else "us-east-1"


def s3_compatible_force_path_style(
    config: AnyS3CompatibleConfig, endpoint: str | None = None
) -> bool | None:
    """Path-style addressing: explicit setting, AWS default for S3, else on for custom endpoints."""
    configured = getattr(config, "force_path_style", None)
    if isinstance(configured, bool):
        return configured
    if config.provider == "s3":
        return None
    if endpoint is None:
        endpoint = s3_compatible_endpoint(config)
    return bool(endpoint)


def _s3_client_kwargs(config: AnyS3CompatibleConfig) -> dict[str, Any]:
    endpoint = s3_compatible_endpoint(config)
    kwargs: dict[str, Any] = {
        "region_name": s3_compatible_region(config),
        "aws_access_key_id": config.access_key_id,
        "aws_secret_access_key": config.secret_access_key.get_secret_value(),
        "config": _s3v4_config(s3_compatible_force_path_style(config, endpoint)),
    }
    session_token = getattr(config, "session_token", None)
    if session_token is not None and session_token.get_secret_value():
        kwargs["aws_session_token"] = session_token.get_secret_value()
    if endpoint is not None:
        kwargs["endpoint_url"] = endpoint
    return kwargs


def _s3_client(config: AnyS3CompatibleConfig, client_factory: ClientFactory | None) -> Any:
    """Return an S3 client, reusing one per provider/endpoint/region/credential set.

    Clients from an explicit ``client_factory`` are never cached. Default boto3 clients
    are created under a lock because boto3's default session is not thread-safe.
    """
    if client_factory is not None:
        return client_factory("s3", **_s3_client_kwargs(config))
    factory = _require_boto3_client_factory(None)
    kwargs = _s3_client_kwargs(config)
    cache_key = (
        factory,
        config.provider,
        kwargs.get("endpoint_url"),
        kwargs["region_name"],
        s3_compatible_force_path_style(config, kwargs.get("endpoint_url")),
        kwargs["aws_access_key_id"],
        kwargs["aws_secret_access_key"],
        kwargs.get("aws_session_token"),
    )
    with _s3_client_cache_lock:
        cached = _s3_client_cache.get(cache_key)
        if cached is not None:
            if metrics := current_performance.get():
                metrics.increment("provider_clients_reused")
            _s3_client_cache.move_to_end(cache_key)
            return cached
        started = time.monotonic()
        client = factory("s3", **kwargs)
        if metrics := current_performance.get():
            metrics.observe("sdk.provider_client_setup", started)
            metrics.increment("provider_clients_created")
        _s3_client_cache[cache_key] = client
        while len(_s3_client_cache) > _S3_CLIENT_CACHE_SIZE:
            _s3_client_cache.popitem(last=False)
        return client


def _s3_compatible_metadata(config: AnyS3CompatibleConfig, endpoint: str | None) -> dict[str, Any]:
    return _compact(
        {
            "driver": "s3-compatible",
            "bucket": config.bucket,
            "key": config.key,
            "region": s3_compatible_region(config),
            "storage_location": config.storage_location,
            "endpoint_url": endpoint,
            "account_id": getattr(config, "account_id", None),
        }
    )


def _s3_head_metadata(head: dict[str, Any]) -> dict[str, Any]:
    """Pin the source object: integrity audits sign If-Match/VersionId from these fields."""
    metadata: dict[str, Any] = {"content_length": int(head.get("ContentLength") or 0)}
    if head.get("ETag"):
        metadata["etag"] = str(head["ETag"])
    last_modified = head.get("LastModified")
    if isinstance(last_modified, datetime):
        metadata["last_modified"] = _iso_millis(last_modified)
    elif last_modified:
        metadata["last_modified"] = str(last_modified)
    if head.get("VersionId"):
        metadata["version_id"] = str(head["VersionId"])
    return metadata


def _compact(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _filename(key: str) -> str:
    parts = [part for part in key.split("/") if part]
    return parts[-1] if parts else key


def _require_s3_source(source: ProviderSourceConfig) -> S3CompatibleSource:
    return cast(S3CompatibleSource, source)


def _require_s3_destination(
    destination: ProviderDestinationConfig, operation: str
) -> S3CompatibleDestination:
    if not is_s3_compatible_provider(destination):
        raise TypeError(f"{operation} is not supported for destination: {type(destination)!r}")
    return cast(S3CompatibleDestination, destination)


def _hippius_presign(
    base_url: str, token: str, bucket: str, key: str, action: str, expires_in: int
) -> str:
    """Call the Hippius presigned-URL API and return the signed URL."""
    import httpx  # type: ignore

    resp = httpx.get(
        f"{base_url}/api/objectstore/buckets/{bucket}/presigned-url/",
        params={"key": key, "action": action, "expires_in": expires_in},
        headers={"Authorization": f"Token {token}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return str(resp.json()["url"])


def _hippius_object_size(base_url: str, token: str, bucket: str, key: str) -> int:
    """Return the size in bytes of a Hippius object."""
    import httpx  # type: ignore

    resp = httpx.get(
        f"{base_url}/api/objectstore/buckets/{bucket}/objects/",
        params={"prefix": key, "max_keys": 1},
        headers={"Authorization": f"Token {token}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    contents = resp.json().get("Contents", [])
    if not contents:
        raise RuntimeError(f"Object not found: hippius://{bucket}/{key}")
    return int(contents[0]["Size"])


def _source_id(index: int, configured: str | None) -> str:
    return configured or f"src_{index}"


def _destination_id(index: int, configured: str | None) -> str:
    return configured or f"dst_{index}"


def prepare_provider_source(
    source: ProviderSourceConfig,
    *,
    index: int = 0,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> PreparedHttpSource:
    if is_s3_compatible_provider(source):
        s3_source = _require_s3_source(source)
        endpoint = s3_compatible_endpoint(s3_source)
        client = _s3_client(s3_source, client_factory)
        started = time.monotonic()
        head = client.head_object(Bucket=s3_source.bucket, Key=s3_source.key)
        if metrics := current_performance.get():
            metrics.observe("sdk.metadata_request", started)
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_source.bucket, "Key": s3_source.key},
            ExpiresIn=expires_in,
        )
        return PreparedHttpSource(
            source_id=_source_id(index, s3_source.source_id),
            provider=s3_source.provider,
            url=str(url),
            size=int(head.get("ContentLength") or 0),
            filename=_filename(s3_source.key),
            expires_at=expires_at_iso(expires_in),
            metadata={
                **_s3_compatible_metadata(s3_source, endpoint),
                **_s3_head_metadata(head),
            },
        )

    if isinstance(source, HippiusProviderSource):
        token = source.api_token.get_secret_value()
        size = _hippius_object_size(source.base_url, token, source.bucket, source.key)
        url = _hippius_presign(source.base_url, token, source.bucket, source.key, "get", expires_in)
        return PreparedHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="hippius",
            url=url,
            size=size,
            filename=_filename(source.key),
            expires_at=expires_at_iso(expires_in),
            metadata={
                "bucket": source.bucket,
                "key": source.key,
                "base_url": source.base_url,
                **(
                    {"storage_location": source.storage_location} if source.storage_location else {}
                ),
            },
        )

    if isinstance(source, HuggingFaceProviderSource):
        metadata = hf.file_metadata(source)
        return PreparedHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="huggingface",
            url=metadata.url,
            size=metadata.size,
            filename=_filename(source.path),
            metadata=_huggingface_metadata(source, metadata.etag, metadata.commit_hash),
        )

    if isinstance(source, GCSProviderSource):
        raise NotImplementedError(
            "GCS source signing will be implemented in the GCS provider adapter"
        )

    if isinstance(source, AzureProviderSource):
        raise NotImplementedError(
            "Azure source signing will be implemented in the Azure provider adapter"
        )

    raise TypeError(f"unsupported provider source: {type(source)!r}")


def prepare_provider_source_for_plan(
    source: ProviderSourceConfig,
    *,
    index: int = 0,
    client_factory: ClientFactory | None = None,
) -> PlanningHttpSource:
    """Describe a source for ``plan`` without signing a read URL.

    Reads provider metadata only (an S3 HEAD, a Hippius listing, or a Hub lookup).
    """
    if is_s3_compatible_provider(source):
        s3_source = _require_s3_source(source)
        endpoint = s3_compatible_endpoint(s3_source)
        head = _s3_client(s3_source, client_factory).head_object(
            Bucket=s3_source.bucket, Key=s3_source.key
        )
        return PlanningHttpSource(
            source_id=_source_id(index, s3_source.source_id),
            provider=s3_source.provider,
            size=int(head.get("ContentLength") or 0),
            filename=_filename(s3_source.key),
            metadata={
                **_s3_compatible_metadata(s3_source, endpoint),
                **_s3_head_metadata(head),
            },
        )

    if isinstance(source, HippiusProviderSource):
        size = _hippius_object_size(
            source.base_url, source.api_token.get_secret_value(), source.bucket, source.key
        )
        return PlanningHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="hippius",
            size=size,
            filename=_filename(source.key),
            metadata={
                "bucket": source.bucket,
                "key": source.key,
                "base_url": source.base_url,
                **(
                    {"storage_location": source.storage_location} if source.storage_location else {}
                ),
            },
        )

    if isinstance(source, HuggingFaceProviderSource):
        metadata = hf.file_metadata(source)
        return PlanningHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="huggingface",
            size=metadata.size,
            filename=_filename(source.path),
            metadata=_huggingface_metadata(source, metadata.etag, metadata.commit_hash),
        )

    if isinstance(source, (GCSProviderSource, AzureProviderSource)):
        raise NotImplementedError(f"{source.provider} source planning is not implemented")

    raise TypeError(f"unsupported provider source: {type(source)!r}")


def prepare_provider_destination(
    destination: ProviderDestinationConfig,
    *,
    index: int = 0,
) -> PreparedDestination:
    if is_s3_compatible_provider(destination):
        s3_destination = cast(S3CompatibleDestination, destination)
        return PreparedDestination(
            destination_id=_destination_id(index, s3_destination.destination_id),
            provider=s3_destination.provider,
            logical_prefix=s3_destination.key,
            metadata=_s3_compatible_metadata(
                s3_destination, s3_compatible_endpoint(s3_destination)
            ),
        )

    if isinstance(destination, GCSProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="gcs",
            logical_prefix=destination.key,
            metadata={
                "bucket": destination.bucket,
                "key": destination.key,
                "project_id": destination.project_id,
            },
        )

    if isinstance(destination, AzureProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="azure",
            logical_prefix=f"azure://{destination.container}/{destination.blob}".rstrip("/"),
            metadata={
                "container": destination.container,
                "blob": destination.blob,
                "account_name": destination.account_name,
            },
        )

    if isinstance(destination, HippiusProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="hippius",
            logical_prefix=destination.key.rstrip("/"),
            metadata={
                "bucket": destination.bucket,
                "key": destination.key,
                "base_url": destination.base_url,
                **(
                    {"storage_location": destination.storage_location}
                    if destination.storage_location
                    else {}
                ),
            },
        )

    if isinstance(destination, HuggingFaceProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="huggingface",
            logical_prefix=destination.path,
            metadata=_huggingface_metadata(destination),
        )

    raise TypeError(f"unsupported provider destination: {type(destination)!r}")


def _huggingface_metadata(
    config: HuggingFaceProviderSource | HuggingFaceProviderDestination,
    etag: str | None = None,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "driver": "huggingface",
        "repo_id": config.repo_id,
        "repo_type": config.repo_type,
        "revision": config.revision,
        "path": config.path,
        "endpoint": hf.endpoint(config),
    }
    if config.storage_location:
        metadata["storage_location"] = config.storage_location
    # For an LFS blob the Hub's linked ETag is the object's sha256.
    if etag is not None:
        metadata["sha256"] = etag
    if commit_hash is not None:
        metadata["commit_hash"] = commit_hash
    return metadata


def _range_header_for_chunk(chunk: ChunkSigningPlanItem) -> str:
    start = max(0, int(chunk.source_offset))
    size = max(1, int(chunk.chunk_size))
    return f"bytes={start}-{start + size - 1}"


def _sign_source_route(
    *,
    chunk: ChunkSigningPlanItem,
    source: ProviderSourceConfig | None,
    expires_in: int,
    client_factory: ClientFactory | None = None,
) -> tuple[str, dict[str, str]]:
    range_header = _range_header_for_chunk(chunk)
    headers = {"Range": range_header}

    if source is None:
        return chunk.source_url, headers

    if is_s3_compatible_provider(source):
        s3_source = _require_s3_source(source)
        client = _s3_client(s3_source, client_factory)
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_source.bucket, "Key": s3_source.key, "Range": range_header},
            ExpiresIn=expires_in,
        )
        return str(url), headers

    if isinstance(source, HippiusProviderSource):
        url = _hippius_presign(
            source.base_url,
            source.api_token.get_secret_value(),
            source.bucket,
            source.key,
            "get",
            expires_in,
        )
        return url, headers

    if isinstance(source, HuggingFaceProviderSource):
        # Re-resolving mints a fresh presigned CDN URL; the token stays here.
        return hf.file_metadata(source).url, headers

    raise TypeError(f"source signing is not implemented for provider source: {type(source)!r}")


def sign_source_read_range(
    source: ProviderSourceConfig,
    *,
    offset: int,
    length: int,
    expires_in: int,
    if_match: str | None = None,
    version_id: str | None = None,
    client_factory: ClientFactory | None = None,
) -> dict[str, Any]:
    """Sign a read of ``length`` bytes at ``offset``, optionally pinned to an ETag/version."""
    expires_at = expires_at_iso(expires_in)
    range_header = f"bytes={offset}-{offset + length - 1}"
    headers = {"Range": range_header}
    if is_s3_compatible_provider(source):
        s3_source = _require_s3_source(source)
        client = _s3_client(s3_source, client_factory)
        params: dict[str, Any] = {
            "Bucket": s3_source.bucket,
            "Key": s3_source.key,
            "Range": range_header,
        }
        if if_match:
            params["IfMatch"] = if_match
            headers["If-Match"] = if_match
        if version_id:
            params["VersionId"] = version_id
        url = client.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    elif if_match or version_id:
        raise ValueError("conditional source ranges require S3-compatible storage")
    elif isinstance(source, HippiusProviderSource):
        url = _hippius_presign(
            source.base_url,
            source.api_token.get_secret_value(),
            source.bucket,
            source.key,
            "get",
            expires_in,
        )
    elif isinstance(source, HuggingFaceProviderSource):
        url = hf.file_metadata(source).url
    else:
        raise TypeError(f"source range signing is unsupported for {type(source)!r}")
    return {
        "url": str(url),
        "headers": headers,
        "expires_at": bounded_grant_expiry(str(url), expires_at),
    }


def sign_destination_read_range(
    destination: ProviderDestinationConfig,
    *,
    object_key: str,
    offset: int,
    length: int,
    expires_in: int,
    if_match: str | None = None,
    client_factory: ClientFactory | None = None,
) -> dict[str, Any]:
    """Sign a read-back range of a destination object, optionally pinned to an ETag."""
    expires_at = expires_at_iso(expires_in)
    range_header = f"bytes={offset}-{offset + length - 1}"
    if is_s3_compatible_provider(destination):
        s3_destination = cast(S3CompatibleDestination, destination)
        client = _s3_client(s3_destination, client_factory)
        params: dict[str, Any] = {
            "Bucket": s3_destination.bucket,
            "Key": object_key,
            "Range": range_header,
        }
        if if_match:
            params["IfMatch"] = if_match
        url = client.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    elif if_match:
        raise ValueError("conditional destination ranges require S3-compatible storage")
    elif isinstance(destination, HippiusProviderDestination):
        url = _hippius_presign(
            destination.base_url,
            destination.api_token.get_secret_value(),
            destination.bucket,
            object_key,
            "get",
            expires_in,
        )
    elif isinstance(destination, HuggingFaceProviderDestination):
        # Read-back resolves the committed file; uncommitted LFS parts are not exposed.
        source = HuggingFaceProviderSource(
            repo_id=destination.repo_id,
            path=object_key,
            repo_type=destination.repo_type,
            revision=destination.revision,
            token=destination.token,
            endpoint=destination.endpoint,
        )
        url = hf.file_metadata(source).url
    else:
        raise TypeError(f"destination range signing is unsupported for {type(destination)!r}")
    return {
        "url": str(url),
        "headers": {"Range": range_header, **({"If-Match": if_match} if if_match else {})},
        "expires_at": bounded_grant_expiry(str(url), expires_at),
    }


def bounded_grant_expiry(url: str, upper_bound: str) -> str:
    expiry = datetime.fromisoformat(upper_bound.replace("Z", "+00:00"))
    query = parse_qs(urlsplit(url).query)
    for prefix in ("X-Amz", "X-Goog"):
        try:
            started = datetime.strptime(query[prefix + "-Date"][0], "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
            expiry = min(expiry, started + timedelta(seconds=int(query[prefix + "-Expires"][0])))
        except (KeyError, ValueError, OverflowError):
            pass
    with suppress(KeyError, ValueError, OverflowError, OSError):
        expiry = min(expiry, datetime.fromtimestamp(int(query["Expires"][0]), tz=timezone.utc))
    return expiry.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def source_chunk_grant(
    *,
    chunk: ChunkSigningPlanItem,
    source: ProviderSourceConfig | None,
    expires_in: int,
    client_factory: ClientFactory | None = None,
) -> tuple[str, dict[str, str], str]:
    expires_at = expires_at_iso(expires_in)
    url, headers = _sign_source_route(
        chunk=chunk, source=source, expires_in=expires_in, client_factory=client_factory
    )
    return url, headers, bounded_grant_expiry(url, expires_at)


def release_provider_clients(
    configs: Sequence[ProviderSourceConfig | ProviderDestinationConfig],
) -> None:
    # Evict owned credential references. Existing in-flight users retain their
    # clients; closing a shared pool here would interrupt another transfer.
    credentials = {
        (
            cast(AnyS3CompatibleConfig, config).access_key_id,
            cast(AnyS3CompatibleConfig, config).secret_access_key.get_secret_value(),
        )
        for config in configs
        if is_s3_compatible_provider(config)
    }
    with _s3_client_cache_lock:
        for key in list(_s3_client_cache):
            if (key[5], key[6]) in credentials:
                _s3_client_cache.pop(key, None)


def sign_destination_route(
    *,
    chunk: ChunkSigningPlanItem,
    target: ChunkDestinationSigningTarget,
    source: ProviderSourceConfig | None = None,
    destination: ProviderDestinationConfig,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
    part_number: int | None = None,
    transfer_id: str | None = None,
    multipart_group_id: str | None = None,
    upload_id: str | None = None,
    complete_url: str | None = None,
    abort_url: str | None = None,
    list_page_url: str | None = None,
    final_head_url: str | None = None,
    final_object_key: str | None = None,
    expected_object_size: int | None = None,
    expected_part_count: int | None = None,
    max_part_number: int | None = None,
    final_object_metadata: dict[str, str] | None = None,
    dest_url: str | None = None,
    source_grant: tuple[str, dict[str, str], str] | None = None,
) -> SignedChunkRoute:
    destination_expires = expires_at_iso(expires_in)
    target_object_key = target.object_key
    if not target_object_key:
        raise ValueError("destination signing target is missing object_key")
    target_metadata = target.metadata if isinstance(target.metadata, dict) else {}
    object_key = target_object_key

    if dest_url is not None:
        # Providers such as Hugging Face presign their own upload targets.
        url = dest_url
    elif is_s3_compatible_provider(destination):
        s3_destination = cast(S3CompatibleDestination, destination)
        client = _s3_client(s3_destination, client_factory)
        params: dict[str, Any] = {"Bucket": s3_destination.bucket, "Key": object_key}
        operation = "put_object"
        if upload_id and part_number:
            operation = "upload_part"
            params["UploadId"] = upload_id
            params["PartNumber"] = part_number
        url = client.generate_presigned_url(operation, Params=params, ExpiresIn=expires_in)
    elif isinstance(destination, GCSProviderDestination):
        raise NotImplementedError(
            "GCS destination signing will be implemented in the GCS provider adapter"
        )
    elif isinstance(destination, AzureProviderDestination):
        raise NotImplementedError(
            "Azure destination signing will be implemented in the Azure provider adapter"
        )
    elif isinstance(destination, HippiusProviderDestination):
        # Hippius uses plain per-chunk PUT presigned URLs (no multipart).
        # BeamCore already sets object_key to the chunk-specific path (e.g. prefix/chunk-0).
        url = _hippius_presign(
            destination.base_url,
            destination.api_token.get_secret_value(),
            destination.bucket,
            object_key,
            "put",
            expires_in,
        )
    elif isinstance(destination, HuggingFaceProviderDestination):
        raise ValueError(
            "huggingface destination routes must carry the Hub's presigned part URL as dest_url"
        )
    else:
        raise TypeError(f"unsupported provider destination: {type(destination)!r}")

    source_url, source_headers, source_expires = source_grant or source_chunk_grant(
        chunk=chunk, source=source, expires_in=expires_in, client_factory=client_factory
    )

    return SignedChunkRoute(
        source_id=chunk.source_id,
        destination_id=target.destination_id,
        chunk_index=chunk.chunk_index,
        source_url=source_url,
        dest_url=str(url),
        source_offset=chunk.source_offset,
        chunk_size=chunk.chunk_size,
        expires_at=min(source_expires, bounded_grant_expiry(str(url), destination_expires)),
        headers=source_headers,
        metadata={
            **target_metadata,
            **({"transfer_id": transfer_id} if transfer_id else {}),
            **({"multipart_group_id": multipart_group_id} if multipart_group_id else {}),
            **({"upload_id": upload_id} if upload_id else {}),
            **({"complete_url": complete_url} if complete_url else {}),
            **({"abort_url": abort_url} if abort_url else {}),
            **({"list_page_url": list_page_url} if list_page_url else {}),
            **({"final_head_url": final_head_url} if final_head_url else {}),
            **({"final_object_key": final_object_key} if final_object_key else {}),
            **(
                {"expected_object_size": expected_object_size}
                if expected_object_size is not None
                else {}
            ),
            **(
                {"expected_part_count": expected_part_count}
                if expected_part_count is not None
                else {}
            ),
            **({"max_part_number": max_part_number} if max_part_number is not None else {}),
            **({"final_object_metadata": final_object_metadata} if final_object_metadata else {}),
            **({"part_number": part_number} if part_number else {}),
        },
    )


def sign_destination_url(
    destination: ProviderDestinationConfig,
    *,
    object_key: str,
    expires_in: int = 3600,
    upload_id: str | None = None,
    part_number: int | None = None,
    content_md5: str | None = None,
    client_factory: ClientFactory | None = None,
) -> str:
    """Sign a PUT (or UploadPart, given ``upload_id`` and ``part_number``) URL.

    ``content_md5`` (base64) binds the upload to a checksum; the provider then rejects
    any other payload. Checksum-bound uploads require S3-compatible storage.
    """
    if is_s3_compatible_provider(destination):
        s3_destination = cast(S3CompatibleDestination, destination)
        params: dict[str, Any] = {"Bucket": s3_destination.bucket, "Key": object_key}
        operation = "put_object"
        if upload_id and part_number:
            operation = "upload_part"
            params["UploadId"] = upload_id
            params["PartNumber"] = part_number
        if content_md5:
            params["ContentMD5"] = content_md5
        client = _s3_client(s3_destination, client_factory)
        return str(client.generate_presigned_url(operation, Params=params, ExpiresIn=expires_in))
    if content_md5:
        raise ValueError("checksum-bound uploads require S3-compatible storage")
    if isinstance(destination, HippiusProviderDestination):
        return _hippius_presign(
            destination.base_url,
            destination.api_token.get_secret_value(),
            destination.bucket,
            object_key,
            "put",
            expires_in,
        )
    raise TypeError(f"destination URL signing is unsupported for {type(destination)!r}")


def list_multipart_parts(
    destination: ProviderDestinationConfig,
    *,
    object_key: str,
    upload_id: str,
    cancellation: BeamCancellationToken | None = None,
    client_factory: ClientFactory | None = None,
) -> list[MultipartPart]:
    """List every uploaded part, following pagination. Reads metadata only."""
    s3_destination = _require_s3_destination(destination, "multipart list-parts")
    raise_if_cancelled(cancellation)
    client = _s3_client(s3_destination, client_factory)
    parts: list[MultipartPart] = []
    marker: int | None = None
    while True:
        raise_if_cancelled(cancellation)
        params: dict[str, Any] = {
            "Bucket": s3_destination.bucket,
            "Key": object_key,
            "UploadId": upload_id,
            "MaxParts": 1_000,
        }
        if marker is not None:
            params["PartNumberMarker"] = marker
        page = client.list_parts(**params)
        for part in page.get("Parts") or []:
            part_number = part.get("PartNumber")
            etag = part.get("ETag")
            size = part.get("Size")
            if (
                not isinstance(part_number, int)
                or part_number < 1
                or not etag
                or not isinstance(size, int)
                or size < 0
            ):
                raise ValueError("invalid multipart part metadata")
            parts.append(MultipartPart(part_number=part_number, etag=str(etag), size=size))
        if not page.get("IsTruncated"):
            return parts
        next_marker = page.get("NextPartNumberMarker")
        try:
            next_value = int(next_marker) if next_marker is not None else None
        except (TypeError, ValueError):
            next_value = None
        if not next_value or next_value == marker:
            raise ValueError("invalid multipart pagination")
        marker = next_value


def complete_multipart_upload(
    destination: ProviderDestinationConfig,
    *,
    object_key: str,
    upload_id: str,
    parts: Sequence[MultipartPart | dict[str, Any]],
    cancellation: BeamCancellationToken | None = None,
    client_factory: ClientFactory | None = None,
) -> CompletedMultipartUpload:
    """Complete a multipart upload from its part ETags, in part-number order."""
    s3_destination = _require_s3_destination(destination, "multipart completion")
    validated = sorted(
        (MultipartPart.model_validate(part) if isinstance(part, dict) else part for part in parts),
        key=lambda part: part.part_number,
    )
    raise_if_cancelled(cancellation)
    result = _s3_client(s3_destination, client_factory).complete_multipart_upload(
        Bucket=s3_destination.bucket,
        Key=object_key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [{"PartNumber": part.part_number, "ETag": part.etag} for part in validated]
        },
    )
    return CompletedMultipartUpload(etag=result.get("ETag"), version_id=result.get("VersionId"))


def inspect_destination_object(
    destination: ProviderDestinationConfig,
    *,
    object_key: str,
    cancellation: BeamCancellationToken | None = None,
    client_factory: ClientFactory | None = None,
) -> DestinationObjectInfo:
    """HEAD a destination object: size, ETag, version, and user metadata."""
    s3_destination = _require_s3_destination(destination, "destination inspection")
    raise_if_cancelled(cancellation)
    head = _s3_client(s3_destination, client_factory).head_object(
        Bucket=s3_destination.bucket, Key=object_key
    )
    size = head.get("ContentLength")
    return DestinationObjectInfo(
        size=int(size) if size is not None else None,
        etag=head.get("ETag"),
        version_id=head.get("VersionId"),
        metadata={str(key): str(value) for key, value in (head.get("Metadata") or {}).items()},
    )


def create_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    metadata: dict[str, str],
    client_factory: ClientFactory | None = None,
    cancellation: BeamCancellationToken | None = None,
) -> str:
    raise_if_cancelled(cancellation)
    s3_destination = _require_s3_destination(destination, "multipart upload")
    client = _s3_client(s3_destination, client_factory)
    response = client.create_multipart_upload(
        Bucket=s3_destination.bucket, Key=object_key, Metadata=metadata
    )
    upload_id = response.get("UploadId")
    if not upload_id:
        raise RuntimeError(f"provider did not return UploadId for {object_key}")
    return str(upload_id)


def sign_final_object_head(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> str:
    s3_destination = _require_s3_destination(destination, "final object HEAD signing")
    client = _s3_client(s3_destination, client_factory)
    params = {"Bucket": s3_destination.bucket, "Key": object_key}
    return str(client.generate_presigned_url("head_object", Params=params, ExpiresIn=expires_in))


def sign_complete_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> str:
    s3_destination = _require_s3_destination(destination, "multipart completion")
    client = _s3_client(s3_destination, client_factory)
    params: dict[str, Any] = {
        "Bucket": s3_destination.bucket,
        "Key": object_key,
        "UploadId": upload_id,
    }
    return str(
        client.generate_presigned_url(
            "complete_multipart_upload", Params=params, ExpiresIn=expires_in
        )
    )


def sign_list_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
    max_parts: int | None = None,
    part_number_marker: int | None = None,
) -> str:
    s3_destination = _require_s3_destination(destination, "multipart list-parts")
    client = _s3_client(s3_destination, client_factory)
    params: dict[str, Any] = {
        "Bucket": s3_destination.bucket,
        "Key": object_key,
        "UploadId": upload_id,
    }
    if max_parts is not None:
        params["MaxParts"] = max_parts
    if part_number_marker is not None:
        params["PartNumberMarker"] = part_number_marker
    return str(client.generate_presigned_url("list_parts", Params=params, ExpiresIn=expires_in))


def sign_abort_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> str:
    s3_destination = _require_s3_destination(destination, "multipart abort")
    client = _s3_client(s3_destination, client_factory)
    return str(
        client.generate_presigned_url(
            "abort_multipart_upload",
            Params={"Bucket": s3_destination.bucket, "Key": object_key, "UploadId": upload_id},
            ExpiresIn=expires_in,
        )
    )


def abort_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    client_factory: ClientFactory | None = None,
    cancellation: BeamCancellationToken | None = None,
) -> None:
    raise_if_cancelled(cancellation)
    s3_destination = _require_s3_destination(destination, "multipart abort")
    client = _s3_client(s3_destination, client_factory)
    client.abort_multipart_upload(Bucket=s3_destination.bucket, Key=object_key, UploadId=upload_id)


def sign_multipart_recovery(
    *,
    destination: ProviderDestinationConfig,
    transfer_id: str,
    requested: dict[str, Any],
    route: SignedChunkRoute,
    expires_in: int,
    client_factory: ClientFactory | None = None,
) -> SignedChunkRoute:
    """Sign Core-only multipart recovery controls without changing the worker contract.

    Mirrors the TypeScript ``signMultipartRecovery``. ``renew`` re-signs the direct
    multipart controls of the original upload; staged ``upload``/``controls`` sign a
    per-attempt recovery object plus the ``UploadPartCopy`` that moves it into the
    original upload's part; ``list``/``delete`` sign cleanup of staged objects. A route
    without a recovery request (or in ``direct`` mode) is returned without staging data.
    """
    recovery = requested.get("recovery")
    metadata = dict(route.metadata or {})
    metadata.pop("recovery_staging", None)
    if recovery and recovery.get("operation") == "renew":
        kwargs: dict[str, Any] = dict(
            destination=destination,
            object_key=requested["final_object_key"],
            upload_id=requested["upload_id"],
            expires_in=expires_in,
            client_factory=client_factory,
        )
        count = int(metadata["expected_part_count"])
        if not 1 <= count <= 10_000:
            raise ValueError("invalid multipart count")
        pages = [
            sign_list_multipart_upload(**kwargs, max_parts=1000, part_number_marker=i)
            for i in range(0, count, 1000)
        ]
        metadata.update(
            complete_url=sign_complete_multipart_upload(**kwargs),
            abort_url=sign_abort_multipart_upload(**kwargs),
            final_head_url=sign_final_object_head(
                destination=destination,
                object_key=requested["final_object_key"],
                expires_in=expires_in,
                client_factory=client_factory,
            ),
            list_page_urls=pages,
            list_page_url=pages[(requested["part_number"] - 1) // 1000],
            control_urls_expires_at=expires_at_iso(expires_in),
        )
        return route.model_copy(update={"metadata": metadata})
    if not recovery or recovery.get("mode") == "direct":
        return route.model_copy(update={"metadata": metadata})
    if not is_s3_compatible_provider(destination):
        raise ValueError("multipart recovery requires an S3-compatible destination")
    s3_destination = _require_s3_destination(destination, "multipart recovery")
    attempt_id = str(recovery["attempt_id"])
    UUID(attempt_id)
    operation = recovery["operation"]
    if operation not in {"upload", "controls", "list", "delete"}:
        raise ValueError("unsupported recovery operation")
    group = quote(requested["multipart_group_id"], safe="")
    prefix = f"{requested['final_object_key']}.beam-recovery/{transfer_id}/{group}/"
    object_key = f"{prefix}{requested['part_number']}/{attempt_id}"
    if recovery.get("object_key", object_key) != object_key:
        raise ValueError("recovery staging identity mismatch")
    client = _s3_client(s3_destination, client_factory)

    def sign(op: str, params: dict[str, Any]) -> str:
        return str(client.generate_presigned_url(op, Params=params, ExpiresIn=expires_in))

    params = {"Bucket": s3_destination.bucket, "Key": object_key}
    if operation == "list":
        listing: dict[str, Any] = {
            "Bucket": s3_destination.bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if recovery.get("continuation_token"):
            listing["ContinuationToken"] = recovery["continuation_token"]
        metadata["recovery_listing"] = {"prefix": prefix, "url": sign("list_objects_v2", listing)}
    elif operation == "delete":
        metadata["recovery_delete"] = {
            "object_key": object_key,
            "url": sign("delete_object", params),
        }
    else:
        source = quote(s3_destination.bucket + "/" + object_key, safe="/")
        # Botocore encodes CopySource itself. Supplying the already escaped header
        # would sign a different value whenever the staging key contains "%".
        copy: dict[str, Any] = {
            "Bucket": s3_destination.bucket,
            "Key": requested["final_object_key"],
            "UploadId": requested["upload_id"],
            "PartNumber": requested["part_number"],
            "CopySource": {"Bucket": s3_destination.bucket, "Key": object_key},
        }
        headers = {"x-amz-copy-source": source}
        # Only AWS S3 promises to enforce copy-source conditions; R2 and other
        # S3-compatible stores rely on each attempt owning its own staged object.
        if s3_destination.provider == "s3" and recovery.get("etag"):
            copy["CopySourceIfMatch"] = '"' + str(recovery["etag"]).strip('"') + '"'
            headers["x-amz-copy-source-if-match"] = copy["CopySourceIfMatch"]
        metadata["recovery_staging"] = {
            "object_key": object_key,
            "attempt_id": attempt_id,
            "head_url": sign("head_object", params),
            "delete_url": sign("delete_object", params),
            "copy_url": sign("upload_part_copy", copy),
            "copy_headers": headers,
            "expires_at": expires_at_iso(expires_in),
        }
    updates: dict[str, Any] = {"metadata": metadata}
    if operation == "upload":
        updates["dest_url"] = sign("put_object", params)
    return route.model_copy(update=updates)
