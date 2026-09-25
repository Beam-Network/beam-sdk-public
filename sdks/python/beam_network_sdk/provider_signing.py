"""Provider adapters for SDK-side signed URL transfer preparation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from beam_network_sdk import huggingface as hf
from beam_network_sdk.models import (
    AzureProviderDestination,
    AzureProviderSource,
    ChunkDestinationSigningTarget,
    ChunkSigningPlanItem,
    GCSProviderDestination,
    GCSProviderSource,
    HippiusProviderDestination,
    HippiusProviderSource,
    HuggingFaceProviderDestination,
    HuggingFaceProviderSource,
    PreparedDestination,
    PreparedHttpSource,
    ProviderDestinationConfig,
    ProviderSourceConfig,
    R2ProviderDestination,
    R2ProviderSource,
    S3ProviderDestination,
    S3ProviderSource,
    SignedChunkRoute,
)

ClientFactory = Callable[..., Any]


class _FallbackS3V4Config:
    signature_version = "s3v4"


def _s3v4_config() -> Any:
    try:
        from botocore.config import Config  # type: ignore

        return Config(signature_version="s3v4")
    except ImportError:
        return _FallbackS3V4Config()


def expires_at_iso(expires_in: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _s3_client_kwargs(source: S3ProviderSource | S3ProviderDestination) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "region_name": source.region,
        "aws_access_key_id": source.access_key_id,
        "aws_secret_access_key": source.secret_access_key.get_secret_value(),
        "config": _s3v4_config(),
    }
    if source.session_token is not None:
        kwargs["aws_session_token"] = source.session_token.get_secret_value()
    if source.endpoint_url is not None:
        kwargs["endpoint_url"] = source.endpoint_url
    return kwargs


def _r2_endpoint(value: R2ProviderSource | R2ProviderDestination) -> str:
    if value.endpoint_url:
        return value.endpoint_url
    if value.account_id:
        return f"https://{value.account_id}.r2.cloudflarestorage.com"
    raise ValueError("R2 signing requires account_id or endpoint_url")


def _r2_client_kwargs(source: R2ProviderSource | R2ProviderDestination) -> dict[str, Any]:
    return {
        "service_name": "s3",
        "endpoint_url": _r2_endpoint(source),
        "aws_access_key_id": source.access_key_id,
        "aws_secret_access_key": source.secret_access_key.get_secret_value(),
        "region_name": "auto",
        "config": _s3v4_config(),
    }


def _destination_bucket(destination: ProviderDestinationConfig) -> str:
    if isinstance(destination, (S3ProviderDestination, R2ProviderDestination)):
        return destination.bucket
    raise TypeError(f"S3-compatible destination required: {type(destination)!r}")


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
    if isinstance(source, S3ProviderSource):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(source))
        head = client.head_object(Bucket=source.bucket, Key=source.key)
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": source.bucket, "Key": source.key},
            ExpiresIn=expires_in,
        )
        return PreparedHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="s3",
            url=url,
            size=int(head["ContentLength"]),
            filename=source.key.split("/")[-1] or source.key,
            expires_at=expires_at_iso(expires_in),
            metadata={"bucket": source.bucket, "key": source.key, "region": source.region},
        )

    if isinstance(source, R2ProviderSource):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(source))
        head = client.head_object(Bucket=source.bucket, Key=source.key)
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": source.bucket, "Key": source.key},
            ExpiresIn=expires_in,
        )
        return PreparedHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="r2",
            url=url,
            size=int(head["ContentLength"]),
            filename=source.key.split("/")[-1] or source.key,
            expires_at=expires_at_iso(expires_in),
            metadata={
                "bucket": source.bucket,
                "key": source.key,
                "endpoint_url": _r2_endpoint(source),
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
            filename=source.key.split("/")[-1] or source.key,
            expires_at=expires_at_iso(expires_in),
            metadata={"bucket": source.bucket, "key": source.key, "base_url": source.base_url},
        )

    if isinstance(source, HuggingFaceProviderSource):
        metadata = hf.file_metadata(source)
        return PreparedHttpSource(
            source_id=_source_id(index, source.source_id),
            provider="huggingface",
            url=metadata.url,
            size=metadata.size,
            filename=source.path.split("/")[-1] or source.path,
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


def prepare_provider_destination(
    destination: ProviderDestinationConfig,
    *,
    index: int = 0,
) -> PreparedDestination:
    if isinstance(destination, S3ProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="s3",
            logical_prefix=destination.key,
            metadata={
                "bucket": destination.bucket,
                "key": destination.key,
                "region": destination.region,
            },
        )

    if isinstance(destination, R2ProviderDestination):
        return PreparedDestination(
            destination_id=_destination_id(index, destination.destination_id),
            provider="r2",
            logical_prefix=destination.key,
            metadata={
                "bucket": destination.bucket,
                "key": destination.key,
                "endpoint_url": _r2_endpoint(destination),
                "account_id": destination.account_id,
            },
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

    if isinstance(source, S3ProviderSource):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(source))
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": source.bucket, "Key": source.key, "Range": range_header},
            ExpiresIn=expires_in,
        )
        return str(url), headers

    if isinstance(source, R2ProviderSource):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(source))
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": source.bucket, "Key": source.key, "Range": range_header},
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
) -> SignedChunkRoute:
    target_object_key = target.object_key
    if not target_object_key:
        raise ValueError("destination signing target is missing object_key")
    target_metadata = target.metadata if isinstance(target.metadata, dict) else {}
    object_key = target_object_key

    if dest_url is not None:
        # Providers such as Hugging Face presign their own upload targets.
        url = dest_url
    elif isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        params: dict[str, Any] = {"Bucket": destination.bucket, "Key": object_key}
        operation = "put_object"
        if upload_id and part_number:
            operation = "upload_part"
            params["UploadId"] = upload_id
            params["PartNumber"] = part_number
        url = client.generate_presigned_url(operation, Params=params, ExpiresIn=expires_in)
    elif isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        params = {"Bucket": destination.bucket, "Key": object_key}
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

    source_url, source_headers = _sign_source_route(
        chunk=chunk,
        source=source,
        expires_in=expires_in,
        client_factory=client_factory,
    )

    return SignedChunkRoute(
        source_id=chunk.source_id,
        destination_id=target.destination_id,
        chunk_index=chunk.chunk_index,
        source_url=source_url,
        dest_url=url,
        source_offset=chunk.source_offset,
        chunk_size=chunk.chunk_size,
        expires_at=expires_at_iso(expires_in),
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


def create_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    metadata: dict[str, str],
    client_factory: ClientFactory | None = None,
) -> str:
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        response = client.create_multipart_upload(
            Bucket=destination.bucket, Key=object_key, Metadata=metadata
        )
    elif isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        response = client.create_multipart_upload(
            Bucket=destination.bucket, Key=object_key, Metadata=metadata
        )
    else:
        raise TypeError(f"multipart upload is not supported for destination: {type(destination)!r}")
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
    params = {"Bucket": _destination_bucket(destination), "Key": object_key}
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
    elif isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
    else:
        raise TypeError(
            f"final object HEAD signing is not supported for destination: {type(destination)!r}"
        )
    return str(client.generate_presigned_url("head_object", Params=params, ExpiresIn=expires_in))


def sign_complete_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> str:
    params: dict[str, Any] = {
        "Bucket": _destination_bucket(destination),
        "Key": object_key,
        "UploadId": upload_id,
    }
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        return str(
            client.generate_presigned_url(
                "complete_multipart_upload", Params=params, ExpiresIn=expires_in
            )
        )
    if isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        return str(
            client.generate_presigned_url(
                "complete_multipart_upload", Params=params, ExpiresIn=expires_in
            )
        )
    raise TypeError(f"multipart completion is not supported for destination: {type(destination)!r}")


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
    params: dict[str, Any] = {
        "Bucket": _destination_bucket(destination),
        "Key": object_key,
        "UploadId": upload_id,
    }
    if max_parts is not None:
        params["MaxParts"] = max_parts
    if part_number_marker is not None:
        params["PartNumberMarker"] = part_number_marker
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        return str(client.generate_presigned_url("list_parts", Params=params, ExpiresIn=expires_in))
    if isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        return str(client.generate_presigned_url("list_parts", Params=params, ExpiresIn=expires_in))
    raise TypeError(f"multipart list-parts is not supported for destination: {type(destination)!r}")


def sign_abort_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    expires_in: int = 3600,
    client_factory: ClientFactory | None = None,
) -> str:
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        return str(
            client.generate_presigned_url(
                "abort_multipart_upload",
                Params={"Bucket": destination.bucket, "Key": object_key, "UploadId": upload_id},
                ExpiresIn=expires_in,
            )
        )
    if isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        return str(
            client.generate_presigned_url(
                "abort_multipart_upload",
                Params={"Bucket": destination.bucket, "Key": object_key, "UploadId": upload_id},
                ExpiresIn=expires_in,
            )
        )
    raise TypeError(f"multipart abort is not supported for destination: {type(destination)!r}")


def abort_multipart_upload(
    *,
    destination: ProviderDestinationConfig,
    object_key: str,
    upload_id: str,
    client_factory: ClientFactory | None = None,
) -> None:
    if isinstance(destination, S3ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory("s3", **_s3_client_kwargs(destination))
        client.abort_multipart_upload(Bucket=destination.bucket, Key=object_key, UploadId=upload_id)
        return
    if isinstance(destination, R2ProviderDestination):
        factory = _require_boto3_client_factory(client_factory)
        client = factory(**_r2_client_kwargs(destination))
        client.abort_multipart_upload(Bucket=destination.bucket, Key=object_key, UploadId=upload_id)
        return
    raise TypeError(f"multipart abort is not supported for destination: {type(destination)!r}")
