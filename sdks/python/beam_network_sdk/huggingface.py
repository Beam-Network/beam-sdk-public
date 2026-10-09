"""Hugging Face Hub protocol helpers.

The Hub is reached over plain HTTP: a resolve URL that redirects to a presigned CDN URL for
reads, and the preupload / LFS batch / completion / commit sequence for writes. The token
never leaves this process; only the presigned URLs the Hub hands back are passed on.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

from beam_network_sdk.models import (
    HuggingFaceProviderDestination,
    HuggingFaceProviderSource,
)

HUGGINGFACE_DEFAULT_ENDPOINT = "https://huggingface.co"
HUGGINGFACE_DEFAULT_REVISION = "main"

LFS_CONTENT_TYPE = "application/vnd.git-lfs+json"

#: Mirrors ``constants.REPO_TYPES_URL_PREFIXES``; models carry no prefix.
REPO_TYPE_URL_PREFIXES = {
    "model": "",
    "dataset": "datasets/",
    "space": "spaces/",
    "kernel": "kernels/",
    "bucket": "buckets/",
}

HuggingFaceConfig = HuggingFaceProviderSource | HuggingFaceProviderDestination

_PART_KEY = re.compile(r"^\d+$")


@dataclass
class HuggingFaceFileMetadata:
    """What a HEAD on the resolve URL tells us about a file."""

    #: Credential-free presigned CDN URL the workers read from.
    url: str
    size: int
    #: sha256 for an LFS blob, git sha1 otherwise.
    etag: str | None = None
    commit_hash: str | None = None


@dataclass
class HuggingFaceLfsUploadPlan:
    """Upload instructions for one object, as returned by the LFS batch endpoint."""

    oid: str
    size: int
    #: Multipart completion endpoint, or the single-part PUT target. ``None`` when the Hub
    #: already stores this content and no upload is needed.
    upload_href: str | None = None
    #: Part size the Hub requires. ``None`` for a single-part upload.
    chunk_size: int | None = None
    #: Presigned part PUT URLs, ordered by part number.
    part_urls: list[str] = field(default_factory=list)
    verify_href: str | None = None


def endpoint(config: HuggingFaceConfig) -> str:
    return (config.endpoint or HUGGINGFACE_DEFAULT_ENDPOINT).rstrip("/")


def resolve_url(config: HuggingFaceConfig) -> str:
    """``{endpoint}/{prefix}{repo_id}/resolve/{revision}/{path}``, as built by ``hf_hub_url``.

    Buckets are unversioned and take no revision segment, and the Hub escapes their whole key
    as one component -- see ``HfApi.get_bucket_file_metadata``.
    """
    prefix = REPO_TYPE_URL_PREFIXES[config.repo_type]
    if config.repo_type == "bucket":
        return f"{endpoint(config)}/{prefix}{config.repo_id}/resolve/{quote(config.path, safe='')}"
    revision = quote(config.revision, safe="")
    return f"{endpoint(config)}/{prefix}{config.repo_id}/resolve/{revision}/{quote(config.path)}"


def api_base(config: HuggingFaceConfig) -> str:
    """``{endpoint}/api/{repo_type}s/{repo_id}``."""
    return f"{endpoint(config)}/api/{config.repo_type}s/{config.repo_id}"


def lfs_batch_url(config: HuggingFaceConfig) -> str:
    """``{endpoint}/{prefix}{repo_id}.git/info/lfs/objects/batch``."""
    prefix = REPO_TYPE_URL_PREFIXES[config.repo_type]
    return f"{endpoint(config)}/{prefix}{config.repo_id}.git/info/lfs/objects/batch"


def describe(config: HuggingFaceConfig) -> str:
    return f"{config.repo_type} {config.repo_id}@{config.revision}/{config.path}"


def file_metadata(config: HuggingFaceConfig) -> HuggingFaceFileMetadata:
    """HEAD the resolve URL and require the Hub to redirect to its CDN.

    The redirect target is presigned and carries no credential, so it is the only form of
    this URL that may be handed to BeamCore and the workers.
    """
    import httpx  # type: ignore

    url = resolve_url(config)
    response = httpx.head(
        url,
        headers={
            "Authorization": f"Bearer {config.token.get_secret_value()}",
            # Compression would report a transformed length instead of the real object size.
            "Accept-Encoding": "identity",
        },
        follow_redirects=False,
        timeout=30.0,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Hugging Face file lookup failed status={response.status_code} for {describe(config)}"
        )

    location = response.headers.get("location")
    if not location or _same_host(url, location):
        raise RuntimeError(
            f"Hugging Face did not redirect {describe(config)} to a presigned CDN URL. "
            "Beam reads this file over plain HTTP without forwarding your token, and the Hub only "
            "redirects for large-file (LFS or Xet) content. A small regular file is served inline "
            "from the Hub instead and cannot be transferred."
        )

    raw_size = response.headers.get("x-linked-size") or response.headers.get("content-length")
    size = int(raw_size) if raw_size is not None else 0
    if size <= 0:
        raise RuntimeError(f"Hugging Face did not report a size for {describe(config)}")

    return HuggingFaceFileMetadata(
        url=location,
        size=size,
        etag=_normalize_etag(response.headers.get("x-linked-etag") or response.headers.get("etag")),
        commit_hash=response.headers.get("x-repo-commit"),
    )


def preupload(
    destination: HuggingFaceProviderDestination, size: int, sample: str
) -> tuple[str, bool]:
    """Ask the Hub whether a path is stored as an LFS blob or as a regular git blob.

    ``sample`` is the base64 of the first 512 bytes, exactly as ``_fetch_upload_modes`` sends it.
    Returns ``(upload_mode, should_ignore)``.
    """
    import httpx  # type: ignore

    url = f"{api_base(destination)}/preupload/{quote(destination.revision, safe='')}"
    response = httpx.post(
        url,
        params={"create_pr": "1"} if destination.create_pr else None,
        headers=_json_headers(destination),
        json={"files": [{"path": destination.path, "sample": sample, "size": size}]},
        timeout=30.0,
    )
    payload = _read_json(response, "preupload", destination)

    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise RuntimeError(
            f"Hugging Face preupload returned a malformed response for {describe(destination)}"
        )
    entry = files[0]
    if entry.get("uploadMode") not in ("lfs", "regular"):
        raise RuntimeError(
            f"Hugging Face preupload returned a malformed response for {describe(destination)}"
        )
    return entry["uploadMode"], bool(entry.get("shouldIgnore"))


def lfs_batch(
    destination: HuggingFaceProviderDestination, oid: str, size: int
) -> HuggingFaceLfsUploadPlan:
    """Request upload instructions for one object.

    The Hub answers with either a single-part PUT target or a completion endpoint plus one
    presigned PUT URL per part.
    """
    import httpx  # type: ignore

    response = httpx.post(
        lfs_batch_url(destination),
        headers={
            "Authorization": f"Bearer {destination.token.get_secret_value()}",
            "Accept": LFS_CONTENT_TYPE,
            "Content-Type": LFS_CONTENT_TYPE,
        },
        json={
            "operation": "upload",
            "transfers": ["basic", "multipart"],
            "hash_algo": "sha256",
            "ref": {"name": destination.revision},
            "objects": [{"oid": oid, "size": size}],
        },
        timeout=60.0,
    )
    payload = _read_json(response, "LFS batch", destination)

    objects = payload.get("objects")
    if not isinstance(objects, list) or not objects:
        raise RuntimeError(
            f"Hugging Face LFS batch returned a malformed response for {describe(destination)}"
        )
    obj = objects[0]
    if obj.get("error"):
        message = obj["error"].get("message", "unknown error")
        raise RuntimeError(f"Hugging Face LFS batch rejected {describe(destination)}: {message}")

    upload_action = (obj.get("actions") or {}).get("upload")
    if not upload_action:
        # No actions means the Hub already stores this content; only the commit is left to do.
        return HuggingFaceLfsUploadPlan(oid=obj["oid"], size=size)
    if not isinstance(upload_action.get("href"), str):
        raise RuntimeError(
            f"Hugging Face LFS batch returned no upload href for {describe(destination)}"
        )

    header = upload_action.get("header") or {}
    raw_chunk_size = header.get("chunk_size")
    chunk_size: int | None = None
    if raw_chunk_size is not None:
        try:
            chunk_size = int(raw_chunk_size)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Hugging Face LFS batch returned a malformed chunk_size "
                f"'{raw_chunk_size}' for {describe(destination)}"
            ) from exc
        if chunk_size <= 0:
            raise RuntimeError(
                f"Hugging Face LFS batch returned a malformed chunk_size "
                f"'{raw_chunk_size}' for {describe(destination)}"
            )

    part_urls = [
        value
        for _, value in sorted(
            ((int(key), value) for key, value in header.items() if _PART_KEY.match(key)),
            key=lambda pair: pair[0],
        )
    ]
    if chunk_size is not None:
        expected = math.ceil(size / chunk_size)
        if len(part_urls) != expected:
            raise RuntimeError(
                f"Hugging Face returned {len(part_urls)} part URLs for {describe(destination)}, "
                f"expected {expected} at chunk_size {chunk_size}"
            )

    verify_action = (obj.get("actions") or {}).get("verify") or {}
    return HuggingFaceLfsUploadPlan(
        oid=obj["oid"],
        size=size,
        upload_href=upload_action["href"],
        chunk_size=chunk_size,
        part_urls=part_urls,
        verify_href=verify_action.get("href"),
    )


def complete_lfs_upload(
    destination: HuggingFaceProviderDestination, href: str, oid: str, etags: list[str]
) -> None:
    """Close a multipart LFS upload: ``{oid, parts:[{partNumber, etag}]}`` to the completion href."""
    import httpx  # type: ignore

    response = httpx.post(
        href,
        headers={"Accept": LFS_CONTENT_TYPE, "Content-Type": LFS_CONTENT_TYPE},
        json={
            "oid": oid,
            "parts": [{"partNumber": index + 1, "etag": etag} for index, etag in enumerate(etags)],
        },
        timeout=60.0,
    )
    _assert_ok(response, "LFS completion", destination)


def verify_lfs_upload(
    destination: HuggingFaceProviderDestination, href: str, oid: str, size: int
) -> None:
    """Optional server-side check that the object landed intact."""
    import httpx  # type: ignore

    response = httpx.post(
        href,
        headers=_json_headers(destination),
        json={"oid": oid, "size": size},
        timeout=60.0,
    )
    _assert_ok(response, "LFS verify", destination)


def commit(destination: HuggingFaceProviderDestination, oid: str, size: int) -> dict[str, Any]:
    """Publish the uploaded blob as a commit.

    The body is NDJSON: a header line then one ``lfsFile`` line.
    """
    import httpx  # type: ignore

    lines = [
        {
            "key": "header",
            "value": {
                "summary": destination.commit_message or f"Upload {destination.path} with Beam",
                "description": destination.commit_description or "",
            },
        },
        {
            "key": "lfsFile",
            "value": {
                "path": destination.path,
                "algo": "sha256",
                "oid": oid,
                "size": size,
            },
        },
    ]
    response = httpx.post(
        f"{api_base(destination)}/commit/{quote(destination.revision, safe='')}",
        params={"create_pr": "1"} if destination.create_pr else None,
        headers={
            "Authorization": f"Bearer {destination.token.get_secret_value()}",
            "Content-Type": "application/x-ndjson",
        },
        content="\n".join(json.dumps(line) for line in lines),
        timeout=120.0,
    )
    return _read_json(response, "commit", destination)


def read_source_sample(url: str, length: int = 512) -> str:
    """Read the first ``length`` bytes of a URL and return them base64-encoded, for ``preupload``."""
    import httpx  # type: ignore

    response = httpx.get(
        url, headers={"Range": f"bytes=0-{length - 1}"}, follow_redirects=True, timeout=60.0
    )
    if response.status_code >= 400:
        raise RuntimeError(f"source sample read failed status={response.status_code}")
    return base64.b64encode(response.content).decode("ascii")


def hash_source_stream(
    url: str, part_size: int | None = None, sha256: bool = True
) -> tuple[str | None, list[str]]:
    """Stream a URL once, returning the sha256 of the whole body and the MD5 of every part.

    The Hub will not issue upload URLs without the sha256, and the per-part MD5 is the ETag
    the completion payload has to quote.
    """
    import httpx  # type: ignore

    whole = hashlib.sha256() if sha256 else None
    part = hashlib.md5() if part_size else None
    part_bytes = 0
    part_etags: list[str] = []

    with httpx.stream("GET", url, follow_redirects=True, timeout=None) as response:
        if response.status_code >= 400:
            raise RuntimeError(f"source hash read failed status={response.status_code}")
        for chunk in response.iter_bytes():
            if whole is not None:
                whole.update(chunk)
            while part_size and part is not None and chunk:
                room = part_size - part_bytes
                if len(chunk) < room:
                    part.update(chunk)
                    part_bytes += len(chunk)
                    break
                part.update(chunk[:room])
                part_etags.append(part.hexdigest())
                part = hashlib.md5()
                part_bytes = 0
                chunk = chunk[room:]

    if part_size and part is not None and part_bytes > 0:
        part_etags.append(part.hexdigest())

    return (whole.hexdigest() if whole is not None else None), part_etags


def _json_headers(config: HuggingFaceConfig) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {config.token.get_secret_value()}",
        "Content-Type": "application/json",
    }


def _read_json(response: Any, step: str, config: HuggingFaceConfig) -> dict[str, Any]:
    _assert_ok(response, step, config)
    return dict(response.json())


def _assert_ok(response: Any, step: str, config: HuggingFaceConfig) -> None:
    if response.status_code < 400:
        return
    detail = (response.text or "")[:512]
    suffix = f": {detail}" if detail else ""
    raise RuntimeError(
        f"Hugging Face {step} failed status={response.status_code} for {describe(config)}{suffix}"
    )


def _same_host(left: str, right: str) -> bool:
    try:
        return urlsplit(left).netloc == urlsplit(right).netloc
    except ValueError:
        return True


def _normalize_etag(value: str | None) -> str | None:
    if value is None:
        return None
    return value.removeprefix("W/").strip('"')
