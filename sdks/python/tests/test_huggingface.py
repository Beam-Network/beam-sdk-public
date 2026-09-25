from __future__ import annotations

import base64
import hashlib
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from beam_network_sdk import huggingface as hf
from beam_network_sdk.models import (
    HuggingFaceProviderDestination,
    HuggingFaceProviderSource,
)
from beam_network_sdk.provider_signing import (
    prepare_provider_destination,
    prepare_provider_source,
)

TOKEN = "hf_test_token"


class HuggingFaceUrlTests(unittest.TestCase):
    def test_urls_follow_the_repo_type_prefix_and_revision_encoding(self) -> None:
        dataset = HuggingFaceProviderSource(
            repo_id="acme/corpus",
            path="data/train.parquet",
            repo_type="dataset",
            revision="refs/pr/4",
            token=TOKEN,
        )
        self.assertEqual(
            hf.resolve_url(dataset),
            "https://huggingface.co/datasets/acme/corpus/resolve/refs%2Fpr%2F4/data/train.parquet",
        )
        self.assertEqual(hf.api_base(dataset), "https://huggingface.co/api/datasets/acme/corpus")
        self.assertEqual(
            hf.lfs_batch_url(dataset),
            "https://huggingface.co/datasets/acme/corpus.git/info/lfs/objects/batch",
        )

        # A model repo carries no prefix and defaults to `main`.
        model = HuggingFaceProviderSource(
            repo_id="acme/net", path="model.safetensors", token=TOKEN
        )
        self.assertEqual(
            hf.resolve_url(model),
            "https://huggingface.co/acme/net/resolve/main/model.safetensors",
        )
        self.assertEqual(hf.api_base(model), "https://huggingface.co/api/models/acme/net")

    def test_a_bucket_is_unversioned_and_escapes_its_whole_key(self) -> None:
        bucket = HuggingFaceProviderSource(
            repo_id="acme/store",
            path="nested/data.bin",
            repo_type="bucket",
            revision="v2",
            token=TOKEN,
        )
        # No revision segment, and the key is escaped whole -- see HfApi.get_bucket_file_metadata.
        self.assertEqual(
            hf.resolve_url(bucket),
            "https://huggingface.co/buckets/acme/store/resolve/nested%2Fdata.bin",
        )


class HuggingFaceSourceTests(unittest.TestCase):
    def test_source_resolves_to_the_credential_free_cdn_redirect(self) -> None:
        with serve_cdn(b"") as cdn, serve_hub(cdn_origin=cdn.origin, size=4096, sha256="a" * 64) as hub:
            prepared = prepare_provider_source(
                HuggingFaceProviderSource(
                    repo_id="acme/corpus",
                    path="data/train.parquet",
                    repo_type="dataset",
                    token=TOKEN,
                    endpoint=hub.origin,
                ),
                index=0,
            )

        self.assertEqual(prepared.provider, "huggingface")
        # X-Linked-Size wins over the redirect body's Content-Length.
        self.assertEqual(prepared.size, 4096)
        self.assertEqual(prepared.filename, "train.parquet")
        self.assertTrue(prepared.url.startswith(cdn.origin))
        # No credential may travel with the prepared source.
        self.assertIsNone(prepared.headers)
        self.assertNotIn(TOKEN, prepared.model_dump_json())
        self.assertEqual(prepared.metadata["sha256"], "a" * 64)
        self.assertEqual(prepared.metadata["commit_hash"], "deadbeef")
        self.assertEqual(prepared.metadata["repo_type"], "dataset")

        self.assertEqual(
            hub.requests, ["HEAD /datasets/acme/corpus/resolve/main/data/train.parquet"]
        )
        self.assertEqual(hub.authorizations[0], f"Bearer {TOKEN}")

    def test_source_without_a_cross_host_redirect_is_rejected(self) -> None:
        with serve_hub(cdn_origin=None, size=512) as hub:
            with self.assertRaises(RuntimeError) as raised:
                prepare_provider_source(
                    HuggingFaceProviderSource(
                        repo_id="acme/corpus",
                        path="README.md",
                        repo_type="dataset",
                        token=TOKEN,
                        endpoint=hub.origin,
                    )
                )
        self.assertIn("A small regular file is served inline from the Hub", str(raised.exception))


class HuggingFaceDestinationTests(unittest.TestCase):
    def test_destination_prepares_as_a_direct_put_target(self) -> None:
        prepared = prepare_provider_destination(
            HuggingFaceProviderDestination(
                repo_id="acme/corpus",
                path="data/out.parquet",
                repo_type="dataset",
                token=TOKEN,
            ),
            index=1,
        )
        self.assertEqual(prepared.destination_id, "dst_1")
        self.assertEqual(prepared.provider, "huggingface")
        self.assertEqual(prepared.logical_prefix, "data/out.parquet")
        self.assertEqual(prepared.metadata["driver"], "huggingface")
        self.assertNotIn(TOKEN, prepared.model_dump_json())

    def test_preupload_batch_completion_and_commit_speak_the_protocol(self) -> None:
        with serve_hub(cdn_origin=None, size=0, chunk_size=1024, part_count=3) as hub:
            destination = HuggingFaceProviderDestination(
                repo_id="acme/corpus",
                path="data/out.parquet",
                repo_type="dataset",
                token=TOKEN,
                endpoint=hub.origin,
                commit_message="Add out.parquet",
            )

            self.assertEqual(hf.preupload(destination, 3000, "AAAA"), ("lfs", False))
            self.assertEqual(
                hub.bodies["/api/datasets/acme/corpus/preupload/main"],
                {"files": [{"path": "data/out.parquet", "sample": "AAAA", "size": 3000}]},
            )

            plan = hf.lfs_batch(destination, "b" * 64, 3000)
            self.assertEqual(
                hub.bodies["/datasets/acme/corpus.git/info/lfs/objects/batch"],
                {
                    "operation": "upload",
                    "transfers": ["basic", "multipart"],
                    "hash_algo": "sha256",
                    "ref": {"name": "main"},
                    "objects": [{"oid": "b" * 64, "size": 3000}],
                },
            )
            self.assertEqual(plan.chunk_size, 1024)
            # Zero-padded part keys must sort numerically, not lexically.
            self.assertEqual(
                plan.part_urls,
                [f"{hub.origin}/part/1", f"{hub.origin}/part/2", f"{hub.origin}/part/3"],
            )
            self.assertEqual(plan.verify_href, f"{hub.origin}/lfs/verify")

            hf.complete_lfs_upload(destination, plan.upload_href, plan.oid, ["e1", "e2", "e3"])
            self.assertEqual(
                hub.bodies["/lfs/complete"],
                {
                    "oid": "b" * 64,
                    "parts": [
                        {"partNumber": 1, "etag": "e1"},
                        {"partNumber": 2, "etag": "e2"},
                        {"partNumber": 3, "etag": "e3"},
                    ],
                },
            )

            hf.commit(destination, plan.oid, 3000)
            self.assertEqual(
                hub.ndjson["/api/datasets/acme/corpus/commit/main"],
                [
                    {
                        "key": "header",
                        "value": {"summary": "Add out.parquet", "description": ""},
                    },
                    {
                        "key": "lfsFile",
                        "value": {
                            "path": "data/out.parquet",
                            "algo": "sha256",
                            "oid": "b" * 64,
                            "size": 3000,
                        },
                    },
                ],
            )

    def test_part_count_disagreeing_with_the_chunk_size_is_rejected(self) -> None:
        with serve_hub(cdn_origin=None, size=0, chunk_size=1024, part_count=2) as hub:
            with self.assertRaises(RuntimeError) as raised:
                hf.lfs_batch(
                    HuggingFaceProviderDestination(
                        repo_id="acme/corpus",
                        path="data/out.parquet",
                        repo_type="dataset",
                        token=TOKEN,
                        endpoint=hub.origin,
                    ),
                    "c" * 64,
                    3000,
                )
        self.assertIn("returned 2 part URLs", str(raised.exception))
        self.assertIn("expected 3 at chunk_size 1024", str(raised.exception))

    def test_already_stored_object_comes_back_with_no_upload_actions(self) -> None:
        with serve_hub(cdn_origin=None, size=0, already_uploaded=True) as hub:
            plan = hf.lfs_batch(
                HuggingFaceProviderDestination(
                    repo_id="acme/corpus",
                    path="data/out.parquet",
                    repo_type="dataset",
                    token=TOKEN,
                    endpoint=hub.origin,
                ),
                "d" * 64,
                3000,
            )
        self.assertIsNone(plan.upload_href)
        self.assertEqual(plan.oid, "d" * 64)


class HuggingFaceHashingTests(unittest.TestCase):
    def test_hash_pass_yields_the_sha256_and_one_md5_per_part(self) -> None:
        body = bytes(index % 251 for index in range(2500))
        with serve_cdn(body) as cdn:
            self.assertEqual(
                hf.read_source_sample(cdn.origin),
                base64.b64encode(body[:512]).decode("ascii"),
            )

            digest, part_etags = hf.hash_source_stream(cdn.origin)
            self.assertEqual(digest, hashlib.sha256(body).hexdigest())
            self.assertEqual(part_etags, [])

            digest, part_etags = hf.hash_source_stream(cdn.origin, part_size=1024, sha256=False)
            self.assertIsNone(digest)
            self.assertEqual(
                part_etags,
                [
                    hashlib.md5(body[:1024]).hexdigest(),
                    hashlib.md5(body[1024:2048]).hexdigest(),
                    hashlib.md5(body[2048:]).hexdigest(),
                ],
            )


class _Server:
    """A local HTTP server exposing what each test needs to assert."""

    def __init__(self, handler_factory) -> None:
        self.requests: list[str] = []
        self.authorizations: list[str | None] = []
        self.bodies: dict[str, object] = {}
        self.ndjson: dict[str, object] = {}
        self._server = HTTPServer(("127.0.0.1", 0), handler_factory(self))
        self.origin = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> "_Server":
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def serve_cdn(body: bytes) -> _Server:
    """Stands in for the presigned CDN the Hub redirects to, and for a plain source object."""

    def factory(state: _Server):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                raw_range = self.headers.get("Range")
                if raw_range and raw_range.startswith("bytes="):
                    start, end = raw_range.removeprefix("bytes=").split("-")
                    chunk = body[int(start) : int(end) + 1]
                    self.send_response(206)
                else:
                    chunk = body
                    self.send_response(200)
                self.send_header("Content-Length", str(len(chunk)))
                self.end_headers()
                self.wfile.write(chunk)

            def log_message(self, *_args: object) -> None:
                return

        return Handler

    return _Server(factory)


def serve_hub(
    *,
    cdn_origin: str | None,
    size: int,
    sha256: str = "0" * 64,
    chunk_size: int | None = None,
    part_count: int = 0,
    already_uploaded: bool = False,
) -> _Server:
    def factory(state: _Server):
        class Handler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:  # noqa: N802
                state.requests.append(f"HEAD {self.path}")
                state.authorizations.append(self.headers.get("Authorization"))
                if cdn_origin:
                    self.send_response(302)
                    self.send_header("Location", f"{cdn_origin}/cas/blob?sig=abc")
                    self.send_header("X-Linked-Size", str(size))
                    self.send_header("X-Linked-Etag", f'"{sha256}"')
                    self.send_header("X-Repo-Commit", "deadbeef")
                    # A redirect body length that must not be mistaken for the object size.
                    self.send_header("Content-Length", "0")
                else:
                    # Served inline, the way a Xet blob or a small regular file is.
                    self.send_response(200)
                    self.send_header("Content-Length", str(size))
                    self.send_header("ETag", '"0123456789abcdef"')
                self.end_headers()

            def do_POST(self) -> None:  # noqa: N802
                path = self.path.split("?")[0]
                state.requests.append(f"POST {self.path}")
                state.authorizations.append(self.headers.get("Authorization"))
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()

                if path.endswith("/commit/main"):
                    state.ndjson[path] = [json.loads(line) for line in raw.splitlines() if line]
                    return self._json({"commitOid": "cafe"})

                state.bodies[path] = json.loads(raw)

                if "/preupload/" in path:
                    return self._json(
                        {
                            "files": [
                                {
                                    "path": "data/out.parquet",
                                    "uploadMode": "lfs",
                                    "shouldIgnore": False,
                                }
                            ]
                        }
                    )
                if path.endswith("/info/lfs/objects/batch"):
                    oid = state.bodies[path]["objects"][0]["oid"]
                    if already_uploaded:
                        return self._json({"objects": [{"oid": oid, "size": size}]})
                    header = {"chunk_size": str(chunk_size)}
                    # Emit the parts out of order so the numeric sort is actually exercised.
                    for part in range(part_count, 0, -1):
                        header[f"{part:05d}"] = f"{state.origin}/part/{part}"
                    return self._json(
                        {
                            "objects": [
                                {
                                    "oid": oid,
                                    "size": size,
                                    "actions": {
                                        "upload": {
                                            "href": f"{state.origin}/lfs/complete",
                                            "header": header,
                                        },
                                        "verify": {"href": f"{state.origin}/lfs/verify"},
                                    },
                                }
                            ]
                        }
                    )
                return self._json({})

            def _json(self, payload: object) -> None:
                encoded = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, *_args: object) -> None:
                return

        return Handler

    return _Server(factory)


if __name__ == "__main__":
    unittest.main()
