from __future__ import annotations

import unittest

from beam_network_sdk.models import (
    AzureProviderDestination,
    AzureProviderSource,
    ChunkDestinationSigningTarget,
    ChunkSigningPlanItem,
    GCSProviderDestination,
    GCSProviderSource,
    R2ProviderDestination,
    R2ProviderSource,
    S3ProviderDestination,
    S3ProviderSource,
)
from beam_network_sdk.provider_signing import (
    abort_multipart_upload,
    create_multipart_upload,
    prepare_provider_destination,
    prepare_provider_source,
    sign_abort_multipart_upload,
    sign_complete_multipart_upload,
    sign_destination_route,
    sign_list_multipart_upload,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def head_object(self, **kwargs: object) -> dict[str, int]:
        self.calls.append(("head_object", kwargs))
        return {"ContentLength": 1234}

    def generate_presigned_url(
        self,
        operation: str,
        *,
        Params: dict[str, object],
        ExpiresIn: int,
    ) -> str:
        self.calls.append((operation, {"Params": Params, "ExpiresIn": ExpiresIn}))
        object_id = Params.get("Key") or Params.get("Prefix") or ""
        return f"https://signed.example/{operation}/{object_id}"

    def create_multipart_upload(self, **kwargs: object) -> dict[str, str]:
        self.calls.append(("create_multipart_upload", kwargs))
        return {"UploadId": "upload_123"}

    def abort_multipart_upload(self, **kwargs: object) -> None:
        self.calls.append(("abort_multipart_upload", kwargs))


class FakeFactory:
    def __init__(self) -> None:
        self.client = FakeS3Client()
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def __call__(self, *args: object, **kwargs: object) -> FakeS3Client:
        self.calls.append((args, kwargs))
        return self.client


class ProviderSigningTests(unittest.TestCase):
    def test_s3_provider_source_destination_and_multipart_signing(self) -> None:
        factory = FakeFactory()
        source = S3ProviderSource(
            bucket="source-bucket",
            key="exports/report.parquet",
            region="eu-west-1",
            access_key_id="ak",
            secret_access_key="sk",
        )
        prepared_source = prepare_provider_source(
            source,
            index=2,
            expires_in=600,
            client_factory=factory,
        )
        self.assertEqual(prepared_source.source_id, "src_2")
        self.assertEqual(prepared_source.provider, "s3")
        self.assertEqual(prepared_source.size, 1234)
        self.assertIn("get_object", prepared_source.url)

        destination = S3ProviderDestination(
            bucket="dest-bucket",
            key="imports/report.parquet",
            region="us-east-1",
            access_key_id="ak",
            secret_access_key="sk",
        )
        prepared_destination = prepare_provider_destination(destination, index=3)
        self.assertEqual(prepared_destination.destination_id, "dst_3")
        self.assertEqual(prepared_destination.provider, "s3")

        upload_id = create_multipart_upload(
            destination=destination,
            object_key="imports/report.parquet",
            metadata={"beam-transfer-id": "transfer_123"},
            client_factory=factory,
        )
        self.assertEqual(upload_id, "upload_123")
        self.assertIn(
            "complete_multipart_upload",
            sign_complete_multipart_upload(
                destination=destination,
                object_key="imports/report.parquet",
                upload_id=upload_id,
                client_factory=factory,
            ),
        )
        self.assertIn(
            "abort_multipart_upload",
            sign_abort_multipart_upload(
                destination=destination,
                object_key="imports/report.parquet",
                upload_id=upload_id,
                client_factory=factory,
            ),
        )
        self.assertIn(
            "list_parts",
            sign_list_multipart_upload(
                destination=destination,
                object_key="imports/report.parquet",
                upload_id=upload_id,
                client_factory=factory,
            ),
        )
        route = sign_destination_route(
            chunk=ChunkSigningPlanItem(
                chunk_index=0,
                source_id="src_2",
                source_chunk_index=0,
                source_offset=0,
                chunk_size=1234,
                source_url="https://source.example/read",
                destinations=[],
            ),
            target=ChunkDestinationSigningTarget(
                destination_id="dst_3",
                provider="s3",
                object_key="imports/report.parquet",
                metadata={"final_object_key": "imports/report.parquet"},
            ),
            destination=destination,
            part_number=1,
            transfer_id="transfer_123",
            multipart_group_id="group_123",
            upload_id=upload_id,
            complete_url="https://dest.example/complete",
            abort_url="https://dest.example/abort",
            list_page_url="https://dest.example/list-parts",
            final_head_url="https://dest.example/head",
            final_object_key="imports/report.parquet",
            expected_object_size=1234,
            expected_part_count=1,
            max_part_number=3,
            final_object_metadata={"beam-transfer-id": "transfer_123"},
            client_factory=factory,
        )
        self.assertIn("upload_part", route.dest_url)
        self.assertIn("imports/report.parquet", route.dest_url)
        self.assertEqual(route.metadata["upload_id"], upload_id)
        self.assertEqual(route.metadata["multipart_group_id"], "group_123")
        self.assertEqual(route.metadata["final_object_key"], "imports/report.parquet")
        self.assertEqual(route.headers["Range"], "bytes=0-1233")

        self.assertIn(
            (
                "list_parts",
                {
                    "Params": {
                        "Bucket": "dest-bucket",
                        "Key": "imports/report.parquet",
                        "UploadId": upload_id,
                    },
                    "ExpiresIn": 3600,
                },
            ),
            factory.client.calls,
        )

        abort_multipart_upload(
            destination=destination,
            object_key="imports/report.parquet",
            upload_id=upload_id,
            client_factory=factory,
        )
        self.assertIn(("abort_multipart_upload", {"Bucket": "dest-bucket", "Key": "imports/report.parquet", "UploadId": upload_id}), factory.client.calls)

    def test_r2_and_placeholder_provider_behaviour(self) -> None:
        factory = FakeFactory()
        source = R2ProviderSource(
            bucket="source-bucket",
            key="exports/report.parquet",
            account_id="acct",
            access_key_id="ak",
            secret_access_key="sk",
        )
        prepared = prepare_provider_source(source, client_factory=factory)
        self.assertEqual(prepared.provider, "r2")
        self.assertEqual(
            prepared.metadata["endpoint_url"],
            "https://acct.r2.cloudflarestorage.com",
        )

        r2_dest = R2ProviderDestination(
            bucket="dest-bucket",
            key="imports/report.parquet",
            endpoint_url="https://r2.example",
            access_key_id="ak",
            secret_access_key="sk",
        )
        prepared_r2_dest = prepare_provider_destination(r2_dest)
        self.assertEqual(prepared_r2_dest.provider, "r2")

        gcs_dest = GCSProviderDestination(bucket="b", key="k", project_id="p")
        azure_dest = AzureProviderDestination(container="c", blob="b", account_name="acct")
        self.assertEqual(prepare_provider_destination(gcs_dest).provider, "gcs")
        self.assertEqual(prepare_provider_destination(azure_dest).provider, "azure")

        with self.assertRaises(NotImplementedError):
            prepare_provider_source(GCSProviderSource(bucket="b", key="k"))
        with self.assertRaises(NotImplementedError):
            prepare_provider_source(
                AzureProviderSource(container="c", blob="b", account_name="acct")
            )
        with self.assertRaises(NotImplementedError):
            sign_destination_route(
                chunk=ChunkSigningPlanItem(
                    chunk_index=0,
                    source_id="src",
                    source_chunk_index=0,
                    source_offset=0,
                    chunk_size=1,
                    source_url="https://source",
                    destinations=[],
                ),
                target=ChunkDestinationSigningTarget(destination_id="dst", object_key="k"),
                destination=gcs_dest,
            )


if __name__ == "__main__":
    unittest.main()
