from urllib.parse import quote
import unittest
from beam_network_sdk.models import R2ProviderDestination, S3ProviderDestination, SignedChunkRoute
from beam_network_sdk.provider_signing import sign_multipart_recovery

ATTEMPT = "22222222-2222-4222-8222-222222222222"
GROUP = "group:one/(a)"

def check_staging_grants(provider):
    calls = []
    class Client:
        def generate_presigned_url(self, op, *, Params, ExpiresIn):
            calls.append((op, Params))
            return "https://dev.example/" + op
    destination = provider(bucket="dev", key="file.bin", access_key_id="dev", secret_access_key="dev", endpoint_url="https://dev.example")
    route = SignedChunkRoute(source_id="src", destination_id="dst", chunk_index=1000, source_url="https://dev.example/source", dest_url="https://dev.example/dest", source_offset=0, chunk_size=8,
                             metadata={"upload_id": "original", "part_number": 1001, "expected_part_count": 10000})
    requested = dict(final_object_key="file.bin", multipart_group_id=GROUP, part_number=1001, upload_id="original", recovery=dict(operation="upload", mode="staged", attempt_id=ATTEMPT, etag="opaque"))
    def sign():
        return sign_multipart_recovery(destination=destination, transfer_id="transfer", requested=requested, route=route, expires_in=600, client_factory=lambda *a, **k: Client())
    result = sign()
    assert result.metadata["upload_id"] == "original"
    assert result.metadata["recovery_staging"]["object_key"] == f"file.bin.beam-recovery/transfer/{quote(GROUP, safe='')}/1001/{ATTEMPT}"
    copy = next(params for op, params in calls if op == "upload_part_copy")
    assert copy["UploadId"] == "original" and copy["PartNumber"] == 1001
    assert copy["CopySource"] == {"Bucket": "dev", "Key": result.metadata["recovery_staging"]["object_key"]}
    assert (copy.get("CopySourceIfMatch") == '"opaque"') == (provider is S3ProviderDestination)
    requested["recovery"].update(operation="list", continuation_token="a+/=&")
    sign()
    assert calls[-1][1]["ContinuationToken"] == "a+/=&"
    assert calls[-1][1]["Prefix"].endswith(quote(GROUP, safe="") + "/")
    requested["recovery"].update(operation="renew", mode="direct")
    renewed = sign()
    assert len(renewed.metadata["list_page_urls"]) == 10
    assert [p["PartNumberMarker"] for op, p in calls if op == "list_parts"] == list(range(0,10000,1000))
    requested["recovery"].update(operation="controls", mode="staged", object_key="wrong")
    with unittest.TestCase().assertRaisesRegex(ValueError, "identity"):
        sign()

class MultipartRecoveryTests(unittest.TestCase):
    def test_staging_grants_preserve_identity_and_scope(self):
        for provider in [R2ProviderDestination, S3ProviderDestination]:
            with self.subTest(provider=provider.__name__):
                check_staging_grants(provider)
