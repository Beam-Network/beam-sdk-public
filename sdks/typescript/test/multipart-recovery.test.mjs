import assert from "node:assert/strict";
import test from "node:test";
import { signMultipartRecovery } from "../dist/provider-signing.js";

const destination = {provider: "r2", bucket: "dev-bucket", key: "file.bin", endpoint_url: "https://r2.example", access_key_id: "dev-key", secret_access_key: "dev-secret"};
const attempt = "22222222-2222-4222-8222-222222222222";
const requested = {multipart_group_id: "group:one/(a)", final_object_key: "file.bin", upload_id: "original", part_number: 1001,
  recovery: {operation: "upload", mode: "staged", attempt_id: attempt}};
const route = {dest_url: "https://r2.example/direct", metadata: {upload_id: "original", final_object_key: "file.bin", part_number: 1001, expected_part_count: 10000}};
const sign = (r = requested, provider = destination) => signMultipartRecovery({destination: provider, transferId: "transfer", requested: r, route, expiresIn: 600});

test("recovery signing keeps final identity separate and binds copy to its original part", async () => {
  const result = await sign(); const staging = result.metadata.recovery_staging;
  assert.equal(staging.object_key, `file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/1001/${attempt}`);
  assert.equal(result.metadata.upload_id, "original");
  assert.equal(new URL(result.dest_url).searchParams.has("uploadId"), false);
  assert.equal(new URL(staging.copy_url).searchParams.get("uploadId"), "original");
  assert.equal(new URL(staging.copy_url).searchParams.get("partNumber"), "1001");
  assert.ok(new URL(staging.copy_url).searchParams.get("X-Amz-SignedHeaders").split(";").includes("x-amz-copy-source"));
  assert.equal(new URL(staging.copy_url).searchParams.has("x-amz-copy-source"), false);
  assert.equal(staging.copy_headers["x-amz-copy-source-if-match"], undefined);
  assert.ok(staging.head_url && staging.delete_url);
});

test("each listing page is signed for the exact prefix and continuation", async () => {
  const result = await sign({...requested, recovery: {...requested.recovery, operation: "list", continuation_token: "a+/=&"}});
  const url = new URL(result.metadata.recovery_listing.url);
  assert.equal(url.searchParams.get("continuation-token"), "a+/=&");
  assert.equal(url.searchParams.get("prefix"), result.metadata.recovery_listing.prefix);
  assert.ok(url.searchParams.get("prefix").startsWith("file.bin.beam-recovery/transfer/"));
});

test("renewal regenerates all ten ListParts pages without changing multipart identity", async () => {
  const result = await sign({...requested, recovery: {...requested.recovery, operation: "renew", mode: "direct"}});
  assert.equal(result.metadata.list_page_urls.length, 10);
  for (const [i, page] of result.metadata.list_page_urls.entries()) {
    const url = new URL(page); assert.equal(url.searchParams.get("uploadId"), "original"); assert.equal(url.searchParams.get("part-number-marker"), String(i * 1000));
  }
  assert.equal(result.metadata.list_page_url, result.metadata.list_page_urls[1]);
  assert.equal(result.metadata.part_number, 1001);
});

test("AWS source conditions are quoted and an unrelated staging object is rejected", async () => {
  const result = await sign({...requested, recovery: {...requested.recovery, operation: "controls", etag: "opaque"}}, {...destination, provider: "s3", region: "us-east-1"});
  assert.equal(result.metadata.recovery_staging.copy_headers["x-amz-copy-source-if-match"], '"opaque"');
  await assert.rejects(sign({...requested, recovery: {...requested.recovery, object_key: "another-transfer"}}), /identity/);
});
