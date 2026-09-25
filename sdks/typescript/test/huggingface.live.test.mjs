// Live checks against huggingface.co. Opt in with:
//
//   HF_TOKEN=hf_... HF_LIVE_BUCKET=owner/bucket HF_LIVE_BUCKET_PATH=file.bin \
//     node --test test/huggingface.live.test.mjs
//
// Without HF_TOKEN every test skips, so the default suite stays offline.
import assert from "node:assert/strict";
import test from "node:test";

import { prepareProviderSource } from "../dist/provider-signing.js";
import {
  hashSourceStream,
  huggingFaceCommit,
  huggingFaceCompleteLfsUpload,
  huggingFaceFileMetadata,
  huggingFaceLfsBatch,
  huggingFacePreupload,
  huggingFaceResolveUrl,
  readSourceSample
} from "../dist/huggingface.js";

const TOKEN = process.env.HF_TOKEN;
const skip = TOKEN ? false : "set HF_TOKEN to run the live Hugging Face checks";

// A public dataset whose parquet files are large-file (Xet-backed) content.
const PUBLIC_DATASET = {
  provider: "huggingface",
  repo_id: process.env.HF_LIVE_DATASET ?? "stanfordnlp/imdb",
  path: process.env.HF_LIVE_DATASET_PATH ?? "plain_text/test-00000-of-00001.parquet",
  repo_type: "dataset",
  token: TOKEN ?? ""
};

test("a large file resolves to a credential-free CDN URL that serves ranges", { skip }, async () => {
  const prepared = await prepareProviderSource(PUBLIC_DATASET);

  assert.equal(prepared.provider, "huggingface");
  assert.ok(prepared.size > 0);
  // The redirect target must be a different host than the Hub, or it is not presigned.
  assert.notEqual(new URL(prepared.url).host, new URL(huggingFaceResolveUrl(PUBLIC_DATASET)).host);
  assert.ok(!JSON.stringify(prepared).includes(TOKEN), "the token must not reach BeamCore");

  // A worker holds only this URL and a Range header — no credential of any kind.
  const response = await fetch(prepared.url, { headers: { Range: "bytes=0-1023" } });
  assert.equal(response.status, 206);
  assert.equal(response.headers.get("content-range"), `bytes 0-1023/${prepared.size}`);
  assert.equal((await response.arrayBuffer()).byteLength, 1024);
});

test("the linked ETag is the object's sha256", { skip }, async () => {
  const metadata = await huggingFaceFileMetadata(fetch, PUBLIC_DATASET);
  assert.match(metadata.etag ?? "", /^[0-9a-f]{64}$/, "expected a sha256, got " + metadata.etag);

  const { createHash } = await import("node:crypto");
  const hash = createHash("sha256");
  const response = await fetch(metadata.url);
  for await (const chunk of response.body) hash.update(chunk);
  assert.equal(hash.digest("hex"), metadata.etag);
});

test("a small regular file is rejected, not silently token-forwarded", { skip }, async () => {
  await assert.rejects(
    prepareProviderSource({ ...PUBLIC_DATASET, path: "README.md" }),
    /did not redirect .* to a presigned CDN URL/
  );
});

// Buckets are a fifth repo type with their own unversioned resolve URL.
const bucket = process.env.HF_LIVE_BUCKET;
const bucketSkip = skip || (bucket ? false : "set HF_LIVE_BUCKET to run the bucket checks");

test("a bucket file resolves the same way, and its token never leaves", { skip: bucketSkip }, async () => {
  const source = {
    provider: "huggingface",
    repo_id: bucket,
    path: process.env.HF_LIVE_BUCKET_PATH ?? "test.bin",
    repo_type: "bucket",
    token: TOKEN
  };
  // Unversioned: no revision segment in the path.
  assert.ok(!huggingFaceResolveUrl(source).includes("/resolve/main/"));

  const prepared = await prepareProviderSource(source);
  assert.ok(prepared.size > 0);
  assert.ok(!JSON.stringify(prepared).includes(TOKEN));

  const response = await fetch(prepared.url, { headers: { Range: "bytes=0-1023" } });
  assert.equal(response.status, 206);
  assert.equal((await response.arrayBuffer()).byteLength, 1024);
});

// The write path commits to a real repo, so it needs its own opt-in.
const writeRepo = process.env.HF_LIVE_WRITE_REPO;
const writeSkip = skip || (writeRepo ? false : "set HF_LIVE_WRITE_REPO to run the upload checks");

test("the upload path round-trips a file byte-identically", { skip: writeSkip }, async () => {
  const destination = {
    provider: "huggingface",
    repo_id: writeRepo,
    path: `beam/live-${Date.now()}.bin`,
    repo_type: "dataset",
    token: TOKEN,
    commit_message: "Beam SDK live test"
  };

  const source = await huggingFaceFileMetadata(fetch, PUBLIC_DATASET);
  const oid = source.etag;

  const sample = await readSourceSample(fetch, source.url);
  const pre = await huggingFacePreupload(fetch, destination, { size: source.size, sample });
  assert.equal(pre.uploadMode, "lfs");
  assert.equal(pre.shouldIgnore, false);

  const plan = await huggingFaceLfsBatch(fetch, destination, { oid, size: source.size });
  if (plan.upload?.chunkSize !== undefined) {
    const { partEtags } = await hashSourceStream(fetch, source.url, {
      partSize: plan.upload.chunkSize,
      sha256: false
    });
    assert.equal(partEtags.length, plan.upload.partUrls.length);

    // Stand in for Beam's workers, and check S3 agrees with the ETags we computed.
    for (const [index, partUrl] of plan.upload.partUrls.entries()) {
      const start = index * plan.upload.chunkSize;
      const end = Math.min(start + plan.upload.chunkSize, source.size) - 1;
      const body = await (await fetch(source.url, { headers: { Range: `bytes=${start}-${end}` } })).arrayBuffer();
      const put = await fetch(partUrl, { method: "PUT", body });
      assert.ok(put.ok, `part ${index + 1} PUT failed status=${put.status}`);
      assert.equal(
        (put.headers.get("etag") ?? "").replaceAll('"', ""),
        partEtags[index],
        `part ${index + 1}: S3 ETag disagrees with the locally computed MD5`
      );
    }
    await huggingFaceCompleteLfsUpload(fetch, destination, { href: plan.upload.href, oid, etags: partEtags });
  }

  await huggingFaceCommit(fetch, destination, { oid, size: source.size });

  // Without the commit the parts exist but the file does not; read it back to prove it landed.
  const readback = await huggingFaceFileMetadata(fetch, destination);
  assert.equal(readback.size, source.size);
  assert.equal(readback.etag, oid);
});
