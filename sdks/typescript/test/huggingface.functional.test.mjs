import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import http from "node:http";
import test from "node:test";

import {
  hashSourceStream,
  huggingFaceApiBase,
  huggingFaceCommit,
  huggingFaceCompleteLfsUpload,
  huggingFaceLfsBatch,
  huggingFaceLfsBatchUrl,
  huggingFacePreupload,
  huggingFaceResolveUrl,
  readSourceSample
} from "../dist/huggingface.js";
import {
  isHuggingFaceProvider,
  prepareProviderDestination,
  prepareProviderSource
} from "../dist/provider-signing.js";
import { HuggingFaceProviderConfig } from "../dist/models.js";

const TOKEN = "hf_test_token";

test("hub URLs follow the repo-type prefix and revision encoding", () => {
  const dataset = {
    provider: "huggingface",
    repo_id: "acme/corpus",
    path: "data/train.parquet",
    repo_type: "dataset",
    revision: "refs/pr/4",
    token: TOKEN
  };
  assert.equal(
    huggingFaceResolveUrl(dataset),
    "https://huggingface.co/datasets/acme/corpus/resolve/refs%2Fpr%2F4/data/train.parquet"
  );
  assert.equal(huggingFaceApiBase(dataset), "https://huggingface.co/api/datasets/acme/corpus");
  assert.equal(
    huggingFaceLfsBatchUrl(dataset),
    "https://huggingface.co/datasets/acme/corpus.git/info/lfs/objects/batch"
  );

  // A model repo carries no prefix and defaults to `main`.
  const model = { provider: "huggingface", repo_id: "acme/net", path: "model.safetensors", token: TOKEN };
  assert.equal(
    huggingFaceResolveUrl(model),
    "https://huggingface.co/acme/net/resolve/main/model.safetensors"
  );
  assert.equal(huggingFaceApiBase(model), "https://huggingface.co/api/models/acme/net");
});

test("config validation rejects a bare repo id and an unknown repo type", () => {
  assert.throws(
    () => HuggingFaceProviderConfig.create({ repo_id: "corpus", path: "a.parquet", token: TOKEN }),
    /repo_id must be `namespace\/name`/
  );
  assert.throws(
    () => HuggingFaceProviderConfig.create({
      repo_id: "acme/corpus",
      path: "a.parquet",
      token: TOKEN,
      repo_type: "notarepotype"
    }),
    /repo_type must be one of/
  );
  const config = HuggingFaceProviderConfig.create({
    repo_id: "acme/corpus",
    path: "a.parquet",
    token: TOKEN
  });
  assert.equal(config.provider, "huggingface");
  assert.equal(isHuggingFaceProvider(config), true);
});

test("a bucket is unversioned and escapes its whole key as one component", () => {
  const bucket = {
    provider: "huggingface",
    repo_id: "acme/store",
    path: "nested/data.bin",
    repo_type: "bucket",
    token: TOKEN
  };
  // No revision segment, and the key is escaped whole - see HfApi.get_bucket_file_metadata.
  assert.equal(
    huggingFaceResolveUrl(bucket),
    "https://huggingface.co/buckets/acme/store/resolve/nested%2Fdata.bin"
  );
  // A revision is accepted on the config but ignored for buckets.
  assert.equal(
    huggingFaceResolveUrl({ ...bucket, revision: "v2" }),
    "https://huggingface.co/buckets/acme/store/resolve/nested%2Fdata.bin"
  );
});

test("a source resolves to the credential-free CDN redirect", async () => {
  const cdn = await startCdn(Buffer.alloc(0));
  const hub = await startHub({ cdnOrigin: cdn.origin, size: 4096, sha256: "a".repeat(64) });
  try {
    const source = {
      provider: "huggingface",
      repo_id: "acme/corpus",
      path: "data/train.parquet",
      repo_type: "dataset",
      token: TOKEN,
      endpoint: hub.origin
    };
    const prepared = await prepareProviderSource(source, { index: 0 });

    assert.equal(prepared.provider, "huggingface");
    assert.equal(prepared.type, "http");
    // X-Linked-Size wins over the redirect body's Content-Length.
    assert.equal(prepared.size, 4096);
    assert.equal(prepared.filename, "train.parquet");
    assert.ok(prepared.url.startsWith(cdn.origin), `expected a CDN url, got ${prepared.url}`);
    // No credential may travel with the prepared source.
    assert.equal(prepared.headers, undefined);
    assert.ok(!JSON.stringify(prepared).includes(TOKEN));
    assert.equal(prepared.metadata.sha256, "a".repeat(64));
    assert.equal(prepared.metadata.commit_hash, "deadbeef");
    assert.equal(prepared.metadata.repo_type, "dataset");
    assert.equal(prepared.metadata.revision, "main");

    assert.deepEqual(hub.requests, [
      "HEAD /datasets/acme/corpus/resolve/main/data/train.parquet"
    ]);
    assert.equal(hub.authorizations[0], `Bearer ${TOKEN}`);
  } finally {
    await cdn.close();
    await hub.close();
  }
});

test("a source served inline instead of redirected is rejected", async () => {
  const hub = await startHub({ cdnOrigin: null, size: 512 });
  try {
    await assert.rejects(
      prepareProviderSource({
        provider: "huggingface",
        repo_id: "acme/corpus",
        path: "README.md",
        repo_type: "dataset",
        token: TOKEN,
        endpoint: hub.origin
      }),
      /A small regular file is served inline from the Hub/
    );
  } finally {
    await hub.close();
  }
});

test("a destination prepares as a direct-put target keyed by the repo path", () => {
  const prepared = prepareProviderDestination({
    provider: "huggingface",
    repo_id: "acme/corpus",
    path: "data/out.parquet",
    repo_type: "dataset",
    token: TOKEN
  }, { index: 1 });

  assert.equal(prepared.destination_id, "dst_1");
  assert.equal(prepared.provider, "huggingface");
  assert.equal(prepared.logical_prefix, "data/out.parquet");
  assert.equal(prepared.metadata.driver, "huggingface");
  assert.ok(!JSON.stringify(prepared).includes(TOKEN));
});

test("preupload, LFS batch, completion and commit speak the documented protocol", async () => {
  const hub = await startHub({ cdnOrigin: null, size: 0, chunkSize: 1024, partCount: 3 });
  try {
    const destination = {
      provider: "huggingface",
      repo_id: "acme/corpus",
      path: "data/out.parquet",
      repo_type: "dataset",
      token: TOKEN,
      endpoint: hub.origin,
      commit_message: "Add out.parquet"
    };

    const preupload = await huggingFacePreupload(fetch, destination, { size: 3000, sample: "AAAA" });
    assert.deepEqual(preupload, { uploadMode: "lfs", shouldIgnore: false, oid: undefined });
    assert.deepEqual(hub.bodies["/api/datasets/acme/corpus/preupload/main"], {
      files: [{ path: "data/out.parquet", sample: "AAAA", size: 3000 }]
    });

    const plan = await huggingFaceLfsBatch(fetch, destination, { oid: "b".repeat(64), size: 3000 });
    assert.deepEqual(hub.bodies["/datasets/acme/corpus.git/info/lfs/objects/batch"], {
      operation: "upload",
      transfers: ["basic", "multipart"],
      hash_algo: "sha256",
      ref: { name: "main" },
      objects: [{ oid: "b".repeat(64), size: 3000 }]
    });
    assert.equal(plan.upload.chunkSize, 1024);
    // Zero-padded part keys must sort numerically, not lexically.
    assert.deepEqual(plan.upload.partUrls, [
      `${hub.origin}/part/1`,
      `${hub.origin}/part/2`,
      `${hub.origin}/part/3`
    ]);
    assert.equal(plan.verifyHref, `${hub.origin}/lfs/verify`);

    await huggingFaceCompleteLfsUpload(fetch, destination, {
      href: plan.upload.href,
      oid: plan.oid,
      etags: ["e1", "e2", "e3"]
    });
    assert.deepEqual(hub.bodies["/lfs/complete"], {
      oid: "b".repeat(64),
      parts: [
        { partNumber: 1, etag: "e1" },
        { partNumber: 2, etag: "e2" },
        { partNumber: 3, etag: "e3" }
      ]
    });

    await huggingFaceCommit(fetch, destination, { oid: plan.oid, size: 3000 });
    assert.deepEqual(hub.ndjson["/api/datasets/acme/corpus/commit/main"], [
      { key: "header", value: { summary: "Add out.parquet", description: "" } },
      {
        key: "lfsFile",
        value: { path: "data/out.parquet", algo: "sha256", oid: "b".repeat(64), size: 3000 }
      }
    ]);
  } finally {
    await hub.close();
  }
});

test("a part count that disagrees with the chunk size is rejected", async () => {
  const hub = await startHub({ cdnOrigin: null, size: 0, chunkSize: 1024, partCount: 2 });
  try {
    await assert.rejects(
      huggingFaceLfsBatch(fetch, {
        provider: "huggingface",
        repo_id: "acme/corpus",
        path: "data/out.parquet",
        repo_type: "dataset",
        token: TOKEN,
        endpoint: hub.origin
      }, { oid: "c".repeat(64), size: 3000 }),
      /returned 2 part URLs .* expected 3 at chunk_size 1024/
    );
  } finally {
    await hub.close();
  }
});

test("an already-stored object comes back with no upload actions", async () => {
  const hub = await startHub({ cdnOrigin: null, size: 0, alreadyUploaded: true });
  try {
    const plan = await huggingFaceLfsBatch(fetch, {
      provider: "huggingface",
      repo_id: "acme/corpus",
      path: "data/out.parquet",
      repo_type: "dataset",
      token: TOKEN,
      endpoint: hub.origin
    }, { oid: "d".repeat(64), size: 3000 });
    assert.equal(plan.upload, undefined);
    assert.equal(plan.oid, "d".repeat(64));
  } finally {
    await hub.close();
  }
});

test("the hashing pass yields the object sha256 and one MD5 per part", async () => {
  const body = Buffer.from(Array.from({ length: 2500 }, (_, index) => index % 251));
  const cdn = await startCdn(body);
  try {
    const sample = await readSourceSample(fetch, cdn.origin);
    assert.equal(sample, body.subarray(0, 512).toString("base64"));

    const whole = await hashSourceStream(fetch, cdn.origin);
    assert.equal(whole.sha256, createHash("sha256").update(body).digest("hex"));
    assert.deepEqual(whole.partEtags, []);

    const parts = await hashSourceStream(fetch, cdn.origin, { partSize: 1024, sha256: false });
    assert.equal(parts.sha256, undefined);
    assert.deepEqual(parts.partEtags, [
      createHash("md5").update(body.subarray(0, 1024)).digest("hex"),
      createHash("md5").update(body.subarray(1024, 2048)).digest("hex"),
      createHash("md5").update(body.subarray(2048)).digest("hex")
    ]);
  } finally {
    await cdn.close();
  }
});

/** Stands in for the presigned CDN the Hub redirects to, and for a plain source object. */
async function startCdn(body) {
  const server = http.createServer((request, response) => {
    const range = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    const slice = range ? body.subarray(Number(range[1]), Number(range[2]) + 1) : body;
    response.statusCode = range ? 206 : 200;
    response.setHeader("Content-Length", String(slice.length));
    response.end(slice);
  });
  await listen(server);
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    close: () => close(server)
  };
}

async function startHub(options) {
  const state = { requests: [], authorizations: [], bodies: {}, ndjson: {} };
  const server = http.createServer(async (request, response) => {
    const [path] = request.url.split("?");
    state.requests.push(`${request.method} ${request.url}`);
    state.authorizations.push(request.headers.authorization);

    if (request.method === "HEAD") {
      if (options.cdnOrigin) {
        response.statusCode = 302;
        response.setHeader("Location", `${options.cdnOrigin}/cas/blob?sig=abc`);
        response.setHeader("X-Linked-Size", String(options.size));
        response.setHeader("X-Linked-Etag", `"${options.sha256}"`);
        response.setHeader("X-Repo-Commit", "deadbeef");
        // A redirect body length that must not be mistaken for the object size.
        response.setHeader("Content-Length", "0");
      } else {
        // Served inline, the way a Xet blob or a small regular file is.
        response.statusCode = 200;
        response.setHeader("Content-Length", String(options.size));
        response.setHeader("ETag", '"0123456789abcdef"');
      }
      response.end();
      return;
    }

    const raw = await readBody(request);
    if (path.endsWith("/commit/main")) {
      state.ndjson[path] = raw.split("\n").filter(Boolean).map((line) => JSON.parse(line));
      json(response, { commitOid: "cafe" });
      return;
    }
    state.bodies[path] = JSON.parse(raw);

    if (path.includes("/preupload/")) {
      json(response, { files: [{ path: "data/out.parquet", uploadMode: "lfs", shouldIgnore: false }] });
      return;
    }
    if (path.endsWith("/info/lfs/objects/batch")) {
      const oid = state.bodies[path].objects[0].oid;
      if (options.alreadyUploaded) {
        json(response, { objects: [{ oid, size: options.size, actions: undefined }] });
        return;
      }
      const header = { chunk_size: String(options.chunkSize) };
      // Emit the parts out of order so the numeric sort is actually exercised.
      for (let part = options.partCount; part >= 1; part -= 1) {
        header[String(part).padStart(5, "0")] = `${state.origin}/part/${part}`;
      }
      json(response, {
        objects: [{
          oid,
          size: options.size,
          actions: {
            upload: { href: `${state.origin}/lfs/complete`, header },
            verify: { href: `${state.origin}/lfs/verify` }
          }
        }]
      });
      return;
    }
    json(response, {});
  });

  await listen(server);
  state.origin = `http://127.0.0.1:${server.address().port}`;
  return {
    origin: state.origin,
    requests: state.requests,
    authorizations: state.authorizations,
    bodies: state.bodies,
    ndjson: state.ndjson,
    close: () => close(server)
  };
}

function json(response, payload) {
  response.statusCode = 200;
  response.setHeader("Content-Type", "application/json");
  response.end(JSON.stringify(payload));
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let raw = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => { raw += chunk; });
    request.on("end", () => resolve(raw));
    request.on("error", reject);
  });
}

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
}

function close(server) {
  return new Promise((resolve) => server.close(resolve));
}
