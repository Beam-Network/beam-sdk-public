import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import {
  abortMultipartUpload,
  listMultipartParts,
  completeMultipartUpload,
  inspectDestinationObject,
  createMultipartUpload,
  prepareProviderDestination,
  prepareProviderSource,
  signAbortMultipartUpload,
  signCompleteMultipartUpload,
  signDestinationRoute,
  signDestinationUrl,
  signSourceReadRange,
  signListMultipartUpload,
  s3CompatibleEndpoint,
  s3CompatibleForcePathStyle,
  s3CompatibleRegion
} from "../dist/provider-signing.js";

test("provider signing handles S3/R2 and generic S3-compatible helpers", async () => {
  const requests = [];
  const server = http.createServer((request, response) => {
    requests.push(`${request.method} ${request.url}`);
    if (request.method === "HEAD") {
      response.statusCode = 200;
      response.setHeader("Content-Length", "1234");
      response.end();
      return;
    }
    if (request.method === "POST" && request.url.includes("uploads")) {
      response.statusCode = 200;
      response.setHeader("Content-Type", "application/xml");
      response.end(
        "<CreateMultipartUploadResult><Bucket>dest-bucket</Bucket><Key>imports/report.parquet</Key><UploadId>upload_123</UploadId></CreateMultipartUploadResult>"
      );
      return;
    }
    if (request.method === "DELETE") {
      response.statusCode = 204;
      response.end();
      return;
    }
    response.statusCode = 404;
    response.end("not found");
  });

  await listen(server);
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const s3Source = {
      provider: "s3",
      bucket: "source-bucket",
      key: "exports/report.parquet",
      region: "us-east-1",
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const preparedSource = await prepareProviderSource(s3Source, { expiresIn: 600 });
    assert.equal(preparedSource.provider, "s3");
    assert.equal(preparedSource.size, 1234);
    assert.match(preparedSource.url, /X-Amz-Signature=/);

    const r2Source = {
      provider: "r2",
      bucket: "source-bucket",
      key: "exports/report.parquet",
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const preparedR2Source = await prepareProviderSource(r2Source, { expiresIn: 600 });
    assert.equal(preparedR2Source.provider, "r2");
    assert.equal(preparedR2Source.metadata.driver, "s3-compatible");
    assert.equal(preparedR2Source.metadata.endpoint_url, endpoint);

    const wasabiSource = {
      provider: "wasabi",
      driver: "s3-compatible",
      bucket: "custom-source-bucket",
      key: "objects/custom.bin",
      region: "us-east-1",
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const preparedWasabiSource = await prepareProviderSource(wasabiSource, { expiresIn: 600 });
    assert.equal(preparedWasabiSource.provider, "wasabi");
    assert.equal(preparedWasabiSource.metadata.driver, "s3-compatible");
    assert.equal(preparedWasabiSource.metadata.bucket, "custom-source-bucket");
    assert.equal(preparedWasabiSource.metadata.key, "objects/custom.bin");
    assert.equal(preparedWasabiSource.metadata.region, "us-east-1");
    assert.equal(preparedWasabiSource.metadata.endpoint_url, endpoint);
    assert.ok(preparedWasabiSource.url.startsWith(`${endpoint}/custom-source-bucket/objects/custom.bin`));

    const destination = {
      provider: "s3",
      bucket: "dest-bucket",
      key: "imports/report.parquet",
      region: "us-east-1",
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const preparedDestination = prepareProviderDestination(destination);
    assert.equal(preparedDestination.provider, "s3");

    const customDestination = {
      provider: "minio",
      driver: "s3-compatible",
      bucket: "archive",
      key: "file.bin",
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const preparedCustomDestination = prepareProviderDestination(customDestination);
    assert.equal(preparedCustomDestination.provider, "minio");
    assert.equal(preparedCustomDestination.logical_prefix, "file.bin");
    assert.equal(preparedCustomDestination.metadata.driver, "s3-compatible");
    assert.equal(preparedCustomDestination.metadata.endpoint_url, endpoint);

    const uploadId = await createMultipartUpload({
      destination,
      objectKey: "imports/report.parquet",
      metadata: {
        "beam-transfer-id": "transfer_123"
      }
    });
    assert.equal(uploadId, "upload_123");
    assert.match(
      await signCompleteMultipartUpload(destination, "imports/report.parquet", uploadId, 600),
      /X-Amz-Signature=/
    );
    assert.match(
      await signAbortMultipartUpload(destination, "imports/report.parquet", uploadId, 600),
      /X-Amz-Signature=/
    );
    const listUrl = await signListMultipartUpload(destination, "imports/report.parquet", uploadId, 600);
    assert.match(listUrl, /X-Amz-Signature=/);
    assert.match(listUrl, /x-id=ListParts/);
    const route = await signDestinationRoute({
      chunk: {
        chunk_index: 0,
        source_id: "src_0",
        source_chunk_index: 0,
        source_offset: 0,
        chunk_size: 1234,
        source_url: "https://source.example/read",
        destinations: []
      },
      target: {
        destination_id: "dst_0",
        object_key: "imports/report.parquet",
        metadata: { final_object_key: "imports/report.parquet" }
      },
      destination,
      expiresIn: 600,
      partNumber: 1,
      uploadId,
      completeUrl: "https://dest.example/complete",
      abortUrl: "https://dest.example/abort",
      listPageUrl: "https://dest.example/list-parts",
      finalHeadUrl: "https://dest.example/head",
      finalObjectKey: "imports/report.parquet",
      expectedObjectSize: 1234,
      expectedPartCount: 1,
      maxPartNumber: 1,
      finalObjectMetadata: { "beam-transfer-id": "transfer_123" }
    });
    assert.match(route.dest_url, /X-Amz-Signature=/);
    assert.match(route.dest_url, /UploadPart/);
    assert.ok(decodeURIComponent(route.dest_url).includes("imports/report.parquet"));
    assert.equal(route.metadata.upload_id, uploadId);
    assert.equal(route.metadata.final_object_key, "imports/report.parquet");
    assert.equal(route.headers.Range, "bytes=0-1233");

    const customRoute = await signDestinationRoute({
      chunk: {
        chunk_index: 1,
        source_id: "src_0",
        source_chunk_index: 1,
        source_offset: 1234,
        chunk_size: 512,
        source_url: "https://source.example/read",
        destinations: []
      },
      target: {
        destination_id: "dst_custom",
        object_key: "file.bin",
        metadata: { final_object_key: "file.bin" }
      },
      destination: customDestination,
      expiresIn: 600
    });
    assert.ok(customRoute.dest_url.startsWith(`${endpoint}/archive/file.bin`));
    assert.match(customRoute.dest_url, /X-Amz-Signature=/);
    assert.equal(customRoute.headers.Range, "bytes=1234-1745");

    await abortMultipartUpload(destination, "imports/report.parquet", uploadId);

    assert.throws(
      () => prepareProviderDestination({ provider: "r2", bucket: "b", key: "k", access_key_id: "ak", secret_access_key: "sk" }),
      /account_id or endpoint_url/
    );
    assert.ok(requests.some((request) => request.startsWith("HEAD ")));
    assert.ok(requests.some((request) => request.startsWith("HEAD /custom-source-bucket/objects/custom.bin")));
    assert.ok(requests.some((request) => request.startsWith("POST ")));
    assert.ok(requests.some((request) => request.startsWith("DELETE ")));
  } finally {
    await close(server);
  }
});

test("S3-compatible endpoint, region, and path-style resolution is provider-aware", () => {
  const r2 = {
    provider: "r2",
    bucket: "bucket",
    key: "file.bin",
    account_id: "account123",
    access_key_id: "ak",
    secret_access_key: "sk"
  };
  assert.equal(s3CompatibleEndpoint(r2), "https://account123.r2.cloudflarestorage.com");
  assert.equal(s3CompatibleRegion(r2), "auto");
  assert.equal(s3CompatibleForcePathStyle(r2), true);

  const custom = {
    provider: "wasabi",
    driver: "s3-compatible",
    bucket: "bucket",
    key: "file.bin",
    endpoint_url: "https://s3.us-east-1.wasabisys.com",
    access_key_id: "ak",
    secret_access_key: "sk"
  };
  assert.equal(s3CompatibleEndpoint(custom), "https://s3.us-east-1.wasabisys.com");
  assert.equal(s3CompatibleRegion(custom), "us-east-1");
  assert.equal(s3CompatibleForcePathStyle(custom), true);
  assert.equal(s3CompatibleForcePathStyle({ ...custom, force_path_style: false }), false);

  const s3 = {
    provider: "s3",
    bucket: "bucket",
    key: "file.bin",
    access_key_id: "ak",
    secret_access_key: "sk"
  };
  assert.equal(s3CompatibleEndpoint(s3), undefined);
  assert.equal(s3CompatibleRegion(s3), "us-east-1");
  assert.equal(s3CompatibleForcePathStyle(s3), undefined);
});

test("S3-compatible multipart cleanup retries transient provider failures", async () => {
  let abortAttempts = 0;
  const server = http.createServer((request, response) => {
    if (request.method === "DELETE") {
      abortAttempts += 1;
      if (abortAttempts < 3) {
        response.statusCode = 503;
        response.setHeader("Content-Type", "application/xml");
        response.end("<Error><Code>SlowDown</Code><Message>retry later</Message></Error>");
        return;
      }
      response.statusCode = 204;
      response.end();
      return;
    }
    response.statusCode = 404;
    response.end("not found");
  });

  await listen(server);
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    await abortMultipartUpload(
      {
        provider: "r2",
        bucket: "destination",
        key: "output.bin",
        endpoint_url: endpoint,
        access_key_id: "ak",
        secret_access_key: "sk"
      },
      "output.bin",
      "upload-retry"
    );
    assert.equal(abortAttempts, 3);
  } finally {
    await close(server);
  }
});

function listen(server) {
  return new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

test("hybrid signing composes frozen source ranges and checksum-bound uploads without data reads", async () => {
  for (const provider of ["r2", "hippius", "huggingface"]) {
    const config = { provider, driver: "s3-compatible", bucket: "bucket", key: "file.bin",
      access_key_id: "fixture-key", secret_access_key: "fixture-secret", region: "us-east-1",
      endpoint_url: "https://storage.example.test", force_path_style: true };
    const noRead = async () => { throw new Error("signing must not read source data"); };
    const source = await signSourceReadRange({ source: config, offset: 10, length: 20, expiresIn: 60,
      ifMatch: '"frozen-etag"', versionId: "version-1", fetchImpl: noRead });
    const sourceUrl = new URL(source.url);
    assert.equal(source.headers.Range, "bytes=10-29");
    assert.equal(source.headers["If-Match"], '"frozen-etag"');
    assert.equal(sourceUrl.searchParams.get("versionId"), "version-1");
    assert.ok(sourceUrl.searchParams.get("X-Amz-SignedHeaders").includes("if-match"));
    const destination = new URL(await signDestinationUrl({ destination: config, objectKey: "final.bin",
      uploadId: "upload-1", partNumber: 1, expiresIn: 60, contentMd5: "1B2M2Y8AsgTpgAmY7PhCfg==", fetchImpl: noRead }));
    assert.equal(destination.pathname, "/bucket/final.bin");
    assert.equal(destination.searchParams.get("uploadId"), "upload-1");
    assert.equal(destination.searchParams.get("partNumber"), "1");
    assert.ok(destination.searchParams.get("X-Amz-SignedHeaders").includes("content-md5"));
  }
});


test("provider verification paginates parts and completes without reading object payloads", async () => {
  const requests = [];
  const server = http.createServer((request, response) => {
    requests.push(request.method + " " + request.url);
    if (request.method === "HEAD") {
      response.setHeader("Content-Length", "12"); response.setHeader("ETag", '"final"');
      response.setHeader("x-amz-meta-beam-room-operation-id", "operation"); response.end(); return;
    }
    response.setHeader("Content-Type", "application/xml");
    if (request.method === "GET" && request.url.includes("uploadId=")) {
      const second = request.url.includes("part-number-marker=1");
      response.end('<ListPartsResult><IsTruncated>' + !second + '</IsTruncated>' +
        (second ? '' : '<NextPartNumberMarker>1</NextPartNumberMarker>') +
        '<Part><PartNumber>' + (second ? 2 : 1) + '</PartNumber><ETag>' + (second ? 'b' : 'a') + '</ETag><Size>6</Size></Part></ListPartsResult>'); return;
    }
    if (request.method === "POST") { response.end('<CompleteMultipartUploadResult><ETag>final</ETag></CompleteMultipartUploadResult>'); return; }
    response.statusCode = 500; response.end();
  });
  await listen(server);
  try {
    const config = { provider: "s3", bucket: "bucket", key: "file", endpoint_url: 'http://127.0.0.1:' + server.address().port,
      force_path_style: true, access_key_id: "test", secret_access_key: "test" };
    const parts = await listMultipartParts(config, "file", "upload");
    assert.deepEqual(parts, [{ partNumber: 1, etag: "a", size: 6 }, { partNumber: 2, etag: "b", size: 6 }]);
    assert.equal((await completeMultipartUpload({ destination: config, objectKey: "file", uploadId: "upload", parts })).etag, "final");
    const head = await inspectDestinationObject(config, "file");
    assert.equal(head.size, 12); assert.equal(head.metadata["beam-room-operation-id"], "operation");
    assert.equal(requests.length, 4); assert.equal(requests.filter((value) => value.startsWith("GET ")).length, 2);
  } finally { await close(server); }
});

test("provider metadata cancellation stops every in-flight HTTP operation", async () => {
 const operations = [
  (destination, signal) => createMultipartUpload({destination,objectKey:"file",metadata:{},signal}),
  (destination, signal) => listMultipartParts(destination,"file","upload",signal),
  (destination, signal) => completeMultipartUpload({destination,objectKey:"file",uploadId:"upload",parts:[{partNumber:1,etag:"part"}],signal}),
  (destination, signal) => inspectDestinationObject(destination,"file",signal),
  (destination, signal) => abortMultipartUpload(destination,"file","upload",signal),
 ];
 for (const operation of operations) {
  const controller = new AbortController();
  let received = false;
  const server = http.createServer(() => { received = true; controller.abort(); });
  await listen(server);
  try {
   const destination = { provider:"s3",bucket:"bucket",key:"file",region:"us-east-1",access_key_id:"test",secret_access_key:"test",endpoint_url:`http://127.0.0.1:${server.address().port}` };
   await assert.rejects(operation(destination, controller.signal), {name:"AbortError"});
   assert.equal(received,true,"request must reach the provider before cancellation");
  } finally { server.closeAllConnections(); await close(server); }
 }
});
