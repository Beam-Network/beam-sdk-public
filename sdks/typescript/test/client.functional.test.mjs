import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { decode, encode } from "@msgpack/msgpack";

import {
  BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY,
  BeamClient,
  BeamProviderTransferError,
  BeamRouteRecoveryPendingError,
  BeamStorageAccessError,
  BeamTransferFailedError,
  transferFailedError
} from "../dist/index.js";
import { multipartPartNumber } from "../dist/multipart-limits.js";
import {
  BEAM_DEFAULT_MAX_PAYLOAD_BYTES,
  BeamTransferControl,
  buildConnectionOptions,
  isRecoverableRouteStreamError
} from "../dist/nats-control.js";

test("route messages default to an 8 MiB encoded payload guard", () => {
  assert.equal(BEAM_DEFAULT_MAX_PAYLOAD_BYTES, 8 * 1024 * 1024);
});

test("integrity grant failures remain visible without exposing signed URLs", async () => {
  const transferId = "c61fdfd2-3c09-44c7-bbb4-048a9d0e9c7c";
  const status = {
    transfer_id: transferId,
    status: "in_progress",
    integrity_audit_challenge: { transfer_id: transferId, audit_id: "audit-1", chunks: [] }
  };
  const client = new BeamClient({ apiKey: "b1m_test", natsUrl: "nats://127.0.0.1:4222" });
  client.control.request = async () => status;
  try {
    assert.equal((await client.transferStatus(transferId)).integrity_audit_submission_error, "integrity audit signer unavailable");
    client.integrityAuditSigners.set(transferId, async () => {
      throw new Error("failed to sign https://private.example/?X-Amz-Signature=secret");
    });
    assert.equal((await client.transferStatus(transferId)).integrity_audit_submission_error, "Error");
  } finally {
    await client.close();
  }
});

test("route splitting targets 4 MiB while honoring encoded guards and oversized routes", () => {
  const route = (index, bytes) => ({
    source_id: "src_0",
    destination_id: "dest_0",
    chunk_index: index,
    source_url: `https://storage.example/${"x".repeat(bytes)}`,
    dest_url: "https://storage.example/destination",
    source_offset: index * 1024,
    chunk_size: 1024
  });
  const routes = [route(0, 4_500_000), route(1, 4_500_000)];
  const defaults = new BeamTransferControl({ apiKey: "beam_test", natsUrl: "nats://127.0.0.1:4222" });
  const overridden = new BeamTransferControl({
    apiKey: "beam_test",
    natsUrl: "nats://127.0.0.1:4222",
    maxPayloadBytes: 10 * 1024 * 1024
  });

  assert.deepEqual(defaults.splitRoutesForPayload("transfer.route_stream.batch", {}, routes).map((batch) => batch.length), [1, 1]);
  assert.deepEqual(overridden.splitRoutesForPayload("transfer.route_stream.batch", {}, routes).map((batch) => batch.length), [1, 1]);
  const nearLimitRoutes = [route(3, 4_190_000), route(4, 4_190_000)];
  assert.deepEqual(
    defaults.splitRoutesForPayload("transfer.route_stream.batch", {}, nearLimitRoutes).map((batch) => batch.length),
    [1, 1]
  );
  assert.throws(
    () => defaults.splitRoutesForPayload("transfer.route_stream.batch", {}, [route(2, 8 * 1024 * 1024)]),
    /single signed route is \d+ bytes, above maxPayloadBytes=8388608/
  );
  assert.deepEqual(
    defaults.splitRoutesForPayload("transfer.route_stream.batch", {}, [route(5, 6 * 1024 * 1024)]).map((batch) => batch.length),
    [1]
  );
  const lowerGuard = new BeamTransferControl({
    apiKey: "beam_test",
    natsUrl: "nats://127.0.0.1:4222",
    maxPayloadBytes: 3 * 1024 * 1024
  });
  assert.throws(
    () => lowerGuard.splitRoutesForPayload("transfer.route_stream.batch", {}, [route(6, 3 * 1024 * 1024)]),
    /single signed route is \d+ bytes, above maxPayloadBytes=3145728/
  );
});

test("recovery-pending errors retain the prepared transfer identity and cause", () => {
  const cause = new Error("transport disconnected");
  const error = new BeamRouteRecoveryPendingError("transfer-1", cause);
  assert.equal(error.transferId, "transfer-1");
  assert.equal(error.cause, cause);
});

test("route recovery signer ignores subscription error frames", async () => {
  const control = new BeamTransferControl({
    apiKey: "b1m_test",
    natsUrl: "nats://127.0.0.1:4222"
  });
  let handlerCalls = 0;
  let unsubscribeCalls = 0;
  let callback;
  control.connection = async () => ({
    subscribe(_subject, options) {
      callback = options.callback;
      return {
        unsubscribe() {
          unsubscribeCalls += 1;
        }
      };
    },
    async flush() {}
  });

  const stop = await control.serveRouteRecoverySigner("transfer-1", async () => {
    handlerCalls += 1;
    return { routes: [] };
  });
  callback(new Error("subscription status"), {
    data: new Uint8Array(),
    respond() {
      throw new Error("subscription status frames must not be answered");
    }
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(handlerCalls, 0);
  stop();
  assert.equal(unsubscribeCalls, 1);
});

test("idempotent prepare reuses the same route generation", async () => {
  const client = new BeamClient({
    apiKey: "b1m_test",
    natsUrl: "nats://127.0.0.1:4222"
  });
  const control = new FakeTransferControl();
  client.control = control;
  const request = {
    sources: [{ source_id: "src_0", type: "http", url: "https://source.example/file.bin", size: 4096 }],
    destinations: [{ destination_id: "dst_0", provider: "http", mode: "http_chunks", logical_prefix: "out/file.bin" }],
    idempotencyKey: "studio-step-retry"
  };

  const first = await client.prepareTransfer(request);
  const second = await client.prepareTransfer(request);
  const prepares = control.calls.filter((call) => call.messageType === "transfer.prepare");

  assert.equal(first.transfer_id, second.transfer_id);
  assert.equal(prepares.length, 2);
  assert.equal(prepares[0].payload.transfer_id, prepares[1].payload.transfer_id);
  assert.equal(prepares[0].payload.route_generation_id, prepares[1].payload.route_generation_id);
  assert.equal(prepares[0].options.idempotencyKey, prepares[1].options.idempotencyKey);
});

test("lifecycle transport retries reuse the identical request envelope", async () => {
  const control = new BeamTransferControl({
    apiKey: "beam_test",
    natsUrl: "nats://127.0.0.1:4222"
  });
  const sent = [];
  control.authToken = "test-token";
  control.authTokenExpiresAt = Math.floor(Date.now() / 1_000) + 3_600;
  control.connection = async () => ({
    async request(_subject, bytes) {
      sent.push(Uint8Array.from(bytes));
      if (sent.length === 1) throw new Error("connection closed");
      return {
        data: encode({
          ok: true,
          status: 200,
          runtime_epoch: "runtime-one",
          transport_epoch: "transport-one",
          payload: { success: true }
        })
      };
    }
  });

  await control.request(
    "transfer.prepare",
    { transfer_id: "transfer-one", route_generation_id: "generation-one" },
    { transferId: "transfer-one", idempotencyKey: "resume-generation-one" }
  );

  assert.equal(sent.length, 2);
  assert.deepEqual(sent[0], sent[1]);
});

test("expired lifecycle auth retries with fresh token and stable request identity", async () => {
  const control = new BeamTransferControl({
    apiKey: "beam_test",
    natsUrl: "nats://127.0.0.1:4222"
  });
  const sent = [];
  let authResolves = 0;
  control.authToken = "stale-token";
  control.authTokenExpiresAt = Math.floor(Date.now() / 1_000) + 3_600;
  control.connection = async () => ({
    async request(subject, bytes) {
      if (subject.includes(".auth.")) {
        authResolves += 1;
        return { data: new TextEncoder().encode(JSON.stringify({ ok: true, token: unsignedTestJwt({ exp: Math.floor(Date.now() / 1_000) + 3_600 }) })) };
      }
      sent.push(decode(bytes));
      if (sent.length === 1) {
        return {
          data: encode({
            ok: false,
            status: 401,
            runtime_epoch: "runtime-one",
            transport_epoch: "transport-one",
            error: { code: "auth_token_expired", message: "auth token expired" }
          })
        };
      }
      return {
        data: encode({
          ok: true,
          status: 200,
          runtime_epoch: "runtime-one",
          transport_epoch: "transport-one",
          payload: { success: true }
        })
      };
    }
  });

  await control.request(
    "transfer.route_stream.batch",
    {
      transfer_id: "transfer-one",
      route_generation_id: "generation-one",
      stream_id: "stream-one",
      batch_id: "batch-one",
      batch_index: 1,
      route_batch: { source_chunks: [], routes: [] },
      route_count: 0,
      route_keys_checksum: "checksum"
    },
    { transferId: "transfer-one", idempotencyKey: "route-batch-one" }
  );

  assert.equal(sent.length, 2);
  assert.equal(authResolves, 1);
  assert.equal(sent[0].request_id, sent[1].request_id);
  assert.deepEqual(sent[0].payload, sent[1].payload);
  assert.equal(sent[0].auth_token, "stale-token");
  assert.notEqual(sent[1].auth_token, "stale-token");
});

test("provider transfer errors retain nested causes and cleanup outcomes", () => {
  const providerError = Object.assign(new Error("R2 request throttled"), {
    name: "SlowDown",
    $metadata: { httpStatusCode: 503 }
  });
  const cleanupError = new AggregateError(
    [Object.assign(new Error("abort throttled"), { $metadata: { httpStatusCode: 503 } })],
    "multipart cleanup failed"
  );
  const error = new BeamProviderTransferError({
    transferId: "transfer-1",
    cause: providerError,
    cleanupError
  });

  assert.equal(error.transferId, "transfer-1");
  assert.equal(error.transferCancelled, true);
  assert.equal(error.multipartCleanupComplete, false);
  assert.deepEqual(error.errors, [providerError, cleanupError]);
});

function unsignedTestJwt(claims) {
  const encodePart = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encodePart({ alg: "none", typ: "JWT" })}.${encodePart({
    token_type: "beam-sdk-auth",
    environment: "prod",
    client_id: "00000000-0000-4000-8000-000000000001",
    client_key: "beam_test",
    client_name: null,
    key_prefix: "beam_test",
    key_role: "client",
    scopes: [],
    iat: Math.floor(Date.now() / 1_000),
    ...claims
  })}.signature`;
}

test("multipart provider control is bounded independently from route signing", async () => {
  let activeCreates = 0;
  let maxActiveCreates = 0;
  let createCount = 0;
  const server = http.createServer((request, response) => {
    if (request.method === "HEAD") {
      response.statusCode = 200;
      response.setHeader("Content-Length", "1024");
      response.end();
      return;
    }
    if (request.method === "POST" && request.url?.includes("uploads")) {
      activeCreates += 1;
      createCount += 1;
      maxActiveCreates = Math.max(maxActiveCreates, activeCreates);
      const uploadId = `upload-${createCount}`;
      setTimeout(() => {
        activeCreates -= 1;
        response.statusCode = 200;
        response.setHeader("Content-Type", "application/xml");
        response.end(
          `<CreateMultipartUploadResult><Bucket>destination</Bucket><Key>output.bin</Key><UploadId>${uploadId}</UploadId></CreateMultipartUploadResult>`
        );
      }, 20);
      return;
    }
    response.statusCode = 404;
    response.end("not found");
  });

  await listen(server);
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const client = new BeamClient({
      apiKey: "b1m_test",
      natsUrl: "nats://127.0.0.1:4222",
      routeSigningConcurrency: 64
    });
    const control = new MultiDestinationTransferControl();
    client.control = control;
    const credentials = {
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };
    const multipartIdentities = [];

    const result = await client.createTransfer({
      sources: [{
        provider: "r2",
        bucket: "source",
        key: "input.bin",
        ...credentials
      }],
      destinations: Array.from({ length: 5 }, (_, index) => ({
        provider: "r2",
        bucket: "destination",
        key: `output-${index}.bin`,
        ...credentials
      })),
      signedUrlFlow: "signed_url",
      distribute: false,
      onMultipartGroupReady: (identity) => multipartIdentities.push(identity)
    });

    assert.equal(result.success, true);
    assert.equal(BEAM_DEFAULT_MULTIPART_CONTROL_CONCURRENCY, 2);
    assert.equal(createCount, 5);
    assert.equal(maxActiveCreates, 2);
    assert.equal(multipartIdentities.length, 5);
    assert.deepEqual(
      multipartIdentities.map((identity) => identity.uploadId).sort(),
      Array.from({ length: 5 }, (_, index) => `upload-${index + 1}`).sort()
    );
    for (const identity of multipartIdentities) {
      const serialized = JSON.stringify(identity);
      assert.equal(serialized.includes("access_key"), false);
      assert.equal(serialized.includes("secret_access"), false);
      assert.equal(serialized.includes("http://"), false);
      assert.equal(serialized.includes("https://"), false);
    }
    const manifestGroupIds = control.calls
      .filter((call) => call.messageType === "transfer.route_stream.manifest")
      .flatMap((call) => call.payload.groups.map((group) => group.multipart_group_id));
    const routeGroupIds = control.calls
      .filter((call) => call.messageType === "transfer.route_stream.batch")
      .flatMap((call) => call.payload.route_batch.routes.map((route) => route.multipart_group_id));
    assert.equal(manifestGroupIds.length, 5);
    assert.deepEqual(
      [...routeGroupIds].sort(),
      [...manifestGroupIds].sort()
    );
    const prepareCall = control.calls.find((call) => call.messageType === "transfer.prepare");
    assert.equal("chunk_size" in prepareCall.payload, false);
    assert.equal("provider_part_size" in prepareCall.payload, false);
  } finally {
    await close(server);
  }
});

test("non-recoverable route failure preserves scoped authority through multipart cleanup", async () => {
  let abortAuthorization = "";
  let abortCount = 0;
  const server = http.createServer((request, response) => {
    if (request.method === "HEAD") {
      response.statusCode = 200;
      response.setHeader("Content-Length", "1024");
      response.end();
      return;
    }
    if (request.method === "POST" && request.url?.includes("uploads")) {
      response.statusCode = 200;
      response.setHeader("Content-Type", "application/xml");
      response.end(
        "<CreateMultipartUploadResult><Bucket>destination</Bucket><Key>output.bin</Key><UploadId>upload-cleanup</UploadId></CreateMultipartUploadResult>"
      );
      return;
    }
    if (request.method === "DELETE" && request.url?.includes("uploadId=upload-cleanup")) {
      abortCount += 1;
      abortAuthorization = String(request.headers.authorization ?? "");
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
    const client = new BeamClient({
      apiKey: "b1m_test",
      natsUrl: "nats://127.0.0.1:4222"
    });
    const control = new FakeTransferControl();
    control.failOnceAt = "transfer.route_stream.complete";
    control.failOnceError = Object.assign(new Error("route contract rejected"), { status: 400 });
    client.control = control;
    const credentials = {
      access_key_id: "cleanup-access",
      secret_access_key: "cleanup-secret",
      endpoint_url: endpoint
    };

    await assert.rejects(
      client.createTransfer({
        sources: [{
          provider: "r2",
          bucket: "source",
          key: "input.bin",
          ...credentials
        }],
        destinations: [{
          provider: "r2",
          bucket: "destination",
          key: "output.bin",
          ...credentials
        }],
        signedUrlFlow: "signed_url",
        distribute: false
      }),
      (error) => {
        assert.equal(error instanceof BeamProviderTransferError, true);
        assert.equal(error.transferCancelled, true);
        assert.equal(error.multipartCleanupComplete, true);
        return true;
      }
    );

    assert.equal(abortCount, 1);
    assert.match(abortAuthorization, /Credential=cleanup-access\//);
    assert.equal(control.calls.filter((call) => call.messageType === "transfer.cancel").length, 1);
    assert.equal(control.recoveryLeases.size, 0);
    await client.close();
  } finally {
    await close(server);
  }
});

test("resumeProviderTransfer replays routes with retained uploads and fences replaced owners", async () => {
  let headCount = 0;
  let createMultipartCount = 0;
  const server = http.createServer((request, response) => {
    if (request.method === "HEAD") {
      headCount += 1;
      response.statusCode = 200;
      response.setHeader("Content-Length", "1024");
      response.end();
      return;
    }
    if (request.method === "POST" && request.url?.includes("uploads")) {
      createMultipartCount += 1;
    }
    response.statusCode = 404;
    response.end("not found");
  });

  await listen(server);
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const transferId = "77777777-7777-4777-8777-777777777777";
    const client = new BeamClient({
      apiKey: "b1m_test",
      natsUrl: "nats://127.0.0.1:4222",
      routeSigningConcurrency: 4
    });
    const control = new FakeTransferControl();
    client.control = control;
    const credentials = {
      access_key_id: "ak",
      secret_access_key: "sk",
      endpoint_url: endpoint
    };

    const multipartGroups = [{ transferId, multipartGroupId: `${transferId}:dst_0:src_0:out/file.bin`,
      sourceId: "src_0", destinationId: "dst_0", objectKey: "out/file.bin", uploadId: "upload-existing",
      expectedObjectSize: 1024, expectedPartCount: 1, expiresAt: new Date(Date.now()+60_000).toISOString() }];
    const ownership = new AbortController();
    const prepared = await client.resumeProviderTransfer({
      transferId,
      multipartGroups,
      signal: ownership.signal,
      onPrepared: async () => assert.equal(control.calls.filter(call=>call.messageType === "transfer.route_stream.begin").length, 0),
      sources: [{
        provider: "r2",
        bucket: "source",
        key: "input.bin",
        ...credentials
      }],
      destinations: [{
        provider: "r2",
        bucket: "destination",
        key: "out/file.bin",
        ...credentials
      }],
      signedUrlFlow: "signed_url"
    });

    assert.equal(prepared.transfer_id, transferId);
    assert.equal(headCount, 1);
    assert.equal(createMultipartCount, 0);
    assert.equal(control.routeRecoverySigner?.transferId, transferId);
    assert.equal(control.recoveryLeases.has(transferId), true);

    const secondClient = new BeamClient({
      apiKey: "b1m_test",
      natsUrl: "nats://127.0.0.1:4222",
      routeSigningConcurrency: 4
    });
    secondClient.control = control;
    await secondClient.resumeProviderTransfer({
      transferId,
      multipartGroups,
      signal: ownership.signal,
      sources: [{
        provider: "r2",
        bucket: "source",
        key: "input.bin",
        ...credentials
      }],
      destinations: [{
        provider: "r2",
        bucket: "destination",
        key: "out/file.bin",
        ...credentials
      }],
      signedUrlFlow: "signed_url"
    });
    const prepareCalls = control.calls.filter(
      (call) => call.messageType === "transfer.prepare"
    );
    assert.equal(prepareCalls.length, 2);
    assert.notEqual(
      prepareCalls[0].options.idempotencyKey,
      prepareCalls[1].options.idempotencyKey
    );
    assert.notEqual(
      prepareCalls[0].payload.route_generation_id,
      prepareCalls[1].payload.route_generation_id
    );

    const reply = await control.routeRecoverySigner.handler({
      transfer_id: transferId,
      route_generation_id: "recover-1",
      chunks: [{
        source_id: "src_0",
        destination_id: "dst_0",
        chunk_index: 0,
        delivery_index: 0,
        source_offset: 0,
        chunk_size: 1024,
        logical_attempt_index: 1,
        attempt_slot: 0,
        part_number: 1,
        route_generation_id: "recover-1",
        multipart_group_id: `${transferId}:dst_0:src_0:out/file.bin`,
        final_object_key: "out/file.bin",
        upload_id: "upload-existing"
      }]
    });

    assert.equal(reply.transfer_id, transferId);
    assert.equal(reply.route_generation_id, "recover-1");
    assert.equal(reply.chunk_routes.length, 1);
    assert.equal(reply.chunk_routes[0].metadata.upload_id, "upload-existing");
    assert.equal(reply.chunk_routes[0].metadata.multipart_group_id, `${transferId}:dst_0:src_0:out/file.bin`);
    assert.equal(reply.chunk_routes[0].metadata.part_number, 1);
    assert.match(reply.chunk_routes[0].dest_url, /upload-existing/);
    assert.equal(control.calls.filter(call => call.messageType === "transfer.route_stream.complete").length, 2);
    const signer = control.routeRecoverySigner;
    const lease = control.recoveryLeases.get(transferId);
    await lease.replayRoutes("33333333-3333-4333-8333-333333333333");
    assert.equal(createMultipartCount, 0, "route replay must reuse the existing upload");
    assert.equal(control.calls.filter(call => call.messageType === "transfer.route_stream.complete").length, 3);
    const recoveryInput = {transferId,sources:[{provider:"r2",bucket:"source",key:"input.bin",...credentials}],
      destinations:[{provider:"r2",bucket:"destination",key:"out/file.bin",...credentials}]};
    for (const invalidGroups of [[], [...multipartGroups, ...multipartGroups], [{...multipartGroups[0],objectKey:"different"}], [{...multipartGroups[0],uploadId:""}]]) {
      await assert.rejects(secondClient.resumeProviderTransfer({...recoveryInput,multipartGroups:invalidGroups}), /provider_multipart_recovery/);
    }
    assert.equal(createMultipartCount, 0, "invalid retained state must never create another upload");
    ownership.abort(new Error("owner replaced"));
    await assert.rejects(signer.handler({route_generation_id:"later",chunks:[]}), /owner replaced/);
    await assert.rejects(lease.replayRoutes("44444444-4444-4444-8444-444444444444"), /owner replaced/);
    assert.equal(control.calls.filter(call=>call.messageType === "transfer.cancel").length, 0);
    await client.close();
    await secondClient.close();
  } finally {
    await close(server);
  }
});

test("BeamClient rejects multipart routes without their manifest group identity", async () => {
  const { client } = clientWithFakeControl();
  await assert.rejects(
    client.attachSignedUrls("11111111-1111-4111-8111-111111111111", {
      chunkRoutes: [{
        source_id: "src_0",
        destination_id: "dst_0",
        chunk_index: 0,
        source_url: "https://source.example/file.bin",
        dest_url: "https://dest.example/file.bin?uploadId=upload-0&partNumber=1",
        source_offset: 0,
        chunk_size: 512,
        metadata: {
          upload_id: "upload-0",
          final_object_key: "file.bin",
          part_number: 1
        }
      }],
      multipartGroupManifest: [],
      routeGenerationId: "22222222-2222-4222-8222-222222222222",
      planFingerprint: "a".repeat(64),
      coordinateChecksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
      recoveryFactory: async () => ({ chunkRoutes: [], multipartGroupManifest: [] })
    }),
    /missing multipart_group_id/
  );
});

test("multipart parts are consecutive with a 10,000-part limit", () => {
  assert.equal(multipartPartNumber(0), 1);
  assert.equal(multipartPartNumber(1), 2);
  assert.equal(multipartPartNumber(9_999), 10_000);
  assert.throws(() => multipartPartNumber(-1), /non-negative integer/);
  assert.throws(() => multipartPartNumber(10_000), /less than 10000/);
  assert.throws(() => multipartPartNumber(0, 1), /attempt_slot/);
});

test("runtime state loss keeps the route recovery lease", () => {
  assert.equal(isRecoverableRouteStreamError({ status: 404 }), true);
  assert.equal(isRecoverableRouteStreamError({ status: 409 }), true);
  assert.equal(isRecoverableRouteStreamError({ status: 400 }), false);
});

test("runtime epoch recovery coalesces, survives reconnects, and clears retained secrets", async () => {
  const control = new BeamTransferControl({ apiKey: "b1m_recovery", natsUrl: "nats://127.0.0.1:4222" });
  control.ensureHelloMonitor = () => {};
  control.pollRuntimeHello = async () => {};
  let resumeCalls = 0;
  control.request = async () => {
    resumeCalls += 1;
    return { recovery: "route_replay_required", route_replay_required: true };
  };
  let releaseFirstReplay;
  const firstReplayGate = new Promise((resolve) => { releaseFirstReplay = resolve; });
  let replays = 0;
  let activeReplays = 0;
  let maxActiveReplays = 0;
  const replayGenerations = [];
  let disposed = 0;
  const transferId = "11111111-1111-4111-8111-111111111111";
  control.registerRecoveryLease({
    transferId,
    planFingerprint: "a".repeat(64),
    coordinateChecksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
    replayRoutes: async (generationId) => {
      replayGenerations.push(generationId);
      replays += 1;
      activeReplays += 1;
      maxActiveReplays = Math.max(maxActiveReplays, activeReplays);
      if (replays === 1) await firstReplayGate;
      activeReplays -= 1;
    },
    disposeSecrets: () => { disposed += 1; }
  });
  control.observeRuntimeEpoch(0, "runtime-a", "transport-a");
  control.observeRuntimeEpoch(0, "runtime-b", "transport-b");
  await waitFor(() => replays === 1);
  control.observeRuntimeEpoch(0, "runtime-c", "transport-c");
  releaseFirstReplay();
  await waitFor(() => replays === 2);
  assert.equal(resumeCalls, 2);
  assert.equal(maxActiveReplays, 1);
  assert.equal(new Set(replayGenerations).size, 2);
  assert.equal(buildConnectionOptions({ natsUrl: "nats://127.0.0.1:4222", apiKey: "key", keyPrefix: "key" }).maxReconnectAttempts, -1);
  control.releaseRecoveryLease(transferId);
  assert.equal(disposed, 1);
  await control.close();
});

test("Hippius route recovery signs the planned per-chunk key, never the final object key", async () => {
  const transferId = "88888888-8888-4888-8888-888888888888";
  const presignedKeys = [];
  const fetchImpl = async (url) => {
    const requested = new URL(url);
    const key = requested.searchParams.get("key");
    const action = requested.searchParams.get("action");
    presignedKeys.push({ key, action });
    return new Response(JSON.stringify({ url: `https://hippius.example/${key}?action=${action}` }), {
      status: 200,
      headers: { "Content-Type": "application/json" }
    });
  };
  const client = new BeamClient({ apiKey: "b1m_test", natsUrl: "nats://127.0.0.1:4222", fetch: fetchImpl });
  const control = new FakeTransferControl();
  client.control = control;
  const source = { source_id: "src_0", url: "https://source.example/input.bin", size: 2048 };
  const hippius = { provider: "hippius", api_token: "token", bucket: "bucket", key: "out/file.bin", base_url: "https://api.hippius.test" };
  const descriptor = fakePlanDescriptor(source, { destination_id: "dst_0", provider: "hippius" }, 1024);
  descriptor.sources[0].chunk_count = 2;
  descriptor.logical_chunk_count = 2;
  descriptor.delivery_route_count = 2;
  try {
    await client.startProviderRouteRecoverySigner({
      owner: {},
      prepared: { transfer_id: transferId, plan_descriptor: descriptor },
      sourcesById: new Map([["src_0", {
        provider: "r2", bucket: "source", key: "input.bin", access_key_id: "ak", secret_access_key: "sk",
        endpoint_url: "https://r2.example.test"
      }]]),
      destinationsById: new Map([["dst_0", hippius]]),
      multipartUploads: new Map(),
      expiresIn: 600
    });
    const reply = await control.routeRecoverySigner.handler({
      transfer_id: transferId,
      route_generation_id: "recover-hippius",
      chunks: [{
        source_id: "src_0",
        destination_id: "dst_0",
        chunk_index: 1,
        delivery_index: 1,
        source_offset: 1024,
        chunk_size: 1024,
        logical_attempt_index: 1,
        attempt_slot: 0,
        part_number: multipartPartNumber(1),
        route_generation_id: "recover-hippius",
        multipart_group_id: "",
        final_object_key: "out/file.bin",
        upload_id: ""
      }]
    });
    // Initial materialization uses `${final}/${plan_nonce}/chunk-NNNNNN`; recovery must match it.
    const chunkKey = "out/file.bin/testplan/chunk-000001";
    assert.deepEqual(presignedKeys.filter((entry) => entry.action === "put"), [{ key: chunkKey, action: "put" }]);
    assert.equal(reply.chunk_routes.length, 1);
    assert.equal(reply.chunk_routes[0].dest_url, `https://hippius.example/${chunkKey}?action=put`);
    assert.equal(reply.chunk_routes[0].chunk_index, 1);
  } finally {
    await client.close();
  }
});

function recoveryControl() {
  const control = new BeamTransferControl({ apiKey: "b1m_recovery", natsUrl: "nats://127.0.0.1:4222" });
  control.ensureHelloMonitor = () => {};
  control.pollRuntimeHello = async () => {};
  return control;
}

function recordingLease(transferId, owner, events) {
  return {
    transferId,
    planFingerprint: owner,
    coordinateChecksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
    replayRoutes: async () => { events.push(`replay:${owner}`); },
    disposeSecrets: () => { events.push(`dispose:${owner}`); }
  };
}

for (const [name, outcome] of [
  ["replay required", () => ({ recovery: "route_replay_required", route_replay_required: true })],
  ["non-retryable failure", () => { throw Object.assign(new Error("bad request"), { status: 400 }); }]
]) {
  test(`fenced owner's in-flight recovery (${name}) neither replays into nor releases a replacement lease`, async () => {
    const control = recoveryControl();
    const transferId = "99999999-9999-4999-8999-999999999999";
    const events = [];
    const fenced = recordingLease(transferId, "fenced", events);
    const replacement = recordingLease(transferId, "replacement", events);
    control.request = async (_messageType, payload) => {
      events.push(`resume:${payload.plan_fingerprint}`);
      // The replacement owner takes over while the fenced owner awaits transfer.resume.
      if (control.recoveryLeases.get(transferId) === fenced) control.registerRecoveryLease(replacement);
      if (payload.plan_fingerprint === "replacement") return { recovery: "reconciling" };
      return outcome();
    };
    control.registerRecoveryLease(fenced);
    await control.recoverLease(fenced);
    await waitFor(() => control.recoveryOperations.size === 0);
    assert.equal(events.includes("replay:fenced"), false, "a fenced-off owner must not replay routes");
    assert.equal(events.includes("dispose:replacement"), false, "a fenced-off owner must not release its replacement");
    assert.equal(control.recoveryLeases.get(transferId), replacement);
    assert.deepEqual(events.slice(0, 2), ["resume:fenced", "dispose:fenced"]);
    control.releaseRecoveryLease(transferId, fenced);
    assert.equal(control.recoveryLeases.get(transferId), replacement, "release is keyed by lease identity");
    await control.close();
  });
}

test("a replacement lease recovers even while the fenced owner's recovery loop is in flight", async () => {
  const control = recoveryControl();
  const transferId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const events = [];
  const fenced = recordingLease(transferId, "fenced", events);
  const replacement = recordingLease(transferId, "replacement", events);
  let releaseFencedResume;
  const fencedResumeGate = new Promise((resolve) => { releaseFencedResume = resolve; });
  control.request = async (_messageType, payload) => {
    events.push(`resume:${payload.plan_fingerprint}`);
    if (payload.plan_fingerprint === "fenced") await fencedResumeGate;
    return { recovery: "route_replay_required", route_replay_required: true };
  };
  control.registerRecoveryLease(fenced);
  control.continueRecoveryLease(transferId);
  await waitFor(() => events.includes("resume:fenced"));
  control.registerRecoveryLease(replacement);
  control.continueRecoveryLease(transferId, replacement);
  control.continueRecoveryLease(transferId, fenced);
  await waitFor(() => events.includes("replay:replacement"));
  releaseFencedResume();
  await waitFor(() => control.recoveryOperations.size === 0);
  assert.equal(events.includes("replay:fenced"), false);
  assert.equal(events.filter((event) => event === "resume:fenced").length, 1);
  assert.equal(control.recoveryLeases.get(transferId), replacement);
  await control.close();
});

test("a fenced owner's aborted replay keeps the replacement owner's lease and recovery signer on the same client", async () => {
  const server = http.createServer((request, response) => {
    if (request.method === "HEAD") {
      response.statusCode = 200;
      response.setHeader("Content-Length", "1024");
      response.end();
      return;
    }
    response.statusCode = 404;
    response.end("not found");
  });
  await listen(server);
  try {
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const transferId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const client = new BeamClient({ apiKey: "b1m_test", natsUrl: "nats://127.0.0.1:4222" });
    const control = new FakeTransferControl();
    client.control = control;
    const credentials = { access_key_id: "ak", secret_access_key: "sk", endpoint_url: endpoint };
    const resumeInput = (signal) => ({
      transferId,
      signal,
      multipartGroups: [{ transferId, multipartGroupId: `${transferId}:dst_0:src_0:out/file.bin`,
        sourceId: "src_0", destinationId: "dst_0", objectKey: "out/file.bin", uploadId: "upload-existing",
        expectedObjectSize: 1024, expectedPartCount: 1, expiresAt: new Date(Date.now() + 60_000).toISOString() }],
      sources: [{ provider: "r2", bucket: "source", key: "input.bin", ...credentials }],
      destinations: [{ provider: "r2", bucket: "destination", key: "out/file.bin", ...credentials }],
      signedUrlFlow: "signed_url"
    });
    const fencedOwnership = new AbortController();
    await client.resumeProviderTransfer(resumeInput(fencedOwnership.signal));
    const fencedLease = control.recoveryLeases.get(transferId);
    await client.resumeProviderTransfer(resumeInput(new AbortController().signal));
    const replacementLease = control.recoveryLeases.get(transferId);
    const replacementSigner = control.routeRecoverySigner;
    assert.notEqual(replacementLease, fencedLease);
    assert.ok(replacementSigner);

    // Owner A is fenced off in the middle of a background route replay.
    control.onRequest = (messageType) => {
      if (messageType === "transfer.route_stream.begin") fencedOwnership.abort(new Error("owner A fenced"));
    };
    await assert.rejects(fencedLease.replayRoutes("cccccccc-cccc-4ccc-8ccc-cccccccccccc"), /owner A fenced/);
    control.onRequest = undefined;

    assert.equal(control.recoveryLeases.get(transferId), replacementLease, "replacement lease must survive");
    assert.equal(control.routeRecoverySigner, replacementSigner, "replacement recovery signer must keep serving");
    assert.equal(client.recoverySigners.has(transferId), true, "replacement signer registration must survive");
    assert.equal(client.integrityAuditSigners.has(transferId), true, "replacement integrity signer must survive");
    await client.close();
  } finally {
    await close(server);
  }
});

async function waitFor(predicate) {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for recovery state");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

class FakeTransferControl {
  constructor() {
    this.calls = [];
    this.recoveryLeases = new Map();
    this.recoveryPresentAtRouteBegin = false;
    this.routeRecoverySigner = null;
    this.continuedRecovery = [];
    this.failOnceAt = null;
  }

  async request(messageType, payload, options = {}) {
    this.calls.push({ messageType, payload, options });
    this.onRequest?.(messageType, payload);
    if (messageType === "transfer.route_stream.begin") {
      this.recoveryPresentAtRouteBegin = this.recoveryLeases.has(payload.transfer_id);
    }
    if (this.failOnceAt === messageType) {
      this.failOnceAt = null;
      const error = this.failOnceError ?? new Error("connection closed during route streaming");
      this.failOnceError = null;
      throw error;
    }
    if (messageType === "transfer.create") {
      return {
        success: true,
        transfer_id: payload.transfer_id,
        total_chunks: 2,
        total_sources: payload.sources.length,
        total_destinations: payload.destinations.length
      };
    }
    if (messageType === "transfer.plan") {
      return {
        success: true,
        chunk_size: 5242880,
        total_size: 1024,
        total_sources: 1,
        total_destinations: 1,
        logical_chunks: 1,
        total_chunks: 1,
        signed_url_flow: "signed_url",
        plan_fingerprint: "a".repeat(64),
        coordinate_checksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
        plan_descriptor: fakePlanDescriptor(payload.sources[0], payload.destinations[0], 5242880)
      };
    }
    if (messageType === "transfer.prepare") {
      return {
        success: true,
        transfer_id: payload.transfer_id,
        transfer_key: "tk_test",
        chunk_size: 1024,
        total_size: 1024,
        total_sources: 1,
        total_destinations: 1,
        logical_chunks: 1,
        total_chunks: 1,
        signed_url_flow: "signed_url",
        plan_fingerprint: "a".repeat(64),
        coordinate_checksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
        route_generation_id: payload.route_generation_id,
        plan_descriptor: fakePlanDescriptor(payload.sources[0], payload.destinations[0], 1024)
      };
    }
    if (messageType === "transfer.route_stream.complete") {
      return { success: true, transfer_id: payload.transfer_id, total_routes_received: 2 };
    }
    if (messageType === "transfer.distribute") {
      return { success: true, transfer_id: payload.transfer_id, orchestrators_assigned: 1 };
    }
    if (messageType === "transfer.status") {
      if (this.statusResponse) return { transfer_id: payload.transfer_id, ...this.statusResponse };
      return {
        transfer_id: payload.transfer_id,
        status: "completed",
        error_message: null,
        started_at: "2026-06-22T22:40:20.000Z",
        completed_at: "2026-06-22T22:41:20.000Z"
      };
    }
    if (messageType === "transfer.cancel") {
      return { success: true, message: "cancelled" };
    }
    return { success: true };
  }

  splitRoutesForPayload(_messageType, _basePayload, routes) {
    return [routes];
  }

  registerRecoveryLease(lease) {
    this.recoveryLeases.set(lease.transferId, lease);
  }

  releaseRecoveryLease(transferId, owner) {
    const lease = this.recoveryLeases.get(transferId);
    if (!lease) return;
    if (owner && lease !== owner) return;
    this.recoveryLeases.delete(transferId);
    lease.disposeSecrets?.();
  }

  async serveRouteRecoverySigner(transferId, handler) {
    this.routeRecoverySigner = { transferId, handler };
    return () => {
      if (this.routeRecoverySigner?.transferId === transferId) {
        this.routeRecoverySigner = null;
      }
    };
  }

  continueRecoveryLease(transferId, owner) {
    if (owner && this.recoveryLeases.get(transferId) !== owner) return;
    this.continuedRecovery.push(transferId);
  }

  async close() {}
}

function clientWithFakeControl() {
  const client = new BeamClient({ apiKey: "b1m_test", natsUrl: "nats://127.0.0.1:4222" });
  const control = new FakeTransferControl();
  client.control = control;
  return { client, control };
}

test("BeamClient sends lifecycle operations through NATS control messages", async () => {
  const { client, control } = clientWithFakeControl();

  const created = await client.createRawTransfer({
    sources: [{ type: "http", url: "https://source.example/file.bin" }],
    destinations: [{ type: "http", url: "https://dest.example/file.bin" }],
    totalSize: 10485760
  });
  assert.equal(created.success, true);
  assert.match(created.transfer_id, /^[0-9a-f-]{36}$/i);

  const distributed = await client.distributeTransfer(created.transfer_id);
  assert.equal(distributed.orchestrators_assigned, 1);

  const status = await client.waitForTransfer(created.transfer_id, { timeoutMs: 100, pollIntervalMs: 1 });
  assert.equal(status.status, "completed");

  const cancelled = await client.cancelTransfer(created.transfer_id);
  assert.equal(cancelled.success, true);

  assert.deepEqual(
    control.calls.map((call) => call.messageType),
    ["transfer.create", "transfer.distribute", "transfer.status", "transfer.cancel"]
  );
  assert.equal("chunk_size" in control.calls[0].payload, false);
  assert.equal(control.calls[1].options.transferId, created.transfer_id);
});

test("waitForTransfer surfaces a storage access failure verbatim as BeamStorageAccessError", async () => {
  const { client, control } = clientWithFakeControl();
  const transferId = "3f0c9a52-8a8f-4d44-9d7c-4b2f7f0c1a11";
  const errorMessage =
    "destination_access_denied: The destination storage refused Beam's requests (403 AccessDenied). " +
    "Check that the credentials allow writes to this bucket and path and are not restricted to specific IP addresses or networks.";
  control.statusResponse = { status: "failed", error_message: errorMessage, started_at: null, completed_at: null };

  await assert.rejects(client.waitForTransfer(transferId, { timeoutMs: 100, pollIntervalMs: 1 }), (error) => {
    assert.ok(error instanceof BeamStorageAccessError);
    assert.ok(error instanceof BeamTransferFailedError);
    assert.equal(error.code, "destination_access_denied");
    assert.equal(error.errorMessage, errorMessage);
    assert.equal(error.transferId, transferId);
    assert.ok(error.message.includes(errorMessage));
    return true;
  });
});

test("waitForTransfer keeps other failures as BeamTransferFailedError with the server message", async () => {
  const { client, control } = clientWithFakeControl();
  const transferId = "3f0c9a52-8a8f-4d44-9d7c-4b2f7f0c1a12";
  control.statusResponse = { status: "failed", error_message: "source object changed during the transfer", started_at: null, completed_at: null };

  await assert.rejects(client.waitForTransfer(transferId, { timeoutMs: 100, pollIntervalMs: 1 }), (error) => {
    assert.ok(error instanceof BeamTransferFailedError);
    assert.equal(error instanceof BeamStorageAccessError, false);
    assert.equal(error.errorMessage, "source object changed during the transfer");
    return true;
  });
});

test("transferFailedError classifies only the storage access code prefix", () => {
  const source = transferFailedError("t-1", "source_access_denied: The source storage refused Beam's requests (403 AccessDenied).");
  assert.ok(source instanceof BeamStorageAccessError);
  assert.equal(source.code, "source_access_denied");
  assert.equal(transferFailedError("t-2", "upstream said source_access_denied: later") instanceof BeamStorageAccessError, false);
  assert.equal(transferFailedError("t-3", "destination_access_denied_extra: x") instanceof BeamStorageAccessError, false);
  const unknown = transferFailedError("t-4", null);
  assert.equal(unknown instanceof BeamStorageAccessError, false);
  assert.equal(unknown.errorMessage, null);
});

test("BeamClient plans, prepares, and attaches signed URLs through chunked NATS messages", async () => {
  const { client, control } = clientWithFakeControl();

  const source = {
    source_id: "src_0",
    type: "http",
    url: "https://source.example/file.bin",
    size: 1024
  };
  const destination = {
    destination_id: "dst_0",
    provider: "http",
    mode: "http_chunks",
    logical_prefix: "out/file.bin"
  };

  const planned = await client.planTransfer({
    sources: [source],
    destinations: [destination]
  });
  assert.equal(planned.success, true);

  const prepared = await client.prepareTransfer({
    sources: [source],
    destinations: [destination]
  });
  assert.equal(prepared.transfer_key, "tk_test");

  const attached = await client.attachSignedUrls(prepared.transfer_id, {
    chunkRoutes: [
      {
        source_id: "src_0",
        destination_id: "dst_0",
        chunk_index: 0,
        source_url: "https://source.example/file.bin",
        dest_url: "https://dest.example/file.bin.part0",
        source_offset: 0,
        chunk_size: 512,
        metadata: {
          multipart_group_id: "group-0",
          upload_id: "upload-0",
          final_object_key: "file.bin",
          bucket: "dest-bucket",
          part_number: 1,
          delivery_index: 0
        }
      },
      {
        source_id: "src_0",
        destination_id: "dst_0",
        chunk_index: 1,
        source_url: "https://source.example/file.bin",
        dest_url: "https://dest.example/file.bin.part1",
        source_offset: 512,
        chunk_size: 512,
        metadata: {
          multipart_group_id: "group-0",
          upload_id: "upload-0",
          final_object_key: "file.bin",
          bucket: "dest-bucket",
          part_number: 2,
          delivery_index: 1
        }
      }
    ],
    multipartGroupManifest: [{
      multipart_group_id: "group-0",
      source_id: "src_0",
      destination_id: "dst_0",
      final_object_key: "file.bin",
      upload_id: "upload-0",
      expected_object_size: 1024,
      expected_part_count: 2,
      max_part_number: 2,
      complete_url: "https://dest.example/complete",
      abort_url: "https://dest.example/abort",
      list_page_urls: ["https://dest.example/list"],
      final_head_url: "https://dest.example/final-head",
      final_object_metadata: {
        "beam-transfer-id": prepared.transfer_id
      },
      urls_expires_at: "2026-07-16T00:00:00.000Z"
    }],
    transferKey: prepared.transfer_key,
    routeGenerationId: prepared.route_generation_id,
    planFingerprint: prepared.plan_fingerprint,
    coordinateChecksum: prepared.coordinate_checksum,
    recoveryFactory: async () => ({ chunkRoutes: [], multipartGroupManifest: [] })
  });
  assert.equal(attached.success, true);
  assert.equal(control.recoveryPresentAtRouteBegin, true);
  for (const call of control.calls.slice(0, 2)) {
    assert.equal("chunk_size" in call.payload, false);
    assert.equal("provider_part_size" in call.payload, false);
  }

  assert.deepEqual(
    control.calls.map((call) => call.messageType),
    [
      "transfer.plan",
      "transfer.prepare",
      "transfer.route_stream.begin",
      "transfer.route_stream.manifest",
      "transfer.route_stream.batch",
      "transfer.route_stream.complete"
    ]
  );
  assert.equal(control.calls[2].payload.total_routes, 2);
  assert.equal(control.calls[2].payload.route_contract_version, "signed_url");
  assert.equal(control.calls.slice(2, 6).every((call) => call.payload.route_generation_id === prepared.route_generation_id), true);
  assert.equal("multipart_group_manifest" in control.calls[2].payload, false);
  assert.equal(control.calls[3].payload.groups[0].max_part_number, 2);
  assert.equal(control.calls[4].payload.batch_index, 0);
  assert.equal(control.calls[4].payload.route_count, 2);
  assert.equal("multipart_groups" in control.calls[4].payload.route_batch, false);
  assert.deepEqual(
    control.calls[4].payload.route_batch.routes.map((route) => route.multipart_group_id),
    ["group-0", "group-0"]
  );
  assert.deepEqual(control.calls[4].payload.route_batch.routes.map((route) => route.metadata), [
    { part_number: 1 },
    { part_number: 2 }
  ]);
  assert.match(control.calls[5].payload.route_keys_checksum, /^sha256-xor-v1:2:/);
});

test("BeamClient flushes 1,024-route batches and the final partial batch", async () => {
  const { client, control } = clientWithFakeControl();
  const chunkRoutes = Array.from({ length: 5_121 }, (_, chunkIndex) => ({
    source_id: "src_0",
    destination_id: "dst_0",
    chunk_index: chunkIndex,
    source_url: `https://source.example/file.bin?chunk=${chunkIndex}`,
    dest_url: `https://dest.example/file.bin.part${chunkIndex}`,
    source_offset: chunkIndex * 512,
    chunk_size: 512
  }));

  await client.attachSignedUrls("11111111-1111-4111-8111-111111111111", {
    chunkRoutes,
    multipartGroupManifest: [],
    transferKey: "tk_test",
    routeGenerationId: "22222222-2222-4222-8222-222222222222",
    planFingerprint: "a".repeat(64),
    coordinateChecksum: `sha256-xor-v1:5121:${"0".repeat(64)}`,
    recoveryFactory: async () => ({ chunkRoutes, multipartGroupManifest: [] })
  });

  const batches = control.calls.filter((call) => call.messageType === "transfer.route_stream.batch");
  assert.equal(batches.length, 6);
  assert.deepEqual(batches.map((call) => call.payload.batch_index), [0, 1, 2, 3, 4, 5]);
  assert.deepEqual(batches.map((call) => call.payload.route_count), [1_024, 1_024, 1_024, 1_024, 1_024, 1]);
  const complete = control.calls.at(-1);
  assert.equal(complete.messageType, "transfer.route_stream.complete");
  assert.equal(complete.payload.expected_batches, 6);
});

for (const boundary of ["transfer.route_stream.begin", "transfer.route_stream.batch"]) {
  test(`BeamClient retains recovery when transport stops at ${boundary}`, async () => {
    const { client, control } = clientWithFakeControl();
    const transferId = "11111111-1111-4111-8111-111111111111";
    const routes = [{
      source_id: "src_0",
      destination_id: "dst_0",
      chunk_index: 0,
      delivery_index: 0,
      source_url: "https://source.example/file.bin",
      dest_url: "https://dest.example/file.bin.part0",
      source_offset: 0,
      chunk_size: 512
    }];
    control.failOnceAt = boundary;
    await assert.rejects(
      client.attachSignedUrls(transferId, {
        chunkRoutes: routes,
        multipartGroupManifest: [],
        routeGenerationId: "22222222-2222-4222-8222-222222222222",
        planFingerprint: "a".repeat(64),
        coordinateChecksum: `sha256-xor-v1:1:${"0".repeat(64)}`,
        recoveryFactory: async () => ({ chunkRoutes: routes, multipartGroupManifest: [] })
      }),
      (error) => error instanceof BeamRouteRecoveryPendingError
        && error.transferId === transferId
        && /connection closed/.test(String(error.cause))
    );
    assert.equal(control.recoveryPresentAtRouteBegin, true);
    assert.deepEqual(control.continuedRecovery, [transferId]);
    assert.equal(control.recoveryLeases.has(transferId), true);
  });
}

class MultiDestinationTransferControl extends FakeTransferControl {
  async request(messageType, payload, options = {}) {
    if (messageType !== "transfer.prepare") {
      return super.request(messageType, payload, options);
    }
    this.calls.push({ messageType, payload, options });
    const source = payload.sources[0];
    const planDescriptor = {
      version: "compact-transfer-plan/v1",
      plan_nonce: "multipart-concurrency",
      chunk_size: 1024,
      sources: [{ ...source, global_chunk_start: 0, chunk_count: 1 }],
      destinations: payload.destinations.map((destination, destinationIndex) => ({
        ...destination,
        destination_index: destinationIndex,
        final_object_keys: { [source.source_id]: `output-${destinationIndex}.bin` }
      })),
      logical_chunk_count: 1,
      delivery_route_count: payload.destinations.length,
      multipart_attempt_slots: 1,
      formulas: {
        source_offset: "source_chunk_index * chunk_size",
        delivery_index: "chunk_index * destination_count + destination_index",
        part_number: "source_chunk_index + 1",
        route_generation_id: "initial-{chunk_index}-{destination_id}"
      }
    };
    return {
      success: true,
      transfer_id: payload.transfer_id,
      transfer_key: "tk_test",
      chunk_size: 1024,
      total_size: 1024,
      total_sources: 1,
      total_destinations: payload.destinations.length,
      logical_chunks: 1,
      total_chunks: payload.destinations.length,
      signed_url_flow: "signed_url",
      plan_fingerprint: "a".repeat(64),
      coordinate_checksum: `sha256-xor-v1:${payload.destinations.length}:${"0".repeat(64)}`,
      route_generation_id: payload.route_generation_id,
      plan_descriptor: planDescriptor
    };
  }
}

function fakePlanDescriptor(source, destination, chunkSize) {
  return {
    version: "compact-transfer-plan/v1",
    plan_nonce: "testplan",
    chunk_size: chunkSize,
    sources: [{ ...source, global_chunk_start: 0, chunk_count: 1 }],
    destinations: [{
      ...destination,
      destination_index: 0,
      final_object_keys: { [source.source_id]: "out/file.bin" }
    }],
    logical_chunk_count: 1,
    delivery_route_count: 1,
    multipart_attempt_slots: 1,
    formulas: {
      source_offset: "source_chunk_index * chunk_size",
      delivery_index: "chunk_index * destination_count + destination_index",
      part_number: "source_chunk_index + 1",
      route_generation_id: "initial-{chunk_index}-{destination_id}"
    }
  };
}

function listen(server) {
  return new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

test("BeamClient rejects HTTP lifecycle endpoints", () => {
  assert.throws(
    () => new BeamClient({ apiKey: "b1m_test", natsUrl: "http://beamcore.test" }),
    /must be nats:\/\/, tls:\/\//
  );
});
