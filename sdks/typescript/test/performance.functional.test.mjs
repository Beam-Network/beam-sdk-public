import assert from "node:assert/strict";
import test from "node:test";
import { BeamClient } from "../dist/index.js";
import { boundedGrantExpiry } from "../dist/provider-signing.js";

test("integrity grant renewal is shared, rejects conflicts, and retries unavailable delivery", async () => {
  const client = new BeamClient({apiKey: "b1m_test_key"});
  const challenge = {transfer_id: "transfer", audit_id: "audit", chunks: []};
  let signatures = 0, deliveries = 0, published = false;
  client.integrityAuditSigners.set("transfer", async () => {
    signatures++;
    const expires_at = new Date(Date.now() + 60_000).toISOString();
    return {submitted_at: String(signatures), chunks: [{source: {expires_at}, destination: {expires_at}}]};
  });
  const originalRequest = client.control.request;
  client.control.request = async () => { deliveries++; return {published}; };
  try {
    const status = {integrity_audit_challenge: challenge};
    await assert.rejects(client.submitIntegrityAuditGrantsIfPresent(status), /delivery unavailable/);
    published = true;
    await Promise.all(Array.from({length: 8}, () => client.submitIntegrityAuditGrantsIfPresent(status)));
    assert.equal(signatures, 1);
    assert.equal(deliveries, 2);
    await assert.rejects(client.submitIntegrityAuditGrantsIfPresent({integrity_audit_challenge: {...challenge, chunks: [{task_id: "changed"}]}}), /identity changed/);
    client.integrityGrantCache.get("audit").expiresAt = 0;
    await Promise.all(Array.from({length: 8}, () => client.submitIntegrityAuditGrantsIfPresent(status)));
    assert.equal(signatures, 2);
    assert.equal(deliveries, 3);
  } finally {
    client.control.request = originalRequest;
    await client.close();
  }
});

test("a reused provider grant never receives a later expiration", () => {
  const url = "https://example.invalid/read?X-Amz-Date=20260926T100000Z&X-Amz-Expires=3600";
  assert.equal(boundedGrantExpiry(url, "2026-09-26T12:00:00Z"), "2026-09-26T11:00:00.000Z");
  assert.equal(boundedGrantExpiry(url, "2026-09-26T10:30:00Z"), "2026-09-26T10:30:00.000Z");
});

test("provider dispatch preserves the shared source grant across destinations", async () => {
  let puts = 0;
  const client = new BeamClient({ apiKey: "b1m_test_key", fetch: async (url) => {
    assert.equal(new URL(url).searchParams.get("action"), "put");
    puts++;
    return Response.json({ url: "https://example.invalid/upload" });
  } });
  const sourceGrant = Promise.resolve({url:"https://example.invalid/shared", headers:{Range:"bytes=8-11"}, expiresAt:new Date(Date.now()+60_000).toISOString()});
  try {
    const routes = await Promise.all([0,1,2].map(index => client.signProviderRoute({
      chunk: {source_id:"source", chunk_index:0, source_chunk_index:0, source_offset:8, chunk_size:4, source_url:"https://example.invalid/old", destinations:[]},
      target: {destination_id:`dest-${index}`, provider:"hippius", object_key:`part-${index}`, metadata:{}},
      source: {provider:"hippius",bucket:"source",key:"file",api_token:"test"},
      sourceGrant,
      destination: {provider:"hippius",bucket:"destination",key:"file",api_token:"test",base_url:"https://example.invalid"},
      expiresIn:3600, transferId:"transfer", finalObjectKey:"file", signedUrlFlow:"signed_url", partNumber:1,
    })));
    assert.equal(puts,3);
    for (const route of routes) {
      assert.equal(route.source_url,"https://example.invalid/shared");
      assert.equal(route.headers.Range,"bytes=8-11");
      assert.equal(route.expires_at,(await sourceGrant).expiresAt);
    }
  } finally { await client.close(); }
});
