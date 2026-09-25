'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');

const app = require('../index');
const trigger = require('../triggers/transfer-completed');
const search = require('../searches/find-transfer');

/** Records what the operation asked Beam for, and answers with a fixture. */
function zStub(response) {
  const calls = [];
  return {
    calls,
    z: {
      request: async (options) => {
        calls.push(options);
        return response;
      },
      errors: {
        Error: class AppError extends Error {
          constructor(message, code, status) {
            super(message);
            this.status = status;
          }
        },
      },
    },
  };
}

const beamTransfer = {
  id: 'b246a692-775c-4106-88a6-b18bdbfa21c0',
  transfer_key: 'archive-2026-q3',
  status: 'completed',
  total_bytes: '1048576',
  bytes_transferred: '1048576',
  metadata: {},
  created_at: '2026-09-06T02:11:04.000Z',
  started_at: '2026-09-06T02:11:09.000Z',
  completed_at: '2026-09-06T02:11:40.000Z',
  test_mode: false,
};

test('the trigger orders by completion, not creation', async () => {
  const { z, calls } = zStub({ data: { transfers: [beamTransfer] } });
  await trigger.operation.perform(z, { inputData: {} });

  // Ordering by created_at would bury a long-running transfer that finishes
  // today below everything created since, and Zapier only sees the first page.
  assert.equal(calls[0].params.order_by, 'completed_at');
});

test('the trigger filters server-side so in-flight work never fills the page', async () => {
  const { z, calls } = zStub({ data: { transfers: [] } });
  await trigger.operation.perform(z, { inputData: {} });
  assert.equal(calls[0].params.status, 'completed');
  assert.equal(calls[0].params.limit, 100);
});

test('the trigger can watch failures instead, for an alerting Zap', async () => {
  const { z, calls } = zStub({ data: { transfers: [] } });
  await trigger.operation.perform(z, { inputData: { status: 'failed' } });
  assert.equal(calls[0].params.status, 'failed');
});

test('the trigger returns mapped records carrying an id', async () => {
  const { z } = zStub({ data: { transfers: [beamTransfer] } });
  const results = await trigger.operation.perform(z, { inputData: {} });
  assert.equal(results.length, 1);
  assert.equal(results[0].id, beamTransfer.id);
  assert.equal(results[0].total_bytes, 1048576);
});

test('an empty response is an empty poll, not a crash', async () => {
  const { z } = zStub({ data: {} });
  assert.deepEqual(await trigger.operation.perform(z, { inputData: {} }), []);
});

test('a search miss returns nothing rather than halting the Zap', async () => {
  const { z, calls } = zStub({ status: 404, data: {} });
  const results = await search.operation.perform(z, {
    inputData: { transfer: 'does-not-exist' },
  });
  assert.deepEqual(results, []);
  // Without this the platform turns the 404 into an error before we see it.
  assert.equal(calls[0].skipThrowForStatus, true);
});

test('a search hit returns exactly one mapped record', async () => {
  const { z } = zStub({ status: 200, data: beamTransfer });
  const results = await search.operation.perform(z, {
    inputData: { transfer: 'archive-2026-q3' },
  });
  assert.equal(results.length, 1);
  assert.equal(results[0].transfer_key, 'archive-2026-q3');
});

test('a blank search term does not become a request for /transfers/', async () => {
  const { z, calls } = zStub({ status: 200, data: beamTransfer });
  assert.deepEqual(
    await search.operation.perform(z, { inputData: { transfer: '  ' } }),
    [],
  );
  assert.equal(calls.length, 0);
});

test('an identifier is escaped into the path', async () => {
  const { z, calls } = zStub({ status: 200, data: beamTransfer });
  await search.operation.perform(z, { inputData: { transfer: 'a/b c' } });
  assert.ok(calls[0].url.endsWith('/transfers/a%2Fb%20c'));
});

test('the app declares auth, a trigger, a search and a create', () => {
  assert.equal(app.authentication.type, 'custom');
  assert.ok(app.triggers.transfer_completed);
  assert.ok(app.searches.find_transfer);
  // Creating a transfer needs a process that outlives a Zapier action, so the
  // create asks a Studio workflow to do it rather than doing it here.
  assert.ok(app.creates.start_transfer);
  assert.ok(app.version, 'version must come from package.json');
  assert.ok(app.platformVersion, 'platformVersion must come from core');
});

test('the compiled app validates against zapier-platform-schema', () => {
  // This is the check `zapier validate` runs before a push. Functions have to
  // be replaced by their $func$ references first, which is what the platform
  // does at build time — validating the raw object reports every perform as a
  // type error and tells you nothing.
  const { recurseCleanFuncs } = require('zapier-platform-core/src/tools/cleaner');
  const schema = require('zapier-platform-schema');

  const { errors } = schema.validateAppDefinition(recurseCleanFuncs(app));
  assert.deepEqual(
    errors.map((error) => `${error.property}: ${error.message}`),
    [],
  );
});

const startTransfer = require('../creates/start-transfer');

test('start-transfer posts the input to the Studio hook', async () => {
  const { z, calls } = zStub({ status: 202, data: { accepted: true, runId: 'wfr_1', pending: false } });
  const result = await startTransfer.operation.perform(z, {
    inputData: {
      hook_url: 'https://api.studio.b1m.ai/hooks/workflows/w1/t1/tok',
      payload: { file: 'a.csv' },
      idempotency_key: 'file-123',
    },
  });

  assert.equal(calls[0].method, 'POST');
  assert.deepEqual(calls[0].body, { file: 'a.csv' });
  assert.equal(calls[0].headers['Idempotency-Key'], 'file-123');
  assert.equal(result.run_id, 'wfr_1');
  assert.equal(result.accepted, true);
});

test('start-transfer omits the idempotency header when none is given', async () => {
  const { z, calls } = zStub({ status: 202, data: { accepted: true } });
  await startTransfer.operation.perform(z, {
    inputData: { hook_url: 'https://api.studio.b1m.ai/hooks/workflows/w/t/k' },
  });
  assert.equal(calls[0].headers['Idempotency-Key'], undefined);
});

test('start-transfer refuses a non-https hook url before sending anything', async () => {
  const { z, calls } = zStub({ status: 202, data: {} });
  await assert.rejects(
    () => startTransfer.operation.perform(z, { inputData: { hook_url: 'http://example.com/x' } }),
    (/** @type {any} */ error) => error.status === 400,
  );
  assert.equal(calls.length, 0, 'nothing should be sent to a rejected URL');
});

test('start-transfer explains a 404 rather than passing it through', async () => {
  const { z } = zStub({ status: 404, data: {} });
  await assert.rejects(
    () =>
      startTransfer.operation.perform(z, {
        inputData: { hook_url: 'https://api.studio.b1m.ai/hooks/workflows/w/t/bad' },
      }),
    (/** @type {any} */ error) =>
      error.status === 404 && /copied whole/.test(error.message),
  );
});

test('a coalesced run has no id yet, and that is not a failure', async () => {
  const { z } = zStub({ status: 202, data: { accepted: true, runId: null, pending: true } });
  const result = await startTransfer.operation.perform(z, {
    inputData: { hook_url: 'https://api.studio.b1m.ai/hooks/workflows/w/t/k' },
  });
  assert.equal(result.run_id, null);
  assert.equal(result.pending, true);
  assert.equal(result.accepted, true);
});

test('the Beam api key is never attached to a non-Beam URL', () => {
  const { beforeRequest } = require('../api');
  const bundle = { authData: { api_key: 'b1m_secret' } };

  const toBeam = beforeRequest({ url: 'https://beamcore.b1m.ai/transfers' }, null, bundle);
  assert.equal(toBeam.headers['x-api-key'], 'b1m_secret');

  // A user pastes this URL in themselves; sending their Beam key to it would
  // hand the credential to whoever owns that host.
  const toStudio = beforeRequest(
    { url: 'https://api.studio.b1m.ai/hooks/workflows/w/t/k' },
    null,
    bundle,
  );
  assert.equal(toStudio.headers, undefined);

  const toAnywhere = beforeRequest({ url: 'https://evil.example/collect' }, null, bundle);
  assert.equal(toAnywhere.headers, undefined);
});
