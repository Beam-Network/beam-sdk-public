'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');

const {
  BASE_URL,
  TRANSFER_OUTPUT_FIELDS,
  TRANSFER_SAMPLE,
  afterResponse,
  beforeRequest,
  toZapierTransfer,
} = require('../api');

/** Minimal stand-ins for what the platform injects. */
const errors = {
  Error: class AppError extends Error {
    constructor(message, code, status) {
      super(message);
      this.name = 'AppError';
      this.code = code;
      this.status = status;
    }
  },
  ExpiredAuthError: class ExpiredAuthError extends Error {},
  ThrottledError: class ThrottledError extends Error {
    constructor(message, delay) {
      super(message);
      this.delay = delay;
    }
  },
};
const z = { errors };

const beamTransfer = {
  id: '57423d64-8b94-4695-b8dc-297c1288a234',
  transfer_key: 'nightly-sync',
  status: 'completed',
  total_bytes: '4831838208',
  bytes_transferred: '4831838208',
  error_message: null,
  metadata: { name: 'Nightly sync' },
  created_at: '2026-09-06T02:11:04.000Z',
  started_at: '2026-09-06T02:11:09.000Z',
  completed_at: '2026-09-06T02:14:43.000Z',
  test_mode: false,
};

test('the api key is sent as x-api-key, never as a bearer token', () => {
  const request = beforeRequest({ url: `${BASE_URL}/transfers` }, z, {
    authData: { api_key: 'b1m_secret' },
  });
  assert.equal(request.headers['x-api-key'], 'b1m_secret');
  assert.equal(request.headers.Authorization, undefined);
});

test('an unauthenticated request is left alone rather than sent a blank header', () => {
  const request = beforeRequest({ url: `${BASE_URL}/transfers` }, z, {});
  assert.deepEqual(request.headers, undefined);
});

test('a transfer keeps a stable id, which is what Zapier de-duplicates on', () => {
  const record = toZapierTransfer(beamTransfer);
  assert.equal(record.id, beamTransfer.id);
  assert.equal(typeof record.id, 'string');
});

test('bytes arrive as strings from Postgres and must not reach a Zap that way', () => {
  const record = toZapierTransfer(beamTransfer);
  assert.equal(record.total_bytes, 4831838208);
  assert.equal(record.bytes_transferred, 4831838208);
  assert.equal(record.total_gb, 4.832);
});

test('duration is computed, because a Zap step cannot do arithmetic', () => {
  const record = toZapierTransfer(beamTransfer);
  assert.equal(record.duration_seconds, 214);
});

test('a transfer that never started has no duration rather than a wrong one', () => {
  const record = toZapierTransfer({
    ...beamTransfer,
    started_at: null,
    completed_at: null,
  });
  assert.equal(record.duration_seconds, null);
});

test('a failed transfer is reported as not succeeded, with its error', () => {
  const record = toZapierTransfer({
    ...beamTransfer,
    status: 'failed',
    error_message: 'destination refused the connection',
  });
  assert.equal(record.succeeded, false);
  assert.equal(record.error_message, 'destination refused the connection');
});

test('the name falls back to the transfer key when metadata carries none', () => {
  const { metadata, ...withoutMetadata } = beamTransfer;
  assert.equal(toZapierTransfer(withoutMetadata).name, 'nightly-sync');
});

test('the sample covers every declared output field', () => {
  for (const field of TRANSFER_OUTPUT_FIELDS) {
    assert.ok(
      Object.hasOwn(TRANSFER_SAMPLE, field.key),
      `sample is missing "${field.key}", so the Zap editor cannot preview it`,
    );
  }
});

test('the sample is shaped exactly like a mapped transfer', () => {
  assert.deepEqual(
    Object.keys(toZapierTransfer(beamTransfer)).sort(),
    Object.keys(TRANSFER_SAMPLE).sort(),
  );
});

test('a 2xx response passes through untouched', () => {
  const response = { status: 200, json: {} };
  assert.equal(afterResponse(response, z), response);
});

test('401 asks for reconnection rather than reporting a generic failure', () => {
  assert.throws(
    () => afterResponse({ status: 401, json: {} }, z),
    errors.ExpiredAuthError,
  );
});

test('403 names the cause, since a permitted key is the usual problem', () => {
  assert.throws(
    () =>
      afterResponse(
        { status: 403, json: { error: 'transfers are not enabled for this API key' } },
        z,
      ),
    (error) =>
      error instanceof errors.Error &&
      error.status === 403 &&
      error.message.includes('transfers are not enabled'),
  );
});

test('429 becomes a throttle with a delay Zapier can honour', () => {
  const response = {
    status: 429,
    json: {},
    getHeader: (name) => (name === 'retry-after' ? '30' : null),
  };
  assert.throws(
    () => afterResponse(response, z),
    (error) => error instanceof errors.ThrottledError && error.delay === 30,
  );
});

test('a throttle with no Retry-After still gets a sane delay', () => {
  assert.throws(
    () => afterResponse({ status: 429, json: {}, getHeader: () => null }, z),
    (error) => error instanceof errors.ThrottledError && error.delay === 60,
  );
});

test('a 500 surfaces the status and any detail Beam gave', () => {
  assert.throws(
    () => afterResponse({ status: 500, json: { error: 'boom' } }, z),
    (error) => error instanceof errors.Error && error.message.includes('500'),
  );
});
