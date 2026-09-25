'use strict';

/**
 * The Beam side of the integration: where requests go, how they are
 * authenticated, and how a transfer is shaped for Zapier.
 *
 * Everything here talks to BeamCore's public HTTP API. The SDK is deliberately
 * not used: it reaches BeamCore over NATS and answers a re-signing callback,
 * which needs a long-lived process, and a Zapier integration runs as a
 * short-lived function.
 */

/**
 * Overridable with `zapier env:set <version> BEAM_API_BASE_URL=...` so a
 * version can be pointed at dev for testing. It is not an auth field: users
 * connect to Beam's hosted service, and asking them for a hostname would
 * invite typos and phishing-shaped mistakes.
 */
const BASE_URL = process.env.BEAM_API_BASE_URL || 'https://beamcore.b1m.ai';

/** Terminal states. `completed_at` is set for all three, not just success. */
const TERMINAL_STATUSES = ['completed', 'failed', 'cancelled'];

/**
 * Beam accepts the key as `x-api-key`, `x-beam-key` or a bearer token. The
 * dedicated header is used so an Authorization header never appears in a log
 * that might be shared.
 */
/**
 * True only when the request is going to Beam's own API.
 *
 * The Start a Transfer action posts to a Studio webhook URL that the user
 * pastes in, so not every request this app makes goes to Beam. An unparseable
 * or relative URL is treated as foreign: refusing to send the key somewhere
 * unrecognised is the safe failure.
 */
const isBeamApi = (url) => {
  try {
    return new URL(String(url)).origin === new URL(BASE_URL).origin;
  } catch {
    return false;
  }
};

const beforeRequest = (request, z, bundle) => {
  // The key goes to Beam and nowhere else. Attaching it unconditionally would
  // hand a customer's credential to whoever owns whatever URL they pasted into
  // a Zap -- a mistake the user could not see and could not undo.
  if (!isBeamApi(request.url)) {
    return request;
  }
  if (bundle.authData && bundle.authData.api_key) {
    request.headers = request.headers || {};
    request.headers['x-api-key'] = bundle.authData.api_key;
  }
  return request;
};

/**
 * Turns a Beam error response into something a user can act on.
 *
 * Zapier shows this text directly in a Zap's error, so it says what to do
 * rather than restating the status code. A 403 is the interesting one: it means
 * the key authenticated but is not permitted to read transfers, which on
 * production has a specific cause worth naming.
 */
const afterResponse = (response, z) => {
  if (response.status < 400) {
    return response;
  }

  const detail = errorDetail(response);

  if (response.status === 401) {
    throw new z.errors.ExpiredAuthError(
      'Beam rejected this API key. Reconnect the account with a current key.',
    );
  }
  if (response.status === 403) {
    throw new z.errors.Error(
      `This Beam API key is not permitted to read transfers${detail ? `: ${detail}` : '.'}`,
      'Forbidden',
      403,
    );
  }
  if (response.status === 429) {
    throw new z.errors.ThrottledError(
      'Beam is rate limiting this connection.',
      retryAfterSeconds(response),
    );
  }

  throw new z.errors.Error(
    `Beam returned ${response.status}${detail ? `: ${detail}` : '.'}`,
    'BeamApiError',
    response.status,
  );
};

function errorDetail(response) {
  const body = response.json || {};
  const text = typeof body.error === 'string' ? body.error : '';
  return text.slice(0, 200);
}

function retryAfterSeconds(response) {
  const raw = response.getHeader ? response.getHeader('retry-after') : null;
  const seconds = Number(raw);
  return Number.isFinite(seconds) && seconds > 0 ? seconds : 60;
}

/**
 * Flattens a Beam transfer into the record a Zap consumes.
 *
 * `id` is what Zapier de-duplicates on, so it has to be present and stable on
 * every item; Beam's transfer id is a UUID primary key and never changes.
 *
 * The convenience fields are computed here rather than left to the user: a Zap
 * step cannot do arithmetic, so `total_gb` and `duration_seconds` would
 * otherwise be unreachable in a Zap that wants to say how big or how long.
 */
function toZapierTransfer(transfer) {
  const totalBytes = Number(transfer.total_bytes || 0);
  const startedAt = transfer.started_at ? Date.parse(transfer.started_at) : NaN;
  const completedAt = transfer.completed_at
    ? Date.parse(transfer.completed_at)
    : NaN;
  const durationSeconds =
    Number.isFinite(startedAt) && Number.isFinite(completedAt)
      ? Math.max(0, Math.round((completedAt - startedAt) / 1000))
      : null;

  return {
    id: transfer.id,
    transfer_key: transfer.transfer_key,
    name: (transfer.metadata && transfer.metadata.name) || transfer.transfer_key,
    status: transfer.status,
    succeeded: transfer.status === 'completed',
    error_message: transfer.error_message || null,
    total_bytes: totalBytes,
    total_gb: Math.round((totalBytes / 1e9) * 1000) / 1000,
    bytes_transferred: Number(transfer.bytes_transferred || 0),
    duration_seconds: durationSeconds,
    created_at: transfer.created_at,
    started_at: transfer.started_at || null,
    completed_at: transfer.completed_at || null,
    test_mode: Boolean(transfer.test_mode),
  };
}

/** Shared by the trigger and the search so both describe fields identically. */
const TRANSFER_OUTPUT_FIELDS = [
  { key: 'id', label: 'Transfer ID' },
  { key: 'transfer_key', label: 'Transfer key' },
  { key: 'name', label: 'Name' },
  { key: 'status', label: 'Status' },
  { key: 'succeeded', label: 'Succeeded', type: 'boolean' },
  { key: 'error_message', label: 'Error message' },
  { key: 'total_bytes', label: 'Total bytes', type: 'integer' },
  { key: 'total_gb', label: 'Total GB', type: 'number' },
  { key: 'bytes_transferred', label: 'Bytes transferred', type: 'integer' },
  { key: 'duration_seconds', label: 'Duration (seconds)', type: 'integer' },
  { key: 'created_at', label: 'Created at', type: 'datetime' },
  { key: 'started_at', label: 'Started at', type: 'datetime' },
  { key: 'completed_at', label: 'Completed at', type: 'datetime' },
  { key: 'test_mode', label: 'Test mode', type: 'boolean' },
];

/**
 * Shown in the Zap editor before a Zap has ever run, so a user can map fields
 * without first producing a real transfer. Zapier requires one.
 */
const TRANSFER_SAMPLE = {
  id: '57423d64-8b94-4695-b8dc-297c1288a234',
  transfer_key: 'nightly-warehouse-sync',
  name: 'nightly-warehouse-sync',
  status: 'completed',
  succeeded: true,
  error_message: null,
  total_bytes: 4831838208,
  total_gb: 4.832,
  bytes_transferred: 4831838208,
  duration_seconds: 214,
  created_at: '2026-09-06T02:11:04.000Z',
  started_at: '2026-09-06T02:11:09.000Z',
  completed_at: '2026-09-06T02:14:43.000Z',
  test_mode: false,
};

module.exports = {
  BASE_URL,
  isBeamApi,
  TERMINAL_STATUSES,
  TRANSFER_OUTPUT_FIELDS,
  TRANSFER_SAMPLE,
  afterResponse,
  beforeRequest,
  toZapierTransfer,
};
