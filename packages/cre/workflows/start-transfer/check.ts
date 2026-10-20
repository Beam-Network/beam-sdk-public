// Offline checks for the workflow's pure functions. Run with `bun run check`; not part of the workflow bundle.
import { createHmac } from 'node:crypto'
import { readFileSync } from 'node:fs'
import {
  normalizeRequestId,
  projectStudioResponse,
  TRANSFER_REQUESTED_TOPIC,
  toBase64,
  webhookBody,
  webhookSignature,
} from './studio'
import { configSchema, describeOutcome, webhookSecretsFor } from './workflow'

const check = (name: string, actual: unknown, expected: unknown) => {
  const a = JSON.stringify(actual)
  const e = JSON.stringify(expected)
  if (a !== e) {
    console.error(`FAIL ${name}\n  actual:   ${a}\n  expected: ${e}`)
    process.exit(1)
  }
  console.log(`ok   ${name}: ${a}`)
}

const bytes = (value: unknown) => new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value))
const failure = (run: () => unknown): string => {
  try {
    run()
    return 'no error'
  } catch (error) {
    return (error as Error).message
  }
}

const requestId = `0x${'ab'.repeat(32)}` as const
const requester = '0x9C296Eed0f4f4773BFcD2Cf991509556E4B922B7'

// Studio verifies `X-Beam-Signature: v1=<hex>` as HMAC-SHA256 over `v1:<timestamp>:<raw body>`
// (apps/api/src/studio/webhook-signature.ts); Node's crypto is the independent implementation.
const body = webhookBody(requestId, requester, 'example input — ü')
check('body keeps a fixed key order and lowercases the requester', body,
  JSON.stringify({ requestId, requester: requester.toLowerCase(), input: 'example input — ü' }))
check('signature matches Studio’s HMAC over v1:<timestamp>:<body>', webhookSignature('trigger-secret', 1792450000, body),
  `v1=${createHmac('sha256', 'trigger-secret').update(`v1:1792450000:${body}`).digest('hex')}`)
for (const sample of ['', 'a', 'ab', 'abc', 'abcd', body]) {
  check(`base64 matches Buffer for ${JSON.stringify(sample).slice(0, 20)}`, toBase64(new TextEncoder().encode(sample)),
    Buffer.from(sample, 'utf8').toString('base64'))
}

// topic0 of a TransferRequested log emitted by BeamTransferReceipts on Sepolia (tx 0xf4837f9b…8125).
check('event topic matches the deployed contract', TRANSFER_REQUESTED_TOPIC,
  '0x20d28bc6406b346ce4198ff55ba67ba5cef2289bc4a70cbfd72791bcff891a44')
check('request id is lowercased', normalizeRequestId(requestId.toUpperCase().replace('0X', '0x')), requestId)
check('zero request id is refused', failure(() => normalizeRequestId(`0x${'0'.repeat(64)}`)), `Invalid request id: 0x${'0'.repeat(64)}`)

// A first delivery answers 202 and a repeat 200 deduplicated: both must project to the same outcome.
check('first delivery', projectStudioResponse(202, bytes({ accepted: true, runId: 'wfr_1', pending: 0 })),
  { state: 'accepted', runId: 'wfr_1' })
check('repeat delivery agrees with the first', projectStudioResponse(200, bytes({ accepted: true, deduplicated: true, runId: 'wfr_1' })),
  { state: 'accepted', runId: 'wfr_1' })
check('coalesced delivery', projectStudioResponse(202, bytes({ accepted: true, runId: null, pending: 2 })),
  { state: 'accepted', runId: null })
check('rejected signature', projectStudioResponse(401, bytes({ code: 'webhook_signature_rejected', details: { reason: 'stale_timestamp' } })),
  { state: 'refused', status: 401, reason: 'stale_timestamp' })
check('unknown webhook', projectStudioResponse(404, bytes({ error: 'Webhook not found' })), { state: 'refused', status: 404, reason: null })
check('rate limited', projectStudioResponse(429, bytes({ code: 'webhook_rate_limited' })), { state: 'unavailable', status: 429 })
check('server error', projectStudioResponse(503, bytes('<html>')), { state: 'unavailable', status: 503 })
check('2xx without acceptance', projectStudioResponse(200, bytes({})), { state: 'unavailable', status: 200 })

check('accepted outcome', describeOutcome(requestId, { state: 'accepted', runId: 'wfr_1' }), `start: ${requestId} accepted as Studio run wfr_1`)
check('refusal fails the execution', failure(() => describeOutcome(requestId, { state: 'refused', status: 404, reason: null })),
  `Studio refused ${requestId} with HTTP 404`)
check('unavailable fails the execution', failure(() => describeOutcome(requestId, { state: 'unavailable', status: 0 })),
  `Studio was unavailable for ${requestId} (HTTP none)`)

for (const target of ['staging', 'production']) {
  const config = JSON.parse(readFileSync(new URL(`./config.${target}.json`, import.meta.url), 'utf8'))
  check(`config.${target}.json matches the config schema`, configSchema.safeParse(config).success ? 'valid' : 'invalid', 'valid')
}

// Each kind of request goes to its own Studio workflow; Room deliveries only when the deployment starts them.
const NONE = { startRoomDeliveries: false, startJobs: false }
check('a standard request goes to the transfer webhook', webhookSecretsFor(1, NONE)?.url, 'STUDIO_WEBHOOK_URL')
check('a Room delivery request goes to the Room webhook when enabled', webhookSecretsFor(2, { ...NONE, startRoomDeliveries: true }),
  { url: 'STUDIO_ROOM_WEBHOOK_URL', signingSecret: 'STUDIO_ROOM_WEBHOOK_SIGNING_SECRET' })
check('a Room delivery request is not started when disabled', webhookSecretsFor(2, NONE), null)
check('a job goes to the Room webhook, which delivers its input, when enabled', webhookSecretsFor(3, { ...NONE, startJobs: true })?.url, 'STUDIO_ROOM_WEBHOOK_URL')
check('a job is not started when disabled, even with Room deliveries on', webhookSecretsFor(3, { ...NONE, startRoomDeliveries: true }), null)
check('an unknown kind is never started', webhookSecretsFor(4, { startRoomDeliveries: true, startJobs: true }), null)
