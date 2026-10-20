import { hmac } from '@noble/hashes/hmac.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { bytesToHex, utf8ToBytes } from '@noble/hashes/utils.js'
import { type Address, type Hex, parseAbi, toEventSelector } from 'viem'

/** The `BeamTransferReceipts` event this workflow starts transfers from. */
export const requestsAbi = parseAbi([
  'event TransferRequested(bytes32 indexed requestId, address indexed requester, uint8 kind, string input)',
])

export const TRANSFER_REQUESTED_TOPIC = toEventSelector('TransferRequested(bytes32,address,uint8,string)')

/** `BeamTransferReceipts.TransferKind`. */
export const TransferKind = { Standard: 1, RoomDelivery: 2, Job: 3 } as const

const BYTES32_PATTERN = /^0x[0-9a-f]{64}$/

/** The request id as lowercase 0x-prefixed hex: the idempotency key Studio's transfer step uses. */
export const normalizeRequestId = (requestId: string): Hex => {
  const value = requestId.toLowerCase()
  if (!BYTES32_PATTERN.test(value) || /^0x0+$/.test(value)) {
    throw new Error(`Invalid request id: ${requestId}`)
  }
  return value as Hex
}

/** What every node posts to the Studio webhook for one request. */
export type WebhookDelivery = {
  url: string
  requestId: Hex
  body: string
  timestampSeconds: number
  signature: string | null
}

/**
 * The Studio webhook body, which becomes the run's `workflow.input`. Keys are written in a fixed order
 * so every node sends identical bytes.
 */
export const webhookBody = (requestId: Hex, requester: Address, input: string): string =>
  JSON.stringify({ requestId, requester: requester.toLowerCase(), input })

/** Studio's `X-Beam-Signature`: HMAC-SHA256 over `v1:<timestamp>:<raw body>`. */
export const webhookSignature = (secret: string, timestampSeconds: number, body: string): string =>
  `v1=${bytesToHex(hmac(sha256, utf8ToBytes(secret), utf8ToBytes(`v1:${timestampSeconds}:${body}`)))}`

const BASE64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

/** Standard base64 with padding; the WASM runtime has no `Buffer`. */
export const toBase64 = (bytes: Uint8Array): string => {
  let out = ''
  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i]
    const b = i + 1 < bytes.length ? bytes[i + 1] : 0
    const c = i + 2 < bytes.length ? bytes[i + 2] : 0
    const triple = (a << 16) | (b << 8) | c
    out += BASE64_ALPHABET[(triple >> 18) & 63] + BASE64_ALPHABET[(triple >> 12) & 63]
    out += i + 1 < bytes.length ? BASE64_ALPHABET[(triple >> 6) & 63] : '='
    out += i + 2 < bytes.length ? BASE64_ALPHABET[triple & 63] : '='
  }
  return out
}

/**
 * What the nodes agree on. A first delivery answers 202 and a repeat answers 200 `deduplicated`, so only
 * the run id is compared, never the status code or the dedupe flag.
 */
export type StudioOutcome =
  | { state: 'accepted'; runId: string | null }
  | { state: 'refused'; status: number; reason: string | null }
  | { state: 'unavailable'; status: number }

export const UNAVAILABLE = JSON.stringify({ state: 'unavailable', status: 0 } satisfies StudioOutcome)

export const projectStudioResponse = (statusCode: number, body: Uint8Array): StudioOutcome => {
  const reply = parseJson(body)
  if ((statusCode === 200 || statusCode === 202) && isRecord(reply) && reply.accepted === true) {
    return { state: 'accepted', runId: typeof reply.runId === 'string' ? reply.runId : null }
  }
  // 401 is a rejected signature, 404 a wrong workflow, trigger or token; neither succeeds on a retry.
  if (statusCode === 401 || statusCode === 404) {
    const details = isRecord(reply) && isRecord(reply.details) ? reply.details : null
    return { state: 'refused', status: statusCode, reason: typeof details?.reason === 'string' ? details.reason : null }
  }
  return { state: 'unavailable', status: statusCode }
}

const parseJson = (body: Uint8Array): unknown => {
  try {
    return JSON.parse(new TextDecoder().decode(body))
  } catch {
    return null
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
