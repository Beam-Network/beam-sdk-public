import { createHmac, timingSafeEqual } from 'node:crypto'
import type { Attestation } from '../workflows/transfer-receipts/attestations'
import { normalizeTransferId } from '../workflows/transfer-receipts/transfer-id'

// The Room webhook side of the bridge: the host agent's signed deliveries and polls, and what a Room member's
// message asks for.

/** A signed request older or newer than this is refused, as Room webhook receivers do. */
export const SIGNATURE_TOLERANCE_SECONDS = 300

/**
 * Checks `X-Beam-Signature` (`v1=<hex HMAC-SHA256(secret, "v1:<timestamp>:<body>")>`, several during a secret
 * rotation) against every secret, and the timestamp against the clock. A poll signs an empty body.
 */
export const verifyWebhookSignature = (
  secrets: string[],
  timestamp: string | null,
  signatureHeader: string | null,
  body: Uint8Array,
  nowSeconds: number,
): boolean => {
  if (!timestamp || !signatureHeader || !/^[0-9]{1,12}$/.test(timestamp)) return false
  if (Math.abs(nowSeconds - Number(timestamp)) > SIGNATURE_TOLERANCE_SECONDS) return false
  const given = signatureHeader
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.startsWith('v1='))
    .map((part) => part.slice(3))
    .filter((hex) => /^[0-9a-f]{64}$/.test(hex))
    .map((hex) => Buffer.from(hex, 'hex'))
  return secrets.some((secret) => {
    const expected = createHmac('sha256', secret).update(`v1:${timestamp}:`).update(body).digest()
    return given.some((signature) => timingSafeEqual(signature, expected))
  })
}

/** A message delivery's envelope (`payload_mode: envelope`), with the fields the bridge reads. */
export type Envelope = {
  type: string
  room_id: string
  channel_id: string
  publication_id: string
  publisher_member_id: string
  webhook_member_id: string
  hop: number
  text: string | null
}

export const parseEnvelope = (body: Uint8Array): Envelope | null => {
  let value: unknown
  try {
    value = JSON.parse(new TextDecoder().decode(body))
  } catch {
    return null
  }
  if (!isRecord(value)) return null
  const strings = ['type', 'room_id', 'channel_id', 'publication_id', 'publisher_member_id', 'webhook_member_id'] as const
  if (!strings.every((field) => typeof value[field] === 'string')) return null
  if (typeof value.hop !== 'number' || !Number.isInteger(value.hop)) return null
  return {
    type: value.type as string,
    room_id: value.room_id as string,
    channel_id: value.channel_id as string,
    publication_id: value.publication_id as string,
    publisher_member_id: value.publisher_member_id as string,
    webhook_member_id: value.webhook_member_id as string,
    hop: value.hop,
    text: typeof value.text === 'string' ? value.text : null,
  }
}

/** What the bridge may attest, set by its operator. */
export type AttestationPolicy = {
  rooms: Record<string, { channels: string[] }>
  allowTransferAttestations: boolean
}

/**
 * What a Room message asks the bridge. A message that is not a JSON object with an `op` is ordinary Room
 * traffic and gets no reply. An `op` the bridge does not attest gets a refusal, since only attestations need
 * Chainlink.
 */
export type RoomRequest =
  | { kind: 'ignore' }
  | { kind: 'refuse'; reason: string; message: string }
  | { kind: 'attest'; attestation: Attestation; publishOnchain: boolean }

const PUBLICATION_ID = /^btr_pub_[a-z2-7]{26}$/

export const parseRoomRequest = (envelope: Envelope, policy: AttestationPolicy): RoomRequest => {
  if (envelope.type !== 'message' || envelope.text === null) return { kind: 'ignore' }
  let body: unknown
  try {
    body = JSON.parse(envelope.text)
  } catch {
    return { kind: 'ignore' }
  }
  if (!isRecord(body) || typeof body.op !== 'string') return { kind: 'ignore' }

  const publishOnchain = body.publish_onchain ?? false
  if (typeof publishOnchain !== 'boolean') return refuse('invalid_request', '`publish_onchain` must be true or false.')

  switch (body.op) {
    case 'attest_room_publication': {
      if (!hasOnly(body, ['op', 'channel', 'publication', 'publish_onchain'])) {
        return refuse('invalid_request', 'attest_room_publication takes `channel`, `publication` and `publish_onchain`.')
      }
      const { channel, publication } = body
      if (typeof channel !== 'string' || typeof publication !== 'string' || !PUBLICATION_ID.test(publication)) {
        return refuse('invalid_request', 'Name the object `channel` and the `publication` id (btr_pub_…).')
      }
      if (!policy.rooms[envelope.room_id]?.channels.includes(channel)) {
        return refuse('channel_not_attestable', 'This bridge does not attest publications of that channel in this Room.')
      }
      return {
        kind: 'attest',
        attestation: { op: 'attest_room_publication', room: envelope.room_id, channel, publication },
        publishOnchain,
      }
    }
    case 'attest_transfer': {
      if (!hasOnly(body, ['op', 'transfer_id', 'publish_onchain'])) {
        return refuse('invalid_request', 'attest_transfer takes `transfer_id` and `publish_onchain`.')
      }
      if (!policy.allowTransferAttestations) {
        return refuse('transfer_attestations_disabled', 'This bridge does not attest standard transfers.')
      }
      let transferId: string
      try {
        transferId = normalizeTransferId(typeof body.transfer_id === 'string' ? body.transfer_id : '')
      } catch {
        return refuse('invalid_request', '`transfer_id` must be a transfer UUID.')
      }
      return { kind: 'attest', attestation: { op: 'attest_transfer', transferId }, publishOnchain }
    }
    case 'submit_result_for_attestation':
      return refuse(
        'not_attestable',
        'The DON attests only what it reads from Beam. Publish the result as a Room transfer, then ask attest_room_publication.',
      )
    default:
      return refuse(
        'not_an_attestation',
        'This bridge only forwards attest_room_publication and attest_transfer. Other Room messages need no Chainlink verification.',
      )
  }
}

const refuse = (reason: string, message: string): RoomRequest => ({ kind: 'refuse', reason, message })

const hasOnly = (record: Record<string, unknown>, fields: string[]): boolean =>
  Object.keys(record).every((key) => fields.includes(key))

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
