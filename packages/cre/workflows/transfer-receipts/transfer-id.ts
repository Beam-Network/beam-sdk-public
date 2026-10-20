import { sha256 } from '@noble/hashes/sha2.js'
import { bytesToHex, utf8ToBytes } from '@noble/hashes/utils.js'
import type { Hex } from 'viem'

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const BYTES16_PATTERN = /^0x[0-9a-f]{32}$/
const BYTES32_PATTERN = /^0x[0-9a-f]{64}$/

/**
 * The transfer id Beam assigns to a transfer created with `idempotencyKey` by the client `clientId`.
 *
 * Same derivation as `transferIdForIdempotencyKey` in the Beam TypeScript SDK: the first 16 bytes of
 * sha256("beam-transfer:<clientId>:<idempotencyKey>"), with the UUID version nibble set to 5 and the
 * RFC 4122 variant bits, formatted as a lowercase UUID.
 */
export const transferIdForIdempotencyKey = (clientId: string, idempotencyKey: string): string => {
  const key = idempotencyKey.trim()
  if (!clientId || !key) {
    throw new Error('A client id and an idempotency key are required to derive a transfer id')
  }
  const bytes = sha256(utf8ToBytes(`beam-transfer:${clientId}:${key}`)).slice(0, 16)
  bytes[6] = (bytes[6] & 0x0f) | 0x50
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = bytesToHex(bytes)
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

/**
 * The idempotency key a `requestTransfer(requestId, input)` transfer is created with: the request id as
 * lowercase 0x-prefixed hex. The workflow that starts the transfer must use this exact string.
 */
export const idempotencyKeyForRequest = (requestId: Hex): string => normalizeRequestId(requestId)

export const transferIdForRequest = (clientId: string, requestId: Hex): string =>
  transferIdForIdempotencyKey(clientId, idempotencyKeyForRequest(requestId))

export const normalizeRequestId = (requestId: string): Hex => {
  const value = requestId.toLowerCase()
  if (!BYTES32_PATTERN.test(value) || /^0x0+$/.test(value)) {
    throw new Error(`Invalid request id: ${requestId}`)
  }
  return value as Hex
}

export const normalizeTransferId = (transferId: string): string => {
  const value = transferId.toLowerCase()
  if (!UUID_PATTERN.test(value)) {
    throw new Error(`Invalid transfer id: ${transferId}`)
  }
  return value
}

export const transferIdToBytes16 = (transferId: string): Hex =>
  `0x${normalizeTransferId(transferId).replace(/-/g, '')}` as Hex

export const bytes16ToTransferId = (value: Hex): string => {
  const hex = value.toLowerCase()
  if (!BYTES16_PATTERN.test(hex)) {
    throw new Error(`Invalid bytes16 transfer id: ${value}`)
  }
  const h = hex.slice(2)
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}
