import { sha256 } from '@noble/hashes/sha2.js'
import { bytesToHex, concatBytes, utf8ToBytes } from '@noble/hashes/utils.js'
import type { Hex } from 'viem'
import { ReceiptStatus, type ReceiptStatusCode } from './receipts'

/**
 * What one node saw when it asked Beam about one subject. Every node builds it from the same fixed
 * fields, serialized in a fixed order, so honest nodes produce byte-identical strings and the DON can
 * reach identical consensus on them.
 */
export type Observation =
  | { state: 'unavailable' }
  | { state: 'absent' }
  | { state: 'pending' }
  | { state: 'error'; httpStatus: number }
  | { state: 'invalid'; reason: string }
  /** A bucket-only publication: no Room receipt; `transferId` is its standard transfer, or '' if Beam gave none. */
  | { state: 'not_room'; transferId: string }
  | {
      state: 'transfer'
      status: ReceiptStatusCode
      totalBytes: string
      bytesTransferred: string
      completedAtMs: string
      planFingerprint: Hex
      coordinateChecksum: Hex
    }
  | {
      state: 'room'
      status: ReceiptStatusCode
      fileSizeBytes: string
      manifestSha256: Hex
      targetCount: number
      targetSigned: boolean
      evidenceSha256: Hex
      assignmentsSha256: Hex
      evidenceVersion: number
    }

/** The value consensus falls back to when nodes do not agree, for example while a transfer is finishing. */
export const UNAVAILABLE = JSON.stringify({ state: 'unavailable' } satisfies Observation)

export const ZERO_BYTES32 = `0x${'0'.repeat(64)}` as Hex
export const ZERO_BYTES16 = `0x${'0'.repeat(32)}` as Hex
const MAX_UINT64 = (1n << 64n) - 1n
const SHA256_HEX = /^[0-9a-f]{64}$/
const DECIMAL = /^(0|[1-9][0-9]*)$/
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/

/** Statuses of a finished standard transfer, as `GET /transfers/:id` reports them. */
const STANDARD_TERMINAL = new Map<string, ReceiptStatusCode>([
  ['completed', ReceiptStatus.Completed],
  ['failed', ReceiptStatus.Failed],
  ['cancelled', ReceiptStatus.Cancelled],
])

/** Statuses of a finished Room transfer in a `beam-room-receipt-evidence/v1` envelope. */
const ROOM_TERMINAL = new Map<string, ReceiptStatusCode>([
  ['completed', ReceiptStatus.Completed],
  ['partial', ReceiptStatus.Partial],
  ['failed', ReceiptStatus.Failed],
  ['cancelled', ReceiptStatus.Cancelled],
])

type RoomEvidenceVersion = {
  version: number
  /** Domain tag of `evidenceSha256`. */
  domain: Uint8Array
  /** Every field the envelope and each target carry, and no other, sorted. */
  envelopeFields: readonly string[]
  targetFields: readonly string[]
}

/** Room evidence versions this workflow can read. */
const ROOM_EVIDENCE_VERSIONS = new Map<string, RoomEvidenceVersion>([
  [
    'beam-room-receipt-evidence/v1',
    {
      version: 1,
      domain: utf8ToBytes('beam:room-receipt-evidence:v1\u0000'),
      envelopeFields: [
        'assignment_count',
        'assignments_sha256',
        'channel_id',
        'chunk_count',
        'chunk_size_bytes',
        'evidence_version',
        'file_size_bytes',
        'manifest_sha256',
        'protection',
        'publication_id',
        'room_id',
        'status',
        'targets',
        'transfer_id',
      ],
      targetFields: ['final_receipt', 'kind', 'member_id', 'storage_finalization', 'verification_basis'],
    },
  ],
])

/** What the coordinator's `publications/by-key` lookup answered for one publisher and publication key. */
export type PublicationLookup =
  | { state: 'unavailable' }
  | { state: 'absent' }
  | { state: 'error'; httpStatus: number }
  | { state: 'invalid'; reason: string }
  | { state: 'found'; publicationId: string }

const PUBLICATION_ID = /^btr_pub_[a-z2-7]{26}$/

/** `GET …/publications/by-key`: 404 means no preflight of that publisher used the key yet. */
export const projectPublicationLookup = (statusCode: number, body: Uint8Array): PublicationLookup => {
  if (statusCode === 404) return { state: 'absent' }
  if (statusCode !== 200) return { state: 'error', httpStatus: statusCode }
  const reply = parseJson(body)
  const publicationId = isRecord(reply) ? reply.publication_id : undefined
  if (typeof publicationId !== 'string' || !PUBLICATION_ID.test(publicationId)) {
    return { state: 'invalid', reason: 'publication_id is not a publication id' }
  }
  return { state: 'found', publicationId }
}

/** `GET /auth/me`: the `user_id` behind the API key, which is the client id transfer ids derive from. */
export const projectClientId = (statusCode: number, body: Uint8Array): { userId: string | null; httpStatus: number } => {
  if (statusCode !== 200) return { userId: null, httpStatus: statusCode }
  const identity = parseJson(body)
  const userId = isRecord(identity) && typeof identity.user_id === 'string' ? identity.user_id : null
  return { userId, httpStatus: statusCode }
}

export const projectTransfer = (statusCode: number, body: Uint8Array, transferId: string): Observation => {
  if (statusCode === 404) return { state: 'absent' }
  if (statusCode !== 200) return { state: 'error', httpStatus: statusCode }

  const transfer = parseJson(body)
  if (!isRecord(transfer)) return invalid('body is not a JSON object')
  if (transfer.id !== transferId) return invalid('id does not match the requested transfer')
  const status = typeof transfer.status === 'string' ? STANDARD_TERMINAL.get(transfer.status) : undefined
  if (status === undefined) return { state: 'pending' }

  const totalBytes = safeUint(transfer.total_bytes)
  const bytesTransferred = safeUint(transfer.bytes_transferred)
  if (totalBytes === null || bytesTransferred === null) return invalid('byte counts are not safe integers')
  const completedAtMs = timestampMs(transfer.completed_at)
  if (completedAtMs === null) return invalid('completed_at is not an ISO 8601 timestamp')
  const metadata = isRecord(transfer.metadata) ? transfer.metadata : {}

  return {
    state: 'transfer',
    status,
    totalBytes,
    bytesTransferred,
    completedAtMs,
    planFingerprint: sha256HexOrZero(metadata.plan_fingerprint),
    coordinateChecksum: sha256HexOrZero(metadata.coordinate_checksum),
  }
}

export const projectRoomReceipt = (
  statusCode: number,
  body: Uint8Array,
  lookup: { room: string; channel: string; publication: string },
): Observation => {
  if (statusCode === 409) return projectRoomConflict(body)
  if (statusCode === 404) return { state: 'absent' }
  if (statusCode !== 200) return { state: 'error', httpStatus: statusCode }

  const envelope = parseJson(body)
  if (!isRecord(envelope)) return invalid('body is not a JSON object')
  const evidence =
    typeof envelope.evidence_version === 'string' ? ROOM_EVIDENCE_VERSIONS.get(envelope.evidence_version) : undefined
  if (!evidence) return invalid('unsupported evidence_version')
  if (!hasExactly(envelope, evidence.envelopeFields)) return invalid('envelope fields do not match the evidence version')
  if (
    envelope.room_id !== lookup.room ||
    envelope.channel_id !== lookup.channel ||
    envelope.publication_id !== lookup.publication
  ) {
    return invalid('envelope does not match the requested publication')
  }
  const status = typeof envelope.status === 'string' ? ROOM_TERMINAL.get(envelope.status) : undefined
  if (status === undefined) return invalid('status is not terminal')

  const fileSizeBytes = uint64String(envelope.file_size_bytes)
  if (fileSizeBytes === null) return invalid('file_size_bytes is not a uint64 decimal string')
  const manifestSha256 = envelope.manifest_sha256 === null ? ZERO_BYTES32 : sha256Hex(envelope.manifest_sha256)
  const assignmentsSha256 = sha256Hex(envelope.assignments_sha256)
  if (manifestSha256 === null || assignmentsSha256 === null) return invalid('a sha256 field is not lowercase hex')
  if (!Array.isArray(envelope.targets)) return invalid('targets is not a list')
  const targets: unknown[] = envelope.targets
  if (!targets.every((target) => isRecord(target) && hasExactly(target, evidence.targetFields))) {
    return invalid('target fields do not match the evidence version')
  }
  const kinds = targets.map((target) => (target as Record<string, unknown>).kind)
  if (!kinds.every((kind) => kind === 'agent' || kind === 'object_storage')) return invalid('unknown target kind')

  return {
    state: 'room',
    status,
    fileSizeBytes,
    manifestSha256,
    targetCount: targets.length,
    // L2 needs a target-signed final receipt from every target. A storage target is finalized by its provider
    // and reported by Beam (L1), so any storage target keeps the receipt at L1 + L3.
    targetSigned:
      targets.length > 0 &&
      targets.every((target) => {
        const t = target as Record<string, unknown>
        return t.kind === 'agent' && isRecord(t.final_receipt)
      }),
    evidenceSha256: `0x${bytesToHex(sha256(concatBytes(evidence.domain, body)))}`,
    assignmentsSha256,
    evidenceVersion: evidence.version,
  }
}

/**
 * `409 not_terminal` is a Room transfer still running. `409 bucket_only_publication` is a publication that ran as
 * a standard transfer: it has no Room receipt, and its standard `transfer_id` is the one to ask about instead.
 */
const projectRoomConflict = (body: Uint8Array): Observation => {
  const error = parseJson(body)
  const code = isRecord(error) ? error.code : undefined
  if (code === 'not_terminal') return { state: 'pending' }
  if (code === 'bucket_only_publication') {
    const transferId = isRecord(error) && typeof error.transfer_id === 'string' ? error.transfer_id.toLowerCase() : ''
    return { state: 'not_room', transferId: UUID.test(transferId) ? transferId : '' }
  }
  return { state: 'error', httpStatus: 409 }
}

const invalid = (reason: string): Observation => ({ state: 'invalid', reason })

const parseJson = (body: Uint8Array): unknown => {
  try {
    return JSON.parse(new TextDecoder().decode(body))
  } catch {
    return undefined
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const safeUint = (value: unknown): string | null =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? String(value) : null

const uint64String = (value: unknown): string | null =>
  typeof value === 'string' && DECIMAL.test(value) && BigInt(value) <= MAX_UINT64 ? value : null

const timestampMs = (value: unknown): string | null => {
  if (value === null || value === undefined) return '0'
  if (typeof value !== 'string' || !ISO_TIMESTAMP.test(value)) return null
  const ms = Date.parse(value)
  return Number.isSafeInteger(ms) && ms >= 0 ? String(ms) : null
}

const sha256Hex = (value: unknown): Hex | null =>
  typeof value === 'string' && SHA256_HEX.test(value) ? `0x${value}` : null

const sha256HexOrZero = (value: unknown): Hex => sha256Hex(value) ?? ZERO_BYTES32

/** `fields` is sorted, as JCS orders object members. */
const hasExactly = (record: Record<string, unknown>, fields: readonly string[]): boolean => {
  const keys = Object.keys(record).sort()
  return keys.length === fields.length && keys.every((key, index) => key === fields[index])
}
