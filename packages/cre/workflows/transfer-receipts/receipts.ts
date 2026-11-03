import { decodeAbiParameters, encodeAbiParameters, type Hex, keccak256, toEventSelector } from 'viem'

// ABIs here are JSON, never viem's human-readable strings: its parser recurses once per character,
// which exhausts the WASM stack on a CRE DON (the simulator's stack is larger). `check.ts` compares
// each one with viem's parse of its Solidity signature.

/** The parts of `BeamTransferReceipts` the workflow reads, and the request events it listens to. */
export const receiptsAbi = [
  { name: 'openRequestCount', type: 'function', stateMutability: 'view', inputs: [], outputs: [{ type: 'uint256' }] },
  {
    name: 'openRequests',
    type: 'function',
    stateMutability: 'view',
    inputs: [
      { type: 'uint256', name: 'offset' },
      { type: 'uint256', name: 'limit' },
    ],
    outputs: [
      {
        type: 'tuple[]',
        name: 'page',
        components: [
          { type: 'bytes32', name: 'key' },
          { type: 'uint8', name: 'kind' },
          { type: 'uint8', name: 'transferKind' },
          { type: 'address', name: 'requester' },
          { type: 'uint64', name: 'openedAt' },
          { type: 'bytes32', name: 'requestId' },
          { type: 'bytes16', name: 'transferId' },
          { type: 'string', name: 'room' },
          { type: 'string', name: 'channel' },
          { type: 'string', name: 'publication' },
        ],
      },
    ],
  },
  {
    name: 'ReceiptRequested',
    type: 'event',
    inputs: [
      { type: 'bytes16', name: 'transferId', indexed: true },
      { type: 'address', name: 'requester', indexed: true },
    ],
  },
  {
    name: 'RoomReceiptRequested',
    type: 'event',
    inputs: [
      { type: 'bytes32', name: 'roomKey', indexed: true },
      { type: 'address', name: 'requester', indexed: true },
      { type: 'string', name: 'room' },
      { type: 'string', name: 'channel' },
      { type: 'string', name: 'publication' },
    ],
  },
] as const

export const RECEIPT_REQUESTED_TOPIC = toEventSelector('ReceiptRequested(bytes16,address)')
export const ROOM_RECEIPT_REQUESTED_TOPIC = toEventSelector(
  'RoomReceiptRequested(bytes32,address,string,string,string)',
)

/** `BeamTransferReceipts.RequestKind`. */
export const RequestKind = { Transfer: 1, Receipt: 2, RoomReceipt: 3 } as const

/** `BeamTransferReceipts.TransferKind`: what a `requestTransfer` asks for. */
export const TransferKind = { Standard: 1, RoomDelivery: 2, Job: 3 } as const

/** `BeamTransferReceipts.Status`. Standard transfers use the first three. */
export const ReceiptStatus = { Completed: 1, Failed: 2, Cancelled: 3, Partial: 4 } as const
export type ReceiptStatusCode = (typeof ReceiptStatus)[keyof typeof ReceiptStatus]

/** Evidence level bits; see the trust levels in the contract and README. */
export const Level = { BeamReported: 0x01, TargetSigned: 0x02, DonAttested: 0x04 } as const

/**
 * The levels a `BeamTransferReceipts` deployment records (`standardReceiptLevel()` and the Room ones): with
 * `DonAttested` when a CRE workflow writes its receipts, without it when a relay does.
 */
export const receiptLevels = (donAttested: boolean) => {
  const attestation = donAttested ? Level.DonAttested : 0
  return {
    standard: Level.BeamReported | attestation,
    room: Level.BeamReported | attestation,
    roomSigned: Level.BeamReported | Level.TargetSigned | attestation,
  }
}

export type ReceiptLevels = ReturnType<typeof receiptLevels>

/** `BeamTransferReceipts.CLOSURE_*`: why a request was settled without a receipt. */
export const ClosureReason = { BucketOnlyPublication: 1 } as const

export const REPORT_VERSION = 2
/** CRE `PerWorkflow.ChainWrite.ReportSizeLimit`. */
export const MAX_REPORT_BYTES = 50 * 1024

export type StandardReceiptEntry = {
  transferId: Hex
  requestId: Hex
  status: number
  totalBytes: bigint
  bytesTransferred: bigint
  completedAtMs: bigint
  planFingerprint: Hex
  coordinateChecksum: Hex
  level: number
}

export type RoomReceiptEntry = {
  room: string
  channel: string
  publication: string
  requestId: Hex
  status: number
  fileSizeBytes: bigint
  manifestSha256: Hex
  targetCount: number
  evidenceSha256: Hex
  assignmentsSha256: Hex
  evidenceVersion: number
  level: number
}

export type RequestClosureEntry = {
  key: Hex
  reason: number
  transferId: Hex
}

/** A job's result: the Room receipts of its input and output, which the same report or an earlier one records. */
export type JobResultEntry = {
  requestId: Hex
  inputRoomKey: Hex
  outputRoomKey: Hex
  computeMember: string
}

/** `BeamTransferReceipts.roomReceiptKey`. */
export const roomReceiptKey = (room: string, channel: string, publication: string): Hex =>
  keccak256(encodeAbiParameters([{ type: 'string' }, { type: 'string' }, { type: 'string' }], [room, channel, publication]))

/** `BeamTransferReceipts.roomRequestKey`: the open-request key of a `requestRoomReceipt`. */
export const roomRequestKey = (roomKey: Hex): Hex =>
  keccak256(encodeAbiParameters([{ type: 'uint8' }, { type: 'bytes32' }], [RequestKind.RoomReceipt, roomKey]))

/** The report's parameters, `_processReport`'s `abi.decode` types. */
export const reportParameters = [
  { type: 'uint8', name: 'version' },
  {
    type: 'tuple[]',
    name: 'standard',
    components: [
      { type: 'bytes16', name: 'transferId' },
      { type: 'bytes32', name: 'requestId' },
      { type: 'uint8', name: 'status' },
      { type: 'uint64', name: 'totalBytes' },
      { type: 'uint64', name: 'bytesTransferred' },
      { type: 'uint64', name: 'completedAtMs' },
      { type: 'bytes32', name: 'planFingerprint' },
      { type: 'bytes32', name: 'coordinateChecksum' },
      { type: 'uint8', name: 'level' },
    ],
  },
  {
    type: 'tuple[]',
    name: 'rooms',
    components: [
      { type: 'string', name: 'room' },
      { type: 'string', name: 'channel' },
      { type: 'string', name: 'publication' },
      { type: 'bytes32', name: 'requestId' },
      { type: 'uint8', name: 'status' },
      { type: 'uint64', name: 'fileSizeBytes' },
      { type: 'bytes32', name: 'manifestSha256' },
      { type: 'uint32', name: 'targetCount' },
      { type: 'bytes32', name: 'evidenceSha256' },
      { type: 'bytes32', name: 'assignmentsSha256' },
      { type: 'uint16', name: 'evidenceVersion' },
      { type: 'uint8', name: 'level' },
    ],
  },
  {
    type: 'tuple[]',
    name: 'closures',
    components: [
      { type: 'bytes32', name: 'key' },
      { type: 'uint8', name: 'reason' },
      { type: 'bytes16', name: 'transferId' },
    ],
  },
  {
    type: 'tuple[]',
    name: 'jobs',
    components: [
      { type: 'bytes32', name: 'requestId' },
      { type: 'bytes32', name: 'inputRoomKey' },
      { type: 'bytes32', name: 'outputRoomKey' },
      { type: 'string', name: 'computeMember' },
    ],
  },
] as const

/** The version 2 report `BeamTransferReceipts._processReport` decodes. */
export const encodeReport = (
  standard: StandardReceiptEntry[],
  rooms: RoomReceiptEntry[],
  closures: RequestClosureEntry[],
  jobs: JobResultEntry[],
): Hex => {
  const report = encodeAbiParameters(reportParameters, [REPORT_VERSION, standard, rooms, closures, jobs])
  const size = (report.length - 2) / 2
  if (size > MAX_REPORT_BYTES) {
    throw new Error(`Report is ${size} bytes, above the ${MAX_REPORT_BYTES}-byte CRE report limit`)
  }
  return report
}

export type DecodedReport = {
  standard: StandardReceiptEntry[]
  rooms: RoomReceiptEntry[]
  closures: RequestClosureEntry[]
  jobs: JobResultEntry[]
}

/** The inverse of `encodeReport`, for readers of a report outside the contract. */
export const decodeReport = (report: Hex): DecodedReport => {
  const [version, standard, rooms, closures, jobs] = decodeAbiParameters(reportParameters, report)
  if (version !== REPORT_VERSION) throw new Error(`Unsupported report version ${version}`)
  return { standard: [...standard], rooms: [...rooms], closures: [...closures], jobs: [...jobs] }
}
