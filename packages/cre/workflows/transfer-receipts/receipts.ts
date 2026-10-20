import {
  decodeAbiParameters,
  encodeAbiParameters,
  type Hex,
  keccak256,
  parseAbi,
  parseAbiParameters,
  toEventSelector,
} from 'viem'

/** The parts of `BeamTransferReceipts` the workflow reads, and the request events it listens to. */
export const receiptsAbi = parseAbi([
  'struct OpenRequest { bytes32 key; uint8 kind; uint8 transferKind; address requester; uint64 openedAt; bytes32 requestId; bytes16 transferId; string room; string channel; string publication; }',
  'function openRequestCount() view returns (uint256)',
  'function openRequests(uint256 offset, uint256 limit) view returns (OpenRequest[] page)',
  'event ReceiptRequested(bytes16 indexed transferId, address indexed requester)',
  'event RoomReceiptRequested(bytes32 indexed roomKey, address indexed requester, string room, string channel, string publication)',
])

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
  keccak256(encodeAbiParameters(parseAbiParameters('string, string, string'), [room, channel, publication]))

/** `BeamTransferReceipts.roomRequestKey`: the open-request key of a `requestRoomReceipt`. */
export const roomRequestKey = (roomKey: Hex): Hex =>
  keccak256(encodeAbiParameters(parseAbiParameters('uint8, bytes32'), [RequestKind.RoomReceipt, roomKey]))

const reportParameters = parseAbiParameters(
  'uint8 version, (bytes16 transferId, bytes32 requestId, uint8 status, uint64 totalBytes, uint64 bytesTransferred, uint64 completedAtMs, bytes32 planFingerprint, bytes32 coordinateChecksum, uint8 level)[] standard, (string room, string channel, string publication, bytes32 requestId, uint8 status, uint64 fileSizeBytes, bytes32 manifestSha256, uint32 targetCount, bytes32 evidenceSha256, bytes32 assignmentsSha256, uint16 evidenceVersion, uint8 level)[] rooms, (bytes32 key, uint8 reason, bytes16 transferId)[] closures, (bytes32 requestId, bytes32 inputRoomKey, bytes32 outputRoomKey, string computeMember)[] jobs',
)

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
