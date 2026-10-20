import { bytesToHex, type Hex } from 'viem'
import {
  type DecodedReport,
  type RequestClosureEntry,
  type RoomReceiptEntry,
  roomReceiptKey,
  roomRequestKey,
  type StandardReceiptEntry,
} from './receipts'
import type { RoomSubject, TransferSubject } from './subjects'
import { normalizeTransferId, transferIdToBytes16 } from './transfer-id'

// What a Room bridge asks the receipts workflow to attest, and the DON-signed reports it gets back. Shared by
// the workflow and the bridge.

export type Attestation =
  | { op: 'attest_transfer'; transferId: string }
  | { op: 'attest_room_publication'; room: string; channel: string; publication: string }

/** The HTTP trigger body a bridge sends. `publishOnchain` also writes the answers to the contract. */
export type AttestationRequest = { attest: Attestation[]; publishOnchain: boolean }

/**
 * Attestations per execution. Each spends two of CRE's 15 HTTP requests: the Beam read, then the delivery of
 * its report to the bridge.
 */
export const MAX_ATTESTATIONS = 7

export const attestationSubject = (attestation: Attestation): TransferSubject | RoomSubject =>
  attestation.op === 'attest_transfer'
    ? { kind: 'transfer', transferId: normalizeTransferId(attestation.transferId), requestId: null }
    : {
        kind: 'room',
        room: attestation.room,
        channel: attestation.channel,
        publication: attestation.publication,
        requestId: null,
      }

/**
 * The body a bridge's results endpoint takes: CRE's offchain report format, every field hex without `0x`. A
 * verifier hashes `report` and `context` and recovers the DON signers from `signatures`.
 */
export type ReportSubmission = { report: string; context: string; signatures: string[] }

export const reportSubmission = (
  rawReport: Uint8Array,
  reportContext: Uint8Array,
  signatures: Uint8Array[],
): ReportSubmission => ({
  report: bytesToHex(rawReport).slice(2),
  context: bytesToHex(reportContext).slice(2),
  signatures: signatures.map((signature) => bytesToHex(signature).slice(2)),
})

/** What one report says about one attestation: its receipt, or that a Room publication ran as a standard transfer. */
export type AttestationAnswer =
  | { kind: 'standard'; entry: StandardReceiptEntry }
  | { kind: 'room'; entry: RoomReceiptEntry }
  | { kind: 'closure'; entry: RequestClosureEntry }

/** The entry of a decoded report that answers an attestation, or null when the report is about something else. */
export const answerFor = (attestation: Attestation, report: DecodedReport): AttestationAnswer | null => {
  if (attestation.op === 'attest_transfer') {
    const transferId = transferIdToBytes16(attestation.transferId)
    const entry = report.standard.find((candidate) => candidate.transferId.toLowerCase() === transferId)
    return entry ? { kind: 'standard', entry } : null
  }
  const { room, channel, publication } = attestation
  const entry = report.rooms.find(
    (candidate) => candidate.room === room && candidate.channel === channel && candidate.publication === publication,
  )
  if (entry) return { kind: 'room', entry }
  const closureKey: Hex = roomRequestKey(roomReceiptKey(room, channel, publication))
  const closure = report.closures.find((candidate) => candidate.key.toLowerCase() === closureKey)
  return closure ? { kind: 'closure', entry: closure } : null
}
