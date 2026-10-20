import { concatHex, getAddress, type Hex, keccak256, recoverAddress, toHex } from 'viem'
import type { ReportSubmission } from '../workflows/transfer-receipts/attestations'

// CRE reports received over HTTP: their metadata header and the DON signatures over them, checked the way the CRE
// SDK's `Report.parse` checks them.

/** Bytes of the metadata header in front of a report's body. */
export const REPORT_HEADER_BYTES = 109

export type ReportHeader = {
  version: number
  executionId: string
  timestamp: number
  donId: number
  donConfigVersion: number
  workflowId: string
  workflowName: string
  workflowOwner: string
  reportId: string
}

export type SignedReport = { rawReport: Uint8Array; context: Uint8Array; signatures: Uint8Array[] }

const HEX = /^[0-9a-f]*$/

/** A report submission's hex fields as bytes, or null when one is not even-length lowercase or uppercase hex. */
export const parseSubmission = (value: unknown): SignedReport | null => {
  if (typeof value !== 'object' || value === null) return null
  const { report, context, signatures } = value as Partial<ReportSubmission>
  const bytes = (field: unknown): Uint8Array | null => {
    if (typeof field !== 'string' || field.length % 2 !== 0 || !HEX.test(field.toLowerCase())) return null
    return Uint8Array.from(Buffer.from(field, 'hex'))
  }
  const rawReport = bytes(report)
  const reportContext = bytes(context)
  if (!rawReport || !reportContext || !Array.isArray(signatures) || signatures.length > 64) return null
  const sigs = signatures.map(bytes)
  if (sigs.some((sig) => sig === null)) return null
  return { rawReport, context: reportContext, signatures: sigs as Uint8Array[] }
}

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex')

export const parseHeader = (rawReport: Uint8Array): ReportHeader => {
  if (rawReport.length < REPORT_HEADER_BYTES) {
    throw new Error(`The report is ${rawReport.length} bytes, shorter than its ${REPORT_HEADER_BYTES}-byte header`)
  }
  const view = new DataView(rawReport.buffer, rawReport.byteOffset, rawReport.byteLength)
  return {
    version: rawReport[0],
    executionId: hex(rawReport.subarray(1, 33)),
    timestamp: view.getUint32(33),
    donId: view.getUint32(37),
    donConfigVersion: view.getUint32(41),
    workflowId: hex(rawReport.subarray(45, 77)),
    workflowName: hex(rawReport.subarray(77, 87)),
    workflowOwner: hex(rawReport.subarray(87, 107)),
    reportId: hex(rawReport.subarray(107, 109)),
  }
}

export const reportBody = (rawReport: Uint8Array): Hex => toHex(rawReport.subarray(REPORT_HEADER_BYTES))

/** The digest every DON node signs: keccak256(keccak256(rawReport) || reportContext). */
export const reportDigest = (rawReport: Uint8Array, context: Uint8Array): Hex =>
  keccak256(concatHex([keccak256(toHex(rawReport)), toHex(context)]))

/** A DON's fault tolerance `f` and its signers' addresses, lowercase. */
export type SignerSet = { f: number; signers: Set<string> }

/**
 * The distinct DON signers among a report's signatures, once f+1 of them are found. Throws when fewer than
 * f+1 signatures recover to distinct signers of the set.
 */
export const verifyReportSignatures = async (report: SignedReport, set: SignerSet): Promise<string[]> => {
  const required = set.f + 1
  const digest = reportDigest(report.rawReport, report.context)
  const accepted = new Set<string>()
  for (const signature of report.signatures) {
    if (accepted.size === required) break
    if (signature.length !== 65) continue
    const normalized = Uint8Array.from(signature)
    if (normalized[64] >= 27) normalized[64] -= 27
    let signer: string
    try {
      signer = (await recoverAddress({ hash: digest, signature: toHex(normalized) })).toLowerCase()
    } catch {
      continue
    }
    if (set.signers.has(signer)) accepted.add(signer)
  }
  if (accepted.size < required) {
    throw new Error(`The report has ${accepted.size} valid signatures from distinct DON signers; it needs ${required}`)
  }
  return [...accepted]
}

/** `getDON(uint32)` and `getNodesByP2PIds(bytes32[])` of the Capability Registry. */
const GET_DON = '0x23537405'
const GET_NODES_BY_P2P_IDS = '0x05a51966'
/** Head of one `NodeInfo` tuple in `getNodesByP2PIds`; the signer is its fourth word. */
const NODE_INFO_HEAD_BYTES = 288

export type EthCall = (to: Hex, data: Hex) => Promise<Hex>

const word = (data: Uint8Array, offset: number): number => {
  if (offset + 32 > data.length) throw new Error('Capability Registry answer is shorter than expected')
  const value = BigInt(toHex(data.subarray(offset, offset + 32)))
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('Capability Registry answer has an out-of-range word')
  return Number(value)
}

const uint256 = (value: number) => value.toString(16).padStart(64, '0')

/**
 * Reads a DON's `f` and signers from the Capability Registry, with the ABI offsets the CRE SDK reads: `f` is the
 * third word of `DONInfo`, its node P2P ids are the sixth field, and each node's signer is the first 20 bytes
 * of the fourth word of its `NodeInfo`.
 */
export const readDonSigners = async (call: EthCall, registry: Hex, donId: number): Promise<SignerSet> => {
  const don = Buffer.from((await call(registry, `${GET_DON}${uint256(donId)}`)).slice(2), 'hex')
  const f = word(don, 96)
  const idsOffset = 32 + word(don, 192)
  const count = word(don, idsOffset)
  const ids: string[] = []
  for (let i = 0; i < count; i += 1) {
    const start = idsOffset + 32 + i * 32
    if (start + 32 > don.length) throw new Error('Capability Registry answer is shorter than expected')
    ids.push(hex(don.subarray(start, start + 32)))
  }
  const signers = new Set<string>()
  if (ids.length > 0) {
    const nodes = Buffer.from(
      (await call(registry, `${GET_NODES_BY_P2P_IDS}${uint256(32)}${uint256(ids.length)}${ids.join('')}`)).slice(2),
      'hex',
    )
    const outer = word(nodes, 0)
    const returned = word(nodes, outer)
    for (let i = 0; i < returned; i += 1) {
      const tuple = outer + 32 + word(nodes, outer + 32 + i * 32)
      if (tuple + NODE_INFO_HEAD_BYTES > nodes.length) throw new Error('Capability Registry answer is shorter than expected')
      signers.add(getAddress(toHex(nodes.subarray(tuple + 96, tuple + 116))).toLowerCase())
    }
  }
  return { f, signers }
}
