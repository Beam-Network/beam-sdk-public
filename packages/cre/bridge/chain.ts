import {
  createPublicClient,
  createWalletClient,
  defineChain,
  type Hex,
  http,
  parseAbi,
  parseAbiItem,
  type PublicClient,
} from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import type { Attestation, AttestationAnswer } from '../workflows/transfer-receipts/attestations'
import { ZERO_BYTES32 } from '../workflows/transfer-receipts/beam'
import { roomReceiptKey, roomRequestKey } from '../workflows/transfer-receipts/receipts'
import { transferIdToBytes16 } from '../workflows/transfer-receipts/transfer-id'
import type { ChainAnswer, ChainIO } from './bridge'
import type { ContractAnswers } from './config'

// A `BeamTransferReceipts` contract as the bridge and `verify.ts` read it, and how the bridge opens requests on it.

const receiptsAbi = parseAbi([
  'struct StandardReceipt { bytes32 requestId; bytes32 planFingerprint; bytes32 coordinateChecksum; uint64 totalBytes; uint64 bytesTransferred; uint64 completedAtMs; uint8 status; uint8 level; }',
  'struct RoomReceipt { bytes32 manifestSha256; bytes32 evidenceSha256; bytes32 assignmentsSha256; uint64 fileSizeBytes; uint32 targetCount; uint16 evidenceVersion; uint8 status; uint8 level; }',
  'function getReceipt(bytes16 transferId) view returns (StandardReceipt)',
  'function getRoomReceipt(bytes32 roomKey) view returns (RoomReceipt)',
  'function requestReceipt(bytes16 transferId)',
  'function requestRoomReceipt(string room, string channel, string publication)',
  'function isRequester(address account) view returns (bool)',
  'function owner() view returns (address)',
  'function donAttested() view returns (bool)',
])

const RECEIPT_RECORDED = parseAbiItem(
  'event ReceiptRecorded(bytes16 indexed transferId, bytes32 indexed requestId, uint8 status, uint64 totalBytes, uint64 bytesTransferred, uint8 level)',
)
const ROOM_RECEIPT_RECORDED = parseAbiItem(
  'event RoomReceiptRecorded(bytes32 indexed roomKey, bytes32 indexed requestId, string room, string channel, string publication, uint8 status, uint64 fileSizeBytes, bytes32 evidenceSha256, uint8 level)',
)
const REQUEST_CLOSED = parseAbiItem('event RequestClosed(bytes32 indexed key, uint8 reason, bytes16 transferId)')

/** A receipt as the contract stores it, for the attestation that asked about it. */
export type ReceiptAnswer = Extract<AttestationAnswer, { kind: 'standard' | 'room' }>

/** The receipt the contract holds for an attestation, or null while it holds none. Closures need a log search. */
export const readReceipt = async (client: PublicClient, contract: Hex, attestation: Attestation): Promise<ReceiptAnswer | null> => {
  if (attestation.op === 'attest_transfer') {
    const transferId = transferIdToBytes16(attestation.transferId)
    const receipt = await client.readContract({ address: contract, abi: receiptsAbi, functionName: 'getReceipt', args: [transferId] })
    if (receipt.status === 0) return null
    return {
      kind: 'standard',
      entry: {
        transferId,
        requestId: receipt.requestId,
        status: receipt.status,
        totalBytes: receipt.totalBytes,
        bytesTransferred: receipt.bytesTransferred,
        completedAtMs: receipt.completedAtMs,
        planFingerprint: receipt.planFingerprint,
        coordinateChecksum: receipt.coordinateChecksum,
        level: receipt.level,
      },
    }
  }
  const { room, channel, publication } = attestation
  const receipt = await client.readContract({
    address: contract,
    abi: receiptsAbi,
    functionName: 'getRoomReceipt',
    args: [roomReceiptKey(room, channel, publication)],
  })
  if (receipt.status === 0) return null
  return {
    kind: 'room',
    entry: {
      room,
      channel,
      publication,
      requestId: ZERO_BYTES32,
      status: receipt.status,
      fileSizeBytes: receipt.fileSizeBytes,
      manifestSha256: receipt.manifestSha256,
      targetCount: receipt.targetCount,
      evidenceSha256: receipt.evidenceSha256,
      assignmentsSha256: receipt.assignmentsSha256,
      evidenceVersion: receipt.evidenceVersion,
      level: receipt.level,
    },
  }
}

export type ContractLink = { io: ChainIO; account: Hex }

/**
 * Connects to the contract and checks it before the bridge starts: the chain id, that a contract answers at the
 * address, and that the bridge's account may open requests on it.
 */
export const connectContract = async (chain: ContractAnswers['chain'], privateKey: Hex): Promise<ContractLink> => {
  const network = defineChain({
    id: chain.chainId,
    name: `chain ${chain.chainId}`,
    nativeCurrency: { name: 'native', symbol: 'ETH', decimals: 18 },
    rpcUrls: { default: { http: [chain.rpcUrl] } },
  })
  const transport = http(chain.rpcUrl, { timeout: 20_000 })
  const client = createPublicClient({ chain: network, transport }) as PublicClient
  const account = privateKeyToAccount(privateKey)
  const wallet = createWalletClient({ chain: network, transport, account })
  const contract = chain.receiptsAddress as Hex

  const chainId = await client.getChainId()
  if (chainId !== chain.chainId) throw new Error(`The RPC serves chain ${chainId}, not ${chain.chainId}`)
  if (!(await client.getCode({ address: contract }))) throw new Error(`No contract at ${contract}`)
  const [donAttested, owner, requester] = await Promise.all([
    client.readContract({ address: contract, abi: receiptsAbi, functionName: 'donAttested' }),
    client.readContract({ address: contract, abi: receiptsAbi, functionName: 'owner' }),
    client.readContract({ address: contract, abi: receiptsAbi, functionName: 'isRequester', args: [account.address] }),
  ])
  if (!requester && owner.toLowerCase() !== account.address.toLowerCase()) {
    throw new Error(`${account.address} may not open requests on ${contract}; the owner must call setRequester`)
  }

  /** The first log of `event` matching `args` from block `since`, searched in windows of `logRangeBlocks`. */
  const firstLog = async (
    event: typeof RECEIPT_RECORDED | typeof ROOM_RECEIPT_RECORDED | typeof REQUEST_CLOSED,
    args: Record<string, Hex>,
    since: bigint,
  ) => {
    const latest = await client.getBlockNumber()
    for (let from = since; from <= latest; from += BigInt(chain.logRangeBlocks)) {
      const to = from + BigInt(chain.logRangeBlocks) - 1n < latest ? from + BigInt(chain.logRangeBlocks) - 1n : latest
      const logs = await client.getLogs({ address: contract, event, args, fromBlock: from, toBlock: to } as Parameters<
        typeof client.getLogs
      >[0])
      if (logs.length > 0) return logs[0]
    }
    return null
  }

  const io: ChainIO = {
    deployment: { chainId: chain.chainId, contract, donAttested },
    lookup: async (attestation, since): Promise<ChainAnswer | null> => {
      const answer = await readReceipt(client, contract, attestation)
      if (answer) {
        if (since === null) return { answer, transaction: null, block: null }
        const log =
          answer.kind === 'standard'
            ? await firstLog(RECEIPT_RECORDED, { transferId: answer.entry.transferId }, since)
            : await firstLog(
                ROOM_RECEIPT_RECORDED,
                { roomKey: roomReceiptKey(answer.entry.room, answer.entry.channel, answer.entry.publication) },
                since,
              )
        return { answer, transaction: log?.transactionHash ?? null, block: log?.blockNumber ?? null }
      }
      // A Room publication that ran as a standard transfer is settled by closing its request, not by a receipt.
      if (attestation.op !== 'attest_room_publication' || since === null) return null
      const key = roomRequestKey(roomReceiptKey(attestation.room, attestation.channel, attestation.publication))
      const closed = await firstLog(REQUEST_CLOSED, { key }, since)
      if (!closed) return null
      const { reason, transferId } = (closed as unknown as { args: { reason: number; transferId: Hex } }).args
      return {
        answer: { kind: 'closure', entry: { key, reason, transferId } },
        transaction: closed.transactionHash,
        block: closed.blockNumber,
      }
    },
    request: async (attestation) => {
      const hash =
        attestation.op === 'attest_transfer'
          ? await wallet.writeContract({
              address: contract,
              abi: receiptsAbi,
              functionName: 'requestReceipt',
              args: [transferIdToBytes16(attestation.transferId)],
            })
          : await wallet.writeContract({
              address: contract,
              abi: receiptsAbi,
              functionName: 'requestRoomReceipt',
              args: [attestation.room, attestation.channel, attestation.publication],
            })
      const mined = await client.waitForTransactionReceipt({ hash, timeout: 120_000 })
      if (mined.status !== 'success') throw new Error(`The request transaction ${hash} reverted`)
      return { transaction: hash, block: mined.blockNumber }
    },
  }
  return { io, account: account.address }
}
