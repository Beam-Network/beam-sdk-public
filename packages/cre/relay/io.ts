import { readFile, rename, writeFile } from 'node:fs/promises'
import { type Address, createPublicClient, createWalletClient, defineChain, type Hex, http, parseAbi } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import {
  normalizeRequestId,
  projectStudioResponse,
  requestsAbi,
  type StudioOutcome,
  webhookBody,
  webhookSignature,
} from '../workflows/start-transfer/studio'
import {
  type Observation,
  projectClientId,
  projectPublicationLookup,
  projectRoomReceipt,
  projectTransfer,
  type PublicationLookup,
} from '../workflows/transfer-receipts/beam'
import { receiptsAbi } from '../workflows/transfer-receipts/receipts'
import {
  type OpenRequest,
  type JobSubject,
  type RoomRequestSubject,
  roomRequestPublicationKey,
  type Subject,
} from '../workflows/transfer-receipts/subjects'
import type { RelayConfig, RelaySecrets } from './config'
import type { RelayIO, TransferRequestLog } from './relay'

/** The parts of a relay deployment the relay calls beyond what the workflows read. */
export const relayAbi = parseAbi([
  'function onReport(bytes metadata, bytes report)',
  'function donAttested() view returns (bool)',
  'function getForwarderAddress() view returns (address)',
])

const OPEN_REQUEST_PAGE = 50n
const HTTP_TIMEOUT_MS = 30_000

export const createRelayIO = (config: RelayConfig, secrets: RelaySecrets, log: (message: string) => void) => {
  const chain = defineChain({
    id: config.chain.chainId,
    name: `chain ${config.chain.chainId}`,
    nativeCurrency: { name: 'native', symbol: 'NATIVE', decimals: 18 },
    rpcUrls: { default: { http: [config.chain.rpcUrl] } },
  })
  const transport = http(config.chain.rpcUrl)
  const publicClient = createPublicClient({ chain, transport })
  const account = privateKeyToAccount(secrets.relayPrivateKey)
  const walletClient = createWalletClient({ chain, transport, account })
  const contract = config.chain.receiptsAddress as Address

  const fetchBytes = async (url: string, init: RequestInit): Promise<{ status: number; body: Uint8Array } | null> => {
    try {
      const response = await fetch(url, { ...init, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) })
      return { status: response.status, body: new Uint8Array(await response.arrayBuffer()) }
    } catch {
      // The Studio URL carries the trigger token, so a network error is never logged with its message.
      return null
    }
  }

  const io: RelayIO = {
    finalizedBlock: async () => (await publicClient.getBlock({ blockTag: 'finalized' })).number,

    transferRequests: async (fromBlock, toBlock) => {
      const logs = await publicClient.getLogs({ address: contract, event: requestsAbi[0], fromBlock, toBlock, strict: true })
      return logs.map(
        (entry): TransferRequestLog => ({
          requestId: normalizeRequestId(entry.args.requestId),
          requester: entry.args.requester,
          kind: entry.args.kind,
          input: entry.args.input,
          blockNumber: entry.blockNumber,
          logIndex: entry.logIndex,
          transactionHash: entry.transactionHash,
        }),
      )
    },

    deliver: async (request, workflow): Promise<StudioOutcome> => {
      const url = workflow === 'room' ? secrets.studioRoomWebhookUrl : secrets.studioWebhookUrl
      const signingSecret = workflow === 'room' ? secrets.studioRoomSigningSecret : secrets.studioSigningSecret
      if (!url) return { state: 'refused', status: 0, reason: 'no Room workflow webhook is configured' }
      const body = webhookBody(request.requestId, request.requester, request.input)
      const timestamp = Math.floor(Date.now() / 1000)
      const headers: Record<string, string> = {
        'content-type': 'application/json',
        'idempotency-key': request.requestId,
      }
      if (signingSecret) {
        headers['x-beam-timestamp'] = String(timestamp)
        headers['x-beam-signature'] = webhookSignature(signingSecret, timestamp, body)
      }
      const response = await fetchBytes(url, { method: 'POST', body, headers })
      return response ? projectStudioResponse(response.status, response.body) : { state: 'unavailable', status: 0 }
    },

    openRequests: async () => {
      const total = await publicClient.readContract({ address: contract, abi: receiptsAbi, functionName: 'openRequestCount' })
      const requests: OpenRequest[] = []
      for (let offset = 0n; offset < total; offset += OPEN_REQUEST_PAGE) {
        const page = await publicClient.readContract({
          address: contract,
          abi: receiptsAbi,
          functionName: 'openRequests',
          args: [offset, OPEN_REQUEST_PAGE],
        })
        requests.push(...page)
      }
      return requests
    },

    findPublication: async (subject: RoomRequestSubject): Promise<PublicationLookup> => {
      if (!secrets.beamOrgId) {
        return { state: 'invalid', reason: 'BEAM_ORG_ID is not set, so Room publications cannot be looked up' }
      }
      const path = `studio/v1/rooms/${encodeURIComponent(subject.room)}/channels/${encodeURIComponent(subject.channel)}/publications/by-key`
      const query = `publisher=${encodeURIComponent(subject.publisherMemberId)}&key=${encodeURIComponent(roomRequestPublicationKey(subject.requestId))}`
      const response = await fetchBytes(`${config.roomApiBaseUrl}/${path}?${query}`, {
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${secrets.beamApiKey}`,
          'x-beam-organization-id': secrets.beamOrgId,
        },
      })
      return response ? projectPublicationLookup(response.status, response.body) : { state: 'unavailable' }
    },

    observe: async (subject: Exclude<Subject, RoomRequestSubject | JobSubject>): Promise<Observation> => {
      if (subject.kind === 'transfer') {
        const response = await fetchBytes(`${config.beamApiBaseUrl}/transfers/${subject.transferId}`, {
          headers: { accept: 'application/json', 'x-api-key': secrets.beamApiKey },
        })
        return response ? projectTransfer(response.status, response.body, subject.transferId) : { state: 'unavailable' }
      }
      if (!secrets.beamOrgId) return { state: 'invalid', reason: 'BEAM_ORG_ID is not set, so Room receipts cannot be read' }
      const path = ['studio/v1/rooms', subject.room, 'channels', subject.channel, 'objects', subject.publication, 'receipt']
        .map((part, index) => (index % 2 === 1 ? encodeURIComponent(part) : part))
        .join('/')
      const response = await fetchBytes(`${config.roomApiBaseUrl}/${path}`, {
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${secrets.beamApiKey}`,
          'x-beam-organization-id': secrets.beamOrgId,
        },
      })
      return response ? projectRoomReceipt(response.status, response.body, subject) : { state: 'unavailable' }
    },

    writeReport: async (report: Hex) => {
      const hash = await walletClient.writeContract({
        address: contract,
        abi: relayAbi,
        functionName: 'onReport',
        args: ['0x', report],
      })
      const receipt = await publicClient.waitForTransactionReceipt({ hash })
      if (receipt.status !== 'success') throw new Error(`onReport reverted in ${hash}`)
      return hash
    },

    readCursor: async () => {
      try {
        const saved = JSON.parse(await readFile(config.cursorPath, 'utf8')) as { nextBlock?: string }
        return saved.nextBlock === undefined ? null : BigInt(saved.nextBlock)
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null
        throw error
      }
    },

    saveCursor: async (nextBlock) => {
      const temporary = `${config.cursorPath}.tmp`
      await writeFile(temporary, `${JSON.stringify({ nextBlock: nextBlock.toString() })}\n`)
      await rename(temporary, config.cursorPath)
    },

    log,
  }

  /**
   * Refuses to start against the wrong chain, a CRE deployment, a contract that would reject the relay's
   * reports, or a Beam key of another account, whose transfer ids would never match the requests.
   */
  const preflight = async () => {
    const chainId = await publicClient.getChainId()
    if (chainId !== config.chain.chainId) throw new Error(`RPC is chain ${chainId}, config says ${config.chain.chainId}`)
    const code = await publicClient.getCode({ address: contract })
    if (!code || code === '0x') throw new Error(`No contract at ${contract}`)
    if (await publicClient.readContract({ address: contract, abi: relayAbi, functionName: 'donAttested' })) {
      throw new Error(`${contract} is a CRE deployment (donAttested); a relay writes only to relay deployments`)
    }
    const forwarder = await publicClient.readContract({ address: contract, abi: relayAbi, functionName: 'getForwarderAddress' })
    if (forwarder.toLowerCase() !== account.address.toLowerCase()) {
      throw new Error(`${contract} accepts reports from ${forwarder}, not from the relay account ${account.address}`)
    }
    const response = await fetchBytes(`${config.beamApiBaseUrl}/auth/me`, {
      headers: { accept: 'application/json', 'x-api-key': secrets.beamApiKey },
    })
    if (!response) throw new Error('GET /auth/me is unreachable')
    const identity = projectClientId(response.status, response.body)
    if (identity.userId !== config.beamClientId) {
      throw new Error(
        identity.userId
          ? 'beamClientId does not match GET /auth/me for BEAM_API_KEY'
          : `GET /auth/me did not confirm the client id (HTTP ${identity.httpStatus})`,
      )
    }
    const balance = await publicClient.getBalance({ address: account.address })
    log(`relay ${account.address} on chain ${chainId}, contract ${contract}, balance ${balance} wei`)
  }

  return { io, preflight }
}
