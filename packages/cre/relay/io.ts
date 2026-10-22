import { readFile, rename, writeFile } from 'node:fs/promises'
import { type Address, createPublicClient, createWalletClient, defineChain, type Hex, http, parseAbi, parseEventLogs } from 'viem'
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
import { bytes16ToTransferId } from '../workflows/transfer-receipts/transfer-id'
import {
  type OpenRequest,
  type JobSubject,
  type RoomRequestSubject,
  roomRequestPublicationKey,
  type Subject,
} from '../workflows/transfer-receipts/subjects'
import type { RelayConfig, RelaySecrets } from './config'
import {
  type ChargeSubject,
  type Customer,
  type ListedTransfer,
  projectCharge,
  projectListedTransfer,
  type Reader,
  registrySchema,
  sealedKeyContext,
  toCustomer,
} from './customers'
import { recordedKey, type RelayIO, type TransferRequestLog } from './relay'
import { openSealed } from './sealed-box'

/** The parts of a relay deployment the relay calls beyond what the workflows read. */
export const relayAbi = parseAbi([
  'function onReport(bytes metadata, bytes report)',
  'function donAttested() view returns (bool)',
  'function getForwarderAddress() view returns (address)',
  'function owner() view returns (address)',
  'function setRequester(address account, bool allowed)',
  'function isRequester(address account) view returns (bool)',
  'event ReceiptRecorded(bytes16 indexed transferId, bytes32 indexed requestId, uint8 status, uint64 totalBytes, uint64 bytesTransferred, uint8 level)',
  'event RoomReceiptRecorded(bytes32 indexed roomKey, bytes32 indexed requestId, string room, string channel, string publication, uint8 status, uint64 fileSizeBytes, bytes32 evidenceSha256, uint8 level)',
])

const OPEN_REQUEST_PAGE = 50n
const HTTP_TIMEOUT_MS = 30_000
/** Transfers per page of `GET /transfers`, its maximum, and the most pages read per organization and pass. */
const TRANSFER_PAGE = 100
const MAX_TRANSFER_PAGES = 10

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

  /** Beam's own key and organization, or a customer's. */
  const readerKey = (reader: Reader) =>
    reader ? { apiKey: reader.apiKey, organizationId: reader.organizationId } : { apiKey: secrets.beamApiKey, organizationId: secrets.beamOrgId }

  /** A call to the website's relay routes, authenticated by the relay's own secret. */
  const registry = (method: 'GET' | 'POST', path: string, body?: unknown) =>
    fetchBytes(`${config.customers?.registryUrl ?? ''}/api/onchain-receipts/${path}`, {
      method,
      headers: {
        accept: 'application/json',
        'x-onchain-relay-secret': secrets.onchainRelaySecret ?? '',
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    })

  const chargeReceipt = (subject: ChargeSubject) => ({ chainId: config.chain.chainId, contract, subject })

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
      if (!url) return { state: 'refused', status: 0, reason: `no ${workflow} workflow webhook is configured` }
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

    findPublication: async (subject: RoomRequestSubject, reader: Reader): Promise<PublicationLookup> => {
      const { apiKey, organizationId } = readerKey(reader)
      if (!organizationId) {
        return { state: 'invalid', reason: 'BEAM_ORG_ID is not set, so Room publications cannot be looked up' }
      }
      const path = `studio/v1/rooms/${encodeURIComponent(subject.room)}/channels/${encodeURIComponent(subject.channel)}/publications/by-key`
      const query = `publisher=${encodeURIComponent(subject.publisherMemberId)}&key=${encodeURIComponent(roomRequestPublicationKey(subject.requestId))}`
      const response = await fetchBytes(`${config.roomApiBaseUrl}/${path}?${query}`, {
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${apiKey}`,
          'x-beam-organization-id': organizationId,
        },
      })
      return response ? projectPublicationLookup(response.status, response.body) : { state: 'unavailable' }
    },

    observe: async (subject: Exclude<Subject, RoomRequestSubject | JobSubject>, reader: Reader): Promise<Observation> => {
      const { apiKey, organizationId } = readerKey(reader)
      if (subject.kind === 'transfer') {
        const response = await fetchBytes(`${config.beamApiBaseUrl}/transfers/${subject.transferId}`, {
          headers: { accept: 'application/json', 'x-api-key': apiKey },
        })
        return response ? projectTransfer(response.status, response.body, subject.transferId) : { state: 'unavailable' }
      }
      if (!organizationId) return { state: 'invalid', reason: 'BEAM_ORG_ID is not set, so Room receipts cannot be read' }
      const path = ['studio/v1/rooms', subject.room, 'channels', subject.channel, 'objects', subject.publication, 'receipt']
        .map((part, index) => (index % 2 === 1 ? encodeURIComponent(part) : part))
        .join('/')
      const response = await fetchBytes(`${config.roomApiBaseUrl}/${path}`, {
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${apiKey}`,
          'x-beam-organization-id': organizationId,
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
      const recorded = new Set<string>()
      for (const event of parseEventLogs({ abi: relayAbi, logs: receipt.logs, strict: true })) {
        if (event.address.toLowerCase() !== contract.toLowerCase()) continue
        if (event.eventName === 'ReceiptRecorded') {
          recorded.add(recordedKey({ kind: 'transfer', transferId: bytes16ToTransferId(event.args.transferId) }))
        }
        if (event.eventName === 'RoomReceiptRecorded') recorded.add(recordedKey({ kind: 'room', roomKey: event.args.roomKey }))
      }
      return { hash, recorded }
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

    customers: async (): Promise<Customer[]> => {
      const response = await registry('GET', `accounts?chainId=${config.chain.chainId}`)
      if (!response || response.status !== 200) throw new Error(`the onchain receipts registry answered ${response ? `HTTP ${response.status}` : 'nothing'}`)
      const listed = registrySchema.parse(JSON.parse(new TextDecoder().decode(response.body)))
      const customers: Customer[] = []
      for (const account of listed.accounts) {
        try {
          customers.push(toCustomer(account, openSealed(secrets.sealingPrivateKey!, account.sealedApiKey, sealedKeyContext(account.organizationId, account.apiKeyId))))
        } catch (error) {
          log(`customers: the key of ${account.organizationId} does not open (${(error as Error).message}); it is skipped`)
        }
      }
      return customers
    },

    setRequester: async (address, allowed) => {
      const hash = await walletClient.writeContract({ address: contract, abi: relayAbi, functionName: 'setRequester', args: [address, allowed] })
      const receipt = await publicClient.waitForTransactionReceipt({ hash })
      if (receipt.status !== 'success') throw new Error(`setRequester reverted in ${hash}`)
      return hash
    },

    reportAllowlist: async (requesterId, allowed, txHash) => {
      const response = await registry('POST', `requesters/${encodeURIComponent(requesterId)}/allowlist`, { allowed, txHash })
      if (!response || response.status !== 200) {
        throw new Error(`reporting ${txHash} answered ${response ? `HTTP ${response.status}` : 'nothing'}`)
      }
    },

    finishedTransfers: async (reader, completedAfter): Promise<ListedTransfer[]> => {
      const transfers: ListedTransfer[] = []
      for (let page = 0; page < MAX_TRANSFER_PAGES; page += 1) {
        const query = `order_by=completed_at&completed_after=${encodeURIComponent(completedAfter)}&limit=${TRANSFER_PAGE}&offset=${page * TRANSFER_PAGE}`
        const response = await fetchBytes(`${config.beamApiBaseUrl}/transfers?${query}`, {
          headers: { accept: 'application/json', 'x-api-key': reader.apiKey },
        })
        if (!response || response.status !== 200) throw new Error(`GET /transfers answered ${response ? `HTTP ${response.status}` : 'nothing'}`)
        const rows = (JSON.parse(new TextDecoder().decode(response.body)) as { transfers?: unknown[] }).transfers ?? []
        for (const row of rows) {
          const transfer = projectListedTransfer(row)
          if (transfer) transfers.push(transfer)
        }
        if (rows.length < TRANSFER_PAGE) return transfers
      }
      log(`customers: ${reader.organizationId} has more than ${MAX_TRANSFER_PAGES * TRANSFER_PAGE} finished transfers to look at; the rest wait`)
      return transfers
    },

    reserveCharge: async ({ subject, organizationId, via }) => {
      const response = await registry('POST', 'charges/reserve', { ...chargeReceipt(subject), organizationId, via })
      return response ? projectCharge(response.status, response.body) : { state: 'unavailable', status: 0 }
    },

    commitCharge: async (subject) => {
      const response = await registry('POST', 'charges/commit', chargeReceipt(subject))
      return response ? projectCharge(response.status, response.body) : { state: 'unavailable', status: 0 }
    },

    readTransferCursors: async () => {
      if (!config.customers) return {}
      try {
        return JSON.parse(await readFile(config.customers.cursorPath, 'utf8')) as Record<string, string>
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return {}
        throw error
      }
    },

    saveTransferCursors: async (cursors) => {
      if (!config.customers) return
      const temporary = `${config.customers.cursorPath}.tmp`
      await writeFile(temporary, `${JSON.stringify(cursors, null, 2)}\n`)
      await rename(temporary, config.customers.cursorPath)
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
    if (config.customers) {
      const owner = await publicClient.readContract({ address: contract, abi: relayAbi, functionName: 'owner' })
      if (owner.toLowerCase() !== account.address.toLowerCase()) {
        throw new Error(`${contract} is owned by ${owner}; customers mode allows addresses, which only the owner can`)
      }
      const listed = await registry('GET', `accounts?chainId=${config.chain.chainId}`)
      if (!listed || listed.status !== 200) {
        throw new Error(`the onchain receipts registry answered ${listed ? `HTTP ${listed.status}` : 'nothing'}; check registryUrl and ONCHAIN_RELAY_SECRET`)
      }
    }
    const balance = await publicClient.getBalance({ address: account.address })
    log(`relay ${account.address} on chain ${chainId}, contract ${contract}, balance ${balance} wei${config.customers ? ', customers mode' : ''}`)
  }

  return { io, preflight }
}
