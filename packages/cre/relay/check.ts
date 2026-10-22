// Offline checks for the relay's logic against an in-memory chain, Studio and Beam. Run with `bun run check`.
import type { Address, Hex } from 'viem'
import type { StudioOutcome } from '../workflows/start-transfer/studio'
import type { Observation, PublicationLookup } from '../workflows/transfer-receipts/beam'
import { decodeReport, Level, receiptLevels, RequestKind, roomReceiptKey, TransferKind } from '../workflows/transfer-receipts/receipts'
import type { OpenRequest, Subject } from '../workflows/transfer-receipts/subjects'
import { bytes16ToTransferId, transferIdForRequest } from '../workflows/transfer-receipts/transfer-id'
import { relayConfigSchema, secretsFromEnv } from './config'
import {
  type ChargeOutcome,
  type ChargeSubject,
  type Customer,
  type ListedTransfer,
  nextTransferCursor,
  projectCharge,
  type Via,
} from './customers'
import {
  ABSENT_GRACE_MS,
  createRelayState,
  MAX_ENTRIES_PER_REPORT,
  recordedKey,
  type RelayIO,
  type RelaySettings,
  runOnce,
  type TransferRequestLog,
} from './relay'
import { openSealed } from './sealed-box'

const check = (name: string, actual: unknown, expected: unknown) => {
  const replacer = (_: string, value: unknown) => (typeof value === 'bigint' ? value.toString() : value)
  const a = JSON.stringify(actual, replacer)
  const e = JSON.stringify(expected, replacer)
  if (a !== e) {
    console.error(`FAIL ${name}\n  actual:   ${a}\n  expected: ${e}`)
    process.exit(1)
  }
  console.log(`ok   ${name}: ${a}`)
}

const CLIENT_ID = '11111111-2222-4333-8444-555555555555'
const settings: RelaySettings = {
  startBlock: 100n,
  logRangeBlocks: 5n,
  clientId: CLIENT_ID,
  levels: receiptLevels(false),
  routes: { roomDelivery: null, jobOutput: null },
  starts: true,
  relayAccount: `0x${'ee'.repeat(20)}`,
  customers: null,
}
const requestId = (n: number) => `0x${n.toString(16).padStart(64, '0')}` as Hex

/** A chain with `TransferRequested` logs and an open-request set, Studio with idempotent runs, and Beam. */
class World {
  finalized = 0n
  logs: TransferRequestLog[] = []
  open = new Map<Hex, OpenRequest>()
  studioUp = true
  runs = new Map<Hex, string>()
  deliveries: Hex[] = []
  finished = new Set<string>()
  roomDeliveries: Hex[] = []
  /** Publications Studio made under request:<requestId>, by `<publisher>|<requestId>`, as the coordinator lookup answers them. */
  publications = new Map<string, string>()
  finishedRooms = new Set<string>()
  reports: {
    standard: { level: number; requestId: Hex }[]
    rooms: { publication: string; level: number; requestId: Hex }[]
    jobs: { requestId: Hex; computeMember: string }[]
  }[] = []
  cursor: bigint | null = null
  lines: string[] = []
  state = createRelayState()

  request(id: Hex, block: bigint, kind: number = TransferKind.Standard) {
    this.logs.push({ requestId: id, requester: `0x${'ab'.repeat(20)}`, kind, input: 'in', blockNumber: block, logIndex: 0, transactionHash: id })
    this.open.set(id, {
      key: id,
      kind: RequestKind.Transfer,
      transferKind: kind,
      requester: `0x${'ab'.repeat(20)}`,
      openedAt: 0n,
      requestId: id,
      transferId: `0x${'00'.repeat(16)}`,
      room: '',
      channel: '',
      publication: '',
    })
  }

  io(): RelayIO {
    return {
      finalizedBlock: async () => this.finalized,
      transferRequests: async (from, to) => this.logs.filter((log) => log.blockNumber >= from && log.blockNumber <= to),
      deliver: async (request, workflow): Promise<StudioOutcome> => {
        if (workflow === 'room') this.roomDeliveries.push(request.requestId)
        else this.deliveries.push(request.requestId)
        if (!this.studioUp) return { state: 'unavailable', status: 503 }
        const runId = this.runs.get(request.requestId) ?? `wfr_${this.runs.size + 1}`
        this.runs.set(request.requestId, runId)
        return { state: 'accepted', runId }
      },
      openRequests: async () => [...this.open.values()],
      findPublication: async (subject, _reader): Promise<PublicationLookup> => {
        const publicationId = this.publications.get(`${subject.publisherMemberId}|${subject.requestId}`)
        return publicationId ? { state: 'found', publicationId } : { state: 'absent' }
      },
      observe: async (subject: Exclude<Subject, { kind: 'roomRequest' | 'job' }>, _reader): Promise<Observation> => {
        if (subject.kind === 'room') {
          if (!this.finishedRooms.has(subject.publication)) return { state: 'pending' }
          return {
            state: 'room',
            status: 1,
            fileSizeBytes: '1048576',
            manifestSha256: `0x${'cc'.repeat(32)}`,
            targetCount: 1,
            targetSigned: false,
            evidenceSha256: `0x${'dd'.repeat(32)}`,
            assignmentsSha256: `0x${'ee'.repeat(32)}`,
            evidenceVersion: 1,
          }
        }
        if (!this.finished.has(subject.transferId)) return this.runs.size ? { state: 'pending' } : { state: 'absent' }
        return {
          state: 'transfer',
          status: 1,
          totalBytes: '1048576',
          bytesTransferred: '1048576',
          completedAtMs: '1792467120355',
          planFingerprint: `0x${'aa'.repeat(32)}`,
          coordinateChecksum: `0x${'bb'.repeat(32)}`,
        }
      },
      writeReport: async (report: Hex) => {
        const { standard, rooms, jobs } = decodeReport(report)
        this.reports.push({
          standard: standard.map((entry) => ({ level: entry.level, requestId: entry.requestId })),
          rooms: rooms.map((entry) => ({ publication: entry.publication, level: entry.level, requestId: entry.requestId })),
          jobs: jobs.map((entry) => ({ requestId: entry.requestId, computeMember: entry.computeMember })),
        })
        for (const entry of [...standard, ...rooms, ...jobs]) this.open.delete(entry.requestId)
        return { hash: `0x${String(this.reports.length).padStart(64, '0')}` as Hex, recorded: new Set<string>() }
      },
      readCursor: async () => this.cursor,
      saveCursor: async (next) => {
        this.cursor = next
      },
      customers: async () => [],
      setRequester: async () => {
        throw new Error('no customers mode')
      },
      cancelRequest: async () => {
        throw new Error('no customers mode')
      },
      reportAllowlist: async () => {},
      finishedTransfers: async () => [],
      reserveCharge: async () => ({ state: 'unavailable', status: 0 }),
      commitCharge: async () => ({ state: 'unavailable', status: 0 }),
      readTransferCursors: async () => ({}),
      saveTransferCursors: async () => {},
      log: (line) => this.lines.push(line),
    }
  }
}

// One request: started once, recorded once its transfer finishes, at the relay's Beam-reported level.
{
  const world = new World()
  const r1 = requestId(1)
  world.request(r1, 102n)
  world.finalized = 101n
  check('a request in an unfinalized block is not started', (await runOnce(world.io(), settings, world.state)).started, 0)
  check('the cursor stops after the finalized block', world.cursor, 102n)

  world.finalized = 108n
  const first = await runOnce(world.io(), settings, world.state)
  check('a finalized request is delivered once and nothing is recorded yet', [first.started, first.recorded, world.deliveries], [1, 0, [r1]])
  check('the cursor covers every finalized block', world.cursor, 109n)

  world.finished.add(transferIdForRequest(CLIENT_ID, r1))
  const second = await runOnce(world.io(), settings, world.state)
  check('the finished transfer is recorded without a new delivery', [second.started, second.recorded, world.deliveries.length], [0, 1, 1])
  check('the receipt answers the request at the Beam-reported level only', world.reports[0].standard, [{ level: Level.BeamReported, requestId: r1 }])
  check('the open-request set is empty', world.open.size, 0)
  check('a later pass writes nothing', (await runOnce(world.io(), settings, world.state)).recorded, 0)
}

// Studio down: the cursor stays at the request's block, and the next pass delivers it again.
{
  const world = new World()
  const r2 = requestId(2)
  world.request(r2, 103n)
  world.finalized = 110n
  world.studioUp = false
  check('nothing starts while Studio is unavailable', (await runOnce(world.io(), settings, world.state)).started, 0)
  check('the cursor stays at the request block', world.cursor, 103n)
  world.studioUp = true
  check('the next pass delivers it', (await runOnce(world.io(), settings, world.state)).started, 1)
  check('one Studio run for two deliveries', [world.deliveries.length, world.runs.size], [2, 1])
}

// Restart between start and receipt: the saved cursor prevents a second start, and the receipt still lands.
{
  const world = new World()
  const r3 = requestId(3)
  world.request(r3, 104n)
  world.finalized = 104n
  await runOnce(world.io(), settings, world.state)
  const restarted = world.io() // a new process: same chain, Studio, Beam and cursor file, no memory
  world.finished.add(transferIdForRequest(CLIENT_ID, r3))
  const pass = await runOnce(restarted, settings, createRelayState())
  check('after a restart: no second delivery, one receipt', [world.deliveries.length, pass.started, pass.recorded], [1, 0, 1])
}

// Without a Room route, a Room delivery request is neither started nor looked up; it waits for a push.
{
  const world = new World()
  world.request(requestId(4), 100n, TransferKind.RoomDelivery)
  world.finalized = 100n
  const pass = await runOnce(world.io(), settings, world.state)
  check('without a Room route, a Room delivery request is skipped',
    [pass.started, pass.recorded, world.deliveries.length, world.roomDeliveries.length], [0, 0, 0, 0])
  check('and stays open', world.open.size, 1)
}

// With a Room route: started through the Room workflow, found by its key once published, recorded once finished.
{
  const world = new World()
  const roomSettings = {
    ...settings,
    routes: { roomDelivery: { room: `btr_room_${'a'.repeat(26)}`, channel: `btr_channel_${'a'.repeat(26)}`, publisherMemberId: `btr_member_${'a'.repeat(26)}` }, jobOutput: null },
  }
  const r5 = requestId(5)
  world.request(r5, 100n, TransferKind.RoomDelivery)
  world.finalized = 100n
  const first = await runOnce(world.io(), roomSettings, world.state)
  check('a Room delivery request goes to the Room workflow only',
    [first.started, world.roomDeliveries, world.deliveries.length, first.recorded], [1, [r5], 0, 0])
  const publication = `btr_pub_${'b'.repeat(26)}`
  world.publications.set(`${roomSettings.routes.roomDelivery.publisherMemberId}|${r5}`, publication)
  check('a published delivery still running records nothing', (await runOnce(world.io(), roomSettings, world.state)).recorded, 0)
  world.finishedRooms.add(publication)
  const done = await runOnce(world.io(), roomSettings, world.state)
  check('its Room receipt answers the request at the Beam-reported Room level',
    [done.recorded, done.started, world.reports.flatMap((report) => report.rooms)],
    [1, 0, [{ publication, level: Level.BeamReported, requestId: r5 }]])
  check('the open-request set is empty after the Room receipt', world.open.size, 0)
}

// With a job output route too: a job starts through the Room workflow and is answered by both Room receipts and
// the job result once the compute member's output is published and finished.
{
  const world = new World()
  const source = `btr_member_${'a'.repeat(26)}`
  const compute = `btr_member_${'c'.repeat(26)}`
  const jobSettings = {
    ...settings,
    routes: {
      roomDelivery: { room: `btr_room_${'a'.repeat(26)}`, channel: `btr_channel_${'a'.repeat(26)}`, publisherMemberId: source },
      jobOutput: { channel: `btr_channel_${'o'.repeat(26)}`, computeMemberId: compute },
    },
  }
  const r7 = requestId(7)
  world.request(r7, 100n, TransferKind.Job)
  world.finalized = 100n
  const first = await runOnce(world.io(), jobSettings, world.state)
  check('a job goes to the Room workflow, which delivers its input', [first.started, world.roomDeliveries, world.deliveries.length], [1, [r7], 0])
  const input = `btr_pub_${'i'.repeat(26)}`
  const output = `btr_pub_${'o'.repeat(26)}`
  world.publications.set(`${source}|${r7}`, input)
  world.finishedRooms.add(input)
  check('a job whose output is not published yet records nothing', (await runOnce(world.io(), jobSettings, world.state)).recorded, 0)
  world.publications.set(`${compute}|${r7}`, output)
  check('a job whose output is still running records nothing', (await runOnce(world.io(), jobSettings, world.state)).recorded, 0)
  world.finishedRooms.add(output)
  const done = await runOnce(world.io(), jobSettings, world.state)
  check('a finished job is recorded as its two Room receipts and its result',
    [done.recorded, world.reports.flatMap((report) => report.rooms.map((room) => room.publication)), world.reports.flatMap((report) => report.jobs)],
    [3, [input, output], [{ requestId: r7, computeMember: compute }]])
  check('the open-request set is empty after the job result', world.open.size, 0)
  check('without a job output route, a job is not started',
    (await runOnce((() => { const w = new World(); w.request(requestId(8), 100n, TransferKind.Job); w.finalized = 100n; return w.io() })(), { ...settings, routes: { ...jobSettings.routes, jobOutput: null } }, createRelayState())).started, 0)
}

// More finished requests than one report holds are written in several reports.
{
  const world = new World()
  const count = MAX_ENTRIES_PER_REPORT + 3
  for (let n = 0; n < count; n += 1) world.request(requestId(100 + n), 100n)
  world.finalized = 100n
  await runOnce(world.io(), settings, world.state)
  for (let n = 0; n < count; n += 1) world.finished.add(transferIdForRequest(CLIENT_ID, requestId(100 + n)))
  const pass = await runOnce(world.io(), settings, world.state)
  check('17 receipts go out as 14 + 3', [pass.recorded, world.reports.map((report) => report.standard.length)], [count, [14, 3]])
}

// Receipts only: no start pass, and receipts of transfers started elsewhere are still recorded.
{
  const world = new World()
  const r9 = requestId(9)
  world.request(r9, 101n)
  world.finalized = 105n
  const receiptsOnly = { ...settings, starts: false }
  const pass = await runOnce(world.io(), receiptsOnly, world.state)
  check('a receipts-only relay starts nothing', [pass.started, world.deliveries.length, world.roomDeliveries.length], [0, 0, 0])
  world.finished.add(transferIdForRequest(CLIENT_ID, r9))
  check('and records the receipt of a transfer started elsewhere', (await runOnce(world.io(), receiptsOnly, world.state)).recorded, 1)
}

const exampleJson = await Bun.file(new URL('config.bittensor-testnet.json', import.meta.url)).json()
const example = relayConfigSchema.safeParse(exampleJson)
check('config.bittensor-testnet.json matches the config schema', example.success ? 'valid' : example.error.issues, 'valid')
const relayEnv = { RELAY_PRIVATE_KEY: `0x${'11'.repeat(32)}`, BEAM_API_KEY: 'b1m_check' }
const receiptsOnlySecrets = secretsFromEnv(relayEnv, relayConfigSchema.parse({ ...exampleJson, starts: false, signDeliveries: false }))
check('a receipts-only config needs no Studio secrets',
  [receiptsOnlySecrets.studioWebhookUrl, receiptsOnlySecrets.studioSigningSecret, receiptsOnlySecrets.studioRoomWebhookUrl], [null, null, null])
check('signDeliveries without starts is refused', relayConfigSchema.safeParse({ ...exampleJson, starts: false, signDeliveries: true }).success, false)

// Customers mode: organizations set up in the Beam console, their keys sealed to the relay, charged one credit
// per receipt through the website's relay routes.
const SHARED_VECTOR = {
  privateKey: '2DdGFawc0KoPWycO-YVgIBA8DK2KZLznCa4HrprnInI',
  context: 'onchain-receipts:org_vector:key_vector',
  plaintext: 'b1m_vector_0123456789abcdefghijklmnopqrstuv',
  sealed: 'v1.ttbWPfsOMOjXJ1pQl2WhehGqbVzISN2IgH2eqGCJpwo.Mq1pFWNwjiiI_mMX.D3vdPu0MBiZu2fuVRUYw9i-OVRWI1XOSm1kJS-xY_9rxB7DWQYDUoU5vmxbLrVyLZHhwKKjRr9uVY-0',
}
check('the relay opens the website sealed-box test vector', openSealed(SHARED_VECTOR.privateKey, SHARED_VECTOR.sealed, SHARED_VECTOR.context), SHARED_VECTOR.plaintext)
check('a vector under another context does not open', (() => { try { openSealed(SHARED_VECTOR.privateKey, SHARED_VECTOR.sealed, 'onchain-receipts:org_b:key_vector'); return 'opened' } catch { return 'refused' } })(), 'refused')

const enc = (value: unknown) => new TextEncoder().encode(JSON.stringify(value))
check('charge answers map to what the relay does',
  [
    projectCharge(200, enc({ operation: { status: 'RESERVED' } })).state,
    projectCharge(200, enc({ operation: { status: 'COMMITTED' } })).state,
    projectCharge(200, enc({ operation: { status: 'CANCELED', releaseReason: 'expired' } })).state,
    projectCharge(200, enc({ operation: { status: 'CANCELED', releaseReason: 'write reverted' } })).state,
    projectCharge(402, enc({ reason: 'pool_exhausted' })),
    projectCharge(503, enc({})).state,
  ],
  ['held', 'charged', 'held', 'released', { state: 'refused', status: 402, reason: 'pool_exhausted' }, 'unavailable'])
check('a listing cursor stops just before the oldest transfer not done',
  nextTransferCursor('2026-10-21T09:00:00.000Z', [
    { completedAt: '2026-10-21T10:00:00.000Z', done: true },
    { completedAt: '2026-10-21T11:00:00.000Z', done: false },
    { completedAt: '2026-10-21T12:00:00.000Z', done: true },
  ]), '2026-10-21T10:59:59.999Z')
check('and passes the newest when all are done, or stays without transfers',
  [nextTransferCursor('2026-10-21T09:00:00.000Z', [{ completedAt: '2026-10-21T12:00:00.000Z', done: true }]), nextTransferCursor('2026-10-21T09:00:00.000Z', [])],
  ['2026-10-21T12:00:00.000Z', '2026-10-21T09:00:00.000Z'])

const ORG = 'org_customer'
const APPROVED: Address = `0x${'a1'.repeat(20)}`
const REVOKED: Address = `0x${'b2'.repeat(20)}`
const STRANGER: Address = `0x${'c3'.repeat(20)}`
const PERIOD_START = Date.parse('2026-10-21T10:00:00.000Z')
const transferId = (n: number) => `9e6540e8-1d2b-4c3a-8f00-${n.toString(16).padStart(12, '0')}`
const finished = (id: string): Observation => ({
  state: 'transfer',
  status: 1,
  totalBytes: '1048576',
  bytesTransferred: '1048576',
  completedAtMs: '1792467120355',
  planFingerprint: `0x${'aa'.repeat(32)}`,
  coordinateChecksum: `0x${'bb'.repeat(32)}`,
})
const customerSettings: RelaySettings = { ...settings, starts: false, customers: { maxPerOrganization: 2 } }

/** The receipts contract, the website's registry and ledger, and the customer's transfers at Beam. */
class CustomerWorld {
  open: OpenRequest[] = []
  onchain = new Set<string>()
  allowlist = new Map<Address, boolean>()
  requesters = [
    { id: 'req_approved', address: APPROVED, status: 'APPROVED' as const, allowlist: 'NONE' as const },
    { id: 'req_revoked', address: REVOKED, status: 'REVOKED' as const, allowlist: 'ALLOWED' as const },
  ]
  reported: Array<[string, boolean]> = []
  listed: ListedTransfer[] = []
  readable = new Set<string>()
  credits = 10
  ledger = new Map<string, 'RESERVED' | 'COMMITTED'>()
  reserves: Array<{ key: string; via: Via['kind'] }> = []
  writes = 0
  failWrites = 0
  failCommits = 0
  cancels: Hex[] = []
  failCancels = 0
  cursors: Record<string, string> = {}
  lines: string[] = []
  state = createRelayState()

  customer(): Customer {
    return {
      organizationId: ORG,
      reader: { apiKey: 'b1m_customer', organizationId: ORG },
      periods: [{ startedAt: PERIOD_START, endedAt: null }],
      requesters: this.requesters,
    }
  }

  /** `openedAt` in seconds, as the contract stores it; the epoch by default, long past the absent grace. */
  receiptRequest(requester: Address, id: string, openedAt = 0n) {
    this.open.push({
      key: `0x${id.replaceAll('-', '').padStart(64, '0')}` as Hex,
      kind: RequestKind.Receipt,
      transferKind: 0,
      requester,
      openedAt,
      requestId: `0x${'00'.repeat(32)}`,
      transferId: `0x${id.replaceAll('-', '')}` as Hex,
      room: '',
      channel: '',
      publication: '',
    })
  }

  transferRequest(requester: Address, n: number) {
    this.open.push({
      key: `0x${'7e'.repeat(31)}${n.toString(16).padStart(2, '0')}` as Hex,
      kind: RequestKind.Transfer,
      transferKind: TransferKind.Standard,
      requester,
      openedAt: 0n,
      requestId: requestId(n),
      transferId: `0x${'00'.repeat(16)}`,
      room: '',
      channel: '',
      publication: '',
    })
  }

  finishedTransfer(n: number, createdAt: string, completedAt: string) {
    this.listed.push({ id: transferId(n), createdAt, completedAt, observation: finished(transferId(n)) })
  }

  io(): RelayIO {
    const chargeKey = (subject: ChargeSubject) => (subject.kind === 'request' ? `request:${subject.requestKey}` : recordedKey(subject))
    return {
      ...new World().io(),
      openRequests: async () => [...this.open],
      observe: async (subject, reader) => {
        if (subject.kind !== 'transfer') return { state: 'absent' }
        // Reads are scoped to the key's owner: Beam's key sees none of the customer's transfers.
        return reader && this.readable.has(subject.transferId) ? finished(subject.transferId) : { state: 'absent' }
      },
      writeReport: async (report: Hex) => {
        this.writes += 1
        if (this.failWrites > 0) {
          this.failWrites -= 1
          throw new Error('onReport reverted')
        }
        const recorded = new Set<string>()
        for (const entry of decodeReport(report).standard) {
          const key = recordedKey({ kind: 'transfer', transferId: bytes16ToTransferId(entry.transferId) })
          if (!this.onchain.has(key)) recorded.add(key)
          this.onchain.add(key)
          this.open = this.open.filter((request) => recordedKey({ kind: 'transfer', transferId: bytes16ToTransferId(request.transferId) }) !== key)
        }
        return { hash: `0x${String(this.writes).padStart(64, '0')}` as Hex, recorded }
      },
      customers: async () => [this.customer()],
      setRequester: async (address, allowed) => {
        this.allowlist.set(address, allowed)
        return `0x${'f0'.repeat(32)}` as Hex
      },
      cancelRequest: async (key) => {
        if (this.failCancels > 0) {
          this.failCancels -= 1
          throw new Error('cancelRequest reverted')
        }
        this.cancels.push(key)
        this.open = this.open.filter((request) => request.key !== key)
        return `0x${'c0'.repeat(32)}` as Hex
      },
      reportAllowlist: async (id, allowed) => {
        this.reported.push([id, allowed])
        this.requesters = this.requesters.map((requester) =>
          requester.id === id ? { ...requester, allowlist: (allowed ? 'ALLOWED' : 'REMOVED') as never } : requester)
      },
      finishedTransfers: async (_reader, after) => this.listed.filter((transfer) => Date.parse(transfer.completedAt) > Date.parse(after)),
      reserveCharge: async ({ subject, via }): Promise<ChargeOutcome> => {
        const key = chargeKey(subject)
        this.reserves.push({ key, via: via.kind })
        const existing = this.ledger.get(key)
        if (existing) return { state: existing === 'COMMITTED' ? 'charged' : 'held' }
        if (this.credits < 1) return { state: 'refused', status: 402, reason: 'pool_exhausted' }
        this.ledger.set(key, 'RESERVED')
        return { state: 'held' }
      },
      commitCharge: async (subject): Promise<ChargeOutcome> => {
        if (this.failCommits > 0) {
          this.failCommits -= 1
          return { state: 'unavailable', status: 503 }
        }
        const key = chargeKey(subject)
        if (this.ledger.get(key) === 'RESERVED') this.credits -= 1
        this.ledger.set(key, 'COMMITTED')
        return { state: 'charged' }
      },
      readTransferCursors: async () => ({ ...this.cursors }),
      saveTransferCursors: async (cursors) => {
        this.cursors = { ...cursors }
      },
      log: (line) => this.lines.push(line),
    }
  }
}

// The allowlist: an approved address is allowed onchain, a revoked one still allowed is removed, both reported.
{
  const world = new CustomerWorld()
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('approved addresses are allowed and revoked ones removed, then reported',
    [pass.allowlisted, [...world.allowlist.entries()], world.reported],
    [2, [[APPROVED, true], [REVOKED, false]], [['req_approved', true], ['req_revoked', false]]])
  check('a synced allowlist writes nothing on the next pass', (await runOnce(world.io(), customerSettings, world.state)).allowlisted, 0)
}

// Every transfer: a transfer created while the switch was on is recorded with no request and charged once.
{
  const world = new CustomerWorld()
  world.finishedTransfer(1, '2026-10-21T09:59:59.000Z', '2026-10-21T10:30:00.000Z') // created before the switch
  world.finishedTransfer(2, '2026-10-21T10:05:00.000Z', '2026-10-21T10:40:00.000Z')
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('only the transfer created during the period is recorded, as every transfer',
    [pass.recorded, [...world.onchain], world.reserves], [1, [`transfer:${transferId(2)}`], [{ key: `transfer:${transferId(2)}`, via: 'every_transfer' }]])
  check('and charged one credit', [world.credits, world.ledger.get(`transfer:${transferId(2)}`)], [9, 'COMMITTED'])
  check('the cursor passes both', world.cursors[ORG], '2026-10-21T10:40:00.000Z')
  const again = await runOnce(world.io(), customerSettings, world.state)
  check('a later pass lists nothing new and charges nothing', [again.recorded, world.credits, world.writes], [0, 9, 1])
}

// Requests: an approved address's request is charged to its organization; Beam's own and a stranger's are not.
{
  const world = new CustomerWorld()
  world.readable.add(transferId(3))
  world.receiptRequest(APPROVED, transferId(3))
  world.receiptRequest(STRANGER, transferId(4))
  world.receiptRequest(settings.relayAccount, transferId(5))
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('the approved address gets its receipt, charged via its request',
    [pass.recorded, world.reserves, world.credits], [1, [{ key: `transfer:${transferId(3)}`, via: 'requester' }], 9])
  check("a stranger's request stays open and Beam's own reads with Beam's key, uncharged",
    [world.open.map((request) => request.requester), world.lines.some((line) => line.includes('1 open requests stay open'))],
    [[STRANGER, settings.relayAccount], true])
}

// Asked for both ways: the transfer is listed and requested; one receipt, one charge.
{
  const world = new CustomerWorld()
  world.readable.add(transferId(6))
  world.finishedTransfer(6, '2026-10-21T10:10:00.000Z', '2026-10-21T10:50:00.000Z')
  world.receiptRequest(APPROVED, transferId(6))
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('a receipt owed both ways is recorded once and charged once', [pass.recorded, world.reserves.length, world.credits, world.cursors[ORG]], [1, 1, 9, '2026-10-21T10:50:00.000Z'])
}

// Out of credits: nothing is written, the request stays open and the listing waits; a top-up records both.
{
  const world = new CustomerWorld()
  world.credits = 0
  world.readable.add(transferId(7))
  world.receiptRequest(APPROVED, transferId(7))
  world.finishedTransfer(8, '2026-10-21T10:20:00.000Z', '2026-10-21T11:00:00.000Z')
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('with no credits nothing is written', [pass.recorded, world.writes, world.open.length], [0, 0, 1])
  check('and the listing stays before the unpaid transfer', world.cursors[ORG], '2026-10-21T10:59:59.999Z')
  world.credits = 5
  const paid = await runOnce(world.io(), customerSettings, world.state)
  check('after a top-up both receipts are recorded and charged', [paid.recorded, world.credits, world.open.length, world.cursors[ORG]], [2, 3, 0, '2026-10-21T11:00:00.000Z'])
}

// A failed write keeps the holds; the next pass writes again and charges once.
{
  const world = new CustomerWorld()
  world.finishedTransfer(9, '2026-10-21T10:30:00.000Z', '2026-10-21T11:10:00.000Z')
  world.failWrites = 1
  const failed = await runOnce(world.io(), customerSettings, world.state)
  check('a failed write records nothing and keeps the hold', [failed.recorded, world.ledger.get(`transfer:${transferId(9)}`), world.credits], [0, 'RESERVED', 10])
  check('the listing waits for it', world.cursors[ORG], '2026-10-21T11:09:59.999Z')
  const retried = await runOnce(world.io(), customerSettings, world.state)
  check('the next pass records it and charges once', [retried.recorded, world.credits, world.ledger.get(`transfer:${transferId(9)}`)], [1, 9, 'COMMITTED'])
}

// A failed commit: the receipt is onchain; the next pass finds the hold, the contract skips it, and it is charged.
{
  const world = new CustomerWorld()
  world.finishedTransfer(10, '2026-10-21T10:30:00.000Z', '2026-10-21T11:20:00.000Z')
  world.failCommits = 1
  await runOnce(world.io(), customerSettings, world.state)
  check('a recorded receipt whose commit failed is not charged yet', [world.onchain.size, world.credits, world.cursors[ORG]], [1, 10, '2026-10-21T11:19:59.999Z'])
  await runOnce(world.io(), customerSettings, world.state)
  check('the next pass charges it once, noting it was already onchain',
    [world.credits, world.ledger.get(`transfer:${transferId(10)}`), world.lines.some((line) => line.includes('was already onchain'))], [9, 'COMMITTED', true])
}

// A receipt already charged is never written again, and each pass records at most maxPerOrganization listed transfers.
{
  const world = new CustomerWorld()
  world.ledger.set(`transfer:${transferId(11)}`, 'COMMITTED')
  world.finishedTransfer(11, '2026-10-21T10:30:00.000Z', '2026-10-21T11:30:00.000Z')
  for (const n of [12, 13, 14]) world.finishedTransfer(n, '2026-10-21T10:30:00.000Z', `2026-10-21T11:3${n - 11}:00.000Z`)
  const first = await runOnce(world.io(), customerSettings, world.state)
  check('a charged receipt is skipped but uses one of the two listed slots this pass', [first.recorded, world.credits], [1, 9])
  const second = await runOnce(world.io(), customerSettings, world.state)
  check('the rest follow on the next passes', [second.recorded, (await runOnce(world.io(), customerSettings, world.state)).recorded, world.credits], [2, 0, 7])
}

// Closing what will never be answered: a customer's transfer request is cancelled at one credit to its
// organization; Beam's own transfer request is left alone.
{
  const world = new CustomerWorld()
  world.transferRequest(APPROVED, 20)
  world.transferRequest(settings.relayAccount, 21)
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check("a customer's transfer request is cancelled and charged one credit; Beam's own stays open",
    [pass.closed, world.credits, world.ledger.get(`request:${world.cancels[0]}`), world.open.map((request) => request.requester)],
    [1, 9, 'COMMITTED', [settings.relayAccount]])
}

// A request left by an address its organization no longer has approved is closed at that organization's cost.
{
  const world = new CustomerWorld()
  world.receiptRequest(REVOKED, transferId(22))
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check("a revoked address's request is cancelled and charged to its organization",
    [pass.closed, world.open.length, world.credits, world.reserves], [1, 0, 9, [{ key: `request:${world.cancels[0]}`, via: 'requester' }]])
}

// A receipt request for a transfer its key cannot see stays open during the grace, then is closed and charged.
{
  const world = new CustomerWorld()
  const nowSeconds = BigInt(Math.floor(Date.now() / 1000))
  world.receiptRequest(APPROVED, transferId(23), nowSeconds)
  const fresh = await runOnce(world.io(), customerSettings, world.state)
  check("within the grace, a request for a transfer its key cannot see stays open, uncharged",
    [fresh.closed, world.open.length, world.reserves.length], [0, 1, 0])
  world.open[0].openedAt = nowSeconds - BigInt(ABSENT_GRACE_MS / 1000) - 1n
  const stale = await runOnce(world.io(), customerSettings, world.state)
  check('after it, the request is cancelled and charged one credit', [stale.closed, world.open.length, world.credits], [1, 0, 9])
}

// Out of credits: no cancel is sent, so Beam pays no gas; the request is closed once the organization can pay.
{
  const world = new CustomerWorld()
  world.credits = 0
  world.transferRequest(APPROVED, 24)
  const pass = await runOnce(world.io(), customerSettings, world.state)
  check('with no credits no cancel is sent and the request stays open',
    [pass.closed, world.cancels.length, world.open.length, world.lines.some((line) => line.includes('stays open') && line.includes('pool_exhausted'))],
    [0, 0, 1, true])
  world.credits = 1
  const paid = await runOnce(world.io(), customerSettings, world.state)
  check('after a top-up it is cancelled and charged', [paid.closed, world.credits, world.open.length], [1, 0, 0])
}

// A failed cancel commits nothing and is sent again under the same hold; a failed commit is retried from memory.
{
  const world = new CustomerWorld()
  world.transferRequest(APPROVED, 25)
  const key = world.open[0].key
  world.failCancels = 1
  const failed = await runOnce(world.io(), customerSettings, world.state)
  check('a failed cancel leaves its hold uncommitted and the request open',
    [failed.closed, world.ledger.get(`request:${key}`), world.credits, world.open.length], [0, 'RESERVED', 10, 1])
  world.failCommits = 1
  const cancelled = await runOnce(world.io(), customerSettings, world.state)
  check('the next pass cancels it under the same hold; its failed commit is kept',
    [cancelled.closed, world.reserves.length, world.credits, world.state.uncommittedCloses.size], [1, 2, 10, 1])
  await runOnce(world.io(), customerSettings, world.state)
  check('a later pass commits it once', [world.credits, world.ledger.get(`request:${key}`), world.state.uncommittedCloses.size], [9, 'COMMITTED', 0])
}

const customersConfig = relayConfigSchema.parse({
  ...exampleJson,
  starts: false,
  signDeliveries: false,
  customers: { registryUrl: 'https://api.example.com/', cursorPath: './customers-cursor.json', maxPerOrganization: 50 },
})
check('customers mode needs the relay secret and the sealing key',
  (() => { try { secretsFromEnv(relayEnv, customersConfig); return 'accepted' } catch (error) { return (error as Error).message } })(), 'Set ONCHAIN_RELAY_SECRET')
check('with both, they are read', secretsFromEnv({ ...relayEnv, ONCHAIN_RELAY_SECRET: 's', RELAY_SEALING_PRIVATE_KEY: SHARED_VECTOR.privateKey }, customersConfig).sealingPrivateKey, SHARED_VECTOR.privateKey)
check('the registry URL loses its trailing slash', customersConfig.customers?.registryUrl, 'https://api.example.com')
check('customers mode with starts is refused', relayConfigSchema.safeParse({ ...exampleJson, starts: true, customers: customersConfig.customers }).success, false)
check('roomReceiptKey and recordedKey agree on a Room receipt', recordedKey({ kind: 'room', roomKey: roomReceiptKey('r', 'c', 'p') }), `room:${roomReceiptKey('r', 'c', 'p').toLowerCase()}`)
