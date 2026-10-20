// Offline checks for the relay's logic against an in-memory chain, Studio and Beam. Run with `bun run check`.
import type { Hex } from 'viem'
import type { StudioOutcome } from '../workflows/start-transfer/studio'
import type { Observation, PublicationLookup } from '../workflows/transfer-receipts/beam'
import { decodeReport, Level, receiptLevels, RequestKind, TransferKind } from '../workflows/transfer-receipts/receipts'
import type { OpenRequest, Subject } from '../workflows/transfer-receipts/subjects'
import { transferIdForRequest } from '../workflows/transfer-receipts/transfer-id'
import { relayConfigSchema, secretsFromEnv } from './config'
import { MAX_ENTRIES_PER_REPORT, type RelayIO, type RelaySettings, runOnce, type TransferRequestLog } from './relay'

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
      findPublication: async (subject): Promise<PublicationLookup> => {
        const publicationId = this.publications.get(`${subject.publisherMemberId}|${subject.requestId}`)
        return publicationId ? { state: 'found', publicationId } : { state: 'absent' }
      },
      observe: async (subject: Exclude<Subject, { kind: 'roomRequest' | 'job' }>): Promise<Observation> => {
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
        return `0x${String(this.reports.length).padStart(64, '0')}` as Hex
      },
      readCursor: async () => this.cursor,
      saveCursor: async (next) => {
        this.cursor = next
      },
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
  check('a request in an unfinalized block is not started', (await runOnce(world.io(), settings)).started, 0)
  check('the cursor stops after the finalized block', world.cursor, 102n)

  world.finalized = 108n
  const first = await runOnce(world.io(), settings)
  check('a finalized request is delivered once and nothing is recorded yet', [first.started, first.recorded, world.deliveries], [1, 0, [r1]])
  check('the cursor covers every finalized block', world.cursor, 109n)

  world.finished.add(transferIdForRequest(CLIENT_ID, r1))
  const second = await runOnce(world.io(), settings)
  check('the finished transfer is recorded without a new delivery', [second.started, second.recorded, world.deliveries.length], [0, 1, 1])
  check('the receipt answers the request at the Beam-reported level only', world.reports[0].standard, [{ level: Level.BeamReported, requestId: r1 }])
  check('the open-request set is empty', world.open.size, 0)
  check('a later pass writes nothing', (await runOnce(world.io(), settings)).recorded, 0)
}

// Studio down: the cursor stays at the request's block, and the next pass delivers it again.
{
  const world = new World()
  const r2 = requestId(2)
  world.request(r2, 103n)
  world.finalized = 110n
  world.studioUp = false
  check('nothing starts while Studio is unavailable', (await runOnce(world.io(), settings)).started, 0)
  check('the cursor stays at the request block', world.cursor, 103n)
  world.studioUp = true
  check('the next pass delivers it', (await runOnce(world.io(), settings)).started, 1)
  check('one Studio run for two deliveries', [world.deliveries.length, world.runs.size], [2, 1])
}

// Restart between start and receipt: the saved cursor prevents a second start, and the receipt still lands.
{
  const world = new World()
  const r3 = requestId(3)
  world.request(r3, 104n)
  world.finalized = 104n
  await runOnce(world.io(), settings)
  const restarted = world.io() // a new process: same chain, Studio, Beam and cursor file, no memory
  world.finished.add(transferIdForRequest(CLIENT_ID, r3))
  const pass = await runOnce(restarted, settings)
  check('after a restart: no second delivery, one receipt', [world.deliveries.length, pass.started, pass.recorded], [1, 0, 1])
}

// Without a Room route, a Room delivery request is neither started nor looked up; it waits for a push.
{
  const world = new World()
  world.request(requestId(4), 100n, TransferKind.RoomDelivery)
  world.finalized = 100n
  const pass = await runOnce(world.io(), settings)
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
  const first = await runOnce(world.io(), roomSettings)
  check('a Room delivery request goes to the Room workflow only',
    [first.started, world.roomDeliveries, world.deliveries.length, first.recorded], [1, [r5], 0, 0])
  const publication = `btr_pub_${'b'.repeat(26)}`
  world.publications.set(`${roomSettings.routes.roomDelivery.publisherMemberId}|${r5}`, publication)
  check('a published delivery still running records nothing', (await runOnce(world.io(), roomSettings)).recorded, 0)
  world.finishedRooms.add(publication)
  const done = await runOnce(world.io(), roomSettings)
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
  const first = await runOnce(world.io(), jobSettings)
  check('a job goes to the Room workflow, which delivers its input', [first.started, world.roomDeliveries, world.deliveries.length], [1, [r7], 0])
  const input = `btr_pub_${'i'.repeat(26)}`
  const output = `btr_pub_${'o'.repeat(26)}`
  world.publications.set(`${source}|${r7}`, input)
  world.finishedRooms.add(input)
  check('a job whose output is not published yet records nothing', (await runOnce(world.io(), jobSettings)).recorded, 0)
  world.publications.set(`${compute}|${r7}`, output)
  check('a job whose output is still running records nothing', (await runOnce(world.io(), jobSettings)).recorded, 0)
  world.finishedRooms.add(output)
  const done = await runOnce(world.io(), jobSettings)
  check('a finished job is recorded as its two Room receipts and its result',
    [done.recorded, world.reports.flatMap((report) => report.rooms.map((room) => room.publication)), world.reports.flatMap((report) => report.jobs)],
    [3, [input, output], [{ requestId: r7, computeMember: compute }]])
  check('the open-request set is empty after the job result', world.open.size, 0)
  check('without a job output route, a job is not started',
    (await runOnce((() => { const w = new World(); w.request(requestId(8), 100n, TransferKind.Job); w.finalized = 100n; return w.io() })(), { ...settings, routes: { ...jobSettings.routes, jobOutput: null } })).started, 0)
}

// More finished requests than one report holds are written in several reports.
{
  const world = new World()
  const count = MAX_ENTRIES_PER_REPORT + 3
  for (let n = 0; n < count; n += 1) world.request(requestId(100 + n), 100n)
  world.finalized = 100n
  await runOnce(world.io(), settings)
  for (let n = 0; n < count; n += 1) world.finished.add(transferIdForRequest(CLIENT_ID, requestId(100 + n)))
  const pass = await runOnce(world.io(), settings)
  check('17 receipts go out as 14 + 3', [pass.recorded, world.reports.map((report) => report.standard.length)], [count, [14, 3]])
}

// Receipts only: no start pass, and receipts of transfers started elsewhere are still recorded.
{
  const world = new World()
  const r9 = requestId(9)
  world.request(r9, 101n)
  world.finalized = 105n
  const receiptsOnly = { ...settings, starts: false }
  const pass = await runOnce(world.io(), receiptsOnly)
  check('a receipts-only relay starts nothing', [pass.started, world.deliveries.length, world.roomDeliveries.length], [0, 0, 0])
  world.finished.add(transferIdForRequest(CLIENT_ID, r9))
  check('and records the receipt of a transfer started elsewhere', (await runOnce(world.io(), receiptsOnly)).recorded, 1)
}

const exampleJson = await Bun.file(new URL('config.bittensor-testnet.json', import.meta.url)).json()
const example = relayConfigSchema.safeParse(exampleJson)
check('config.bittensor-testnet.json matches the config schema', example.success ? 'valid' : example.error.issues, 'valid')
const relayEnv = { RELAY_PRIVATE_KEY: `0x${'11'.repeat(32)}`, BEAM_API_KEY: 'b1m_check' }
const receiptsOnlySecrets = secretsFromEnv(relayEnv, relayConfigSchema.parse({ ...exampleJson, starts: false, signDeliveries: false }))
check('a receipts-only config needs no Studio secrets',
  [receiptsOnlySecrets.studioWebhookUrl, receiptsOnlySecrets.studioSigningSecret, receiptsOnlySecrets.studioRoomWebhookUrl], [null, null, null])
check('signDeliveries without starts is refused', relayConfigSchema.safeParse({ ...exampleJson, starts: false, signDeliveries: true }).success, false)
