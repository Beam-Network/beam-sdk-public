import type { Address, Hex } from 'viem'
import type { StudioOutcome } from '../workflows/start-transfer/studio'
import type { Observation, PublicationLookup } from '../workflows/transfer-receipts/beam'
import { encodeReport, type ReceiptLevels, RequestKind, roomReceiptKey, TransferKind } from '../workflows/transfer-receipts/receipts'
import { bytes16ToTransferId } from '../workflows/transfer-receipts/transfer-id'
import {
  type ChargeOutcome,
  type ChargeSubject,
  type Customer,
  createdDuringPeriods,
  initialTransferCursor,
  type ListedTransfer,
  listsTransfers,
  nextTransferCursor,
  type Owner,
  type Reader,
  type ReceiptSubject,
  type Via,
} from './customers'
import {
  type DeploymentRoutes,
  describeObservation,
  describePublicationLookup,
  type JobSubject,
  jobEntriesFor,
  jobLookups,
  type OpenRequest,
  type ReportEntry,
  reportEntryFor,
  type RoomRequestSubject,
  roomSubjectForLookup,
  type Subject,
  subjectForOpenRequest,
  subjectKey,
} from '../workflows/transfer-receipts/subjects'

/** A decoded `TransferRequested` log. */
export type TransferRequestLog = {
  requestId: Hex
  requester: Address
  kind: number
  input: string
  blockNumber: bigint
  logIndex: number
  transactionHash: Hex
}

/** Everything the relay does outside its own logic: the chain, Studio, Beam and the cursor file. */
export type RelayIO = {
  finalizedBlock(): Promise<bigint>
  transferRequests(fromBlock: bigint, toBlock: bigint): Promise<TransferRequestLog[]>
  /** Posts a request to the Studio workflow that starts its kind: the transfer workflow or the Room workflow. */
  deliver(request: TransferRequestLog, workflow: 'transfer' | 'room'): Promise<StudioOutcome>
  openRequests(): Promise<OpenRequest[]>
  /** The coordinator's lookup of the publication a Room delivery request started, read with `reader`'s key. */
  findPublication(subject: RoomRequestSubject, reader: Reader): Promise<PublicationLookup>
  observe(subject: Exclude<Subject, RoomRequestSubject | JobSubject>, reader: Reader): Promise<Observation>
  /**
   * Sends one report to `onReport` and waits until it is mined: the transaction hash, and the `recordedKey` of
   * every receipt its events say it recorded (the contract skips one it already holds).
   */
  writeReport(report: Hex): Promise<{ hash: Hex; recorded: Set<string> }>
  readCursor(): Promise<bigint | null>
  saveCursor(nextBlock: bigint): Promise<void>
  /** Customers mode: the organizations set up in the console, with their keys opened. */
  customers(): Promise<Customer[]>
  /** `setRequester` on the receipts contract (owner only), waited until mined; returns the transaction hash. */
  setRequester(address: Address, allowed: boolean): Promise<Hex>
  /** `cancelRequest` on the receipts contract (owner or requester), waited until mined; returns the transaction hash. */
  cancelRequest(key: Hex): Promise<Hex>
  reportAllowlist(requesterId: string, allowed: boolean, txHash: Hex): Promise<void>
  /** A customer's finished transfers completed after `completedAfter`, read with its key. */
  finishedTransfers(reader: Exclude<Reader, null>, completedAfter: string): Promise<ListedTransfer[]>
  reserveCharge(charge: { subject: ChargeSubject; organizationId: string; via: Via }): Promise<ChargeOutcome>
  commitCharge(subject: ChargeSubject): Promise<ChargeOutcome>
  readTransferCursors(): Promise<Record<string, string>>
  saveTransferCursors(cursors: Record<string, string>): Promise<void>
  log(message: string): void
}

export type RelaySettings = {
  startBlock: bigint
  logRangeBlocks: bigint
  clientId: string
  levels: ReceiptLevels
  /** Where this deployment's Room deliveries and jobs publish; each is null when it starts none. */
  routes: DeploymentRoutes
  /** Whether the relay starts transfers through Studio; false records receipts only. */
  starts: boolean
  /** The relay's account, which owns the receipts contract: its own requests are Beam's. */
  relayAccount: Address
  /** Customers mode; null records Beam's own receipts only. */
  customers: { maxPerOrganization: number } | null
}

/** What the relay keeps between passes, in memory only. */
export type RelayState = {
  /** Requests closed whose charge did not commit yet, by request key, with the organization charged. */
  uncommittedCloses: Map<Hex, string>
}

export const createRelayState = (): RelayState => ({ uncommittedCloses: new Map() })

/**
 * Entries per report, at most one subject each. The contract's gas test covers 14 of each kind, the CRE
 * workflow's per-execution maximum; the relay keeps the same bound.
 */
export const MAX_ENTRIES_PER_REPORT = 14

/**
 * How long a customer's receipt request may name a transfer or publication its key cannot see before the relay
 * closes it: long enough for a new one to become visible, shorter than the website's two-hour open-request alert.
 */
export const ABSENT_GRACE_MS = 60 * 60 * 1000

/**
 * Starts: delivers every `TransferRequested` log up to the finalized block to Studio, in order: standard
 * requests to the transfer workflow, Room delivery requests to the Room workflow when the deployment has a
 * Room route, and jobs to the same Room workflow, which delivers their input to the compute member, when it also
 * has a job output route. The cursor moves past a block range only once every request in it was accepted or refused. When Studio is
 * unavailable, the cursor stops at that request's block, and the next run delivers from there again; Studio
 * keys runs on `Idempotency-Key`, so a repeated delivery returns the run that already exists.
 */
export const runStarts = async (io: RelayIO, settings: RelaySettings, finalized: bigint): Promise<number> => {
  let next = (await io.readCursor()) ?? settings.startBlock
  let delivered = 0
  while (next <= finalized) {
    const toBlock = next + settings.logRangeBlocks - 1n < finalized ? next + settings.logRangeBlocks - 1n : finalized
    const logs = (await io.transferRequests(next, toBlock)).sort((a, b) =>
      a.blockNumber === b.blockNumber ? a.logIndex - b.logIndex : a.blockNumber < b.blockNumber ? -1 : 1,
    )
    for (const request of logs) {
      const { roomDelivery, jobOutput } = settings.routes
      const workflow =
        request.kind === TransferKind.Standard
          ? 'transfer'
          : (request.kind === TransferKind.RoomDelivery && roomDelivery) || (request.kind === TransferKind.Job && roomDelivery && jobOutput)
            ? 'room'
            : null
      if (!workflow) {
        io.log(`start: ${request.requestId} is a kind ${request.kind} request, which this deployment does not start`)
        continue
      }
      const outcome = await io.deliver(request, workflow)
      if (outcome.state === 'unavailable') {
        io.log(`start: Studio unavailable for ${request.requestId} (HTTP ${outcome.status || 'none'}); retrying from block ${request.blockNumber}`)
        await io.saveCursor(request.blockNumber)
        return delivered
      }
      if (outcome.state === 'refused') {
        io.log(`start: Studio refused ${request.requestId} with HTTP ${outcome.status}${outcome.reason ? ` (${outcome.reason})` : ''}; the request stays open`)
        continue
      }
      delivered += 1
      io.log(
        outcome.runId
          ? `start: ${request.requestId} accepted as Studio run ${outcome.runId}`
          : `start: ${request.requestId} accepted into a coalescing window`,
      )
    }
    next = toBlock + 1n
    await io.saveCursor(next)
  }
  return delivered
}


/** The key a recorded receipt goes by: a standard receipt by its transfer, a Room receipt by its Room key. */
export const recordedKey = (subject: ReceiptSubject): string =>
  subject.kind === 'transfer' ? `transfer:${subject.transferId.toLowerCase()}` : `room:${subject.roomKey.toLowerCase()}`

/** The receipt a report entry records, when it records one: closures and job results are never charged. */
const chargeSubjectOf = (entry: ReportEntry): ReceiptSubject | null => {
  if (entry.kind === 'standard') return { kind: 'transfer', transferId: bytes16ToTransferId(entry.entry.transferId) }
  if (entry.kind === 'room') return { kind: 'room', roomKey: roomReceiptKey(entry.entry.room, entry.entry.channel, entry.entry.publication) }
  return null
}

/** `request`: the open request a customer asked with, so it can be closed if it can never be answered. */
type Pending = { subject: Subject; owner: Owner; observation?: Observation; request?: OpenRequest }
type Planned = { entry: ReportEntry; owner: Owner; key: string }
/** An open request this relay will never answer, and the organization whose address opened it. */
type Unanswerable = { request: OpenRequest; organizationId: string; reason: string }

const readerOf = (owner: Owner): Reader => (owner.kind === 'customer' ? owner.reader : null)

/**
 * Customers mode: allows approved addresses on the receipts contract and removes revoked ones still allowed,
 * then reports each transaction. A failure is logged and retried on the next pass.
 */
export const syncAllowlist = async (io: RelayIO, customers: Customer[]): Promise<number> => {
  let changed = 0
  for (const customer of customers) {
    for (const requester of customer.requesters) {
      const allow = requester.status === 'APPROVED' && requester.allowlist !== 'ALLOWED'
      const remove = requester.status === 'REVOKED' && requester.allowlist === 'ALLOWED'
      if (!allow && !remove) continue
      try {
        const hash = await io.setRequester(requester.address as Address, allow)
        await io.reportAllowlist(requester.id, allow, hash)
        changed += 1
        io.log(`allowlist: ${allow ? 'allowed' : 'removed'} ${requester.address} for ${customer.organizationId} in ${hash}`)
      } catch (error) {
        io.log(`allowlist: ${requester.address} for ${customer.organizationId} failed: ${(error as Error).message}`)
      }
    }
  }
  return changed
}

/**
 * Receipts: looks up every open request at Beam, and in customers mode every customer transfer owed a receipt,
 * then writes the finished ones in reports of at most `MAX_ENTRIES_PER_REPORT` entries. A job's two Room receipts
 * precede its result, so a result always lands in the report of its receipts or a later one.
 *
 * Each subject has an owner. Beam's own (requests the relay's account opened, or every request outside customers
 * mode) are read with Beam's key and never charged. A customer's are read with its key and charged one credit:
 * held before the report is written, committed once it is mined. A refused charge leaves the request open, and a
 * listed transfer is listed again. A failed write keeps its holds; the next pass finds them and writes again, and a
 * receipt the contract already holds is committed too, since it is onchain.
 *
 * It also returns the customers' open requests it will never answer, for `closeUnanswerable`: a request to start a
 * transfer (customers mode is receipts only), a request from an address its organization no longer has approved,
 * and a receipt request whose transfer or publication the organization's key has not seen for `ABSENT_GRACE_MS`.
 */
export const runReceipts = async (
  io: RelayIO,
  settings: RelaySettings,
  customers: Customer[],
  now = Date.now(),
): Promise<{ recorded: number; unanswerable: Unanswerable[] }> => {
  const approvedBy = new Map<string, Customer>()
  const revokedBy = new Map<string, Customer>()
  for (const customer of customers) {
    for (const requester of customer.requesters) {
      if (requester.status === 'APPROVED') approvedBy.set(requester.address, customer)
      if (requester.status === 'REVOKED') revokedBy.set(requester.address, customer)
    }
  }

  const pending = new Map<string, Pending>()
  const add = (item: Pending) => {
    const key = subjectKey(item.subject)
    const existing = pending.get(key)
    if (!existing || (!existing.subject.requestId && item.subject.requestId)) pending.set(key, item)
  }

  const unanswerable: Unanswerable[] = []
  let strangers = 0
  for (const request of await io.openRequests()) {
    const requester = request.requester.toLowerCase()
    let owner: Owner = { kind: 'beam' }
    if (settings.customers && requester !== settings.relayAccount.toLowerCase()) {
      const customer = approvedBy.get(requester)
      const revoked = customer ? undefined : revokedBy.get(requester)
      if (revoked) {
        unanswerable.push({ request, organizationId: revoked.organizationId, reason: 'its address is no longer approved' })
        continue
      }
      if (!customer) {
        strangers += 1
        continue
      }
      if (request.kind === RequestKind.Transfer) {
        unanswerable.push({ request, organizationId: customer.organizationId, reason: 'it asks to start a transfer, which this relay never does' })
        continue
      }
      owner = { kind: 'customer', organizationId: customer.organizationId, reader: customer.reader, via: { kind: 'requester', address: request.requester } }
    }
    const subject = subjectForOpenRequest(settings.clientId, settings.routes, request)
    if (subject) add({ subject, owner, request: owner.kind === 'customer' ? request : undefined })
  }
  if (strangers) io.log(`receipt: ${strangers} open requests stay open: from an address no organization registered`)

  // A customer's request for something its key cannot see, once given time to appear.
  const unseen = ({ owner, request, subject }: Pending) => {
    if (owner.kind !== 'customer' || !request || now - Number(request.openedAt) * 1000 < ABSENT_GRACE_MS) return
    unanswerable.push({ request, organizationId: owner.organizationId, reason: `its key cannot see ${subjectKey(subject)}` })
  }

  // Every transfer: what each organization finished since its cursor, created while its switch was on.
  const cursors = settings.customers ? await io.readTransferCursors() : {}
  const listings: Array<{ customer: Customer; cursor: string; transfers: Array<ListedTransfer & { owed: boolean }> }> = []
  for (const customer of settings.customers ? customers : []) {
    if (!customer.periods.length || !listsTransfers(customer, now)) continue
    const cursor = cursors[customer.organizationId] ?? initialTransferCursor(customer)
    let listed: ListedTransfer[]
    try {
      listed = await io.finishedTransfers(customer.reader, cursor)
    } catch (error) {
      io.log(`receipt: listing ${customer.organizationId}'s transfers failed: ${(error as Error).message}`)
      continue
    }
    const transfers = listed
      .sort((a, b) => Date.parse(a.completedAt) - Date.parse(b.completedAt))
      .slice(0, settings.customers!.maxPerOrganization)
      .map((transfer) => ({ ...transfer, owed: createdDuringPeriods(Date.parse(transfer.createdAt), customer.periods) }))
    for (const transfer of transfers) {
      if (!transfer.owed) continue
      add({
        subject: { kind: 'transfer', transferId: transfer.id, requestId: null },
        owner: {
          kind: 'customer',
          organizationId: customer.organizationId,
          reader: customer.reader,
          via: { kind: 'every_transfer', transferCreatedAt: transfer.createdAt },
        },
        observation: transfer.observation,
      })
    }
    listings.push({ customer, cursor, transfers })
  }

  // Look up what is finished, and hold each customer receipt's credit before writing it.
  const planned: Planned[] = []
  const settled = new Set<string>()
  for (const item of pending.values()) {
    const { subject: requested, owner, observation: listedObservation } = item
    const reader = readerOf(owner)
    if (requested.kind === 'job') {
      const lookups = jobLookups(requested)
      const input = await io.findPublication(lookups.input, reader)
      const output = input.state === 'found' ? await io.findPublication(lookups.output, reader) : input
      if (input.state !== 'found' || output.state !== 'found') continue
      const publications = { input: input.publicationId, output: output.publicationId }
      const observations = {
        input: await io.observe({ kind: 'room', room: requested.room, channel: requested.inputChannel, publication: publications.input, requestId: null }, reader),
        output: await io.observe({ kind: 'room', room: requested.room, channel: requested.outputChannel, publication: publications.output, requestId: null }, reader),
      }
      const jobEntries = jobEntriesFor(requested, publications, observations, settings.levels)
      if (jobEntries) planned.push(...jobEntries.map((entry) => ({ entry, owner, key: subjectKey(requested) })))
      continue
    }
    let subject: Exclude<Subject, RoomRequestSubject | JobSubject>
    if (requested.kind === 'roomRequest') {
      const lookup = await io.findPublication(requested, reader)
      const found = roomSubjectForLookup(requested, lookup)
      if (!found) {
        if (lookup.state === 'absent') unseen(item)
        else io.log(`receipt: ${subjectKey(requested)} not recorded: ${describePublicationLookup(lookup)}`)
        continue
      }
      subject = found
    } else {
      subject = requested
    }
    const observation = listedObservation ?? (await io.observe(subject, reader))
    const entry = reportEntryFor(subject, observation, settings.levels)
    if (!entry) {
      if (observation.state === 'absent') unseen(item)
      else if (observation.state !== 'pending') {
        io.log(`receipt: ${subjectKey(subject)} not recorded: ${describeObservation(observation)}`)
        // A listed transfer that can never be recorded is done with.
        if (listedObservation) settled.add(subjectKey(subject))
      }
      continue
    }
    const charge = owner.kind === 'customer' ? chargeSubjectOf(entry) : null
    if (owner.kind === 'customer' && charge) {
      const held = await io.reserveCharge({ subject: charge, organizationId: owner.organizationId, via: owner.via })
      if (held.state === 'charged' || held.state === 'released') {
        settled.add(subjectKey(subject))
        continue
      }
      if (held.state !== 'held') {
        io.log(
          `receipt: ${subjectKey(subject)} for ${owner.organizationId} waits: charge ${held.state} (${held.state === 'refused' ? held.reason : `HTTP ${held.status}`})`,
        )
        continue
      }
    }
    planned.push({ entry, owner, key: subjectKey(subject) })
  }

  // Write, then settle each customer receipt the report recorded or the contract already held.
  let recorded = 0
  for (let i = 0; i < planned.length; i += MAX_ENTRIES_PER_REPORT) {
    const batch = planned.slice(i, i + MAX_ENTRIES_PER_REPORT)
    const standard = batch.flatMap(({ entry }) => (entry.kind === 'standard' ? [entry.entry] : []))
    const rooms = batch.flatMap(({ entry }) => (entry.kind === 'room' ? [entry.entry] : []))
    const closures = batch.flatMap(({ entry }) => (entry.kind === 'closure' ? [entry.entry] : []))
    const jobs = batch.flatMap(({ entry }) => (entry.kind === 'job' ? [entry.entry] : []))
    let written: { hash: Hex; recorded: Set<string> }
    try {
      written = await io.writeReport(encodeReport(standard, rooms, closures, jobs))
    } catch (error) {
      io.log(`receipt: writing a report failed, its holds stay for the next pass: ${(error as Error).message}`)
      break
    }
    recorded += batch.length
    io.log(`receipt: recorded ${standard.length + rooms.length} receipts, ${jobs.length} jobs and ${closures.length} closures in ${written.hash}`)
    for (const { entry, owner, key } of batch) {
      const charge = owner.kind === 'customer' ? chargeSubjectOf(entry) : null
      if (owner.kind !== 'customer' || !charge) continue
      const committed = await io.commitCharge(charge)
      if (committed.state === 'charged') {
        settled.add(key)
        if (!written.recorded.has(recordedKey(charge))) io.log(`receipt: ${recordedKey(charge)} was already onchain; charged its hold`)
      } else {
        io.log(`receipt: committing ${recordedKey(charge)} for ${owner.organizationId} failed (${committed.state}); the next pass settles it`)
      }
    }
  }

  // Move each listing past its transfers that are done: settled, or not owed a receipt.
  if (listings.length) {
    for (const { customer, cursor, transfers } of listings) {
      cursors[customer.organizationId] = nextTransferCursor(
        cursor,
        transfers.map((transfer) => ({
          completedAt: transfer.completedAt,
          done: !transfer.owed || settled.has(subjectKey({ kind: 'transfer', transferId: transfer.id, requestId: null })),
        })),
      )
    }
    await io.saveTransferCursors(cursors)
  }
  return { recorded, unanswerable }
}

/**
 * Customers mode: closes the open requests this relay will never answer, each at one credit to the organization
 * whose address opened it, so Beam pays no gas an organization does not pay for. The credit is held first, and
 * `cancelRequest` is sent only with the hold; a refused charge sends nothing and the request stays open. The charge
 * commits once the cancel is mined. A cancel that fails or reverts (the request was answered or closed meanwhile)
 * is not committed: a request still open is tried again with the same hold, and otherwise the hold expires. A
 * commit that fails is retried on later passes from `state`.
 */
export const closeUnanswerable = async (io: RelayIO, unanswerable: Unanswerable[], state: RelayState): Promise<number> => {
  for (const [requestKey, organizationId] of state.uncommittedCloses) {
    const committed = await io.commitCharge({ kind: 'request', requestKey })
    if (committed.state === 'unavailable') continue
    state.uncommittedCloses.delete(requestKey)
    if (committed.state !== 'charged') io.log(`close: the charge for ${requestKey} to ${organizationId} did not commit (${committed.state})`)
  }

  let closed = 0
  const seen = new Set<Hex>()
  for (const { request, organizationId, reason } of unanswerable) {
    if (seen.has(request.key)) continue
    seen.add(request.key)
    const subject: ChargeSubject = { kind: 'request', requestKey: request.key }
    const held = await io.reserveCharge({ subject, organizationId, via: { kind: 'requester', address: request.requester } })
    if (held.state !== 'held') {
      const detail = held.state === 'refused' ? `: ${held.reason}` : held.state === 'unavailable' ? ` (HTTP ${held.status})` : ''
      io.log(`close: ${request.key} from ${request.requester} stays open (${reason}); charge ${held.state}${detail}`)
      continue
    }
    let hash: Hex
    try {
      hash = await io.cancelRequest(request.key)
    } catch (error) {
      io.log(`close: cancelling ${request.key} failed, its charge is not committed: ${(error as Error).message}`)
      continue
    }
    closed += 1
    const committed = await io.commitCharge(subject)
    if (committed.state === 'charged') {
      io.log(`close: cancelled ${request.key} from ${request.requester} (${reason}) in ${hash}; charged ${organizationId} one credit`)
    } else {
      state.uncommittedCloses.set(request.key, organizationId)
      io.log(`close: cancelled ${request.key} in ${hash}; committing its charge failed (${committed.state}), the next pass retries`)
    }
  }
  return closed
}

/**
 * One relay pass: starts up to the finalized block when the relay starts transfers; in customers mode, the
 * allowlist; then receipts for whatever is open or owed; then, in customers mode, closes what will never be
 * answered.
 */
export const runOnce = async (io: RelayIO, settings: RelaySettings, state: RelayState) => {
  const finalized = await io.finalizedBlock()
  const started = settings.starts ? await runStarts(io, settings, finalized) : 0
  const customers = settings.customers ? await io.customers() : []
  const allowlisted = settings.customers ? await syncAllowlist(io, customers) : 0
  const { recorded, unanswerable } = await runReceipts(io, settings, customers)
  const closed = settings.customers ? await closeUnanswerable(io, unanswerable, state) : 0
  return { finalized, started, allowlisted, recorded, closed }
}
