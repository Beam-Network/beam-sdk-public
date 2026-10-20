import type { Address, Hex } from 'viem'
import type { StudioOutcome } from '../workflows/start-transfer/studio'
import type { Observation, PublicationLookup } from '../workflows/transfer-receipts/beam'
import { encodeReport, type ReceiptLevels, TransferKind } from '../workflows/transfer-receipts/receipts'
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
  uniqueSubjects,
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
  /** The coordinator's lookup of the publication a Room delivery request started. */
  findPublication(subject: RoomRequestSubject): Promise<PublicationLookup>
  observe(subject: Exclude<Subject, RoomRequestSubject | JobSubject>): Promise<Observation>
  /** Sends one report to `onReport` and waits until it is mined; returns the transaction hash. */
  writeReport(report: Hex): Promise<Hex>
  readCursor(): Promise<bigint | null>
  saveCursor(nextBlock: bigint): Promise<void>
  log(message: string): void
}

export type RelaySettings = {
  startBlock: bigint
  logRangeBlocks: bigint
  clientId: string
  levels: ReceiptLevels
  /** Where this deployment's Room deliveries and jobs publish; each is null when it starts none. */
  routes: DeploymentRoutes
}

/**
 * Entries per report, at most one subject each. The contract's gas test covers 14 of each kind, the CRE
 * workflow's per-execution maximum; the relay keeps the same bound.
 */
export const MAX_ENTRIES_PER_REPORT = 14

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

/**
 * Receipts: looks up every open request at Beam and writes the finished ones, in reports of at most
 * `MAX_ENTRIES_PER_REPORT` entries. A job's two Room receipts precede its result, so a result always lands in the
 * report of its receipts or a later one. The contract ignores entries it already holds, so a repeat is harmless.
 */
export const runReceipts = async (io: RelayIO, settings: RelaySettings): Promise<number> => {
  const open = await io.openRequests()
  const subjects = uniqueSubjects(
    open.flatMap((request) => subjectForOpenRequest(settings.clientId, settings.routes, request) ?? []),
  )
  const entries: ReportEntry[] = []
  for (const requested of subjects) {
    if (requested.kind === 'job') {
      const lookups = jobLookups(requested)
      const input = await io.findPublication(lookups.input)
      const output = input.state === 'found' ? await io.findPublication(lookups.output) : input
      if (input.state !== 'found' || output.state !== 'found') continue
      const publications = { input: input.publicationId, output: output.publicationId }
      const observations = {
        input: await io.observe({ kind: 'room', room: requested.room, channel: requested.inputChannel, publication: publications.input, requestId: null }),
        output: await io.observe({ kind: 'room', room: requested.room, channel: requested.outputChannel, publication: publications.output, requestId: null }),
      }
      const jobEntries = jobEntriesFor(requested, publications, observations, settings.levels)
      if (jobEntries) entries.push(...jobEntries)
      continue
    }
    let subject: Exclude<Subject, RoomRequestSubject | JobSubject>
    if (requested.kind === 'roomRequest') {
      const lookup = await io.findPublication(requested)
      const found = roomSubjectForLookup(requested, lookup)
      if (!found) {
        if (lookup.state !== 'absent') io.log(`receipt: ${subjectKey(requested)} not recorded: ${describePublicationLookup(lookup)}`)
        continue
      }
      subject = found
    } else {
      subject = requested
    }
    const observation = await io.observe(subject)
    const entry = reportEntryFor(subject, observation, settings.levels)
    if (entry) entries.push(entry)
    else if (observation.state !== 'pending' && observation.state !== 'absent') {
      io.log(`receipt: ${subjectKey(subject)} not recorded: ${describeObservation(observation)}`)
    }
  }
  for (let i = 0; i < entries.length; i += MAX_ENTRIES_PER_REPORT) {
    const batch = entries.slice(i, i + MAX_ENTRIES_PER_REPORT)
    const standard = batch.flatMap((entry) => (entry.kind === 'standard' ? [entry.entry] : []))
    const rooms = batch.flatMap((entry) => (entry.kind === 'room' ? [entry.entry] : []))
    const closures = batch.flatMap((entry) => (entry.kind === 'closure' ? [entry.entry] : []))
    const jobs = batch.flatMap((entry) => (entry.kind === 'job' ? [entry.entry] : []))
    const txHash = await io.writeReport(encodeReport(standard, rooms, closures, jobs))
    io.log(`receipt: recorded ${standard.length + rooms.length} receipts, ${jobs.length} jobs and ${closures.length} closures in ${txHash}`)
  }
  return entries.length
}

/** One relay pass: starts up to the finalized block, then receipts for whatever is open. */
export const runOnce = async (io: RelayIO, settings: RelaySettings) => {
  const finalized = await io.finalizedBlock()
  const started = await runStarts(io, settings, finalized)
  const recorded = await runReceipts(io, settings)
  return { finalized, started, recorded }
}
