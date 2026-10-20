import {
  bytesToHex,
  type CronPayload,
  consensusIdenticalAggregation,
  cre,
  decodeJson,
  type EVMLog,
  encodeCallMsg,
  getNetwork,
  type HTTPPayload,
  LAST_FINALIZED_BLOCK_NUMBER,
  logTriggerConfig,
  prepareReportRequest,
  type Runtime,
  TxStatus,
} from '@chainlink/cre-sdk'
import { EVM_PB } from '@chainlink/cre-sdk/pb'
import {
  type Address,
  decodeEventLog,
  decodeFunctionResult,
  encodeFunctionData,
  type Hex,
  isAddress,
  zeroAddress,
} from 'viem'
import { z } from 'zod'
import { type AttestationRequest, attestationSubject, MAX_ATTESTATIONS } from './attestations'
import { type Observation, type PublicationLookup, UNAVAILABLE } from './beam'
import {
  deliverReport,
  observeClientId,
  observePublicationByKey,
  observeRoomReceipt,
  observeTransfer,
} from './observe'
import {
  encodeReport,
  RECEIPT_REQUESTED_TOPIC,
  ROOM_RECEIPT_REQUESTED_TOPIC,
  receiptLevels,
  receiptsAbi,
} from './receipts'
import {
  describeObservation,
  describePublicationLookup,
  jobEntriesFor,
  jobLookups,
  type OpenRequest,
  type ReportEntry,
  type RoomRequestSubject,
  reportEntryFor,
  type RoomSubject,
  roomRequestPublicationKey,
  roomSubjectForLookup,
  type Subject,
  subjectForOpenRequest,
  subjectKey,
  type TransferSubject,
  uniqueSubjects,
} from './subjects'
import { bytes16ToTransferId, normalizeRequestId, normalizeTransferId, transferIdForRequest } from './transfer-id'

// ─── Config ──────────────────────────────────────────────────

/**
 * HTTP requests one execution spends on subjects. CRE allows 15 per execution; one is kept for the
 * `GET /auth/me` client id check that request-id lookups need. A Room delivery request costs two: the
 * publication lookup, then its receipt. A job costs four: the lookups and receipts of its input and output.
 */
export const MAX_SUBJECTS = 14

const SUBJECT_COSTS: Record<Subject['kind'], number> = { transfer: 1, room: 1, roomRequest: 2, job: 4 }
const subjectCost = (subject: Subject): number => SUBJECT_COSTS[subject.kind]
const memberId = z.string().regex(/^btr_member_[a-z2-7]{26}$/)

/** A CRE workflow writes DON-signed reports, so its receipts carry the DON-attested level. */
const LEVELS = receiptLevels(true)

const API_KEY_SECRET = 'BEAM_API_KEY'
const ORG_ID_SECRET = 'BEAM_ORG_ID'

const address = z.string().refine((value) => isAddress(value, { strict: false }), 'must be an EVM address')
const baseUrl = z
  .string()
  .regex(/^https?:\/\/[^/\s]+(\/\S*)?$/, 'must be an http(s) URL')
  .transform((value) => value.replace(/\/+$/, ''))
const roomId = z.string().min(1).max(128)

export const configSchema = z.object({
  evms: z
    .array(
      z.object({
        chainSelectorName: z.string().min(1),
        receiptsAddress: address,
        gasLimit: z.string().regex(/^[1-9][0-9]*$/),
      }),
    )
    .length(1),
  /** BeamCore public API, which serves `GET /transfers/:id` and `GET /auth/me`. */
  beamApiBaseUrl: baseUrl,
  /** Beam coordinator, which serves the Room receipt route. */
  roomApiBaseUrl: baseUrl,
  /** `user_id` from `GET /auth/me` for the API key; transfer ids derive from it. */
  beamClientId: z.string().min(1),
  reconcileIntervalMinutes: z
    .number()
    .int()
    .min(1)
    .max(60)
    .refine((minutes) => 60 % minutes === 0, 'must divide 60'),
  reconcileBatchSize: z.number().int().min(1).max(MAX_SUBJECTS),
  /** EVM addresses allowed to call the HTTP trigger. Deployed workflows need at least one. */
  httpTriggerAuthorizedKeys: z.array(address),
  /**
   * The Room route of this deployment's Studio Room workflow, whose step binds `publicationKey` to the request
   * id; `reconcile` then finds Room delivery requests' publications. Null when the deployment starts none.
   */
  roomDelivery: z.object({ room: roomId, channel: roomId, publisherMemberId: memberId }).strict().nullable(),
  /**
   * Where this deployment's jobs publish their output: an object channel of the `roomDelivery` Room, and the compute
   * member whose Studio Room workflow publishes the output under the job's request key. Null when it runs no jobs.
   */
  jobOutput: z.object({ channel: roomId, computeMemberId: memberId }).strict().nullable(),
  /**
   * The results endpoint of the Room bridge (`packages/cre/bridge`) that asks this deployment for attestations;
   * each answer's report is sent there. Null when no bridge does.
   */
  attestationReplyUrl: baseUrl.nullable(),
})
  .refine((config) => !config.jobOutput || config.roomDelivery, {
    message: 'jobOutput needs roomDelivery, the route a job\'s input is published on',
    path: ['jobOutput'],
  })
  .refine(
    (config) => config.reconcileBatchSize * (config.jobOutput ? SUBJECT_COSTS.job : config.roomDelivery ? SUBJECT_COSTS.roomRequest : 1) <= MAX_SUBJECTS,
    {
      message: `reconcileBatchSize is at most ${Math.floor(MAX_SUBJECTS / SUBJECT_COSTS.roomRequest)} with roomDelivery and ${Math.floor(MAX_SUBJECTS / SUBJECT_COSTS.job)} with jobOutput, whose requests cost ${SUBJECT_COSTS.roomRequest} and ${SUBJECT_COSTS.job} reads`,
      path: ['reconcileBatchSize'],
    },
  )

export type Config = z.infer<typeof configSchema>

/** HTTP trigger body: transfers by request id or transfer id, and Room publications. */
const pushSchema = z
  .object({
    transfers: z
      .array(z.union([z.object({ requestId: z.string() }).strict(), z.object({ transferId: z.string() }).strict()]))
      .default([]),
    rooms: z
      .array(
        z.object({ room: roomId, channel: roomId, publication: roomId, requestId: z.string().optional() }).strict(),
      )
      .default([]),
  })
  .strict()

/** HTTP trigger body from a Room bridge: what to attest. */
export const attestSchema = z
  .object({
    attest: z
      .array(
        z.discriminatedUnion('op', [
          z.object({ op: z.literal('attest_transfer'), transferId: z.string() }).strict(),
          z
            .object({ op: z.literal('attest_room_publication'), room: roomId, channel: roomId, publication: roomId })
            .strict(),
        ]),
      )
      .min(1)
      .max(MAX_ATTESTATIONS),
    publishOnchain: z.boolean().default(false),
  })
  .strict()

// ─── Handlers ────────────────────────────────────────────────

/** EVM log trigger: a `requestReceipt` or `requestRoomReceipt` call asks for one receipt now. */
export const onReceiptRequested = (runtime: Runtime<Config>, log: EVMLog): string => {
  if (log.removed) return 'Ignored a request log removed by a reorg'
  const event = decodeEventLog({
    abi: receiptsAbi,
    data: bytesToHex(log.data),
    topics: log.topics.map((topic) => bytesToHex(topic)) as [Hex, ...Hex[]],
  })
  if (event.eventName === 'ReceiptRequested') {
    const subject: Subject = { kind: 'transfer', transferId: bytes16ToTransferId(event.args.transferId), requestId: null }
    return recordReceipts(runtime, [subject], 'log')
  }
  const { room, channel, publication } = event.args
  return recordReceipts(runtime, [{ kind: 'room', room, channel, publication, requestId: null }], 'log')
}

/**
 * Cron trigger: checks a window of the contract's open requests, so a receipt whose push was lost is
 * still recorded. The window advances by `reconcileBatchSize` every run and wraps around the set, so
 * every open request is checked at least once every ceil(open / batch) runs. With a `roomDelivery` route, a
 * Room delivery request's publication is found by the route's publisher and the request's publication key;
 * without one, those requests are skipped and only a push answers them.
 */
export const onReconcile = (runtime: Runtime<Config>, payload: CronPayload): string => {
  const { evms, reconcileBatchSize, reconcileIntervalMinutes } = runtime.config
  const evm = evms[0]
  const evmClient = evmClientFor(evm.chainSelectorName)
  const receipts = evm.receiptsAddress as Address

  const total = readOpenRequestCount(runtime, evmClient, receipts)
  if (total === 0n) return 'reconcile: no open requests'

  const batch = BigInt(reconcileBatchSize)
  const size = total < batch ? total : batch
  const scheduledSeconds =
    payload.scheduledExecutionTime?.seconds ?? BigInt(Math.floor(runtime.now().getTime() / 1000))
  const slot = scheduledSeconds / BigInt(reconcileIntervalMinutes * 60)
  const start = (slot * batch) % total

  let page = readOpenRequests(runtime, evmClient, receipts, start, size)
  if (BigInt(page.length) < size) {
    page = page.concat(readOpenRequests(runtime, evmClient, receipts, 0n, size - BigInt(page.length)))
  }
  const routes = { roomDelivery: runtime.config.roomDelivery, jobOutput: runtime.config.jobOutput }
  const subjects = page.flatMap((request) => subjectForOpenRequest(runtime.config.beamClientId, routes, request) ?? [])
  runtime.log(`reconcile: ${total} open, ${page.length} in the window from offset ${start}, ${subjects.length} to check`)
  return recordReceipts(runtime, subjects, 'reconcile')
}

/**
 * HTTP trigger: a push from a workflow that saw a transfer finish, or attestation requests from a Room bridge.
 * The body only names what to look up; every field that is recorded or attested comes from Beam.
 */
export const onReceiptPush = (runtime: Runtime<Config>, payload: HTTPPayload): string => {
  const body: unknown = decodeJson(payload.input)
  if (typeof body === 'object' && body !== null && 'attest' in body) {
    return answerAttestations(runtime, attestSchema.parse(body))
  }
  const push = pushSchema.parse(body)
  const subjects: Subject[] = [
    ...push.transfers.map((transfer): Subject => {
      if ('requestId' in transfer) {
        const requestId = normalizeRequestId(transfer.requestId)
        return {
          kind: 'transfer',
          transferId: transferIdForRequest(runtime.config.beamClientId, requestId),
          requestId,
        }
      }
      return { kind: 'transfer', transferId: normalizeTransferId(transfer.transferId), requestId: null }
    }),
    ...push.rooms.map(
      (room): Subject => ({
        kind: 'room',
        room: room.room,
        channel: room.channel,
        publication: room.publication,
        requestId: room.requestId === undefined ? null : normalizeRequestId(room.requestId),
      }),
    ),
  ]
  if (subjects.length === 0) throw new Error('The push names no transfers or rooms')
  if (subjects.length > MAX_SUBJECTS) throw new Error(`A push names at most ${MAX_SUBJECTS} receipts`)
  return recordReceipts(runtime, subjects, 'push')
}

// ─── Receipts ────────────────────────────────────────────────

/**
 * Looks every subject up at Beam under identical consensus and writes the finished ones, and the Room
 * receipt requests Beam settled without a receipt, in one report.
 */
const recordReceipts = (runtime: Runtime<Config>, requested: Subject[], source: string): string => {
  const config = runtime.config
  const subjects = uniqueSubjects(requested)
  if (subjects.reduce((cost, subject) => cost + subjectCost(subject), 0) > MAX_SUBJECTS) {
    throw new Error(`At most ${MAX_SUBJECTS} lookups per execution`)
  }

  const needsRoom = subjects.some((subject) => subject.kind !== 'transfer')
  const secrets = runtime
    .getSecrets(needsRoom ? [{ id: API_KEY_SECRET }, { id: ORG_ID_SECRET }] : [{ id: API_KEY_SECRET }])
    .result()
  const apiKey = secrets[API_KEY_SECRET].value
  const organizationId = needsRoom ? secrets[ORG_ID_SECRET].value : ''

  const http = new cre.capabilities.HTTPClient()
  const derivesIds = subjects.some((subject) => subject.kind === 'transfer' && subject.requestId !== null)
  const clientIdConfirmed = derivesIds && confirmClientId(runtime, http, apiKey)

  const aggregation = consensusIdenticalAggregation<string>().withDefault(UNAVAILABLE)
  const credentials = { apiKey, organizationId }
  const lookUp = (lookup: RoomRequestSubject): PublicationLookup =>
    JSON.parse(
      http
        .sendRequest(runtime, observePublicationByKey, aggregation)({
          baseUrl: config.roomApiBaseUrl,
          apiKey,
          organizationId,
          room: lookup.room,
          channel: lookup.channel,
          publisherMemberId: lookup.publisherMemberId,
          key: roomRequestPublicationKey(lookup.requestId),
        })
        .result(),
    ) as PublicationLookup
  const entries: ReportEntry[] = []
  for (const requestedSubject of subjects) {
    let subject = requestedSubject
    if (subject.kind === 'job') {
      const lookups = jobLookups(subject)
      const input = lookUp(lookups.input)
      const output = input.state === 'found' ? lookUp(lookups.output) : input
      if (input.state !== 'found' || output.state !== 'found') {
        runtime.log(`${source}: ${subjectKey(subject)} not recorded: ${describePublicationLookup(input.state !== 'found' ? input : output)}`)
        continue
      }
      const publications = { input: input.publicationId, output: output.publicationId }
      const observations = {
        input: observeSubject(runtime, http, credentials, { kind: 'room', room: subject.room, channel: subject.inputChannel, publication: publications.input, requestId: null }),
        output: observeSubject(runtime, http, credentials, { kind: 'room', room: subject.room, channel: subject.outputChannel, publication: publications.output, requestId: null }),
      }
      const jobEntries = jobEntriesFor(subject, publications, observations, LEVELS)
      if (jobEntries) entries.push(...jobEntries)
      else runtime.log(`${source}: ${subjectKey(subject)} not recorded: input ${describeObservation(observations.input)}, output ${describeObservation(observations.output)}`)
      continue
    }
    if (subject.kind === 'roomRequest') {
      const lookup = lookUp(subject)
      const found = roomSubjectForLookup(subject, lookup)
      if (!found) {
        runtime.log(`${source}: ${subjectKey(subject)} not recorded: ${describePublicationLookup(lookup)}`)
        continue
      }
      subject = found
    }
    if (subject.kind === 'transfer' && subject.requestId !== null && !clientIdConfirmed) continue
    const observation = observeSubject(runtime, http, credentials, subject)
    const entry = reportEntryFor(subject, observation, LEVELS)
    if (entry) entries.push(entry)
    if (entry?.kind !== 'standard' && entry?.kind !== 'room') {
      runtime.log(`${source}: ${subjectKey(subject)} not recorded: ${describeObservation(observation)}`)
    }
  }

  if (entries.length === 0) return `${source}: checked ${subjects.length}, nothing finished yet`
  const count = (kind: ReportEntry['kind']) => entries.filter((entry) => entry.kind === kind).length
  const txHash = writeReceiptReport(runtime, encodeEntries(entries))
  return `${source}: checked ${subjects.length}, reported ${count('standard') + count('room')} receipts, ${count('job')} jobs and ${count('closure')} closures in ${txHash}`
}

/**
 * HTTP trigger, from a Room bridge: looks each subject up at Beam like a receipt, and sends the bridge one
 * DON-signed report per finished subject, so a reply in one Room carries nothing about another. Unfinished and
 * unknown subjects get no report; the bridge asks again. With `publishOnchain`, the answers are also written
 * to the contract, in one report.
 */
const answerAttestations = (runtime: Runtime<Config>, request: AttestationRequest): string => {
  const replyUrl = runtime.config.attestationReplyUrl
  if (!replyUrl) throw new Error('Set attestationReplyUrl to answer attestation requests')
  const subjects = uniqueSubjects(request.attest.map(attestationSubject)) as (TransferSubject | RoomSubject)[]

  const needsRoom = subjects.some((subject) => subject.kind === 'room')
  const secrets = runtime
    .getSecrets(needsRoom ? [{ id: API_KEY_SECRET }, { id: ORG_ID_SECRET }] : [{ id: API_KEY_SECRET }])
    .result()
  const credentials = {
    apiKey: secrets[API_KEY_SECRET].value,
    organizationId: needsRoom ? secrets[ORG_ID_SECRET].value : '',
  }

  const http = new cre.capabilities.HTTPClient()
  const delivery = consensusIdenticalAggregation<string>().withDefault('0')
  const answered: ReportEntry[] = []
  let delivered = 0
  for (const subject of subjects) {
    const observation = observeSubject(runtime, http, credentials, subject)
    const entry = reportEntryFor(subject, observation, LEVELS)
    if (!entry) {
      runtime.log(`attest: ${subjectKey(subject)} not attested: ${describeObservation(observation)}`)
      continue
    }
    answered.push(entry)
    const report = runtime.report(prepareReportRequest(encodeEntries([entry]))).result()
    const status = Number(http.sendRequest(runtime, deliverReport, delivery)({ url: replyUrl, report }).result())
    if (status >= 200 && status < 300) delivered += 1
    else runtime.log(`attest: the bridge answered ${status ? `HTTP ${status}` : 'nothing the nodes agree on'} for ${subjectKey(subject)}`)
  }

  const summary = `attest: checked ${subjects.length}, delivered ${delivered} of ${answered.length} reports`
  if (!request.publishOnchain || answered.length === 0) return summary
  return `${summary}, recorded onchain in ${writeReceiptReport(runtime, encodeEntries(answered))}`
}

/** One subject's Beam observation under identical consensus. */
const observeSubject = (
  runtime: Runtime<Config>,
  http: InstanceType<typeof cre.capabilities.HTTPClient>,
  credentials: { apiKey: string; organizationId: string },
  subject: TransferSubject | RoomSubject,
): Observation => {
  const aggregation = consensusIdenticalAggregation<string>().withDefault(UNAVAILABLE)
  const observed =
    subject.kind === 'transfer'
      ? http.sendRequest(runtime, observeTransfer, aggregation)({
          baseUrl: runtime.config.beamApiBaseUrl,
          apiKey: credentials.apiKey,
          transferId: subject.transferId,
        })
      : http.sendRequest(runtime, observeRoomReceipt, aggregation)({
          baseUrl: runtime.config.roomApiBaseUrl,
          apiKey: credentials.apiKey,
          organizationId: credentials.organizationId,
          room: subject.room,
          channel: subject.channel,
          publication: subject.publication,
        })
  return JSON.parse(observed.result()) as Observation
}

const encodeEntries = (entries: ReportEntry[]): Hex =>
  encodeReport(
    entries.flatMap((entry) => (entry.kind === 'standard' ? [entry.entry] : [])),
    entries.flatMap((entry) => (entry.kind === 'room' ? [entry.entry] : [])),
    entries.flatMap((entry) => (entry.kind === 'closure' ? [entry.entry] : [])),
    entries.flatMap((entry) => (entry.kind === 'job' ? [entry.entry] : [])),
  )

/**
 * Transfer ids derive from the client id in config, so a key from another account would silently
 * look up ids that do not exist. Request-id lookups run only when `GET /auth/me` agrees.
 */
const confirmClientId = (
  runtime: Runtime<Config>,
  http: InstanceType<typeof cre.capabilities.HTTPClient>,
  apiKey: string,
): boolean => {
  const observed = http
    .sendRequest(runtime, observeClientId, consensusIdenticalAggregation<string>().withDefault(UNAVAILABLE))({
      baseUrl: runtime.config.beamApiBaseUrl,
      apiKey,
    })
    .result()
  const identity = JSON.parse(observed) as { userId?: string | null; httpStatus?: number }
  if (identity.userId === runtime.config.beamClientId) return true
  runtime.log(
    identity.userId
      ? 'beamClientId does not match GET /auth/me for BEAM_API_KEY; skipping request-id lookups'
      : `GET /auth/me did not confirm the client id (HTTP ${identity.httpStatus ?? 'unavailable'}); skipping request-id lookups`,
  )
  return false
}

// ─── EVM ─────────────────────────────────────────────────────

const evmClientFor = (chainSelectorName: string) => {
  const network = getNetwork({ chainFamily: 'evm', chainSelectorName })
  if (!network) throw new Error(`Network not found: ${chainSelectorName}`)
  return new cre.capabilities.EVMClient(network.chainSelector.selector)
}

type EVMClient = ReturnType<typeof evmClientFor>

const callView = (runtime: Runtime<Config>, evmClient: EVMClient, to: Address, data: Hex): Hex => {
  const reply = evmClient
    .callContract(runtime, {
      call: encodeCallMsg({ from: zeroAddress, to, data }),
      blockNumber: LAST_FINALIZED_BLOCK_NUMBER,
    })
    .result()
  const result = bytesToHex(reply.data)
  if (result === '0x') throw new Error(`No BeamTransferReceipts contract answered at ${to}`)
  return result
}

const readOpenRequestCount = (runtime: Runtime<Config>, evmClient: EVMClient, to: Address): bigint =>
  decodeFunctionResult({
    abi: receiptsAbi,
    functionName: 'openRequestCount',
    data: callView(runtime, evmClient, to, encodeFunctionData({ abi: receiptsAbi, functionName: 'openRequestCount' })),
  })

const readOpenRequests = (
  runtime: Runtime<Config>,
  evmClient: EVMClient,
  to: Address,
  offset: bigint,
  limit: bigint,
): OpenRequest[] => [
  ...decodeFunctionResult({
    abi: receiptsAbi,
    functionName: 'openRequests',
    data: callView(
      runtime,
      evmClient,
      to,
      encodeFunctionData({ abi: receiptsAbi, functionName: 'openRequests', args: [offset, limit] }),
    ),
  }),
]

/** Signs a report and writes it to the receipts contract; returns the transaction hash. */
const writeReceiptReport = (runtime: Runtime<Config>, encoded: Hex): Hex => {
  const evm = runtime.config.evms[0]
  const report = runtime.report(prepareReportRequest(encoded)).result()
  const reply = evmClientFor(evm.chainSelectorName)
    .writeReport(runtime, {
      receiver: evm.receiptsAddress,
      report,
      gasConfig: { gasLimit: evm.gasLimit },
    })
    .result()
  if (reply.txStatus !== TxStatus.SUCCESS) {
    throw new Error(`Receipt write failed: ${reply.errorMessage ?? TxStatus[reply.txStatus]}`)
  }
  if (reply.receiverContractExecutionStatus === EVM_PB.ReceiverContractExecutionStatus.REVERTED) {
    throw new Error(`BeamTransferReceipts reverted the report: ${reply.errorMessage ?? 'no reason given'}`)
  }
  return bytesToHex(reply.txHash ?? new Uint8Array(32))
}

// ─── Workflow ────────────────────────────────────────────────

const reconcileSchedule = (minutes: number): string => (minutes === 60 ? '0 0 * * * *' : `0 */${minutes} * * * *`)

export const initWorkflow = (config: Config) => {
  const evm = config.evms[0]
  if (evm.receiptsAddress.toLowerCase() === zeroAddress) {
    throw new Error('Set evms[0].receiptsAddress to the deployed BeamTransferReceipts contract')
  }
  const evmClient = evmClientFor(evm.chainSelectorName)
  const cron = new cre.capabilities.CronCapability()
  const http = new cre.capabilities.HTTPCapability()

  return [
    cre.handler(
      evmClient.logTrigger(
        logTriggerConfig({
          addresses: [evm.receiptsAddress as Hex],
          topics: [[RECEIPT_REQUESTED_TOPIC, ROOM_RECEIPT_REQUESTED_TOPIC]],
        }),
      ),
      onReceiptRequested,
    ),
    cre.handler(cron.trigger({ schedule: reconcileSchedule(config.reconcileIntervalMinutes) }), onReconcile),
    cre.handler(
      http.trigger({
        authorizedKeys: config.httpTriggerAuthorizedKeys.map((publicKey) => ({
          type: 'KEY_TYPE_ECDSA_EVM' as const,
          publicKey,
        })),
      }),
      onReceiptPush,
    ),
  ]
}
