import type { Address, Hex } from 'viem'
import { type Observation, type PublicationLookup, ZERO_BYTES16, ZERO_BYTES32 } from './beam'
import {
  ClosureReason,
  type JobResultEntry,
  type ReceiptLevels,
  RequestKind,
  type RequestClosureEntry,
  type RoomReceiptEntry,
  roomReceiptKey,
  roomRequestKey,
  type StandardReceiptEntry,
  TransferKind,
} from './receipts'
import { bytes16ToTransferId, normalizeRequestId, transferIdForRequest, transferIdToBytes16 } from './transfer-id'

// Pure receipt logic shared by the CRE workflow and the relay, which differ only in how they reach Beam
// and the chain.

export type TransferSubject = { kind: 'transfer'; transferId: string; requestId: Hex | null }
export type RoomSubject = { kind: 'room'; room: string; channel: string; publication: string; requestId: Hex | null }
/** A Room delivery request: its publication is found by the route's publisher and the request's key first. */
export type RoomRequestSubject = { kind: 'roomRequest'; room: string; channel: string; publisherMemberId: string; requestId: Hex }
/**
 * A job request: its input is the Room delivery the source publishes under the request's key, and its output the
 * publication the compute member publishes under the same key, in the job output channel.
 */
export type JobSubject = {
  kind: 'job'
  room: string
  inputChannel: string
  sourceMemberId: string
  outputChannel: string
  computeMemberId: string
  requestId: Hex
}
export type Subject = TransferSubject | RoomSubject | RoomRequestSubject | JobSubject

/**
 * Where a deployment's Room deliveries publish: the room and object channel of its Studio Room workflow, and
 * the source member that publishes them.
 */
export type RoomRoute = { room: string; channel: string; publisherMemberId: string }

/**
 * Where a deployment's jobs publish their output: an object channel of the Room of its Room deliveries, and the
 * compute member that publishes there under the job's request key.
 */
export type JobOutputRoute = { channel: string; computeMemberId: string }

/** The routes a deployment reads Room deliveries and jobs through; either is null when it has none. */
export type DeploymentRoutes = { roomDelivery: RoomRoute | null; jobOutput: JobOutputRoute | null }

/**
 * The publication key Studio publishes a Room delivery under when its step binds `publicationKey` to
 * `${workflow.input.requestId}`: `request:` and the request id as `start-transfer` sends it.
 */
export const roomRequestPublicationKey = (requestId: Hex): string => `request:${normalizeRequestId(requestId)}`

/** `BeamTransferReceipts.OpenRequest`, as `openRequests` returns it. */
export type OpenRequest = {
  key: Hex
  kind: number
  transferKind: number
  requester: Address
  openedAt: bigint
  requestId: Hex
  transferId: Hex
  room: string
  channel: string
  publication: string
}

export const subjectKey = (subject: Subject): string =>
  subject.kind === 'transfer'
    ? `transfer:${subject.transferId}`
    : subject.kind === 'room'
      ? `room:${JSON.stringify([subject.room, subject.channel, subject.publication])}`
      : `${subject.kind}:${subject.requestId}`

/** Drops repeats, keeping the request id when one copy carries it. */
export const uniqueSubjects = (subjects: Subject[]): Subject[] => {
  const unique = new Map<string, Subject>()
  for (const subject of subjects) {
    const key = subjectKey(subject)
    const existing = unique.get(key)
    if (!existing || (!existing.requestId && subject.requestId)) unique.set(key, subject)
  }
  return [...unique.values()]
}

/**
 * What to look up for one open request. `clientId` is the Beam client id transfer ids derive from. A Room
 * delivery request is looked up through the deployment's Room route, and a job through that route and its job
 * output route; without them it is null, and only a push answers it (`staleRequests()` shows it if none comes).
 */
export const subjectForOpenRequest = (
  clientId: string,
  routes: DeploymentRoutes,
  request: OpenRequest,
): Subject | null => {
  if (request.kind === RequestKind.Transfer) {
    const { roomDelivery, jobOutput } = routes
    if (request.transferKind === TransferKind.RoomDelivery) {
      return roomDelivery ? { kind: 'roomRequest', ...roomDelivery, requestId: request.requestId } : null
    }
    if (request.transferKind === TransferKind.Job) {
      if (!roomDelivery || !jobOutput) return null
      return {
        kind: 'job',
        room: roomDelivery.room,
        inputChannel: roomDelivery.channel,
        sourceMemberId: roomDelivery.publisherMemberId,
        outputChannel: jobOutput.channel,
        computeMemberId: jobOutput.computeMemberId,
        requestId: request.requestId,
      }
    }
    if (request.transferKind !== TransferKind.Standard) return null
    return {
      kind: 'transfer',
      transferId: transferIdForRequest(clientId, request.requestId),
      requestId: request.requestId,
    }
  }
  if (request.kind === RequestKind.Receipt) {
    return { kind: 'transfer', transferId: bytes16ToTransferId(request.transferId), requestId: null }
  }
  return {
    kind: 'room',
    room: request.room,
    channel: request.channel,
    publication: request.publication,
    requestId: null,
  }
}

/**
 * The Room publication a Room delivery request started, once the lookup found it. Its receipt is then read
 * like any Room receipt, and carries the request id, so recording it answers the request.
 */
export const roomSubjectForLookup = (subject: RoomRequestSubject, lookup: PublicationLookup): RoomSubject | null =>
  lookup.state === 'found'
    ? { kind: 'room', room: subject.room, channel: subject.channel, publication: lookup.publicationId, requestId: subject.requestId }
    : null

/** Why a publication lookup found nothing to read, for logs. */
export const describePublicationLookup = (lookup: PublicationLookup): string => {
  switch (lookup.state) {
    case 'absent':
      return 'no publication under its key yet'
    case 'error':
      return lookup.httpStatus === 401 || lookup.httpStatus === 403
        ? `Beam refused the key (HTTP ${lookup.httpStatus}); it needs room:objects:read for the room's organization`
        : `the coordinator answered HTTP ${lookup.httpStatus}`
    case 'invalid':
      return `unexpected response: ${lookup.reason}`
    case 'unavailable':
      return 'no usable answer: CRE nodes did not agree, or the coordinator was unreachable'
    default:
      return lookup.state
  }
}

export type ReportEntry =
  | { kind: 'standard'; entry: StandardReceiptEntry }
  | { kind: 'room'; entry: RoomReceiptEntry }
  | { kind: 'closure'; entry: RequestClosureEntry }
  | { kind: 'job'; entry: JobResultEntry }

/** The two publication lookups of a job: its input by the source, and its output by the compute member. */
export const jobLookups = (subject: JobSubject): { input: RoomRequestSubject; output: RoomRequestSubject } => ({
  input: {
    kind: 'roomRequest',
    room: subject.room,
    channel: subject.inputChannel,
    publisherMemberId: subject.sourceMemberId,
    requestId: subject.requestId,
  },
  output: {
    kind: 'roomRequest',
    room: subject.room,
    channel: subject.outputChannel,
    publisherMemberId: subject.computeMemberId,
    requestId: subject.requestId,
  },
})

/**
 * What answers a job once both its publications are terminal: their two Room receipts, without a request id, and
 * the job result that links them. Null while either is unfinished, absent or not a Room transfer.
 */
export const jobEntriesFor = (
  subject: JobSubject,
  publications: { input: string; output: string },
  observations: { input: Observation; output: Observation },
  levels: ReceiptLevels,
): ReportEntry[] | null => {
  const input: RoomSubject = { kind: 'room', room: subject.room, channel: subject.inputChannel, publication: publications.input, requestId: null }
  const output: RoomSubject = { kind: 'room', room: subject.room, channel: subject.outputChannel, publication: publications.output, requestId: null }
  const inputEntry = reportEntryFor(input, observations.input, levels)
  const outputEntry = reportEntryFor(output, observations.output, levels)
  if (inputEntry?.kind !== 'room' || outputEntry?.kind !== 'room') return null
  return [
    inputEntry,
    outputEntry,
    {
      kind: 'job',
      entry: {
        requestId: subject.requestId,
        inputRoomKey: roomReceiptKey(input.room, input.channel, input.publication),
        outputRoomKey: roomReceiptKey(output.room, output.channel, output.publication),
        computeMember: subject.computeMemberId,
      },
    },
  ]
}

/**
 * What one observation adds to the report, at the levels of the deployment it is written to. A Room subject
 * whose publication ran as a standard transfer closes its Room receipt request, naming that transfer so the
 * requester can ask for its receipt.
 */
export const reportEntryFor = (
  subject: Subject,
  observation: Observation,
  levels: ReceiptLevels,
): ReportEntry | null => {
  if (subject.kind === 'transfer' && observation.state === 'transfer') {
    return {
      kind: 'standard',
      entry: {
        transferId: transferIdToBytes16(subject.transferId),
        requestId: subject.requestId ?? ZERO_BYTES32,
        status: observation.status,
        totalBytes: BigInt(observation.totalBytes),
        bytesTransferred: BigInt(observation.bytesTransferred),
        completedAtMs: BigInt(observation.completedAtMs),
        planFingerprint: observation.planFingerprint,
        coordinateChecksum: observation.coordinateChecksum,
        level: levels.standard,
      },
    }
  }
  if (subject.kind === 'room' && observation.state === 'room') {
    return {
      kind: 'room',
      entry: {
        room: subject.room,
        channel: subject.channel,
        publication: subject.publication,
        requestId: subject.requestId ?? ZERO_BYTES32,
        status: observation.status,
        fileSizeBytes: BigInt(observation.fileSizeBytes),
        manifestSha256: observation.manifestSha256,
        targetCount: observation.targetCount,
        evidenceSha256: observation.evidenceSha256,
        assignmentsSha256: observation.assignmentsSha256,
        evidenceVersion: observation.evidenceVersion,
        level: observation.targetSigned ? levels.roomSigned : levels.room,
      },
    }
  }
  if (subject.kind === 'room' && observation.state === 'not_room') {
    return {
      kind: 'closure',
      entry: {
        key: roomRequestKey(roomReceiptKey(subject.room, subject.channel, subject.publication)),
        reason: ClosureReason.BucketOnlyPublication,
        transferId: observation.transferId ? transferIdToBytes16(observation.transferId) : ZERO_BYTES16,
      },
    }
  }
  return null
}

/** Why an observation records nothing, for logs. */
export const describeObservation = (observation: Observation): string => {
  switch (observation.state) {
    case 'error':
      return observation.httpStatus === 401 || observation.httpStatus === 403
        ? `Beam refused the key (HTTP ${observation.httpStatus}); check its scopes`
        : `Beam answered HTTP ${observation.httpStatus}`
    case 'invalid':
      return `unexpected response: ${observation.reason}`
    case 'not_room':
      return `bucket-only publication with no Room receipt; closing its Room receipt request (standard transfer ${observation.transferId || 'not given'})`
    case 'unavailable':
      return 'no usable answer: CRE nodes did not agree, or Beam was unreachable'
    default:
      return observation.state
  }
}
