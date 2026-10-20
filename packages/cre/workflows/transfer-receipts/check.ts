// Offline checks for the workflow's pure functions. Run with `bun run check`; not part of the workflow bundle.
import { createHash } from 'node:crypto'
import { keccak256, zeroAddress } from 'viem'
import { answerFor, attestationSubject, MAX_ATTESTATIONS, reportSubmission } from './attestations'
import { projectPublicationLookup, projectRoomReceipt, projectTransfer, ZERO_BYTES32 } from './beam'
import {
  ClosureReason,
  decodeReport,
  encodeReport,
  Level,
  RequestKind,
  receiptLevels,
  roomReceiptKey,
  roomRequestKey,
  TransferKind,
} from './receipts'
import {
  jobEntriesFor,
  jobLookups,
  reportEntryFor,
  roomRequestPublicationKey,
  roomSubjectForLookup,
  subjectForOpenRequest,
} from './subjects'
import { transferIdForRequest, transferIdToBytes16 } from './transfer-id'
import { attestSchema, configSchema } from './workflow'

const CRE_LEVELS = receiptLevels(true)
const RELAY_LEVELS = receiptLevels(false)

const check = (name: string, actual: unknown, expected: unknown) => {
  const a = JSON.stringify(actual)
  const e = JSON.stringify(expected)
  if (a !== e) {
    console.error(`FAIL ${name}\n  actual:   ${a}\n  expected: ${e}`)
    process.exit(1)
  }
  console.log(`ok   ${name}: ${a}`)
}

/** `stableUuidFromIdentity` from the Beam TypeScript SDK, on Node's crypto. */
const sdkTransferId = (clientId: string, idempotencyKey: string): string => {
  const bytes = createHash('sha256').update(`beam-transfer:${clientId}:${idempotencyKey.trim()}`).digest().subarray(0, 16)
  bytes[6] = (bytes[6] & 0x0f) | 0x50
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Buffer.from(bytes).toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

export const CLIENT_ID = '11111111-2222-4333-8444-555555555555'
export const REQUEST_ID = '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
export const TRANSFER_ID = '0e4c57f8-7e67-5852-95f5-418bba42563d'

check('transferId vector', transferIdForRequest(CLIENT_ID, REQUEST_ID), TRANSFER_ID)
check('transferId matches the SDK formula', transferIdForRequest(CLIENT_ID, REQUEST_ID), sdkTransferId(CLIENT_ID, REQUEST_ID))
for (const [clientId, requestId] of [
  ['c0ffee00-0000-4000-8000-000000000001', `0x${'00'.repeat(31)}01`],
  ['user_2xYz', `0x${'ff'.repeat(32)}`],
  ['42', `0x${'5a'.repeat(32)}`],
] as const) {
  check(`SDK formula for ${clientId}`, transferIdForRequest(clientId, requestId), sdkTransferId(clientId, requestId))
}

// The same fixture is decoded in contracts/evm/test/BeamTransferReceipts.t.sol, which checks this hash.
const fixtureReport = encodeReport(
  [
    {
      transferId: transferIdToBytes16(TRANSFER_ID),
      requestId: REQUEST_ID,
      status: 1,
      totalBytes: 1_073_741_824n,
      bytesTransferred: 1_073_741_824n,
      completedAtMs: 1_760_000_000_000n,
      planFingerprint: `0x${'aa'.repeat(32)}`,
      coordinateChecksum: `0x${'bb'.repeat(32)}`,
      level: CRE_LEVELS.standard,
    },
  ],
  [
    {
      room: 'room-1',
      channel: 'channel-1',
      publication: 'publication-1',
      requestId: `0x${'fe'.repeat(32)}`,
      status: 4,
      fileSizeBytes: 5_368_709_120n,
      manifestSha256: `0x${'cc'.repeat(32)}`,
      targetCount: 2,
      evidenceSha256: `0x${'dd'.repeat(32)}`,
      assignmentsSha256: `0x${'ee'.repeat(32)}`,
      evidenceVersion: 1,
      level: CRE_LEVELS.roomSigned,
    },
    {
      room: 'room-1',
      channel: 'channel-out',
      publication: 'publication-out',
      requestId: ZERO_BYTES32,
      status: 1,
      fileSizeBytes: 64n,
      manifestSha256: `0x${'99'.repeat(32)}`,
      targetCount: 1,
      evidenceSha256: `0x${'88'.repeat(32)}`,
      assignmentsSha256: `0x${'77'.repeat(32)}`,
      evidenceVersion: 1,
      level: CRE_LEVELS.room,
    },
  ],
  [
    {
      key: roomRequestKey(roomReceiptKey('room-2', 'channel-2', 'publication-2')),
      reason: ClosureReason.BucketOnlyPublication,
      transferId: transferIdToBytes16(TRANSFER_ID),
    },
  ],
  [
    {
      requestId: `0x${'fa'.repeat(32)}`,
      inputRoomKey: roomReceiptKey('room-1', 'channel-1', 'publication-1'),
      outputRoomKey: roomReceiptKey('room-1', 'channel-out', 'publication-out'),
      computeMember: `btr_member_${'c'.repeat(26)}`,
    },
  ],
)
check('fixture report keccak256', keccak256(fixtureReport), '0x39456b7e8cb3c9b7bce0fb55a52664dfd540dec8a39327002dd35d567079ca4e')

// The beam-room-receipt-evidence/v1 test vector: its envelope bytes exactly as published with the spec.
const VECTOR_ENVELOPE = String.raw`{"assignment_count":3,"assignments_sha256":"70fa3c99749c98d629f443c178e00443f8a67d13558e6edf0ebaa3f2891b6896","channel_id":"btr_channel_receiptvectorchannel222222","chunk_count":3,"chunk_size_bytes":"8","evidence_version":"beam-room-receipt-evidence/v1","file_size_bytes":"20","manifest_sha256":"23dd2149783a19bf8a0abf45a25b8c141584b7ee394b5fb7b25cb84792ec152f","protection":"btr.object.transport.tls.v1","publication_id":"btr_pub_receiptvectorpublication22","room_id":"btr_room_receiptvectorroom222222222","status":"partial","targets":[{"final_receipt":null,"kind":"agent","member_id":"btr_member_receiptvectortargeta222222","storage_finalization":null,"verification_basis":null},{"final_receipt":{"agent_public_key":"VRVPQgZepaG-oFRjgmviaE65LfksEAAnqrquV8pVQgc","agent_signature":"nOYKwy_n5s1sFOsscnaXqdRnQoyqnPnKS4wcveNp0Lhpu_EwU7DnQh5gtLmuZYNFo2iHRx6rV2ut3to3IVggBA","completed_at":"2026-10-18T12:00:05.123456789Z","file_size_bytes":"20","manifest_sha256":"23dd2149783a19bf8a0abf45a25b8c141584b7ee394b5fb7b25cb84792ec152f","receipt_id":"receipt-vector-final-b","target_member_id":"btr_member_receiptvectortargetb222222","transfer_id":"6f1d3c2e-8a4b-4f5c-9d7e-0b1a2c3d4e5f"},"kind":"agent","member_id":"btr_member_receiptvectortargetb222222","storage_finalization":null,"verification_basis":"recipient_final_receipt"},{"final_receipt":null,"kind":"object_storage","member_id":"btr_member_receiptvectortargetc222222","storage_finalization":{"etag":"\"9b2cf535f27731c974343645a3985328-3\"","file_size_bytes":"20","manifest_sha256":"23dd2149783a19bf8a0abf45a25b8c141584b7ee394b5fb7b25cb84792ec152f","operation_id":"op_9a8b7c6d5e4f3a2b","resource_id":"rsc_4f7c2a9e1b3d5f60","upload_id":"VXBsb2FkIElEIGZvciByZWNlaXB0LXZlY3Rvci1jLmJpbg","verified_at":"2026-10-18T12:00:06.48213Z","version_id":"3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"},"verification_basis":"provider_finalization"}],"transfer_id":"6f1d3c2e-8a4b-4f5c-9d7e-0b1a2c3d4e5f"}`
const VECTOR_IDS = {
  room: 'btr_room_receiptvectorroom222222222',
  channel: 'btr_channel_receiptvectorchannel222222',
  publication: 'btr_pub_receiptvectorpublication22',
}
const VECTOR_EVIDENCE_SHA256 = '0x940313b5a1fe1a60a93d33f9db8cbc9c911a0696d6d5bed1ec1da94e7fb63b7d'
const vectorBytes = new TextEncoder().encode(VECTOR_ENVELOPE)
const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value))
const vector = JSON.parse(VECTOR_ENVELOPE) as Record<string, unknown> & { targets: Record<string, unknown>[] }

check(
  'spec vector evidenceSha256 on Node crypto',
  `0x${createHash('sha256')
    .update(Buffer.concat([Buffer.from('beam:room-receipt-evidence:v1\u0000'), Buffer.from(vectorBytes)]))
    .digest('hex')}`,
  VECTOR_EVIDENCE_SHA256,
)
check('spec vector projection', projectRoomReceipt(200, vectorBytes, VECTOR_IDS), {
  state: 'room',
  status: 4,
  fileSizeBytes: '20',
  manifestSha256: '0x23dd2149783a19bf8a0abf45a25b8c141584b7ee394b5fb7b25cb84792ec152f',
  targetCount: 3,
  targetSigned: false,
  evidenceSha256: VECTOR_EVIDENCE_SHA256,
  assignmentsSha256: '0x70fa3c99749c98d629f443c178e00443f8a67d13558e6edf0ebaa3f2891b6896',
  evidenceVersion: 1,
})
const signedOnly = { ...vector, targets: [vector.targets[1]] }
check(
  'every target agent-signed is L2',
  (projectRoomReceipt(200, encode(signedOnly), VECTOR_IDS) as { targetSigned?: boolean }).targetSigned,
  true,
)
const signedAndStorage = { ...vector, targets: [vector.targets[1], vector.targets[2]] }
check(
  'a storage target keeps the receipt below L2',
  (projectRoomReceipt(200, encode(signedAndStorage), VECTOR_IDS) as { targetSigned?: boolean }).targetSigned,
  false,
)
check('an envelope with an unknown field is refused', projectRoomReceipt(200, encode({ ...vector, extra: 1 }), VECTOR_IDS), {
  state: 'invalid',
  reason: 'envelope fields do not match the evidence version',
})
check(
  'a target with an unknown field is refused',
  projectRoomReceipt(200, encode({ ...vector, targets: [{ ...vector.targets[0], extra: 1 }] }), VECTOR_IDS),
  { state: 'invalid', reason: 'target fields do not match the evidence version' },
)
check('an envelope for another publication is refused', projectRoomReceipt(200, vectorBytes, { ...VECTOR_IDS, publication: 'other' }), {
  state: 'invalid',
  reason: 'envelope does not match the requested publication',
})
check(
  '409 not_terminal is pending',
  projectRoomReceipt(409, encode({ code: 'not_terminal', status: 'in_progress' }), VECTOR_IDS),
  { state: 'pending' },
)
const bucketOnly = projectRoomReceipt(
  409,
  encode({ code: 'bucket_only_publication', transfer_id: TRANSFER_ID }),
  VECTOR_IDS,
)
check('409 bucket_only_publication has no Room receipt', bucketOnly, { state: 'not_room', transferId: TRANSFER_ID })
check(
  '409 bucket_only_publication closes the Room receipt request',
  reportEntryFor({ kind: 'room', ...VECTOR_IDS, requestId: null }, bucketOnly, CRE_LEVELS),
  {
    kind: 'closure',
    entry: {
      key: roomRequestKey(roomReceiptKey(VECTOR_IDS.room, VECTOR_IDS.channel, VECTOR_IDS.publication)),
      reason: 1,
      transferId: transferIdToBytes16(TRANSFER_ID),
    },
  },
)
check(
  'a bucket-only answer without a usable transfer id still closes, naming none',
  reportEntryFor(
    { kind: 'room', ...VECTOR_IDS, requestId: null },
    projectRoomReceipt(409, encode({ code: 'bucket_only_publication', transfer_id: 'not-a-uuid' }), VECTOR_IDS),
    CRE_LEVELS,
  ),
  {
    kind: 'closure',
    entry: {
      key: roomRequestKey(roomReceiptKey(VECTOR_IDS.room, VECTOR_IDS.channel, VECTOR_IDS.publication)),
      reason: 1,
      transferId: `0x${'00'.repeat(16)}`,
    },
  },
)
check(
  'a pending Room lookup adds nothing',
  reportEntryFor({ kind: 'room', ...VECTOR_IDS, requestId: null }, { state: 'pending' }, CRE_LEVELS),
  null,
)

const transferBody = (status: string) =>
  new TextEncoder().encode(
    JSON.stringify({
      id: TRANSFER_ID,
      transfer_key: 'server-issued',
      status,
      total_bytes: 1073741824,
      bytes_transferred: 1073741824,
      error_message: null,
      metadata: { plan_fingerprint: 'aa'.repeat(32), coordinate_checksum: 'bb'.repeat(32) },
      created_at: '2026-10-09T08:53:00.000Z',
      started_at: '2026-10-09T08:53:05.000Z',
      completed_at: '2026-10-09T08:53:20.000Z',
    }),
  )
check('standard transfer projection', projectTransfer(200, transferBody('completed'), TRANSFER_ID), {
  state: 'transfer',
  status: 1,
  totalBytes: '1073741824',
  bytesTransferred: '1073741824',
  completedAtMs: String(Date.parse('2026-10-09T08:53:20.000Z')),
  planFingerprint: `0x${'aa'.repeat(32)}`,
  coordinateChecksum: `0x${'bb'.repeat(32)}`,
})
check('standard transfer in flight', projectTransfer(200, transferBody('running'), TRANSFER_ID), { state: 'pending' })
check('standard transfer not created yet', projectTransfer(404, new Uint8Array(), TRANSFER_ID), { state: 'absent' })

for (const file of ['config.staging.json', 'config.production.json']) {
  const parsed = configSchema.safeParse(await Bun.file(new URL(file, import.meta.url)).json())
  check(`${file} matches the config schema`, parsed.success ? 'valid' : parsed.error.issues, 'valid')
}

const config = configSchema.parse(await Bun.file(new URL('config.staging.json', import.meta.url)).json())
const NO_ROUTES = { roomDelivery: null, jobOutput: null }
const openRequest = {
  key: ZERO_BYTES32,
  requester: zeroAddress,
  openedAt: 0n,
  requestId: REQUEST_ID,
  transferId: `0x${'00'.repeat(16)}`,
  room: '',
  channel: '',
  publication: '',
} as const
check(
  'reconcile derives a standard transfer request',
  subjectForOpenRequest(config.beamClientId, NO_ROUTES, { ...openRequest, kind: RequestKind.Transfer, transferKind: TransferKind.Standard }),
  { kind: 'transfer', transferId: transferIdForRequest(config.beamClientId, REQUEST_ID), requestId: REQUEST_ID },
)
const roomDeliveryRequest = { ...openRequest, kind: RequestKind.Transfer, transferKind: TransferKind.RoomDelivery }
check('without a Room route, reconcile skips a Room delivery request', subjectForOpenRequest(config.beamClientId, NO_ROUTES, roomDeliveryRequest), null)

// Room delivery requests: Studio publishes under request:<requestId>, which the coordinator looks up by publisher.
const ROUTE = { room: VECTOR_IDS.room, channel: VECTOR_IDS.channel, publisherMemberId: `btr_member_${'a'.repeat(26)}` }
const roomRequest = subjectForOpenRequest(config.beamClientId, { roomDelivery: ROUTE, jobOutput: null }, roomDeliveryRequest)
check('with a Room route, reconcile looks a Room delivery request up', roomRequest, { kind: 'roomRequest', ...ROUTE, requestId: REQUEST_ID })
check(
  'the publication key is request: and the request id as start-transfer sends it',
  roomRequestPublicationKey(REQUEST_ID.toUpperCase().replace('0X', '0x') as typeof REQUEST_ID),
  `request:${REQUEST_ID}`,
)
const found = projectPublicationLookup(200, encode({ publication_id: VECTOR_IDS.publication }))
check('a found publication', found, { state: 'found', publicationId: VECTOR_IDS.publication })
check('no publication under the key yet', projectPublicationLookup(404, encode({ code: 'not_found' })), { state: 'absent' })
check('a refused lookup', projectPublicationLookup(403, encode({ code: 'permission_denied' })), { state: 'error', httpStatus: 403 })
check(
  'a malformed publication id',
  projectPublicationLookup(200, encode({ publication_id: 'btr_pub_short' })),
  { state: 'invalid', reason: 'publication_id is not a publication id' },
)
const resolved = roomSubjectForLookup(roomRequest as Extract<typeof roomRequest, { kind: 'roomRequest' }>, found)
check('the found publication is read as a Room receipt that carries the request id', resolved, {
  kind: 'room', room: VECTOR_IDS.room, channel: VECTOR_IDS.channel, publication: VECTOR_IDS.publication, requestId: REQUEST_ID,
})
const answering = reportEntryFor(resolved!, projectRoomReceipt(200, vectorBytes, VECTOR_IDS), CRE_LEVELS)
check(
  'its receipt entry answers the request',
  answering?.kind === 'room'
    ? { publication: answering.entry.publication, requestId: answering.entry.requestId, level: answering.entry.level }
    : answering,
  { publication: VECTOR_IDS.publication, requestId: REQUEST_ID, level: CRE_LEVELS.room },
)
check(
  'nothing is read before the publication exists',
  roomSubjectForLookup(roomRequest as Extract<typeof roomRequest, { kind: 'roomRequest' }>, { state: 'absent' }),
  null,
)
const withRoute = (batch: number) =>
  configSchema.safeParse({ ...config, roomDelivery: ROUTE, reconcileBatchSize: batch }).success ? 'valid' : 'invalid'
check('a Room route halves the reconcile batch, since each Room request costs two lookups', [withRoute(7), withRoute(8)], ['valid', 'invalid'])

// The contract derives the same levels from `donAttested` (standardReceiptLevel() and the Room ones).
check('CRE deployment levels', CRE_LEVELS, {
  standard: Level.BeamReported | Level.DonAttested,
  room: Level.BeamReported | Level.DonAttested,
  roomSigned: Level.BeamReported | Level.TargetSigned | Level.DonAttested,
})
check('relay deployment levels never carry the DON-attested bit', RELAY_LEVELS, {
  standard: Level.BeamReported,
  room: Level.BeamReported,
  roomSigned: Level.BeamReported | Level.TargetSigned,
})
const finished = projectTransfer(200, transferBody('completed'), TRANSFER_ID)
check(
  'a relay records a finished transfer as Beam-reported only',
  reportEntryFor({ kind: 'transfer', transferId: TRANSFER_ID, requestId: REQUEST_ID }, finished, RELAY_LEVELS)?.entry.level,
  Level.BeamReported,
)

// Attestations: a Room bridge asks for subjects and gets one DON-signed report each.
const decoded = decodeReport(fixtureReport)
check(
  'a report decodes back to its entries',
  [decoded.standard[0]?.transferId, String(decoded.rooms[0]?.fileSizeBytes), decoded.closures[0]?.reason],
  [transferIdToBytes16(TRANSFER_ID), '5368709120', ClosureReason.BucketOnlyPublication],
)
check(
  'an attested transfer id is normalized',
  attestationSubject({ op: 'attest_transfer', transferId: TRANSFER_ID.toUpperCase() }),
  { kind: 'transfer', transferId: TRANSFER_ID, requestId: null },
)
check(
  'a transfer attestation is answered by its standard receipt',
  answerFor({ op: 'attest_transfer', transferId: TRANSFER_ID }, decoded)?.kind,
  'standard',
)
check(
  'a Room attestation is answered by its Room receipt',
  answerFor({ op: 'attest_room_publication', room: 'room-1', channel: 'channel-1', publication: 'publication-1' }, decoded)?.kind,
  'room',
)
check(
  'a bucket-only publication is answered by its closure',
  answerFor({ op: 'attest_room_publication', room: 'room-2', channel: 'channel-2', publication: 'publication-2' }, decoded)?.kind,
  'closure',
)
check(
  'a report about other subjects answers nothing',
  [
    answerFor({ op: 'attest_room_publication', room: 'room-1', channel: 'channel-1', publication: 'publication-2' }, decoded),
    answerFor({ op: 'attest_transfer', transferId: transferIdForRequest(CLIENT_ID, `0x${'00'.repeat(31)}02`) }, decoded),
  ],
  [null, null],
)
check(
  'a report submission is hex without 0x',
  reportSubmission(new Uint8Array([1, 2]), new Uint8Array([0xab]), [new Uint8Array([0xff, 0x00])]),
  { report: '0102', context: 'ab', signatures: ['ff00'] },
)
const attestations = (count: number) =>
  Array.from({ length: count }, (_, index) => ({
    op: 'attest_room_publication',
    room: 'room-1',
    channel: 'channel-1',
    publication: `publication-${index}`,
  }))
check(
  `an execution attests at most ${MAX_ATTESTATIONS}, each costing a read and a delivery`,
  [MAX_ATTESTATIONS, MAX_ATTESTATIONS + 1].map((count) => attestSchema.safeParse({ attest: attestations(count) }).success),
  [true, false],
)
check(
  'unknown ops and extra fields are refused',
  [
    attestSchema.safeParse({ attest: [{ op: 'attest_message', text: 'x' }] }).success,
    attestSchema.safeParse({ attest: [{ op: 'attest_transfer', transferId: TRANSFER_ID, room: 'room-1' }] }).success,
  ],
  [false, false],
)
check('publishing on chain is off unless asked', attestSchema.parse({ attest: attestations(1) }).publishOnchain, false)
check(
  'the bridge results URL loses its trailing slash',
  configSchema.parse({ ...config, attestationReplyUrl: 'https://bridge.example.com/cre/results/' }).attestationReplyUrl,
  'https://bridge.example.com/cre/results',
)

// Jobs: the input is the Room delivery under request:<R>; the output is the compute member's publication under the
// same key. Both receipts and the job result linking them go in one report.
const COMPUTE = `btr_member_${'c'.repeat(26)}`
const JOB_ROUTES = { roomDelivery: ROUTE, jobOutput: { channel: 'btr_channel_jobs', computeMemberId: COMPUTE } }
const jobRequest = { ...openRequest, kind: RequestKind.Transfer, transferKind: TransferKind.Job }
check('without a job output route, reconcile skips a job', subjectForOpenRequest(config.beamClientId, { roomDelivery: ROUTE, jobOutput: null }, jobRequest), null)
const job = subjectForOpenRequest(config.beamClientId, JOB_ROUTES, jobRequest)
check('with both routes, a job is looked up through its input and output', job, {
  kind: 'job', room: ROUTE.room, inputChannel: ROUTE.channel, sourceMemberId: ROUTE.publisherMemberId,
  outputChannel: 'btr_channel_jobs', computeMemberId: COMPUTE, requestId: REQUEST_ID,
})
const jobSubject = job as Extract<typeof job, { kind: 'job' }>
check(
  'a job looks its input up by the source and its output by the compute member, both under the request key',
  [jobLookups(jobSubject).input.publisherMemberId, jobLookups(jobSubject).output.publisherMemberId, jobLookups(jobSubject).output.channel],
  [ROUTE.publisherMemberId, COMPUTE, 'btr_channel_jobs'],
)
const terminal = projectRoomReceipt(200, vectorBytes, VECTOR_IDS)
const answered = jobEntriesFor(jobSubject, { input: VECTOR_IDS.publication, output: VECTOR_IDS.publication.replace('22', '33') }, { input: terminal, output: terminal }, CRE_LEVELS)
check(
  'a finished job is answered by both Room receipts and the job result linking them',
  answered?.map((entry) => entry.kind === 'job'
    ? { kind: entry.kind, requestId: entry.entry.requestId, computeMember: entry.entry.computeMember, distinct: entry.entry.inputRoomKey !== entry.entry.outputRoomKey }
    : { kind: entry.kind, requestId: entry.kind === 'room' ? entry.entry.requestId : null }),
  [
    { kind: 'room', requestId: ZERO_BYTES32 },
    { kind: 'room', requestId: ZERO_BYTES32 },
    { kind: 'job', requestId: REQUEST_ID, computeMember: COMPUTE, distinct: true },
  ],
)
check(
  'nothing answers a job while its output is unfinished',
  jobEntriesFor(jobSubject, { input: VECTOR_IDS.publication, output: VECTOR_IDS.publication }, { input: terminal, output: { state: 'pending' } }, CRE_LEVELS),
  null,
)
const batchWith = (overrides: Record<string, unknown>, batch: number) =>
  configSchema.safeParse({ ...config, ...overrides, reconcileBatchSize: batch }).success ? 'valid' : 'invalid'
check(
  'jobs cost four reads, so the reconcile batch is at most 3 with a job output route',
  [batchWith({ roomDelivery: ROUTE, jobOutput: JOB_ROUTES.jobOutput }, 3), batchWith({ roomDelivery: ROUTE, jobOutput: JOB_ROUTES.jobOutput }, 4)],
  ['valid', 'invalid'],
)
check('a job output route needs the Room delivery route', batchWith({ roomDelivery: null, jobOutput: JOB_ROUTES.jobOutput }, 1), 'invalid')
check(
  'the fixture decodes with its job',
  decoded.jobs.map((entry) => [entry.requestId, entry.computeMember]),
  [[`0x${'fa'.repeat(32)}`, COMPUTE]],
)
