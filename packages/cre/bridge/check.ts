// Offline checks for the bridge. Run with `bun run check`.
import { createHash, createHmac } from 'node:crypto'
import {
  concatHex,
  encodeAbiParameters,
  type Hex,
  keccak256,
  parseAbiParameters,
  parseSignature,
  recoverMessageAddress,
  serializeSignature,
  toHex,
} from 'viem'
import { generatePrivateKey, privateKeyToAccount, sign } from 'viem/accounts'
import type { Attestation, AttestationRequest } from '../workflows/transfer-receipts/attestations'
import { encodeReport, Level, roomReceiptKey, roomRequestKey } from '../workflows/transfer-receipts/receipts'
import { transferIdToBytes16 } from '../workflows/transfer-receipts/transfer-id'
import {
  ATTESTATION_CLAIM,
  Bridge,
  type BridgeIO,
  type BridgeState,
  type ChainAnswer,
  type ChainIO,
  CHAIN_REPLY,
  DON_RECORD_CLAIM,
  emptyState,
  MAX_CHAIN_FAILURES,
  RELAY_RECORD_CLAIM,
} from './bridge'
import { type BridgeConfig, bridgeConfigSchema, secretsFromEnv } from './config'
import { gatewayBody, gatewayJwt, stableStringify } from './cre'
import { parseHeader, readDonSigners, REPORT_HEADER_BYTES, reportDigest, type SignerSet, verifyReportSignatures } from './report'
import { parseEnvelope, parseRoomRequest, verifyWebhookSignature } from './webhook'

const check = (name: string, actual: unknown, expected: unknown) => {
  const a = JSON.stringify(actual)
  const e = JSON.stringify(expected)
  if (a !== e) {
    console.error(`FAIL ${name}\n  actual:   ${a}\n  expected: ${e}`)
    process.exit(1)
  }
  console.log(`ok   ${name}: ${a.length > 120 ? `${a.slice(0, 117)}...` : a}`)
}

const NOW = 1_800_000_000
const SECRET = 'whsec-current-0123456789'
const PREVIOUS = 'whsec-previous-0123456789'
const ROOM = `btr_room_${'r'.repeat(26)}`
const CHANNEL = `btr_channel_${'c'.repeat(26)}`
const PUBLICATION = `btr_pub_${'a'.repeat(26)}`
const TRIGGER = `btr_pub_${'t'.repeat(26)}`
const TRANSFER_ID = '0e4c57f8-7e67-5852-95f5-418bba42563d'
const sign1 = (secret: string, timestamp: number, body: string) =>
  `v1=${createHmac('sha256', secret).update(`v1:${timestamp}:${body}`).digest('hex')}`
const bytes = (text: string) => new TextEncoder().encode(text)

// ─── Webhook signatures ─────────────────────────────────────
const body = '{"hello":"room"}'
check('a delivery signed with the current secret verifies', verifyWebhookSignature([SECRET], String(NOW), sign1(SECRET, NOW, body), bytes(body), NOW), true)
check(
  'during a rotation, a signature for the previous secret verifies',
  verifyWebhookSignature([SECRET, PREVIOUS], String(NOW), `${sign1('another', NOW, body)},${sign1(PREVIOUS, NOW, body)}`, bytes(body), NOW),
  true,
)
check(
  'a tampered body, an unknown secret and a stale timestamp are refused',
  [
    verifyWebhookSignature([SECRET], String(NOW), sign1(SECRET, NOW, body), bytes(`${body} `), NOW),
    verifyWebhookSignature([SECRET], String(NOW), sign1('another', NOW, body), bytes(body), NOW),
    verifyWebhookSignature([SECRET], String(NOW - 301), sign1(SECRET, NOW - 301, body), bytes(body), NOW),
    verifyWebhookSignature([SECRET], null, sign1(SECRET, NOW, body), bytes(body), NOW),
  ],
  [false, false, false, false],
)
check('a poll signs the empty body', verifyWebhookSignature([SECRET], String(NOW), sign1(SECRET, NOW, ''), new Uint8Array(), NOW), true)

// ─── What Room messages ask ─────────────────────────────────
const POLICY = { rooms: { [ROOM]: { channels: [CHANNEL] } }, allowTransferAttestations: false }
const envelope = (text: string | null, type = 'message') =>
  parseEnvelope(
    bytes(
      JSON.stringify({
        version: 1, type, id: TRIGGER, room_id: ROOM, channel_id: 'btr_channel_messages', webhook_member_id: 'btr_member_webhook',
        publication_id: TRIGGER, publisher_member_id: 'btr_member_asker', content_type: 'application/json', hop: 0,
        occurred_at: '2026-10-20T00:00:00Z', ...(text === null ? {} : { text }),
      }),
    ),
  )!
const ask = (message: unknown, policy = POLICY) =>
  parseRoomRequest(envelope(typeof message === 'string' ? message : JSON.stringify(message)), policy)
check('ordinary Room messages get no reply', [ask('hello room').kind, ask({ text: 'no op' }).kind, parseRoomRequest(envelope(null, 'file'), POLICY).kind], ['ignore', 'ignore', 'ignore'])
check('an op the bridge does not attest is refused', ask({ op: 'summarize' }), {
  kind: 'refuse', reason: 'not_an_attestation',
  message: 'This bridge only forwards attest_room_publication and attest_transfer. Other Room messages need no Chainlink verification.',
})
check('a submitted result is refused: the DON attests only what it reads from Beam', ask({ op: 'submit_result_for_attestation', result: 'x' }).kind === 'refuse' && (ask({ op: 'submit_result_for_attestation' }) as { reason: string }).reason, 'not_attestable')
check('a publication of an allowed channel of the asking Room is attested', ask({ op: 'attest_room_publication', channel: CHANNEL, publication: PUBLICATION }), {
  kind: 'attest', attestation: { op: 'attest_room_publication', room: ROOM, channel: CHANNEL, publication: PUBLICATION }, publishOnchain: false,
})
check(
  'other channels, malformed ids and extra fields are refused',
  [
    ask({ op: 'attest_room_publication', channel: 'btr_channel_other', publication: PUBLICATION }),
    ask({ op: 'attest_room_publication', channel: CHANNEL, publication: 'btr_pub_short' }),
    ask({ op: 'attest_room_publication', channel: CHANNEL, publication: PUBLICATION, room: 'btr_room_elsewhere' }),
  ].map((asked) => (asked.kind === 'refuse' ? asked.reason : asked.kind)),
  ['channel_not_attestable', 'invalid_request', 'invalid_request'],
)
check(
  'transfers are attested only when the operator allows it',
  [
    ask({ op: 'attest_transfer', transfer_id: TRANSFER_ID }).kind,
    ask({ op: 'attest_transfer', transfer_id: TRANSFER_ID.toUpperCase(), publish_onchain: true }, { ...POLICY, allowTransferAttestations: true }),
  ],
  ['refuse', { kind: 'attest', attestation: { op: 'attest_transfer', transferId: TRANSFER_ID }, publishOnchain: true }],
)

// ─── CRE gateway requests ───────────────────────────────────
check('stable JSON sorts keys at every level', stableStringify({ b: 1, a: { d: [{ z: 1, y: 2 }], c: 'x' } }), '{"a":{"c":"x","d":[{"y":2,"z":1}]},"b":1}')
const GATEWAY_KEY = generatePrivateKey()
const input: AttestationRequest = { attest: [{ op: 'attest_transfer', transferId: TRANSFER_ID }], publishOnchain: false }
const rpcBody = gatewayBody(input, 'ab'.repeat(32), 'request-1')
check('the gateway body is stable JSON-RPC', rpcBody.startsWith('{"id":"request-1","jsonrpc":"2.0","method":"workflows.execute","params":{"input":{"attest":'), true)
const jwt = await gatewayJwt(GATEWAY_KEY, rpcBody, NOW, 'jti-1')
const [jwtHeader, jwtPayload, jwtSignature] = jwt.split('.')
const claims = JSON.parse(Buffer.from(jwtPayload, 'base64url').toString())
check('the JWT header is ETH', JSON.parse(Buffer.from(jwtHeader, 'base64url').toString()), { alg: 'ETH', typ: 'JWT' })
check('the JWT digests the exact body and expires within 5 minutes', [claims.digest, claims.exp - claims.iat, claims.iss], [
  `0x${createHash('sha256').update(rpcBody).digest('hex')}`, 300, privateKeyToAccount(GATEWAY_KEY).address,
])
const rawSignature = Buffer.from(jwtSignature, 'base64url')
check(
  'the JWT signature is r || s || recovery id over the EIP-191 message, and recovers the signer',
  [
    rawSignature.length,
    rawSignature[64] <= 1,
    await recoverMessageAddress({
      message: `${jwtHeader}.${jwtPayload}`,
      signature: serializeSignature({ r: toHex(rawSignature.subarray(0, 32)), s: toHex(rawSignature.subarray(32, 64)), yParity: rawSignature[64] }),
    }),
  ],
  [65, true, privateKeyToAccount(GATEWAY_KEY).address],
)

// ─── Reports and DON signatures ─────────────────────────────
const WORKFLOW_ID = '11'.repeat(32)
const OWNER = 'aa'.repeat(20)
const DON_KEYS = Array.from({ length: 4 }, () => generatePrivateKey())
const DON: SignerSet = { f: 1, signers: new Set(DON_KEYS.map((key) => privateKeyToAccount(key).address.toLowerCase())) }
const CONTEXT = Uint8Array.from(Buffer.from('cd'.repeat(96), 'hex'))

const header = (workflowId = WORKFLOW_ID, owner = OWNER) => {
  const head = Buffer.alloc(REPORT_HEADER_BYTES)
  head[0] = 1
  Buffer.from('ee'.repeat(32), 'hex').copy(head, 1)
  head.writeUInt32BE(1_760_000_000, 33)
  head.writeUInt32BE(7, 37)
  head.writeUInt32BE(1, 41)
  Buffer.from(workflowId, 'hex').copy(head, 45)
  Buffer.from('beamrcpts\0').copy(head, 77)
  Buffer.from(owner, 'hex').copy(head, 87)
  head.writeUInt16BE(1, 107)
  return head
}
const roomEntry = {
  room: ROOM, channel: CHANNEL, publication: PUBLICATION, requestId: `0x${'00'.repeat(32)}` as Hex, status: 1,
  fileSizeBytes: 8_388_608n, manifestSha256: `0x${'cc'.repeat(32)}` as Hex, targetCount: 1, evidenceSha256: `0x${'dd'.repeat(32)}` as Hex,
  assignmentsSha256: `0x${'ee'.repeat(32)}` as Hex, evidenceVersion: 1, level: Level.BeamReported | Level.TargetSigned | Level.DonAttested,
}
const rawReportFor = (reportHex: Hex, workflowId?: string, owner?: string) =>
  Uint8Array.from(Buffer.concat([header(workflowId, owner), Buffer.from(reportHex.slice(2), 'hex')]))
const signReport = async (rawReport: Uint8Array, keys: Hex[]) => {
  const digest = reportDigest(rawReport, CONTEXT)
  return Promise.all(keys.map(async (key) => {
    const { r, s, yParity } = parseSignature(await sign({ hash: digest, privateKey: key, to: 'hex' }))
    return Uint8Array.from(Buffer.from(serializeSignature({ r, s, yParity: yParity! }).slice(2), 'hex'))
  }))
}
const roomRaw = rawReportFor(encodeReport([], [roomEntry], [], []))

check('the header layout', parseHeader(roomRaw), {
  version: 1, executionId: 'ee'.repeat(32), timestamp: 1_760_000_000, donId: 7, donConfigVersion: 1, workflowId: WORKFLOW_ID,
  workflowName: Buffer.from('beamrcpts\0').toString('hex'), workflowOwner: OWNER, reportId: '0001',
})
check('the signed digest is keccak256(keccak256(report) || context)', reportDigest(roomRaw, CONTEXT), keccak256(concatHex([keccak256(toHex(roomRaw)), toHex(CONTEXT)])))
const twoSigned = { rawReport: roomRaw, context: CONTEXT, signatures: await signReport(roomRaw, DON_KEYS.slice(0, 2)) }
check('f+1 distinct DON signers verify', (await verifyReportSignatures(twoSigned, DON)).length, 2)
const refusal = async (signatures: Uint8Array[]) =>
  verifyReportSignatures({ rawReport: roomRaw, context: CONTEXT, signatures }, DON).then(() => 'verified', (error: Error) => error.message)
const [firstSignature] = await signReport(roomRaw, DON_KEYS.slice(0, 1))
check(
  'a repeated signer, an outside signer and a signature over other bytes do not count',
  [
    await refusal([firstSignature, firstSignature]),
    await refusal([firstSignature, ...(await signReport(roomRaw, [generatePrivateKey()]))]),
    await refusal([firstSignature, ...(await signReport(rawReportFor(encodeReport([], [{ ...roomEntry, status: 2 }], [], [])), DON_KEYS.slice(1, 2)))]),
  ],
  Array(3).fill('The report has 1 valid signatures from distinct DON signers; it needs 2'),
)

// The Capability Registry answers as the CRE SDK decodes it: DONInfo, then NodeInfo with the signer in its fourth word.
const P2P_IDS = DON_KEYS.map((_, index) => toHex(index + 1, { size: 32 }))
const registryCall = async (_to: Hex, data: Hex): Promise<Hex> => {
  if (data.startsWith('0x23537405')) {
    return encodeAbiParameters(parseAbiParameters('(uint32, uint32, uint8, bool, bool, bytes32[], (bytes32, bytes)[])'), [
      [7, 1, 1, true, true, P2P_IDS, []],
    ])
  }
  return encodeAbiParameters(parseAbiParameters('(uint32, uint32, uint32, bytes32, bytes32, bytes32, bytes32, bytes32[], uint256[])[]'), [
    DON_KEYS.map((key, index) => [index + 1, 1, 7, `${privateKeyToAccount(key).address.toLowerCase()}${'00'.repeat(12)}` as Hex, P2P_IDS[index], `0x${'00'.repeat(32)}`, `0x${'00'.repeat(32)}`, [], []] as const),
  ])
}
const registrySigners = await readDonSigners(registryCall, '0x76c9cf548b4179F8901cda1f8623568b58215E62', 7)
check('the registry decoding finds f and every signer', [registrySigners.f, [...registrySigners.signers].sort()], [1, [...DON.signers].sort()])

// ─── The bridge, end to end ─────────────────────────────────
const CONFIG: BridgeConfig = bridgeConfigSchema.parse({
  listen: { hostname: '127.0.0.1', port: 8098 },
  basePath: '/bridge',
  rooms: { [ROOM]: { channels: [CHANNEL] } },
  allowTransferAttestations: true,
  answers: {
    from: 'cre',
    forward: { mode: 'command', argv: ['true'], cwd: '.' },
    workflow: { id: WORKFLOW_ID, owner: `0x${OWNER}` },
    signers: { source: 'registry', rpcUrl: 'https://ethereum-rpc.example', registryAddress: '0x76c9cf548b4179F8901cda1f8623568b58215E62' },
    paceSeconds: 30,
    retrySeconds: 60,
    maxAttempts: 2,
  },
  statePath: '/dev/null',
})
let clock = NOW
const forwarded: AttestationRequest[] = []
let saves = 0
const makeIO = (signers: SignerSet | null): BridgeIO => ({
  now: () => clock,
  save: async (_state: BridgeState) => {
    saves += 1
  },
  log: () => {},
  cre: {
    forward: async (request) => {
      forwarded.push(request)
      return { accepted: true, executionId: null }
    },
    signers: async () => signers,
  },
  chain: null,
})
const bridge = new Bridge(CONFIG, [SECRET], makeIO(DON), emptyState())

const deliver = (message: unknown, publication = TRIGGER) => {
  const text = JSON.stringify({
    version: 1, type: 'message', id: publication, room_id: ROOM, channel_id: 'btr_channel_messages', webhook_member_id: 'btr_member_webhook',
    publication_id: publication, publisher_member_id: 'btr_member_asker', content_type: 'application/json', hop: 0,
    occurred_at: '2026-10-20T00:00:00Z', text: JSON.stringify(message),
  })
  return bridge.handleDelivery(new Request('https://bridge.example/bridge/hooks', {
    method: 'POST', body: text,
    headers: { 'content-type': 'application/json', 'x-beam-timestamp': String(clock), 'x-beam-signature': sign1(SECRET, clock, text) },
  }))
}
const poll = (id: string, publication = id) =>
  bridge.handlePoll(id, new Request(`https://bridge.example/bridge/replies/${id}`, {
    headers: { 'x-beam-timestamp': String(clock), 'x-beam-signature': sign1(SECRET, clock, ''), 'x-beam-room': ROOM, 'x-beam-publication': publication },
  }))
const result = (rawReport: Uint8Array, signatures: Uint8Array[]) =>
  bridge.handleResult(new Request('https://bridge.example/bridge/cre/results', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ report: Buffer.from(rawReport).toString('hex'), context: Buffer.from(CONTEXT).toString('hex'), signatures: signatures.map((s) => Buffer.from(s).toString('hex')) }),
  }))

const attestRoom = { op: 'attest_room_publication', channel: CHANNEL, publication: PUBLICATION }
const accepted = await deliver(attestRoom)
check('an attestation request is accepted for an asynchronous reply', [accepted.status, accepted.headers.get('x-beam-reply'), accepted.headers.get('location')], [202, 'v1', `replies/${TRIGGER}`])
const repeated = await deliver(attestRoom)
check('a repeated delivery keeps the one request', [repeated.status, Object.keys(bridge.state.requests).length], [202, 1])
const refused = await deliver({ op: 'chat', text: 'hi' }, `btr_pub_${'r'.repeat(26)}`)
check('a refusal is a synchronous reply', [refused.status, refused.headers.get('x-beam-reply'), ((await refused.json()) as { outcome: string }).outcome], [200, 'v1', 'refused'])
const unsigned = await bridge.handleDelivery(new Request('https://bridge.example/bridge/hooks', { method: 'POST', body: '{}', headers: { 'content-type': 'application/json' } }))
check('an unsigned delivery is refused', unsigned.status, 401)
check('a pending reply answers 202', [(await poll(TRIGGER)).status, (await poll(TRIGGER)).headers.get('retry-after')], [202, '15'])
check('a poll for another publication finds nothing', (await poll(TRIGGER, `btr_pub_${'x'.repeat(26)}`)).status, 404)

await bridge.pump()
check('the pump forwards the request once', forwarded, [{ attest: [{ op: 'attest_room_publication', room: ROOM, channel: CHANNEL, publication: PUBLICATION }], publishOnchain: false }])
clock += 10
await bridge.pump()
check('nothing is forwarded again within the pace and retry windows', forwarded.length, 1)

const foreign = rawReportFor(encodeReport([], [roomEntry], [], []), '22'.repeat(32))
check('a report from another workflow is refused', (await result(foreign, await signReport(foreign, DON_KEYS.slice(0, 2)))).status, 403)
check('a report short of f+1 signatures is refused', (await result(roomRaw, await signReport(roomRaw, DON_KEYS.slice(0, 1)))).status, 403)
const taken = await result(roomRaw, twoSigned.signatures)
check('a verified report answers the request', [taken.status, await taken.json()], [200, { accepted: true, answered: 1 }])
const duplicate = await result(roomRaw, await signReport(roomRaw, DON_KEYS.slice(2, 4)))
check('another node\'s copy of the same report is acknowledged unread', await duplicate.json(), { accepted: true, duplicate: true })

const answered = await poll(TRIGGER)
const replyBody = (await answered.json()) as Record<string, any>
check('the reply is published by the host', [answered.status, answered.headers.get('x-beam-reply'), answered.headers.get('content-type')], [200, 'v1', 'application/json'])
check('the reply carries the signed answer and its exact claim', [
  replyBody.outcome, replyBody.answer.kind, replyBody.answer.status, replyBody.answer.file_size_bytes, replyBody.answer.level_names,
  replyBody.signatures.verified, replyBody.signatures.signers.length, replyBody.workflow.id, replyBody.claim,
], ['attested', 'room_publication', 'completed', '8388608', ['beam_reported', 'target_signed', 'don_attested'], true, 2, WORKFLOW_ID, ATTESTATION_CLAIM])
check('the reply holds the report a member can verify again', replyBody.report.report, Buffer.from(roomRaw).toString('hex'))

// A request nothing answers is retried, then answered as not attested.
const pendingTransfer = `btr_pub_${'p'.repeat(26)}`
await deliver({ op: 'attest_transfer', transfer_id: TRANSFER_ID }, pendingTransfer)
clock += 30
await bridge.pump()
clock += 60
await bridge.pump()
check('an unanswered request is forwarded again after retrySeconds', forwarded.slice(1).map((request) => request.attest[0].op), ['attest_transfer', 'attest_transfer'])
clock += 60
await bridge.pump()
const exhausted = (await (await poll(pendingTransfer)).json()) as Record<string, string>
check('after maxAttempts it is answered as not attested', [exhausted.outcome, exhausted.reason], ['not_attested', 'no_finished_receipt'])

// A bucket-only publication is answered by its closure, naming the standard transfer to ask about instead.
const bucketOnly = `btr_pub_${'b'.repeat(26)}`
const closureTrigger = `btr_pub_${'c'.repeat(26)}`
await deliver({ op: 'attest_room_publication', channel: CHANNEL, publication: bucketOnly }, closureTrigger)
const closureRaw = rawReportFor(encodeReport([], [], [{ key: roomRequestKey(roomReceiptKey(ROOM, CHANNEL, bucketOnly)), reason: 1, transferId: transferIdToBytes16(TRANSFER_ID) }], []))
await result(closureRaw, await signReport(closureRaw, DON_KEYS.slice(0, 2)))
check('a bucket-only publication is answered with its standard transfer', ((await (await poll(closureTrigger)).json()) as Record<string, any>).answer, {
  kind: 'bucket_only_publication', standard_transfer_id: TRANSFER_ID,
})

// With unverified signers (the CRE simulator), the reply says its signatures were not checked.
const simulated = new Bridge(CONFIG, [SECRET], makeIO(null), emptyState())
const simulatedText = JSON.stringify({
  version: 1, type: 'message', id: TRIGGER, room_id: ROOM, channel_id: 'btr_channel_messages', webhook_member_id: 'btr_member_webhook',
  publication_id: TRIGGER, publisher_member_id: 'btr_member_asker', content_type: 'application/json', hop: 0, occurred_at: '2026-10-20T00:00:00Z',
  text: JSON.stringify(attestRoom),
})
await simulated.handleDelivery(new Request('https://bridge.example/bridge/hooks', {
  method: 'POST', body: simulatedText,
  headers: { 'content-type': 'application/json', 'x-beam-timestamp': String(clock), 'x-beam-signature': sign1(SECRET, clock, simulatedText) },
}))
await simulated.handleResult(new Request('https://bridge.example/bridge/cre/results', {
  method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ report: Buffer.from(roomRaw).toString('hex'), context: Buffer.from(CONTEXT).toString('hex'), signatures: [] }),
}))
check('unverified signers are reported as such', simulated.state.requests[TRIGGER].reply && 'signatures' in simulated.state.requests[TRIGGER].reply!
  ? simulated.state.requests[TRIGGER].reply!.signatures
  : null, { verified: false, source: 'unverified', f: null, signers: [] })
check('state is saved after every change', saves > 0, true)

// ─── Answers from a relay's contract ────────────────────────
const CONTRACT = `0x${'42'.repeat(20)}`
const CONTRACT_CONFIG: BridgeConfig = bridgeConfigSchema.parse({
  listen: { hostname: '127.0.0.1', port: 8098 },
  basePath: '/bridge',
  rooms: { [ROOM]: { channels: [CHANNEL] } },
  allowTransferAttestations: true,
  answers: {
    from: 'contract',
    chain: { rpcUrl: 'https://test.chain.example', chainId: 945, receiptsAddress: CONTRACT, logRangeBlocks: 1000 },
    pollSeconds: 5,
    answerWithinSeconds: 600,
  },
  statePath: '/dev/null',
})
const subject = (attestation: Attestation) =>
  attestation.op === 'attest_transfer' ? attestation.transferId : `${attestation.room}/${attestation.channel}/${attestation.publication}`
/** What the fake contract holds, by subject, and what was asked of it. */
const held = new Map<string, ChainAnswer['answer']>()
const opened: string[] = []
let failingReads = 0
const RECORDING_TX = `0x${'cd'.repeat(32)}` as const
const fakeChain = (donAttested: boolean): ChainIO => ({
  deployment: { chainId: 945, contract: CONTRACT, donAttested },
  lookup: async (attestation, since) => {
    if (failingReads > 0) {
      failingReads -= 1
      throw new Error('the RPC is down')
    }
    const answer = held.get(subject(attestation))
    if (!answer) return null
    return { answer, transaction: since === null ? null : RECORDING_TX, block: since === null ? null : since + 3n }
  },
  request: async (attestation) => {
    opened.push(subject(attestation))
    return { transaction: `0x${'ab'.repeat(32)}`, block: 100n }
  },
})
const chainBridge = (donAttested: boolean) =>
  new Bridge(CONTRACT_CONFIG, [SECRET], { now: () => clock, save: async () => {}, log: () => {}, cre: null, chain: fakeChain(donAttested) }, emptyState())
const deliverTo = (target: Bridge, message: unknown, publication: string) => {
  const text = JSON.stringify({
    version: 1, type: 'message', id: publication, room_id: ROOM, channel_id: 'btr_channel_messages', webhook_member_id: 'btr_member_webhook',
    publication_id: publication, publisher_member_id: 'btr_member_asker', content_type: 'application/json', hop: 0,
    occurred_at: '2026-10-20T00:00:00Z', text: JSON.stringify(message),
  })
  return target.handleDelivery(new Request('https://bridge.example/bridge/hooks', {
    method: 'POST', body: text,
    headers: { 'content-type': 'application/json', 'x-beam-timestamp': String(clock), 'x-beam-signature': sign1(SECRET, clock, text) },
  }))
}
const replyOf = async (target: Bridge, id: string) => {
  const answered = await target.handlePoll(id, new Request(`https://bridge.example/bridge/replies/${id}`, {
    headers: { 'x-beam-timestamp': String(clock), 'x-beam-signature': sign1(SECRET, clock, ''), 'x-beam-room': ROOM, 'x-beam-publication': id },
  }))
  return answered.status === 200 ? ((await answered.json()) as Record<string, any>) : answered.status
}

const relayBridge = chainBridge(false)
const askA = `btr_pub_${'f'.repeat(26)}`
const askB = `btr_pub_${'g'.repeat(26)}`
await deliverTo(relayBridge, attestRoom, askA)
await deliverTo(relayBridge, attestRoom, askB)
await relayBridge.pump()
check('two members asking about one publication open one request on the contract', [opened, relayBridge.state.requests[askB].requestedBlock], [[`${ROOM}/${CHANNEL}/${PUBLICATION}`], '100'])
check('nothing is answered while the contract holds no receipt', await replyOf(relayBridge, askA), 202)
held.set(`${ROOM}/${CHANNEL}/${PUBLICATION}`, { kind: 'room', entry: { ...roomEntry, level: Level.BeamReported | Level.TargetSigned } })
clock += 5
await relayBridge.pump()
const recorded = (await replyOf(relayBridge, askA)) as Record<string, any>
check('once the relay records it, the reply carries the contract record and the recording transaction', [
  recorded.type, recorded.outcome, recorded.answer.kind, recorded.answer.level_names, recorded.chain, recorded.claim,
], [CHAIN_REPLY, 'recorded', 'room_publication', ['beam_reported', 'target_signed'], {
  chain_id: 945, contract: CONTRACT, don_attested: false, transaction: RECORDING_TX, block: '103',
}, RELAY_RECORD_CLAIM])
check('every member who asked gets the reply', ((await replyOf(relayBridge, askB)) as Record<string, any>).outcome, 'recorded')
check('the relay was asked once in all', opened.length, 1)

const alreadyHeld = `btr_pub_${'h'.repeat(26)}`
held.set(TRANSFER_ID, {
  kind: 'standard',
  entry: {
    transferId: transferIdToBytes16(TRANSFER_ID), requestId: `0x${'00'.repeat(32)}`, status: 1, totalBytes: 10n, bytesTransferred: 10n,
    completedAtMs: 1n, planFingerprint: `0x${'00'.repeat(32)}`, coordinateChecksum: `0x${'00'.repeat(32)}`, level: Level.BeamReported,
  },
})
await deliverTo(relayBridge, { op: 'attest_transfer', transfer_id: TRANSFER_ID }, alreadyHeld)
clock += 5
await relayBridge.pump()
const fromState = (await replyOf(relayBridge, alreadyHeld)) as Record<string, any>
check('a receipt the contract already holds is answered without a new request', [opened.length, fromState.outcome, fromState.chain.transaction, fromState.answer.level_names], [1, 'recorded', null, ['beam_reported']])

const neverRecorded = `btr_pub_${'j'.repeat(26)}`
await deliverTo(relayBridge, { op: 'attest_room_publication', channel: CHANNEL, publication: `btr_pub_${'k'.repeat(26)}` }, neverRecorded)
clock += 5
await relayBridge.pump()
clock += 601
await relayBridge.pump()
const lapsed = (await replyOf(relayBridge, neverRecorded)) as Record<string, any>
check('a receipt the relay never records is answered as not recorded', [lapsed.type, lapsed.outcome, lapsed.reason], [CHAIN_REPLY, 'not_recorded', 'not_recorded'])

const unreadable = `btr_pub_${'m'.repeat(26)}`
await deliverTo(relayBridge, { op: 'attest_room_publication', channel: CHANNEL, publication: `btr_pub_${'n'.repeat(26)}` }, unreadable)
failingReads = MAX_CHAIN_FAILURES
for (let i = 0; i < MAX_CHAIN_FAILURES; i += 1) {
  clock += 5
  await relayBridge.pump()
}
check(`after ${MAX_CHAIN_FAILURES} failed contract calls the request is answered as unavailable`, ((await replyOf(relayBridge, unreadable)) as Record<string, any>).reason, 'chain_unavailable')

const refusedOnChain = await deliverTo(relayBridge, { op: 'summarize' }, `btr_pub_${'p'.repeat(26)}`)
check('a refusal carries the contract reply type', ((await refusedOnChain.json()) as Record<string, any>).type, CHAIN_REPLY)
check('a contract bridge takes no CRE reports', (await relayBridge.handleResult(new Request('https://bridge.example/bridge/cre/results', { method: 'POST', body: '{}' }))).status, 404)

const creDeployment = chainBridge(true)
const onCre = `btr_pub_${'q'.repeat(26)}`
await deliverTo(creDeployment, attestRoom, onCre)
await creDeployment.pump()
check('a CRE deployment\'s contract record says the forwarder checked the DON signatures', ((await replyOf(creDeployment, onCre)) as Record<string, any>).claim, DON_RECORD_CLAIM)

check(
  'contract answers need an account to open requests; the CRE simulator does not',
  [
    (() => { try { secretsFromEnv({ BRIDGE_WEBHOOK_SECRET: SECRET }, CONTRACT_CONFIG); return 'accepted' } catch (error) { return (error as Error).message } })(),
    secretsFromEnv({ BRIDGE_WEBHOOK_SECRET: SECRET }, CONFIG).evmPrivateKey,
  ],
  ['Set BRIDGE_EVM_PRIVATE_KEY to an account that may open requests on the receipts contract', null],
)
