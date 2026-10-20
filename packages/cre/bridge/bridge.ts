import { type Hex, keccak256, toHex } from 'viem'
import {
  type Attestation,
  type AttestationAnswer,
  type AttestationRequest,
  answerFor,
  MAX_ATTESTATIONS,
  type ReportSubmission,
  reportSubmission,
} from '../workflows/transfer-receipts/attestations'
import { decodeReport, Level, ReceiptStatus } from '../workflows/transfer-receipts/receipts'
import { bytes16ToTransferId } from '../workflows/transfer-receipts/transfer-id'
import type { BridgeConfig, ContractAnswers, CreAnswers } from './config'
import type { ForwardOutcome } from './cre'
import { parseHeader, parseSubmission, reportBody, type SignerSet, verifyReportSignatures } from './report'
import { type AttestationPolicy, parseEnvelope, parseRoomRequest, verifyWebhookSignature } from './webhook'

// The bridge between a Room and Beam's receipts: Room members ask in a Room message, the bridge gets the answer, and
// it becomes the reply the Room host polls for. Answers come either from the receipts workflow on a Chainlink DON,
// as DON-signed reports, or from a receipts contract that a relay serves on a chain CRE does not reach.

/** An attestation a Room member asked for, keyed by the publication id of the message that asked. */
export type BridgeRequest = {
  id: string
  room: string
  channel: string
  publisher: string
  receivedAt: number
  attestation: Attestation
  publishOnchain: boolean
  /** CRE: times forwarded. Contract: failed reads or requests. */
  attempts: number
  /** When the request was forwarded to CRE, or opened on the contract. */
  forwardedAt: number | null
  /** Contract: the block of the transaction that opened the request, as a decimal string. */
  requestedBlock: string | null
  reply: BridgeReply | null
  answeredAt: number | null
}

export type BridgeState = {
  requests: Record<string, BridgeRequest>
  /** keccak256 of the raw reports already taken, most recent last. */
  reports: string[]
}

export const emptyState = (): BridgeState => ({ requests: {}, reports: [] })

export const CRE_REPLY = 'beam.cre.attestation.v1'
export const CHAIN_REPLY = 'beam.chain.receipt.v1'

/** What the Room host publishes as the webhook's reply, at most 64 KiB of JSON. */
export type BridgeReply =
  | {
      type: typeof CRE_REPLY
      outcome: 'attested'
      request: RequestView
      answer: AnswerView
      signatures: { verified: boolean; source: 'registry' | 'unverified'; f: number | null; signers: string[] }
      workflow: { id: string; owner: string; name: string; execution_id: string; don_id: number; timestamp: number }
      report: ReportSubmission
      claim: string
    }
  | {
      type: typeof CHAIN_REPLY
      outcome: 'recorded'
      request: RequestView
      answer: AnswerView
      chain: { chain_id: number; contract: string; don_attested: boolean; transaction: string | null; block: string | null }
      claim: string
    }
  | {
      type: typeof CRE_REPLY | typeof CHAIN_REPLY
      outcome: 'not_attested' | 'not_recorded' | 'refused'
      request: RequestView | null
      reason: string
      message: string
    }

type RequestView =
  | { op: 'attest_transfer'; transfer_id: string }
  | { op: 'attest_room_publication'; room: string; channel: string; publication: string }

type AnswerView = Record<string, string | number | string[] | null>

/** What every DON-attested reply claims, and no more. */
export const ATTESTATION_CLAIM =
  'Each signing Chainlink node read this answer from Beam and signed the same report. It shows what Beam reported, ' +
  'not that Beam is right: the DON does not check Beam\'s evidence independently.'

/** What a reply from a relay's contract claims. */
export const RELAY_RECORD_CLAIM =
  'A relay operator read this answer from Beam and recorded it on this contract, where anyone can read it. It is ' +
  'Beam-reported: no Chainlink DON or other independent party attests it.'

/** What a reply from a CRE deployment's contract claims. */
export const DON_RECORD_CLAIM =
  'Chainlink\'s forwarder checked the DON\'s signatures before this answer was recorded on this contract, where anyone ' +
  'can read it. It shows what Beam reported, not that Beam is right.'

/** The bridge's link to the receipts workflow on a Chainlink DON. */
export type CreIO = {
  forward(input: AttestationRequest): Promise<ForwardOutcome>
  /** A DON's signers, or null when the bridge does not verify signatures (CRE simulator). */
  signers(donId: number, refresh: boolean): Promise<SignerSet | null>
}

/** A receipt the contract holds, and the transaction that recorded it when the bridge found it. */
export type ChainAnswer = { answer: AttestationAnswer; transaction: Hex | null; block: bigint | null }

/** The bridge's link to a receipts contract. */
export type ChainIO = {
  deployment: { chainId: number; contract: string; donAttested: boolean }
  /**
   * What the contract holds for an attestation, or null while it holds nothing. From `since`, the block its request
   * was opened in, it also finds the recording transaction; with null it does not look.
   */
  lookup(attestation: Attestation, since: bigint | null): Promise<ChainAnswer | null>
  /** Opens the request on the contract, which ignores one already open or answered. */
  request(attestation: Attestation): Promise<{ transaction: Hex; block: bigint }>
}

/** Everything the bridge does outside its own logic. */
export type BridgeIO = {
  /** Unix seconds. */
  now(): number
  save(state: BridgeState): Promise<void>
  log(message: string): void
  /** Set when answers come from CRE. */
  cre: CreIO | null
  /** Set when answers come from a contract. */
  chain: ChainIO | null
}

/** Seconds a request or its reply is kept: the longest reply window a Room host polls for, plus an hour. */
export const RETENTION_SECONDS = 25 * 3600
/** Seconds the host is asked to wait between polls while a reply is pending. */
export const POLL_RETRY_SECONDS = 15
/** Failed contract reads or requests after which a request is answered as not recorded. */
export const MAX_CHAIN_FAILURES = 5
const MAX_REPLY_BYTES = 64 * 1024
const MAX_REMEMBERED_REPORTS = 1000

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } })

const reply = (body: BridgeReply) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json', 'x-beam-reply': 'v1' },
  })

export class Bridge {
  private nextRunAt = 0
  private readonly policy: AttestationPolicy
  private readonly replyType: typeof CRE_REPLY | typeof CHAIN_REPLY

  constructor(
    private readonly config: BridgeConfig,
    private readonly webhookSecrets: string[],
    private readonly io: BridgeIO,
    readonly state: BridgeState,
  ) {
    this.policy = { rooms: config.rooms, allowTransferAttestations: config.allowTransferAttestations }
    this.replyType = config.answers.from === 'cre' ? CRE_REPLY : CHAIN_REPLY
    if (config.answers.from === 'cre' ? !io.cre : !io.chain) {
      throw new Error(`The bridge's IO has no link for ${config.answers.from} answers`)
    }
  }

  /** `POST <basePath>/hooks`: a message the Room host delivers. */
  async handleDelivery(request: Request): Promise<Response> {
    const body = new Uint8Array(await request.arrayBuffer())
    if (!this.signed(request, body)) return new Response(null, { status: 401 })
    // Files and test events ask nothing; 204 acknowledges them without a reply.
    if (!(request.headers.get('content-type') ?? '').startsWith('application/json')) return new Response(null, { status: 204 })
    const envelope = parseEnvelope(body)
    if (!envelope) return new Response(null, { status: 204 })

    const asked = parseRoomRequest(envelope, this.policy)
    if (asked.kind === 'ignore') return new Response(null, { status: 204 })
    if (asked.kind === 'refuse') {
      return reply({ type: this.replyType, outcome: 'refused', request: null, reason: asked.reason, message: asked.message })
    }

    const id = envelope.publication_id
    if (!this.state.requests[id]) {
      this.state.requests[id] = {
        id,
        room: envelope.room_id,
        channel: envelope.channel_id,
        publisher: envelope.publisher_member_id,
        receivedAt: this.io.now(),
        attestation: asked.attestation,
        publishOnchain: asked.publishOnchain,
        attempts: 0,
        forwardedAt: null,
        requestedBlock: null,
        reply: null,
        answeredAt: null,
      }
      await this.io.save(this.state)
      this.io.log(`request ${id}: ${describe(asked.attestation)} from ${envelope.publisher_member_id}`)
    }
    return new Response(null, {
      status: 202,
      headers: { 'x-beam-reply': 'v1', location: `replies/${encodeURIComponent(id)}`, 'retry-after': String(POLL_RETRY_SECONDS) },
    })
  }

  /** `GET <basePath>/replies/<id>`: the Room host's signed poll for a reply. */
  async handlePoll(id: string, request: Request): Promise<Response> {
    if (!this.signed(request, new Uint8Array())) return new Response(null, { status: 401 })
    const pending = this.state.requests[id]
    if (
      !pending ||
      request.headers.get('x-beam-publication') !== pending.id ||
      request.headers.get('x-beam-room') !== pending.room
    ) {
      return new Response(null, { status: 404 })
    }
    if (!pending.reply) {
      return new Response(null, {
        status: 202,
        headers: { 'x-beam-reply': 'v1', 'retry-after': String(POLL_RETRY_SECONDS) },
      })
    }
    return reply(pending.reply)
  }

  /**
   * `POST <basePath>/cre/results`: a DON-signed report the workflow sends back. Every node sends its own copy; the
   * first that verifies answers the requests it is about, and later copies are acknowledged unread. Only a bridge
   * whose answers come from CRE takes reports.
   */
  async handleResult(request: Request): Promise<Response> {
    const answers = this.config.answers
    const cre = this.io.cre
    if (answers.from !== 'cre' || !cre) return new Response(null, { status: 404 })
    let submitted: unknown
    try {
      submitted = await request.json()
    } catch {
      return json(400, { error: 'invalid_report' })
    }
    const signed = parseSubmission(submitted)
    if (!signed) return json(400, { error: 'invalid_report' })
    const digest = keccak256(toHex(signed.rawReport))
    if (this.state.reports.includes(digest)) return json(200, { accepted: true, duplicate: true })

    let header: ReturnType<typeof parseHeader>
    let decoded: ReturnType<typeof decodeReport>
    try {
      header = parseHeader(signed.rawReport)
      decoded = decodeReport(reportBody(signed.rawReport))
    } catch (error) {
      return json(400, { error: 'invalid_report', message: (error as Error).message })
    }
    if (header.workflowId !== answers.workflow.id || `0x${header.workflowOwner}` !== answers.workflow.owner) {
      this.io.log(`report ${digest}: from workflow ${header.workflowId} of 0x${header.workflowOwner}, not the configured one`)
      return json(403, { error: 'unexpected_workflow' })
    }

    let signers: SignerSet | null
    let accepted: string[] = []
    try {
      signers = await cre.signers(header.donId, false)
    } catch (error) {
      this.io.log(`report ${digest}: DON ${header.donId} signers unavailable: ${(error as Error).message}`)
      return json(503, { error: 'signers_unavailable' })
    }
    if (signers) {
      try {
        accepted = await verifyReportSignatures(signed, signers)
      } catch {
        // The DON may have rotated its signers since they were read: read them once more.
        try {
          signers = await cre.signers(header.donId, true)
        } catch (error) {
          this.io.log(`report ${digest}: DON ${header.donId} signers unavailable: ${(error as Error).message}`)
          return json(503, { error: 'signers_unavailable' })
        }
        try {
          accepted = signers ? await verifyReportSignatures(signed, signers) : []
        } catch (error) {
          this.io.log(`report ${digest}: signatures not accepted: ${(error as Error).message}`)
          return json(403, { error: 'invalid_signatures' })
        }
      }
    }

    const submission = reportSubmission(signed.rawReport, signed.context, signed.signatures)
    let answered = 0
    for (const pending of Object.values(this.state.requests)) {
      if (pending.reply) continue
      const answer = answerFor(pending.attestation, decoded)
      if (!answer) continue
      const attested: BridgeReply = {
        type: CRE_REPLY,
        outcome: 'attested',
        request: requestView(pending.attestation),
        answer: answerView(answer),
        signatures: {
          verified: signers !== null,
          source: signers ? 'registry' : 'unverified',
          f: signers?.f ?? null,
          signers: accepted,
        },
        workflow: {
          id: header.workflowId,
          owner: `0x${header.workflowOwner}`,
          name: header.workflowName,
          execution_id: header.executionId,
          don_id: header.donId,
          timestamp: header.timestamp,
        },
        report: submission,
        claim: ATTESTATION_CLAIM,
      }
      if (JSON.stringify(attested).length > MAX_REPLY_BYTES) {
        this.io.log(`request ${pending.id}: the reply is over 64 KiB; answering without the report`)
        this.answer(pending, unanswered(this.replyType, pending, 'reply_too_large', 'The signed report does not fit in a Room reply.'))
      } else {
        this.answer(pending, attested)
      }
      answered += 1
    }
    this.state.reports = [...this.state.reports, digest].slice(-MAX_REMEMBERED_REPORTS)
    await this.io.save(this.state)
    this.io.log(
      `report ${digest}: execution ${header.executionId}, ${signers ? `${accepted.length} DON signatures verified` : 'signatures not verified'}, answered ${answered}`,
    )
    return json(200, { accepted: true, answered })
  }

  /** Called every second: moves pending requests forward, and drops what is past retention. */
  async pump(): Promise<void> {
    const answers = this.config.answers
    if (answers.from === 'cre') await this.pumpCre(answers)
    else await this.pumpContract(answers)
  }

  /**
   * Forwards the oldest due requests in one batch, at most once per `paceSeconds`, and answers requests forwarded
   * `maxAttempts` times without a report as not attested.
   */
  private async pumpCre(answers: CreAnswers): Promise<void> {
    const now = this.io.now()
    let changed = this.dropExpired(now)
    for (const pending of this.unanswered()) {
      if (
        pending.attempts >= answers.maxAttempts &&
        pending.forwardedAt !== null &&
        pending.forwardedAt + answers.retrySeconds <= now
      ) {
        this.answer(
          pending,
          unanswered(
            CRE_REPLY,
            pending,
            'no_finished_receipt',
            `No DON-signed report came back after ${pending.attempts} requests: Beam reported nothing finished for it, or the workflow could not be reached.`,
          ),
        )
        this.io.log(`request ${pending.id}: not attested after ${pending.attempts} attempts`)
        changed = true
      }
    }
    const due =
      now < this.nextRunAt
        ? []
        : this.unanswered().filter(
            (pending) =>
              pending.attempts < answers.maxAttempts &&
              (pending.forwardedAt === null || pending.forwardedAt + answers.retrySeconds <= now),
          )
    if (due.length === 0) {
      if (changed) await this.io.save(this.state)
      return
    }

    // One execution takes one `publishOnchain`; the oldest request's decides the batch.
    const publishOnchain = due[0].publishOnchain
    const batch: BridgeRequest[] = []
    const attest = new Map<string, Attestation>()
    for (const pending of due) {
      if (pending.publishOnchain !== publishOnchain) continue
      const key = describe(pending.attestation)
      if (!attest.has(key) && attest.size === MAX_ATTESTATIONS) continue
      attest.set(key, pending.attestation)
      batch.push(pending)
    }

    this.nextRunAt = now + answers.paceSeconds
    const outcome = await this.io.cre!.forward({ attest: [...attest.values()], publishOnchain })
    for (const pending of batch) {
      pending.attempts += 1
      pending.forwardedAt = now
    }
    this.io.log(
      outcome.accepted
        ? `forwarded ${attest.size} attestations for ${batch.length} requests${outcome.executionId ? ` as execution ${outcome.executionId}` : ''}`
        : `forwarding ${attest.size} attestations failed: ${outcome.reason}`,
    )
    await this.io.save(this.state)
  }

  /**
   * Every `pollSeconds`: answers each pending request whose receipt the contract holds, and opens the request on the
   * contract for the others, once per subject. The relay records the receipt like any open request's. A request
   * the relay does not answer within `answerWithinSeconds` is answered as not recorded.
   */
  private async pumpContract(answers: ContractAnswers): Promise<void> {
    const now = this.io.now()
    let changed = this.dropExpired(now)
    for (const pending of this.unanswered()) {
      if (pending.forwardedAt !== null && now - pending.forwardedAt > answers.answerWithinSeconds) {
        this.answer(
          pending,
          unanswered(
            CHAIN_REPLY,
            pending,
            'not_recorded',
            `No receipt was recorded within ${answers.answerWithinSeconds} seconds: Beam reported nothing finished for it, or the relay is not running.`,
          ),
        )
        this.io.log(`request ${pending.id}: not recorded within ${answers.answerWithinSeconds} s`)
        changed = true
      }
    }
    if (now < this.nextRunAt) {
      if (changed) await this.io.save(this.state)
      return
    }
    this.nextRunAt = now + answers.pollSeconds

    const chain = this.io.chain!
    const opened = new Map<string, { transaction: Hex; block: bigint }>()
    for (const pending of this.unanswered()) {
      try {
        const found = await chain.lookup(pending.attestation, pending.requestedBlock === null ? null : BigInt(pending.requestedBlock))
        if (found) {
          this.answer(pending, recordedReply(chain.deployment, pending, found))
          this.io.log(`request ${pending.id}: answered from the contract${found.transaction ? ` (recorded in ${found.transaction})` : ''}`)
          changed = true
          continue
        }
        if (pending.forwardedAt !== null) continue
        const key = describe(pending.attestation)
        let opening = opened.get(key)
        if (!opening) {
          opening = await chain.request(pending.attestation)
          opened.set(key, opening)
          this.io.log(`opened the request for ${key} in ${opening.transaction}`)
        }
        pending.forwardedAt = now
        pending.requestedBlock = opening.block.toString()
        changed = true
      } catch (error) {
        pending.attempts += 1
        changed = true
        this.io.log(`request ${pending.id}: contract call failed (${pending.attempts}/${MAX_CHAIN_FAILURES}): ${(error as Error).message}`)
        if (pending.attempts >= MAX_CHAIN_FAILURES) {
          this.answer(
            pending,
            unanswered(CHAIN_REPLY, pending, 'chain_unavailable', 'The receipts contract could not be read or asked for this receipt.'),
          )
        }
      }
    }
    if (changed) await this.io.save(this.state)
  }

  private unanswered(): BridgeRequest[] {
    return Object.values(this.state.requests)
      .filter((pending) => !pending.reply)
      .sort((a, b) => a.receivedAt - b.receivedAt)
  }

  private dropExpired(now: number): boolean {
    let changed = false
    for (const pending of Object.values(this.state.requests)) {
      if (now - pending.receivedAt > RETENTION_SECONDS) {
        delete this.state.requests[pending.id]
        changed = true
      }
    }
    return changed
  }

  private answer(pending: BridgeRequest, bridgeReply: BridgeReply) {
    pending.reply = bridgeReply
    pending.answeredAt = this.io.now()
  }

  private signed(request: Request, body: Uint8Array): boolean {
    return verifyWebhookSignature(
      this.webhookSecrets,
      request.headers.get('x-beam-timestamp'),
      request.headers.get('x-beam-signature'),
      body,
      this.io.now(),
    )
  }
}

const unanswered = (
  type: typeof CRE_REPLY | typeof CHAIN_REPLY,
  pending: BridgeRequest,
  reason: string,
  message: string,
): BridgeReply => ({
  type,
  outcome: type === CRE_REPLY ? 'not_attested' : 'not_recorded',
  request: requestView(pending.attestation),
  reason,
  message,
})

const recordedReply = (deployment: ChainIO['deployment'], pending: BridgeRequest, found: ChainAnswer): BridgeReply => ({
  type: CHAIN_REPLY,
  outcome: 'recorded',
  request: requestView(pending.attestation),
  answer: answerView(found.answer),
  chain: {
    chain_id: deployment.chainId,
    contract: deployment.contract,
    don_attested: deployment.donAttested,
    transaction: found.transaction,
    block: found.block === null ? null : found.block.toString(),
  },
  claim: deployment.donAttested ? DON_RECORD_CLAIM : RELAY_RECORD_CLAIM,
})

const describe = (attestation: Attestation): string =>
  attestation.op === 'attest_transfer'
    ? `transfer ${attestation.transferId}`
    : `publication ${attestation.room}/${attestation.channel}/${attestation.publication}`

const requestView = (attestation: Attestation): RequestView =>
  attestation.op === 'attest_transfer'
    ? { op: 'attest_transfer', transfer_id: attestation.transferId }
    : { op: 'attest_room_publication', room: attestation.room, channel: attestation.channel, publication: attestation.publication }

const STATUS_NAMES = new Map<number, string>(
  Object.entries(ReceiptStatus).map(([name, code]) => [code, name.toLowerCase()]),
)

const levelNames = (level: number): string[] =>
  [
    [Level.BeamReported, 'beam_reported'],
    [Level.TargetSigned, 'target_signed'],
    [Level.DonAttested, 'don_attested'],
  ].flatMap(([bit, name]) => ((level & (bit as number)) !== 0 ? [name as string] : []))

export const answerView = (answer: AttestationAnswer): AnswerView => {
  if (answer.kind === 'standard') {
    const entry = answer.entry
    return {
      kind: 'transfer',
      transfer_id: bytes16ToTransferId(entry.transferId),
      status: STATUS_NAMES.get(entry.status) ?? String(entry.status),
      total_bytes: entry.totalBytes.toString(),
      bytes_transferred: entry.bytesTransferred.toString(),
      completed_at_ms: entry.completedAtMs.toString(),
      plan_fingerprint: entry.planFingerprint,
      coordinate_checksum: entry.coordinateChecksum,
      level: entry.level,
      level_names: levelNames(entry.level),
    }
  }
  if (answer.kind === 'room') {
    const entry = answer.entry
    return {
      kind: 'room_publication',
      room: entry.room,
      channel: entry.channel,
      publication: entry.publication,
      status: STATUS_NAMES.get(entry.status) ?? String(entry.status),
      file_size_bytes: entry.fileSizeBytes.toString(),
      manifest_sha256: entry.manifestSha256,
      target_count: entry.targetCount,
      evidence_sha256: entry.evidenceSha256,
      assignments_sha256: entry.assignmentsSha256,
      evidence_version: entry.evidenceVersion,
      level: entry.level,
      level_names: levelNames(entry.level),
    }
  }
  const transferId = answer.entry.transferId
  return {
    kind: 'bucket_only_publication',
    standard_transfer_id: /^0x0+$/.test(transferId) ? null : bytes16ToTransferId(transferId),
  }
}
