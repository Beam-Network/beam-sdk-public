import {
  bytesToHex,
  consensusIdenticalAggregation,
  cre,
  type EVMLog,
  getNetwork,
  type HTTPSendRequester,
  logTriggerConfig,
  type Runtime,
} from '@chainlink/cre-sdk'
import { utf8ToBytes } from '@noble/hashes/utils.js'
import { type Address, decodeEventLog, type Hex, isAddress, zeroAddress } from 'viem'
import { z } from 'zod'
import {
  normalizeRequestId,
  projectStudioResponse,
  requestsAbi,
  type StudioOutcome,
  TRANSFER_REQUESTED_TOPIC,
  TransferKind,
  toBase64,
  UNAVAILABLE,
  type WebhookDelivery,
  webhookBody,
  webhookSignature,
} from './studio'

// ─── Config ──────────────────────────────────────────────────

/**
 * The secrets of one Studio inbound webhook: its URL, `https://<studio>/hooks/workflows/:workflowId/:triggerId/:token`,
 * and the trigger's signing secret, used when the trigger requires `X-Beam-Signature`.
 */
type WebhookSecrets = { url: string; signingSecret: string }

/** The transfer workflow, which starts standard requests. */
const TRANSFER_WEBHOOK: WebhookSecrets = { url: 'STUDIO_WEBHOOK_URL', signingSecret: 'STUDIO_WEBHOOK_SIGNING_SECRET' }
/**
 * The Room workflow, which starts Room delivery requests and jobs, whose input it delivers to the compute member;
 * its step binds `publicationKey` to the request id.
 */
const ROOM_WEBHOOK: WebhookSecrets = { url: 'STUDIO_ROOM_WEBHOOK_URL', signingSecret: 'STUDIO_ROOM_WEBHOOK_SIGNING_SECRET' }

/** The webhook that starts a request of this kind, or null when this deployment does not start it. */
export const webhookSecretsFor = (
  kind: number,
  starts: { startRoomDeliveries: boolean; startJobs: boolean },
): WebhookSecrets | null =>
  kind === TransferKind.Standard
    ? TRANSFER_WEBHOOK
    : (kind === TransferKind.RoomDelivery && starts.startRoomDeliveries) || (kind === TransferKind.Job && starts.startJobs)
      ? ROOM_WEBHOOK
      : null

const address = z.string().refine((value) => isAddress(value, { strict: false }), 'must be an EVM address')

export const configSchema = z.object({
  evms: z
    .array(
      z.object({
        chainSelectorName: z.string().min(1),
        /** The `BeamTransferReceipts` contract whose `TransferRequested` events start transfers. */
        receiptsAddress: address,
      }),
    )
    .length(1),
  /** Sign every delivery with the trigger's secret; the Studio triggers must then require signatures. */
  signDeliveries: z.boolean(),
  /** Start Room delivery requests (kind 2) through the Room workflow's webhook. */
  startRoomDeliveries: z.boolean(),
  /** Start jobs (kind 3) through the Room workflow's webhook, which delivers their input to the compute member. */
  startJobs: z.boolean(),
})

export type Config = z.infer<typeof configSchema>

// ─── Handler ─────────────────────────────────────────────────

/**
 * EVM log trigger: a `requestTransfer(requestId, kind, input)` call starts one Studio run. Every node posts;
 * Studio keys the run on `Idempotency-Key: requestId`, so repeats answer with the same run.
 * - A standard request goes to the transfer workflow, whose step takes the request id as its idempotency key.
 *   That fixes the transfer id the receipts workflow's `reconcile` derives.
 * - A Room delivery request goes to the Room workflow, whose step publishes under `request:<requestId>`.
 *   `reconcile` finds that publication by its publisher and key.
 * - A job goes to the same Room workflow, which delivers its input to the compute member the same way.
 */
export const onTransferRequested = (runtime: Runtime<Config>, log: EVMLog): string => {
  if (log.removed) return 'Ignored a request log removed by a reorg'
  const event = decodeEventLog({
    abi: requestsAbi,
    data: bytesToHex(log.data),
    topics: log.topics.map((topic) => bytesToHex(topic)) as [Hex, ...Hex[]],
  })
  const requestId = normalizeRequestId(event.args.requestId)
  const webhook = webhookSecretsFor(event.args.kind, runtime.config)
  if (!webhook) {
    return `start: ${requestId} is a kind ${event.args.kind} request, which this deployment does not start`
  }

  const signed = runtime.config.signDeliveries
  const secrets = runtime
    .getSecrets(signed ? [{ id: webhook.url }, { id: webhook.signingSecret }] : [{ id: webhook.url }])
    .result()
  const body = webhookBody(requestId, event.args.requester as Address, event.args.input)
  const timestampSeconds = Math.floor(runtime.now().getTime() / 1000)
  const delivery: WebhookDelivery = {
    url: secrets[webhook.url].value,
    requestId,
    body,
    timestampSeconds,
    signature: signed ? webhookSignature(secrets[webhook.signingSecret].value, timestampSeconds, body) : null,
  }

  const http = new cre.capabilities.HTTPClient()
  const outcome = JSON.parse(
    http
      .sendRequest(runtime, postToStudio, consensusIdenticalAggregation<string>().withDefault(UNAVAILABLE))(delivery)
      .result(),
  ) as StudioOutcome
  return describeOutcome(requestId, outcome)
}

/**
 * The execution result. A refusal or an unavailable Studio fails the execution, so it shows in the CRE
 * execution history; the request stays open and `staleRequests()` lists it. The webhook URL carries the
 * trigger token and is never logged.
 */
export const describeOutcome = (requestId: Hex, outcome: StudioOutcome): string => {
  if (outcome.state === 'accepted') {
    return outcome.runId
      ? `start: ${requestId} accepted as Studio run ${outcome.runId}`
      : `start: ${requestId} accepted into a coalescing window`
  }
  if (outcome.state === 'refused') {
    throw new Error(`Studio refused ${requestId} with HTTP ${outcome.status}${outcome.reason ? ` (${outcome.reason})` : ''}`)
  }
  throw new Error(`Studio was unavailable for ${requestId} (HTTP ${outcome.status || 'none'})`)
}

// ─── Studio ──────────────────────────────────────────────────

/** Runs on each node: one POST to the Studio webhook, projected to the outcome the nodes agree on. */
export const postToStudio = (sendRequester: HTTPSendRequester, delivery: WebhookDelivery): string => {
  const headers: Record<string, { values: string[] }> = {
    'content-type': { values: ['application/json'] },
    'idempotency-key': { values: [delivery.requestId] },
  }
  if (delivery.signature) {
    headers['x-beam-timestamp'] = { values: [String(delivery.timestampSeconds)] }
    headers['x-beam-signature'] = { values: [delivery.signature] }
  }
  const response = sendRequester
    .sendRequest({
      url: delivery.url,
      method: 'POST',
      body: toBase64(utf8ToBytes(delivery.body)),
      multiHeaders: headers,
      // Best effort only: Studio dedupes every delivery by `Idempotency-Key`.
      cacheSettings: { store: true, maxAge: '60s' },
    })
    .result()
  return JSON.stringify(projectStudioResponse(response.statusCode, response.body))
}

// ─── Workflow ────────────────────────────────────────────────

const evmClientFor = (chainSelectorName: string) => {
  const network = getNetwork({ chainFamily: 'evm', chainSelectorName })
  if (!network) throw new Error(`Network not found: ${chainSelectorName}`)
  return new cre.capabilities.EVMClient(network.chainSelector.selector)
}

export const initWorkflow = (config: Config) => {
  const evm = config.evms[0]
  if (evm.receiptsAddress.toLowerCase() === zeroAddress) {
    throw new Error('Set evms[0].receiptsAddress to the deployed BeamTransferReceipts contract')
  }
  return [
    cre.handler(
      evmClientFor(evm.chainSelectorName).logTrigger(
        logTriggerConfig({ addresses: [evm.receiptsAddress as Hex], topics: [[TRANSFER_REQUESTED_TOPIC]] }),
      ),
      onTransferRequested,
    ),
  ]
}
