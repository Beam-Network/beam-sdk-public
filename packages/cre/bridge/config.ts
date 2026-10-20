import { isAddress } from 'viem'
import { z } from 'zod'

const address = z.string().refine((value) => isAddress(value, { strict: false }), 'must be an EVM address')
const httpUrl = z
  .string()
  .regex(/^https?:\/\/[^/\s]+(\/\S*)?$/, 'must be an http(s) URL')
  .transform((value) => value.replace(/\/+$/, ''))
const bytes32Hex = z
  .string()
  .regex(/^(0x)?[0-9a-fA-F]{64}$/, 'must be 32 bytes of hex')
  .transform((value) => value.replace(/^0x/, '').toLowerCase())
const roomId = z.string().min(1).max(128)

/** The Capability Registry on Ethereum mainnet that names every production CRE DON's signers. */
export const CAPABILITY_REGISTRY = '0x76c9cf548b4179F8901cda1f8623568b58215E62'

/** Answers from the receipts workflow on a Chainlink DON: DON-signed reports sent back to the bridge. */
const creAnswers = z
  .object({
    from: z.literal('cre'),
    /** How attestation requests reach the receipts workflow's HTTP trigger. */
    forward: z.discriminatedUnion('mode', [
      /** A deployed workflow, through the CRE gateway; requests are signed with `BRIDGE_EVM_PRIVATE_KEY`. */
      z.object({ mode: z.literal('gateway'), gatewayUrl: httpUrl, workflowId: bytes32Hex }).strict(),
      /**
       * A command run once per batch with the path of a file holding the trigger input appended, for example
       * `cre workflow simulate … --http-payload`. For development before a workflow is deployed.
       */
      z.object({ mode: z.literal('command'), argv: z.array(z.string().min(1)).min(1), cwd: z.string().min(1) }).strict(),
    ]),
    /** The workflow whose reports the bridge accepts, as the report header names it. */
    workflow: z.object({ id: bytes32Hex, owner: address.transform((value) => value.toLowerCase()) }).strict(),
    /**
     * Whose signatures a report needs: f+1 of its DON's signers, read from the Capability Registry over an
     * Ethereum mainnet RPC, or none. `unverified` is for the CRE simulator, whose reports carry throwaway keys; its
     * replies say their signatures were not checked.
     */
    signers: z.discriminatedUnion('source', [
      z.object({ source: z.literal('registry'), rpcUrl: httpUrl, registryAddress: address }).strict(),
      z.object({ source: z.literal('unverified') }).strict(),
    ]),
    /** Seconds between calls to the workflow. Its HTTP trigger takes one call per 30 seconds. */
    paceSeconds: z.number().int().min(30).max(3600),
    /** Seconds to wait for a forwarded request's report before asking again. */
    retrySeconds: z.number().int().min(30).max(3600),
    /** Times a request is forwarded before it is answered as not attested. */
    maxAttempts: z.number().int().min(1).max(50),
  })
  .strict()

/**
 * Answers from a `BeamTransferReceipts` contract that a relay serves, on a chain CRE does not reach: the bridge
 * opens each request on the contract and replies with the receipt the relay records there.
 */
const contractAnswers = z
  .object({
    from: z.literal('contract'),
    chain: z
      .object({
        /** JSON-RPC endpoint of the chain. */
        rpcUrl: httpUrl,
        chainId: z.number().int().positive(),
        /** The contract the relay records receipts on. `BRIDGE_EVM_PRIVATE_KEY` must be its owner or a requester. */
        receiptsAddress: address,
        /** Blocks per `eth_getLogs` call when looking for the transaction that recorded a receipt. */
        logRangeBlocks: z.number().int().min(1).max(10_000),
      })
      .strict(),
    /** Seconds between reads of the contract. */
    pollSeconds: z.number().int().min(2).max(300),
    /** Seconds the relay has to record a receipt after the request is opened, before the reply says it did not. */
    answerWithinSeconds: z.number().int().min(60).max(86_400),
  })
  .strict()

export const bridgeConfigSchema = z
  .object({
    listen: z.object({ hostname: z.string().min(1), port: z.number().int().min(1).max(65535) }).strict(),
    /**
     * Prefix of every route. The Room webhook URL is `https://<host><basePath>/hooks`; replies are polled from
     * `<basePath>/replies/<id>` and, with CRE answers, reports arrive at `<basePath>/cre/results`.
     */
    basePath: z.string().regex(/^(\/[A-Za-z0-9._~-]+)*$/, 'must be empty or /segments without a trailing slash'),
    /**
     * Object channels whose publications each Room may ask about, by Room id. A Room member asks only about its own
     * Room, and only about these channels.
     */
    rooms: z.record(roomId, z.object({ channels: z.array(roomId).min(1) }).strict()),
    /** Whether Room members may ask about standard transfers of the deployment's Beam account, by transfer id. */
    allowTransferAttestations: z.boolean(),
    /** Where answers come from: a Chainlink DON, or a contract a relay serves. */
    answers: z.discriminatedUnion('from', [creAnswers, contractAnswers]),
    /** File holding open requests and their replies, so polls survive a restart. */
    statePath: z.string().min(1),
  })
  .strict()

export type BridgeConfig = z.infer<typeof bridgeConfigSchema>
export type CreAnswers = z.infer<typeof creAnswers>
export type ContractAnswers = z.infer<typeof contractAnswers>

/** Secrets, read from the environment, never from the config file. */
export type BridgeSecrets = {
  /** The webhook's signing secret: the current one, then the previous one during a rotation overlap. */
  webhookSecrets: string[]
  /**
   * Signs CRE gateway requests, whose authorized keys must include its address, or opens requests on the contract,
   * which must allow its address as a requester.
   */
  evmPrivateKey: `0x${string}` | null
}

export const secretsFromEnv = (env: Record<string, string | undefined>, config: BridgeConfig): BridgeSecrets => {
  const secret = env.BRIDGE_WEBHOOK_SECRET?.trim()
  if (!secret) throw new Error('Set BRIDGE_WEBHOOK_SECRET to the Room webhook signing secret')
  const previous = env.BRIDGE_WEBHOOK_PREVIOUS_SECRET?.trim()
  const answers = config.answers
  let evmPrivateKey: `0x${string}` | null = null
  if (answers.from === 'contract' || answers.forward.mode === 'gateway') {
    const key = env.BRIDGE_EVM_PRIVATE_KEY?.trim()
    if (!key) {
      throw new Error(
        answers.from === 'contract'
          ? 'Set BRIDGE_EVM_PRIVATE_KEY to an account that may open requests on the receipts contract'
          : 'Set BRIDGE_EVM_PRIVATE_KEY to sign CRE gateway requests',
      )
    }
    evmPrivateKey = (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`
    if (!/^0x[0-9a-fA-F]{64}$/.test(evmPrivateKey)) throw new Error('BRIDGE_EVM_PRIVATE_KEY must be 32 bytes of hex')
  }
  return { webhookSecrets: previous ? [secret, previous] : [secret], evmPrivateKey }
}
