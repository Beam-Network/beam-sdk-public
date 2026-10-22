import { isAddress } from 'viem'
import { z } from 'zod'

const address = z.string().refine((value) => isAddress(value, { strict: false }), 'must be an EVM address')
const httpUrl = z
  .string()
  .regex(/^https?:\/\/[^/\s]+(\/\S*)?$/, 'must be an http(s) URL')
  .transform((value) => value.replace(/\/+$/, ''))

export const relayConfigSchema = z
  .object({
    chain: z
      .object({
        /** JSON-RPC endpoint; it must support the `finalized` block tag. */
        rpcUrl: httpUrl,
        chainId: z.number().int().positive(),
        /** A relay deployment of `BeamTransferReceipts`: `donAttested()` false, the relay's account as forwarder. */
        receiptsAddress: address,
        /** First block scanned for requests on the first run; after that the cursor file decides. */
        startBlock: z.number().int().nonnegative(),
        /** Blocks per `eth_getLogs` call. */
        logRangeBlocks: z.number().int().min(1).max(10_000),
      })
      .strict(),
    /** BeamCore public API, which serves `GET /transfers/:id` and `GET /auth/me`. */
    beamApiBaseUrl: httpUrl,
    /** Beam coordinator, which serves the Room receipt route. */
    roomApiBaseUrl: httpUrl,
    /** `user_id` from `GET /auth/me` for `BEAM_API_KEY`; transfer ids derive from it. */
    beamClientId: z.string().min(1),
    /**
     * Start the transfers `TransferRequested` logs ask for, through Studio. With false the relay records receipts
     * only and needs no Studio; transfer requests stay open until something else starts them.
     */
    starts: z.boolean(),
    /** Sign every Studio delivery; the Studio triggers must then require signatures. */
    signDeliveries: z.boolean(),
    /**
     * The Room route of this deployment's Studio Room workflow, whose step binds `publicationKey` to the request
     * id. With one, the relay starts Room delivery requests and records their receipts; null starts none.
     */
    roomDelivery: z
      .object({
        room: z.string().min(1).max(128),
        channel: z.string().min(1).max(128),
        publisherMemberId: z.string().regex(/^btr_member_[a-z2-7]{26}$/),
      })
      .strict()
      .nullable(),
    /**
     * Where this deployment's jobs publish their output: an object channel of the `roomDelivery` Room, and the
     * compute member that publishes there under the job's request key. With one, the relay starts jobs through the
     * Room workflow and records their results; null starts none.
     */
    jobOutput: z
      .object({ channel: z.string().min(1).max(128), computeMemberId: z.string().regex(/^btr_member_[a-z2-7]{26}$/) })
      .strict()
      .nullable(),
    /** File holding the next block to scan for requests. */
    cursorPath: z.string().min(1),
    /**
     * Customers mode: also records the receipts of organizations set up in the Beam console,
     * read from the website's relay routes, and charges each one credit. Null records Beam's own receipts only.
     */
    customers: z
      .object({
        /** Beam's API, which serves `/api/onchain-receipts/*`. */
        registryUrl: httpUrl,
        /** File holding each organization's every-transfer listing cursor. */
        cursorPath: z.string().min(1),
        /** Most every-transfer receipts recorded per organization and pass; the rest wait for the next pass. */
        maxPerOrganization: z.number().int().min(1).max(1000),
      })
      .strict()
      .nullable(),
    pollIntervalSeconds: z.number().int().min(5).max(3600),
  })
  .strict()
  .refine((config) => config.starts || !config.signDeliveries, {
    message: 'signDeliveries needs starts: only Studio deliveries are signed',
    path: ['signDeliveries'],
  })
  .refine((config) => !config.jobOutput || config.roomDelivery, {
    message: "jobOutput needs roomDelivery, the route a job's input is published on",
    path: ['jobOutput'],
  })

export type RelayConfig = z.infer<typeof relayConfigSchema>

/** Secrets, read from the environment, never from the config file. */
export type RelaySecrets = {
  relayPrivateKey: `0x${string}`
  beamApiKey: string
  /** Needed only to look up Room receipts. */
  beamOrgId: string | null
  /** The transfer workflow's webhook, when the relay starts transfers. */
  studioWebhookUrl: string | null
  studioSigningSecret: string | null
  /** The Room workflow's webhook, when the relay starts transfers and the config has a Room route. */
  studioRoomWebhookUrl: string | null
  studioRoomSigningSecret: string | null
  /** Customers mode: the secret the website's relay routes require, and the key customer keys are sealed to. */
  onchainRelaySecret: string | null
  sealingPrivateKey: string | null
}

export const secretsFromEnv = (env: Record<string, string | undefined>, config: RelayConfig): RelaySecrets => {
  const required = (name: string): string => {
    const value = env[name]?.trim()
    if (!value) throw new Error(`Set ${name}`)
    return value
  }
  const key = required('RELAY_PRIVATE_KEY')
  const relayPrivateKey = (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`
  if (!/^0x[0-9a-fA-F]{64}$/.test(relayPrivateKey)) throw new Error('RELAY_PRIVATE_KEY must be 32 bytes of hex')
  return {
    relayPrivateKey,
    beamApiKey: required('BEAM_API_KEY'),
    beamOrgId: env.BEAM_ORG_ID?.trim() || null,
    studioWebhookUrl: config.starts ? required('STUDIO_WEBHOOK_URL') : null,
    studioSigningSecret: config.starts && config.signDeliveries ? required('STUDIO_WEBHOOK_SIGNING_SECRET') : null,
    studioRoomWebhookUrl: config.starts && config.roomDelivery ? required('STUDIO_ROOM_WEBHOOK_URL') : null,
    studioRoomSigningSecret:
      config.starts && config.roomDelivery && config.signDeliveries ? required('STUDIO_ROOM_WEBHOOK_SIGNING_SECRET') : null,
    onchainRelaySecret: config.customers ? required('ONCHAIN_RELAY_SECRET') : null,
    sealingPrivateKey: config.customers ? required('RELAY_SEALING_PRIVATE_KEY') : null,
  }
}
