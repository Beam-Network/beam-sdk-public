// The Room bridge for Beam receipts. Usage: bun main.ts <config.json>
import { readFile } from 'node:fs/promises'
import { createPublicClient, type Hex, http } from 'viem'
import { Bridge, type BridgeIO, type CreIO } from './bridge'
import { connectContract } from './chain'
import { bridgeConfigSchema, type CreAnswers, secretsFromEnv } from './config'
import { forwardToCommand, forwardToGateway } from './cre'
import { readDonSigners, type SignerSet } from './report'
import { loadState, stateWriter } from './state'

const configPath = process.argv[2]
if (!configPath) {
  console.error('Usage: bun main.ts <config.json>')
  process.exit(2)
}

const config = bridgeConfigSchema.parse(JSON.parse(await readFile(configPath, 'utf8')))
const secrets = secretsFromEnv(process.env, config)
const log = (message: string) => console.log(`${new Date().toISOString()} ${message}`)
const now = () => Math.floor(Date.now() / 1000)

/** A DON's signers are read again after an hour, or at once when a report does not verify against them. */
const SIGNERS_TTL_SECONDS = 3600

const creLink = (answers: CreAnswers): CreIO => {
  const signerCache = new Map<number, { set: SignerSet; readAt: number }>()
  const signers = answers.signers
  const registry =
    signers.source === 'registry'
      ? {
          address: signers.registryAddress as Hex,
          client: createPublicClient({ transport: http(signers.rpcUrl, { timeout: 15_000 }) }),
        }
      : null
  if (!registry) log('report signatures are NOT verified (signers.source "unverified", for the CRE simulator); replies say so')
  return {
    forward: (input) => {
      const forward = answers.forward
      return forward.mode === 'gateway'
        ? forwardToGateway({ gatewayUrl: forward.gatewayUrl, workflowId: forward.workflowId, privateKey: secrets.evmPrivateKey! }, input, now())
        : forwardToCommand(forward, input)
    },
    signers: async (donId, refresh) => {
      if (!registry) return null
      const cached = signerCache.get(donId)
      if (cached && !refresh && now() - cached.readAt < SIGNERS_TTL_SECONDS) return cached.set
      const set = await readDonSigners(
        async (to, data) => (await registry.client.call({ to, data })).data ?? '0x',
        registry.address,
        donId,
      )
      signerCache.set(donId, { set, readAt: now() })
      return set
    },
  }
}

const answers = config.answers
let chain: BridgeIO['chain'] = null
if (answers.from === 'contract') {
  const link = await connectContract(answers.chain, secrets.evmPrivateKey!)
  chain = link.io
  log(
    `answering from ${link.io.deployment.contract} on chain ${link.io.deployment.chainId} ` +
      `(${link.io.deployment.donAttested ? 'a CRE deployment' : 'a relay deployment'}), requesting as ${link.account}`,
  )
}

const io: BridgeIO = {
  now,
  save: stateWriter(config.statePath),
  log,
  cre: answers.from === 'cre' ? creLink(answers) : null,
  chain,
}

const bridge = new Bridge(config, secrets.webhookSecrets, io, await loadState(config.statePath))

const base = config.basePath
const server = Bun.serve({
  hostname: config.listen.hostname,
  port: config.listen.port,
  // Message deliveries and reports are small; the bridge reads no files.
  maxRequestBodySize: 1024 * 1024,
  fetch: (request) => {
    const { pathname } = new URL(request.url)
    if (request.method === 'POST' && pathname === `${base}/hooks`) return bridge.handleDelivery(request)
    if (request.method === 'GET' && pathname.startsWith(`${base}/replies/`)) {
      return bridge.handlePoll(decodeURIComponent(pathname.slice(`${base}/replies/`.length)), request)
    }
    if (request.method === 'POST' && pathname === `${base}/cre/results`) return bridge.handleResult(request)
    if (request.method === 'GET' && pathname === `${base}/healthz`) {
      return Response.json({ ok: true, open: Object.values(bridge.state.requests).filter((r) => !r.reply).length })
    }
    return new Response(null, { status: 404 })
  },
})
log(`listening on ${server.hostname}:${server.port}${base}`)

let stopping = false
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    stopping = true
    log(`${signal}: stopping`)
  })
}
while (!stopping) {
  try {
    await bridge.pump()
  } catch (error) {
    log(`pump failed: ${(error as Error).message}`)
  }
  await new Promise((resolve) => setTimeout(resolve, 1000))
}
server.stop()
