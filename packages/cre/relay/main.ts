// The Beam relay for chains Chainlink CRE does not serve. Usage: bun main.ts <config.json> [--once]
import { readFile } from 'node:fs/promises'
import { privateKeyToAccount } from 'viem/accounts'
import { receiptLevels } from '../workflows/transfer-receipts/receipts'
import { relayConfigSchema, secretsFromEnv } from './config'
import { createRelayIO } from './io'
import { createRelayState, runOnce } from './relay'

const args = process.argv.slice(2)
const configPath = args.find((arg) => !arg.startsWith('--'))
const once = args.includes('--once')
if (!configPath) {
  console.error('Usage: bun main.ts <config.json> [--once]')
  process.exit(2)
}

const config = relayConfigSchema.parse(JSON.parse(await readFile(configPath, 'utf8')))
const secrets = secretsFromEnv(process.env, config)
const log = (message: string) => console.log(`${new Date().toISOString()} ${message}`)
const relay = createRelayIO(config, secrets, log)
await relay.preflight()

const settings = {
  startBlock: BigInt(config.chain.startBlock),
  logRangeBlocks: BigInt(config.chain.logRangeBlocks),
  clientId: config.beamClientId,
  // A relay is a single operator: its receipts are Beam-reported, never DON-attested.
  levels: receiptLevels(false),
  routes: { roomDelivery: config.roomDelivery, jobOutput: config.jobOutput },
  starts: config.starts,
  relayAccount: privateKeyToAccount(secrets.relayPrivateKey).address,
  customers: config.customers && { maxPerOrganization: config.customers.maxPerOrganization },
}
if (!config.starts) {
  log(
    config.customers
      ? "receipts only: a customer's transfer request is closed, at one credit to its organization"
      : 'receipts only: transfer requests stay open until something else starts them',
  )
}
const state = createRelayState()

let stopping = false
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    stopping = true
    log(`${signal}: stopping after this pass`)
  })
}

while (true) {
  try {
    const pass = await runOnce(relay.io, settings, state)
    log(
      `pass: finalized ${pass.finalized}, ${pass.started} started, ${pass.allowlisted} allowlist changes, ${pass.recorded} recorded, ${pass.closed} closed`,
    )
  } catch (error) {
    log(`pass failed: ${(error as Error).message}`)
    if (once) process.exitCode = 1
  }
  if (once || stopping) break
  for (let waited = 0; waited < config.pollIntervalSeconds && !stopping; waited += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  if (stopping) break
}
