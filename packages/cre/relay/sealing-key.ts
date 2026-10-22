// Creates the relay's sealing key pair for customers mode. Usage: bun sealing-key.ts <relay env file>
// Appends RELAY_SEALING_PRIVATE_KEY to the env file (which must stay readable by the relay only) and prints the
// public key, which the Beam console seals customer keys to (`ONCHAIN_RELAY_PUBLIC_KEY`). It refuses to replace
// an existing key: keys sealed to it would no longer open.
import { appendFile, readFile } from 'node:fs/promises'
import { generateSealingKeyPair } from './sealed-box'

const envPath = process.argv[2]
if (!envPath) {
  console.error('Usage: bun sealing-key.ts <relay env file>')
  process.exit(2)
}
const current = await readFile(envPath, 'utf8').catch((error: NodeJS.ErrnoException) => {
  if (error.code === 'ENOENT') return ''
  throw error
})
if (/^RELAY_SEALING_PRIVATE_KEY=/m.test(current)) {
  console.error(`${envPath} already holds RELAY_SEALING_PRIVATE_KEY; keys sealed to it would no longer open`)
  process.exit(1)
}
const pair = generateSealingKeyPair()
await appendFile(envPath, `${current && !current.endsWith('\n') ? '\n' : ''}RELAY_SEALING_PRIVATE_KEY=${pair.privateKey}\n`, { mode: 0o600 })
console.log(`ONCHAIN_RELAY_PUBLIC_KEY=${pair.publicKey}`)
