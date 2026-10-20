// Checks a bridge reply on its own, without trusting the bridge.
//   A DON-attested reply:  bun verify.ts <reply.json> --rpc <ethereum-mainnet-rpc> --workflow-id <id> --owner <address>
//                          bun verify.ts <reply.json> --skip-signatures
//   A contract reply:      bun verify.ts <reply.json> --rpc <rpc of the reply's chain>
import { readFile } from 'node:fs/promises'
import { createPublicClient, type Hex, http, type PublicClient } from 'viem'
import { answerFor, type Attestation } from '../workflows/transfer-receipts/attestations'
import { decodeReport } from '../workflows/transfer-receipts/receipts'
import { answerView, CHAIN_REPLY, CRE_REPLY } from './bridge'
import { readReceipt } from './chain'
import { CAPABILITY_REGISTRY } from './config'
import { parseHeader, parseSubmission, readDonSigners, reportBody, verifyReportSignatures } from './report'

const args = process.argv.slice(2)
const flag = (name: string) => {
  const index = args.indexOf(name)
  return index >= 0 ? args[index + 1] : undefined
}
const file = args.find((arg, index) => !arg.startsWith('--') && !args[index - 1]?.startsWith('--'))
const skipSignatures = args.includes('--skip-signatures')
const rpcUrl = flag('--rpc') ?? process.env.ETH_MAINNET_RPC_URL
const workflowId = flag('--workflow-id')?.replace(/^0x/, '').toLowerCase()
const owner = flag('--owner')?.toLowerCase()

const usage = () => {
  console.error('Usage: bun verify.ts <reply.json> --rpc <ethereum-mainnet-rpc> --workflow-id <id> --owner <address>')
  console.error('       bun verify.ts <reply.json> --skip-signatures')
  console.error('       bun verify.ts <reply.json> --rpc <rpc of the reply chain>   (a contract reply)')
  process.exit(2)
}
const fail = (message: string): never => {
  console.error(`NOT VERIFIED: ${message}`)
  process.exit(1)
}
const print = (value: unknown) =>
  console.log(JSON.stringify(value, (_key, item) => (typeof item === 'bigint' ? item.toString() : item), 2))

if (!file) usage()
const reply = JSON.parse(await readFile(file!, 'utf8'))
const request = reply?.request
const attestation: Attestation =
  request?.op === 'attest_transfer'
    ? { op: 'attest_transfer', transferId: String(request.transfer_id) }
    : { op: 'attest_room_publication', room: String(request?.room), channel: String(request?.channel), publication: String(request?.publication) }

if (reply?.type === CHAIN_REPLY && reply.outcome === 'recorded') {
  // A contract reply is checked against the contract itself: the reply must say what the chain says.
  if (!rpcUrl) usage()
  const client = createPublicClient({ transport: http(rpcUrl, { timeout: 20_000 }) }) as PublicClient
  const chainId = await client.getChainId()
  if (chainId !== reply.chain?.chain_id) fail(`the RPC serves chain ${chainId}, the reply names ${reply.chain?.chain_id}`)
  const onchain = await readReceipt(client, reply.chain.contract as Hex, attestation)
  if (!onchain) fail('the contract holds no receipt for the request')
  const expected = answerView(onchain!)
  const differing = Object.keys(expected).filter((key) => JSON.stringify(expected[key]) !== JSON.stringify(reply.answer?.[key]))
  if (differing.length > 0) fail(`the reply differs from the contract in ${differing.join(', ')}`)
  console.log(`VERIFIED: the reply matches the receipt held by ${reply.chain.contract} on chain ${chainId}:`)
  print(expected)
  process.exit(0)
}

if (reply?.type !== CRE_REPLY || reply.outcome !== 'attested') fail('not an attested or recorded bridge reply')
if (!skipSignatures && (!rpcUrl || !workflowId || !owner)) usage()
const signed = parseSubmission(reply.report) ?? fail('the reply carries no well-formed report')
const header = parseHeader(signed.rawReport)
const answer = answerFor(attestation, decodeReport(reportBody(signed.rawReport))) ?? fail('the signed report does not answer the request')

if (skipSignatures) {
  console.log('Signatures NOT checked (--skip-signatures). The signed report answers the request:')
} else {
  if (header.workflowId !== workflowId) fail(`the report comes from workflow ${header.workflowId}`)
  if (`0x${header.workflowOwner}` !== owner) fail(`the report comes from a workflow of 0x${header.workflowOwner}`)
  const client = createPublicClient({ transport: http(rpcUrl, { timeout: 15_000 }) })
  const signers = await readDonSigners(
    async (to, data) => (await client.call({ to, data })).data ?? '0x',
    CAPABILITY_REGISTRY as Hex,
    header.donId,
  )
  const accepted = await verifyReportSignatures(signed, signers).catch((error: Error) => fail(error.message))
  console.log(`VERIFIED: ${accepted.length} of DON ${header.donId}'s signers (f = ${signers.f}) signed this report:`)
}
print({
  workflow: { id: header.workflowId, owner: `0x${header.workflowOwner}`, execution_id: header.executionId },
  answer: { kind: answer.kind, ...answer.entry },
})
