import { createHash, randomUUID } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { type Hex, parseSignature } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import type { AttestationRequest } from '../workflows/transfer-receipts/attestations'

// How attestation requests reach the receipts workflow's HTTP trigger.

export type ForwardOutcome = { accepted: true; executionId: string | null } | { accepted: false; reason: string }

/** JSON with object keys sorted at every level and no whitespace, which the gateway hashes into the JWT digest. */
export const stableStringify = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, nested]) => nested !== undefined)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    return `{${entries.map(([key, nested]) => `${JSON.stringify(key)}:${stableStringify(nested)}`).join(',')}}`
  }
  return JSON.stringify(value)
}

const base64Url = (bytes: Uint8Array | string) => Buffer.from(bytes).toString('base64url')

/** The JSON-RPC body of `workflows.execute` for a deployed workflow, by its id. */
export const gatewayBody = (input: AttestationRequest, workflowId: string, requestId: string): string =>
  stableStringify({
    jsonrpc: '2.0',
    id: requestId,
    method: 'workflows.execute',
    params: { input, workflow: { workflowID: workflowId } },
  })

/**
 * The gateway's bearer token: a JWT with `alg: ETH` whose `digest` is the sha256 of the exact body sent, signed as
 * an EIP-191 message by a key the workflow's HTTP trigger authorizes, with the signature as r || s || recovery id.
 */
export const gatewayJwt = async (
  privateKey: Hex,
  body: string,
  nowSeconds: number,
  jti: string = randomUUID(),
): Promise<string> => {
  const account = privateKeyToAccount(privateKey)
  const header = base64Url(JSON.stringify({ alg: 'ETH', typ: 'JWT' }))
  const payload = base64Url(
    JSON.stringify({
      digest: `0x${createHash('sha256').update(body).digest('hex')}`,
      iss: account.address,
      iat: nowSeconds,
      exp: nowSeconds + 300,
      jti,
    }),
  )
  const message = `${header}.${payload}`
  const { r, s, v, yParity } = parseSignature(await account.signMessage({ message }))
  const recovery = v !== undefined ? Number(v >= 27n ? v - 27n : v) : yParity
  const signature = Buffer.concat([
    Buffer.from(r.slice(2).padStart(64, '0'), 'hex'),
    Buffer.from(s.slice(2).padStart(64, '0'), 'hex'),
    Buffer.from([recovery]),
  ])
  return `${message}.${base64Url(signature)}`
}

/** Calls a deployed workflow through the CRE gateway. It answers `ACCEPTED`, never the result. */
export const forwardToGateway = async (
  gateway: { gatewayUrl: string; workflowId: string; privateKey: Hex },
  input: AttestationRequest,
  nowSeconds: number,
): Promise<ForwardOutcome> => {
  const body = gatewayBody(input, gateway.workflowId, randomUUID())
  let response: Response
  try {
    response = await fetch(gateway.gatewayUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${await gatewayJwt(gateway.privateKey, body, nowSeconds)}`,
      },
      body,
      redirect: 'manual',
      signal: AbortSignal.timeout(15_000),
    })
  } catch (error) {
    return { accepted: false, reason: `gateway unreachable: ${(error as Error).message}` }
  }
  const reply = (await response.json().catch(() => null)) as {
    result?: { status?: string; workflow_execution_id?: string }
    error?: { code?: number; message?: string }
  } | null
  if (reply?.result?.status === 'ACCEPTED') {
    return { accepted: true, executionId: reply.result.workflow_execution_id ?? null }
  }
  const error = reply?.error ? `${reply.error.code ?? ''} ${reply.error.message ?? ''}`.trim() : 'no JSON-RPC result'
  return { accepted: false, reason: `gateway answered HTTP ${response.status}: ${error}` }
}

/**
 * Runs a command with the path of a file holding the trigger input appended: one run per batch, for the CRE
 * simulator before a workflow is deployed. Exit status 0 means the workflow ran.
 */
export const forwardToCommand = async (
  command: { argv: string[]; cwd: string },
  input: AttestationRequest,
): Promise<ForwardOutcome> => {
  const dir = await mkdtemp(join(tmpdir(), 'beam-cre-bridge-'))
  const file = join(dir, 'input.json')
  try {
    await writeFile(file, JSON.stringify(input), { mode: 0o600 })
    const child = Bun.spawn([...command.argv, file], { cwd: command.cwd, stdout: 'pipe', stderr: 'pipe' })
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (exitCode === 0) return { accepted: true, executionId: null }
    const tail = `${stdout}\n${stderr}`.trim().split('\n').slice(-1)[0] ?? ''
    return { accepted: false, reason: `command exited ${exitCode}: ${tail.slice(0, 200)}` }
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}
