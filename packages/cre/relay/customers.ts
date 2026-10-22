import type { Address, Hex } from 'viem'
import { z } from 'zod'
import { type Observation, projectTransfer } from '../workflows/transfer-receipts/beam'

/**
 * Customers mode: organizations set up in the Beam console get their receipts recorded by this
 * relay and pay one credit each, through the website's relay routes. They owe a receipt for one of two reasons:
 * the transfer was created while their "every transfer" switch was on, or an address of theirs that Beam
 * approved asked for it onchain. Closing a request of theirs this relay will never answer costs one credit too.
 */

/** Who reads a subject at Beam: null for Beam's own key, else a customer organization's registered key. */
export type Reader = { apiKey: string; organizationId: string } | null

export type Via = { kind: 'every_transfer'; transferCreatedAt: string } | { kind: 'requester'; address: Address }

export type Owner = { kind: 'beam' } | { kind: 'customer'; organizationId: string; reader: Exclude<Reader, null>; via: Via }

const isoDate = z.string().datetime({ offset: true })

/** One organization as `GET /api/onchain-receipts/accounts` lists it. */
export const registryAccountSchema = z.object({
  organizationId: z.string().min(1),
  apiKeyId: z.string().min(1),
  sealedApiKey: z.string().min(1),
  everyTransfer: z.boolean(),
  periods: z.array(z.object({ startedAt: isoDate, endedAt: isoDate.nullable() })),
  requesters: z.array(
    z.object({
      id: z.string().min(1),
      address: z.string().regex(/^0x[0-9a-f]{40}$/),
      status: z.enum(['PENDING', 'APPROVED', 'REVOKED']),
      allowlist: z.enum(['NONE', 'ALLOWED', 'REMOVED']),
    }),
  ),
})
export const registrySchema = z.object({ accounts: z.array(registryAccountSchema) })
export type RegistryAccount = z.infer<typeof registryAccountSchema>

/** A registry account whose key this relay opened. */
export type Customer = {
  organizationId: string
  reader: Exclude<Reader, null>
  periods: Array<{ startedAt: number; endedAt: number | null }>
  requesters: RegistryAccount['requesters']
}

/** The sealed-box context of an organization's key; it must match the console's. */
export const sealedKeyContext = (organizationId: string, apiKeyId: string) => `onchain-receipts:${organizationId}:${apiKeyId}`

export const toCustomer = (account: RegistryAccount, apiKey: string): Customer => ({
  organizationId: account.organizationId,
  reader: { apiKey, organizationId: account.organizationId },
  periods: account.periods.map((period) => ({
    startedAt: Date.parse(period.startedAt),
    endedAt: period.endedAt === null ? null : Date.parse(period.endedAt),
  })),
  requesters: account.requesters,
})

/** Transfers created while the switch was on can still be finishing a while after it went off. */
const TRAILING_LISTING_MS = 7 * 24 * 60 * 60 * 1000

/** Whether to list an organization's finished transfers this pass: its switch is on, or went off lately. */
export const listsTransfers = (customer: Customer, now: number): boolean =>
  customer.periods.some((period) => period.endedAt === null || now - period.endedAt < TRAILING_LISTING_MS)

export const createdDuringPeriods = (createdAtMs: number, periods: Customer['periods']): boolean =>
  periods.some((period) => createdAtMs >= period.startedAt && (period.endedAt === null || createdAtMs < period.endedAt))

/** Where an organization's listing starts before any pass moved it: just before its first period. */
export const initialTransferCursor = (customer: Customer): string =>
  new Date(Math.min(...customer.periods.map((period) => period.startedAt)) - 1).toISOString()

/** One finished transfer from `GET /transfers?order_by=completed_at`, with what its receipt would hold. */
export type ListedTransfer = { id: string; createdAt: string; completedAt: string; observation: Observation }

const listedSchema = z.object({ id: z.string(), created_at: isoDate, completed_at: isoDate })

/** Reads one listed row exactly as a single `GET /transfers/:id` would be read. */
export const projectListedTransfer = (row: unknown): ListedTransfer | null => {
  const parsed = listedSchema.safeParse(row)
  if (!parsed.success) return null
  return {
    id: parsed.data.id,
    createdAt: parsed.data.created_at,
    completedAt: parsed.data.completed_at,
    observation: projectTransfer(200, new TextEncoder().encode(JSON.stringify(row)), parsed.data.id),
  }
}

/**
 * The next listing cursor: just before the oldest transfer not done yet (unresolved, or left for a later pass),
 * so it is listed again; past the newest when every listed transfer is done. `completed_after` is exclusive.
 */
export const nextTransferCursor = (cursor: string, listed: Array<{ completedAt: string; done: boolean }>): string => {
  const pending = listed.filter((transfer) => !transfer.done).map((transfer) => Date.parse(transfer.completedAt))
  if (pending.length) return new Date(Math.max(Date.parse(cursor), Math.min(...pending) - 1)).toISOString()
  const newest = listed.map((transfer) => Date.parse(transfer.completedAt))
  return newest.length ? new Date(Math.max(Date.parse(cursor), ...newest)).toISOString() : cursor
}

/** A receipt the relay records. */
export type ReceiptSubject = { kind: 'transfer'; transferId: string } | { kind: 'room'; roomKey: Hex }

/**
 * What a charge pays for, and what the website keys it by: a recorded receipt, or closing an open request this
 * relay will never answer, whose `cancelRequest` gas the requesting organization pays rather than Beam.
 */
export type ChargeSubject = ReceiptSubject | { kind: 'request'; requestKey: Hex }

/**
 * What the website answered for a charge. `held`: a hold to settle once the receipt is recorded or the request
 * closed (a new one, or an earlier one not settled yet, expired included: a commit still settles it). `charged`:
 * already done and paid. `released`: released for good. `refused`: the organization cannot pay now, or does not
 * owe this; the request stays open and a listed transfer is retried. `unavailable`: try again next pass.
 */
export type ChargeOutcome =
  | { state: 'held' }
  | { state: 'charged' }
  | { state: 'released' }
  | { state: 'refused'; status: number; reason: string }
  | { state: 'unavailable'; status: number }

export const projectCharge = (status: number, body: Uint8Array): ChargeOutcome => {
  let reply: Record<string, unknown> = {}
  try {
    const parsed = JSON.parse(new TextDecoder().decode(body))
    if (parsed && typeof parsed === 'object') reply = parsed as Record<string, unknown>
  } catch {
    // An unreadable body is judged by its status alone.
  }
  if (status >= 500 || status === 0) return { state: 'unavailable', status }
  if (status !== 200) {
    const reason = typeof reply.reason === 'string' ? reply.reason : typeof reply.error === 'string' ? reply.error : `HTTP ${status}`
    return { state: 'refused', status, reason }
  }
  const operation = (reply.operation ?? {}) as { status?: unknown; releaseReason?: unknown }
  if (operation.status === 'RESERVED') return { state: 'held' }
  if (operation.status === 'COMMITTED') return { state: 'charged' }
  if ((operation.status === 'CANCELED' || operation.status === 'FAILED') && operation.releaseReason === 'expired') return { state: 'held' }
  return { state: 'released' }
}
