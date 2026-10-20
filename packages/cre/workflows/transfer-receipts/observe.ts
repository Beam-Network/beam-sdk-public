import { bytesToBase64, type HTTPSendRequester, type Report } from '@chainlink/cre-sdk'
import { utf8ToBytes } from '@noble/hashes/utils.js'
import { reportSubmission } from './attestations'
import { projectClientId, projectPublicationLookup, projectRoomReceipt, projectTransfer } from './beam'

// The workflow's HTTP calls, run on each node through CRE's HTTP capability: the Beam lookups, which return a
// projection from beam.ts that the relay shares, and the delivery of attestation reports to a Room bridge.

export type TransferLookup = { baseUrl: string; apiKey: string; transferId: string }

export type RoomLookup = {
  baseUrl: string
  apiKey: string
  organizationId: string
  room: string
  channel: string
  publication: string
}

export type IdentityLookup = { baseUrl: string; apiKey: string }

export type PublicationKeyLookup = {
  baseUrl: string
  apiKey: string
  organizationId: string
  room: string
  channel: string
  publisherMemberId: string
  key: string
}

/** Runs on each node: `GET /transfers/:id`, projected to the fields a standard receipt records. */
export const observeTransfer = (sendRequester: HTTPSendRequester, lookup: TransferLookup): string => {
  const response = sendRequester
    .sendRequest({
      url: `${lookup.baseUrl}/transfers/${lookup.transferId}`,
      method: 'GET',
      multiHeaders: {
        accept: { values: ['application/json'] },
        'x-api-key': { values: [lookup.apiKey] },
      },
    })
    .result()
  return JSON.stringify(projectTransfer(response.statusCode, response.body, lookup.transferId))
}

/**
 * Runs on each node: the canonical Room receipt route. `evidenceSha256` is computed over the raw
 * response body bytes, which are already the canonical envelope; the body is never re-serialized.
 */
export const observeRoomReceipt = (sendRequester: HTTPSendRequester, lookup: RoomLookup): string => {
  const path = [
    'studio/v1/rooms',
    encodeURIComponent(lookup.room),
    'channels',
    encodeURIComponent(lookup.channel),
    'objects',
    encodeURIComponent(lookup.publication),
    'receipt',
  ].join('/')
  const response = sendRequester
    .sendRequest({
      url: `${lookup.baseUrl}/${path}`,
      method: 'GET',
      multiHeaders: {
        accept: { values: ['application/json'] },
        authorization: { values: [`Bearer ${lookup.apiKey}`] },
        'x-beam-organization-id': { values: [lookup.organizationId] },
      },
    })
    .result()
  return JSON.stringify(projectRoomReceipt(response.statusCode, response.body, lookup))
}

/** Runs on each node: the `user_id` behind the API key, which is the client id transfer ids derive from. */
export const observeClientId = (sendRequester: HTTPSendRequester, lookup: IdentityLookup): string => {
  const response = sendRequester
    .sendRequest({
      url: `${lookup.baseUrl}/auth/me`,
      method: 'GET',
      multiHeaders: {
        accept: { values: ['application/json'] },
        'x-api-key': { values: [lookup.apiKey] },
      },
    })
    .result()
  return JSON.stringify(projectClientId(response.statusCode, response.body))
}

/** Runs on each node: the publication a member's preflight created under a publication key. */
export const observePublicationByKey = (sendRequester: HTTPSendRequester, lookup: PublicationKeyLookup): string => {
  const path = ['studio/v1/rooms', encodeURIComponent(lookup.room), 'channels', encodeURIComponent(lookup.channel),
    'publications/by-key'].join('/')
  const query = `publisher=${encodeURIComponent(lookup.publisherMemberId)}&key=${encodeURIComponent(lookup.key)}`
  const response = sendRequester
    .sendRequest({
      url: `${lookup.baseUrl}/${path}?${query}`,
      method: 'GET',
      multiHeaders: {
        accept: { values: ['application/json'] },
        authorization: { values: [`Bearer ${lookup.apiKey}`] },
        'x-beam-organization-id': { values: [lookup.organizationId] },
      },
    })
    .result()
  return JSON.stringify(projectPublicationLookup(response.statusCode, response.body))
}

export type ReportDelivery = { url: string; report: Report }

/**
 * Runs on each node: sends its copy of a DON-signed report to a Room bridge's results endpoint, and returns
 * the HTTP status. Each node's copy carries its own signatures, so the bridge dedupes by the raw report.
 */
export const deliverReport = (sendRequester: HTTPSendRequester, delivery: ReportDelivery): string => {
  const response = sendRequester
    .sendReport(delivery.report, (signed) => ({
      url: delivery.url,
      method: 'POST',
      body: bytesToBase64(
        utf8ToBytes(
          JSON.stringify(
            reportSubmission(
              signed.rawReport,
              signed.reportContext,
              signed.sigs.map((sig) => sig.signature),
            ),
          ),
        ),
      ),
      multiHeaders: { 'content-type': { values: ['application/json'] } },
      cacheSettings: { store: true, maxAge: '60s' },
    }))
    .result()
  return String(response.statusCode)
}
