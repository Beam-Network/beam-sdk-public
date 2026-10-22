import { createDecipheriv, createPrivateKey, createPublicKey, diffieHellman, generateKeyPairSync, hkdfSync, type KeyObject } from 'node:crypto'

/**
 * Opens what the Beam console sealed to the relay, in the console's format and with its test vector: a customer
 * organization's read-only Beam key, sealed to the relay's X25519 public key, so only this relay can read it. *
 * `v1.<ephemeral public key>.<nonce>.<ciphertext and tag>`, each part base64url: an ephemeral X25519 agreement,
 * HKDF-SHA256 (salt: ephemeral then recipient public key; info: `beam-sealed-box-v1:` and the context) to an
 * AES-256-GCM key, a 12-byte nonce, the context as additional data. Keys travel as their raw 32 bytes in base64url.
 */

const VERSION = 'v1'
const INFO = 'beam-sealed-box-v1'
const KEY_BYTES = 32
const NONCE_BYTES = 12
const TAG_BYTES = 16
const SPKI_PREFIX = Buffer.from('302a300506032b656e032100', 'hex')
const PKCS8_PREFIX = Buffer.from('302e020100300506032b656e04220420', 'hex')

const rawKey = (encoded: string, what: string): Buffer => {
  const raw = Buffer.from(encoded, 'base64url')
  if (raw.length !== KEY_BYTES) throw new Error(`${what} must be ${KEY_BYTES} bytes in base64url`)
  return raw
}

const publicKey = (raw: Buffer): KeyObject => createPublicKey({ key: Buffer.concat([SPKI_PREFIX, raw]), format: 'der', type: 'spki' })

const rawPublicKey = (key: KeyObject): Buffer => (key.export({ format: 'der', type: 'spki' }) as Buffer).subarray(SPKI_PREFIX.length)

const privateKey = (encoded: string): KeyObject =>
  createPrivateKey({ key: Buffer.concat([PKCS8_PREFIX, rawKey(encoded, 'The sealing private key')]), format: 'der', type: 'pkcs8' })

/** Throws unless `encoded` is a usable X25519 private key. */
export const assertSealingPrivateKey = (encoded: string): void => {
  privateKey(encoded)
}

/** The public half of a sealing private key: what the console seals to (`ONCHAIN_RELAY_PUBLIC_KEY`). */
export const sealingPublicKey = (encodedPrivateKey: string): string =>
  rawPublicKey(createPublicKey(privateKey(encodedPrivateKey))).toString('base64url')

/** Throws when the value is malformed, was sealed to another key, or under another context. */
export const openSealed = (recipientPrivateKey: string, sealed: string, context: string): string => {
  const [version, ephemeralPart, noncePart, bodyPart, ...rest] = sealed.split('.')
  if (version !== VERSION || !ephemeralPart || !noncePart || !bodyPart || rest.length > 0) throw new Error('Not a v1 sealed value')
  const recipient = privateKey(recipientPrivateKey)
  const ephemeralRaw = rawKey(ephemeralPart, 'The ephemeral public key')
  const nonce = Buffer.from(noncePart, 'base64url')
  const body = Buffer.from(bodyPart, 'base64url')
  if (nonce.length !== NONCE_BYTES || body.length < TAG_BYTES) throw new Error('Not a v1 sealed value')

  const recipientRaw = rawPublicKey(createPublicKey(recipient))
  const shared = diffieHellman({ privateKey: recipient, publicKey: publicKey(ephemeralRaw) })
  const key = Buffer.from(hkdfSync('sha256', shared, Buffer.concat([ephemeralRaw, recipientRaw]), Buffer.from(`${INFO}:${context}`), KEY_BYTES))
  const decipher = createDecipheriv('aes-256-gcm', key, nonce)
  decipher.setAAD(Buffer.from(context, 'utf8'))
  decipher.setAuthTag(body.subarray(body.length - TAG_BYTES))
  return Buffer.concat([decipher.update(body.subarray(0, body.length - TAG_BYTES)), decipher.final()]).toString('utf8')
}

/** A new sealing key pair, raw and base64url. */
export const generateSealingKeyPair = (): { publicKey: string; privateKey: string } => {
  const pair = generateKeyPairSync('x25519')
  const privateDer = pair.privateKey.export({ format: 'der', type: 'pkcs8' }) as Buffer
  return {
    publicKey: rawPublicKey(pair.publicKey).toString('base64url'),
    privateKey: privateDer.subarray(PKCS8_PREFIX.length).toString('base64url'),
  }
}
