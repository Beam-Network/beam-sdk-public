import { readFile, rename, writeFile } from 'node:fs/promises'
import { type BridgeState, emptyState } from './bridge'

/** The saved state, or an empty one on the first start. */
export const loadState = async (path: string): Promise<BridgeState> => {
  let text: string
  try {
    text = await readFile(path, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return emptyState()
    throw error
  }
  const state = JSON.parse(text) as BridgeState
  if (typeof state.requests !== 'object' || !Array.isArray(state.reports)) throw new Error(`${path} is not bridge state`)
  return state
}

/**
 * Writes the state through a temporary file and a rename, one write at a time, so a crash leaves the previous
 * state or the new one and never half of either.
 */
export const stateWriter = (path: string) => {
  let queue: Promise<void> = Promise.resolve()
  return (state: BridgeState): Promise<void> => {
    const snapshot = JSON.stringify(state)
    queue = queue.then(async () => {
      await writeFile(`${path}.tmp`, snapshot, { mode: 0o600 })
      await rename(`${path}.tmp`, path)
    })
    return queue
  }
}
