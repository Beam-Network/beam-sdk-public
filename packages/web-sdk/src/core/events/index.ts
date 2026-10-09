/**
 * Typed event emitter.
 *
 * Kept deliberately small and dependency-free: the SDK's public surface exposes
 * `on`/`once`/`off` on rooms, sessions, broadcasts, and transfers, and each of
 * those has its own exhaustive event map so `session.on("track.added", …)` gets
 * a fully typed payload and a typo is a compile error.
 */

/**
 * Constraint for event maps.
 *
 * Declare event maps as `type` aliases, not `interface`s: TypeScript gives an
 * object type alias an implicit index signature but withholds one from an
 * interface, so an interface will not satisfy this constraint.
 */
export type EventMap = Record<string, unknown>;

export type Listener<T> = (payload: T) => void;

/** Call to remove the listener. Returned by `on`/`once` so cleanup is one call. */
export type Unsubscribe = () => void;

export interface Emitter<M extends EventMap> {
  on<K extends keyof M & string>(event: K, listener: Listener<M[K]>): Unsubscribe;
  once<K extends keyof M & string>(event: K, listener: Listener<M[K]>): Unsubscribe;
  off<K extends keyof M & string>(event: K, listener: Listener<M[K]>): void;
}

export class EventEmitter<M extends EventMap> implements Emitter<M> {
  readonly #listeners = new Map<string, Set<Listener<never>>>();
  readonly #onError: ((error: unknown, event: string) => void) | undefined;

  constructor(onError?: (error: unknown, event: string) => void) {
    this.#onError = onError;
  }

  on<K extends keyof M & string>(event: K, listener: Listener<M[K]>): Unsubscribe {
    let set = this.#listeners.get(event);
    if (!set) {
      set = new Set();
      this.#listeners.set(event, set);
    }
    set.add(listener as Listener<never>);
    return () => this.off(event, listener);
  }

  once<K extends keyof M & string>(event: K, listener: Listener<M[K]>): Unsubscribe {
    const wrapped: Listener<M[K]> = (payload) => {
      this.off(event, wrapped);
      listener(payload);
    };
    return this.on(event, wrapped);
  }

  off<K extends keyof M & string>(event: K, listener: Listener<M[K]>): void {
    const set = this.#listeners.get(event);
    if (!set) return;
    set.delete(listener as Listener<never>);
    if (set.size === 0) this.#listeners.delete(event);
  }

  /**
   * Emits to a snapshot of the listener set, so a listener that unsubscribes (or
   * subscribes) during dispatch cannot corrupt the iteration. A throwing listener
   * is reported and skipped rather than aborting delivery to the rest — one
   * misbehaving handler must not silently kill a media session.
   */
  emit<K extends keyof M & string>(event: K, payload: M[K]): void {
    const set = this.#listeners.get(event);
    if (!set || set.size === 0) return;
    for (const listener of [...set]) {
      try {
        (listener as Listener<M[K]>)(payload);
      } catch (error) {
        this.#onError?.(error, event);
      }
    }
  }

  listenerCount(event: keyof M & string): number {
    return this.#listeners.get(event)?.size ?? 0;
  }

  removeAllListeners(): void {
    this.#listeners.clear();
  }
}

/**
 * Bridges an event into `for await (…)`.
 *
 * Values that arrive while the consumer is not awaiting are buffered, because a
 * consumer doing async work between iterations must not silently miss progress
 * updates. The buffer is bounded: past `maxBuffer` the oldest value is dropped,
 * which is the right trade for progress-style events where the newest value
 * supersedes older ones.
 *
 * Pass both type arguments explicitly. `Emitter<M>`'s `on` is itself generic,
 * which stops TypeScript inferring `M` from the emitter argument, so an
 * unannotated call silently widens to `EventMap` and yields `unknown`.
 */
export function eventsToAsyncIterable<M extends EventMap, K extends keyof M & string>(
  emitter: Emitter<M>,
  event: K,
  options: { signal?: AbortSignal; until?: () => Promise<unknown>; maxBuffer?: number } = {},
): AsyncIterableIterator<M[K]> {
  const maxBuffer = options.maxBuffer ?? 256;
  const buffer: M[K][] = [];
  const waiters: Array<(result: IteratorResult<M[K]>) => void> = [];
  let done = false;
  let failure: unknown;

  const push = (payload: M[K]): void => {
    if (done) return;
    const waiter = waiters.shift();
    if (waiter) {
      waiter({ value: payload, done: false });
      return;
    }
    if (buffer.length >= maxBuffer) buffer.shift();
    buffer.push(payload);
  };

  const finish = (error?: unknown): void => {
    if (done) return;
    done = true;
    failure = error;
    unsubscribe();
    options.signal?.removeEventListener("abort", onAbort);
    while (waiters.length > 0) {
      const waiter = waiters.shift();
      waiter?.({ value: undefined as never, done: true });
    }
  };

  const onAbort = (): void => finish();
  const unsubscribe = emitter.on(event, push);

  if (options.signal) {
    if (options.signal.aborted) finish();
    else options.signal.addEventListener("abort", onAbort, { once: true });
  }
  void options.until?.().then(
    () => finish(),
    (error: unknown) => finish(error),
  );

  const iterator: AsyncIterableIterator<M[K]> = {
    [Symbol.asyncIterator]() {
      return iterator;
    },
    next(): Promise<IteratorResult<M[K]>> {
      if (buffer.length > 0) {
        return Promise.resolve({ value: buffer.shift() as M[K], done: false });
      }
      if (done) {
        return failure === undefined
          ? Promise.resolve({ value: undefined as never, done: true })
          : Promise.reject(failure);
      }
      return new Promise<IteratorResult<M[K]>>((resolve) => waiters.push(resolve));
    },
    return(): Promise<IteratorResult<M[K]>> {
      finish();
      return Promise.resolve({ value: undefined as never, done: true });
    },
    throw(error?: unknown): Promise<IteratorResult<M[K]>> {
      finish(error);
      return Promise.reject(error);
    },
  };
  return iterator;
}
