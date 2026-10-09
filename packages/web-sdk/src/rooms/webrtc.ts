/**
 * WebRTC helpers shared by rooms and broadcast.
 *
 * Isolated here so the peer-connection mechanics can be tested against a fake
 * `RTCPeerConnection` without dragging in signalling or transport.
 */

import { BeamAbortError, BeamUnsupportedError } from "../core/errors/index.js";

/** Throws a clear error rather than a `ReferenceError` in a non-WebRTC runtime. */
export function requirePeerConnection(): typeof RTCPeerConnection {
  const ctor = globalThis.RTCPeerConnection;
  if (typeof ctor !== "function") {
    throw new BeamUnsupportedError(
      "This environment has no WebRTC support. Beam rooms and broadcast need a browser with RTCPeerConnection.",
    );
  }
  return ctor;
}

/**
 * Waits for ICE gathering to finish, or for `timeoutMs` to elapse.
 *
 * The timeout is not a fallback for a broken connection — it is the normal path.
 * Gathering only "completes" after every STUN/TURN server has answered or timed
 * out, which can take many seconds behind a slow relay, while the candidates
 * needed to connect are almost always present within a few hundred milliseconds.
 * Sending the offer early trades a possible extra renegotiation for a much
 * faster join.
 */
export function waitForIceGathering(peer: RTCPeerConnection, timeoutMs = 1500, signal?: AbortSignal): Promise<void> {
  if (peer.iceGatheringState === "complete") return Promise.resolve();

  return new Promise<void>((resolve, reject) => {
    let settled = false;

    const finish = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      peer.removeEventListener("icegatheringstatechange", onStateChange);
      signal?.removeEventListener("abort", onAbort);
      resolve();
    };

    const onStateChange = (): void => {
      if (peer.iceGatheringState === "complete") finish();
    };

    const onAbort = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      peer.removeEventListener("icegatheringstatechange", onStateChange);
      reject(new BeamAbortError());
    };

    const timer = setTimeout(finish, timeoutMs);
    peer.addEventListener("icegatheringstatechange", onStateChange);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Adds recvonly transceivers for kinds this peer is not already sending.
 *
 * Without these, a viewer-only participant offers no media sections at all and
 * the SFU has nowhere to attach remote tracks.
 */
export function ensureReceiveTransceivers(peer: RTCPeerConnection): void {
  const sending = new Set(
    peer
      .getSenders()
      .map((sender) => sender.track?.kind)
      .filter((kind): kind is string => typeof kind === "string"),
  );
  const receiving = new Set(
    peer
      .getTransceivers()
      .filter((transceiver) => transceiver.direction === "recvonly" || transceiver.direction === "sendrecv")
      .map((transceiver) => transceiver.receiver.track?.kind)
      .filter((kind): kind is string => typeof kind === "string"),
  );
  for (const kind of ["audio", "video"] as const) {
    if (!sending.has(kind) && !receiving.has(kind)) {
      peer.addTransceiver(kind, { direction: "recvonly" });
    }
  }
}

export function normalizeIceServers(servers: RTCIceServer[] | undefined, extra: RTCIceServer[] = []): RTCIceServer[] {
  return [...(Array.isArray(servers) ? servers : []), ...extra];
}

/**
 * Serializes renegotiation.
 *
 * The SFU signals `renegotiation_required` whenever the room's track set
 * changes, which for a busy room can be several times a second. Renegotiating
 * concurrently, or while `signalingState` is not `stable`, corrupts the
 * peer connection. This queue collapses a burst into one trailing run: the
 * caller always ends up negotiating against the room's latest state, and never
 * more than once at a time.
 */
export class RenegotiationQueue {
  #running = false;
  #pending = false;
  readonly #run: () => Promise<void>;
  readonly #isStable: () => boolean;
  readonly #onError: (error: unknown) => void;
  readonly #wait: (ms: number) => Promise<void>;
  #closed = false;

  constructor(options: {
    run: () => Promise<void>;
    isStable: () => boolean;
    onError: (error: unknown) => void;
    wait?: (ms: number) => Promise<void>;
  }) {
    this.#run = options.run;
    this.#isStable = options.isStable;
    this.#onError = options.onError;
    this.#wait = options.wait ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  }

  close(): void {
    this.#closed = true;
    this.#pending = false;
  }

  /** Requests a renegotiation. Safe to call at any rate. */
  schedule(): void {
    if (this.#closed) return;
    if (this.#running) {
      this.#pending = true;
      return;
    }
    void this.#drain();
  }

  async #drain(): Promise<void> {
    if (this.#closed || this.#running) return;
    this.#running = true;
    try {
      do {
        this.#pending = false;

        // Wait out a negotiation started elsewhere (an ICE restart, say) rather
        // than pushing a description into a non-stable connection.
        for (let waited = 0; !this.#isStable() && waited < 5_000; waited += 100) {
          if (this.#closed) return;
          await this.#wait(100);
        }
        if (this.#closed) return;

        try {
          await this.#run();
        } catch (error) {
          // Renegotiation races participants leaving and ICE restarts; a single
          // failure is expected and the next scheduled run supersedes it.
          this.#onError(error);
        }
      } while (this.#pending && !this.#closed);
    } finally {
      this.#running = false;
    }
  }
}
