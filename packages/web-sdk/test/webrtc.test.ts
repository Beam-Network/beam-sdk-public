import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { BeamUnsupportedError } from "../src/core/errors/index.js";
import {
  RenegotiationQueue,
  ensureReceiveTransceivers,
  normalizeIceServers,
  requirePeerConnection,
  waitForIceGathering,
} from "../src/rooms/webrtc.js";
import { FakeMediaStream, FakePeerConnection, installWebrtcFakes } from "./helpers.js";

describe("requirePeerConnection", () => {
  it("explains the problem when WebRTC is missing", () => {
    const original = globalThis.RTCPeerConnection;
    // @ts-expect-error deliberately removing the global
    delete globalThis.RTCPeerConnection;
    try {
      expect(() => requirePeerConnection()).toThrow(BeamUnsupportedError);
      expect(() => requirePeerConnection()).toThrow(/RTCPeerConnection/);
    } finally {
      globalThis.RTCPeerConnection = original;
    }
  });
});

describe("waitForIceGathering", () => {
  let uninstall: () => void;
  beforeEach(() => (uninstall = installWebrtcFakes()));
  afterEach(() => uninstall());

  it("returns immediately when gathering is already complete", async () => {
    const peer = new FakePeerConnection();
    peer.iceGatheringState = "complete";
    await expect(waitForIceGathering(peer as unknown as RTCPeerConnection, 5000)).resolves.toBeUndefined();
  });

  // The timeout is the normal path, not an error path: gathering only completes
  // after every TURN server answers, long after the useful candidates arrive.
  it("resolves on timeout rather than hanging on a slow TURN server", async () => {
    const peer = new FakePeerConnection();
    peer.iceGatheringState = "gathering";

    const started = Date.now();
    await waitForIceGathering(peer as unknown as RTCPeerConnection, 20);
    expect(Date.now() - started).toBeGreaterThanOrEqual(15);
  });

  it("resolves early once gathering completes", async () => {
    const peer = new FakePeerConnection();
    peer.iceGatheringState = "gathering";

    const pending = waitForIceGathering(peer as unknown as RTCPeerConnection, 5000);
    peer.iceGatheringState = "complete";
    peer.dispatchEvent(new Event("icegatheringstatechange"));

    await expect(pending).resolves.toBeUndefined();
  });

  it("rejects when aborted", async () => {
    const peer = new FakePeerConnection();
    peer.iceGatheringState = "gathering";
    const controller = new AbortController();

    const pending = waitForIceGathering(peer as unknown as RTCPeerConnection, 5000, controller.signal);
    controller.abort();

    await expect(pending).rejects.toThrow(/aborted/i);
  });
});

describe("ensureReceiveTransceivers", () => {
  let uninstall: () => void;
  beforeEach(() => (uninstall = installWebrtcFakes()));
  afterEach(() => uninstall());

  // Without these a viewer offers no media sections and the SFU has nowhere to
  // attach anyone else's tracks.
  it("adds recvonly transceivers for a viewer", () => {
    const peer = new FakePeerConnection();
    ensureReceiveTransceivers(peer as unknown as RTCPeerConnection);

    expect(peer.getTransceivers().map((t) => t.direction)).toEqual(["recvonly", "recvonly"]);
  });

  it("does not add a transceiver for a kind already being sent", () => {
    const peer = new FakePeerConnection();
    const stream = new FakeMediaStream(["video"]);
    for (const track of stream.getTracks()) peer.addTrack(track);

    ensureReceiveTransceivers(peer as unknown as RTCPeerConnection);

    expect(peer.getTransceivers()).toHaveLength(1); // audio only
  });

  it("is idempotent", () => {
    const peer = new FakePeerConnection();
    ensureReceiveTransceivers(peer as unknown as RTCPeerConnection);
    ensureReceiveTransceivers(peer as unknown as RTCPeerConnection);
    expect(peer.getTransceivers()).toHaveLength(2);
  });
});

describe("normalizeIceServers", () => {
  it("merges Beam's servers with caller-supplied ones", () => {
    expect(normalizeIceServers([{ urls: "stun:a" }], [{ urls: "turn:b" }])).toEqual([
      { urls: "stun:a" },
      { urls: "turn:b" },
    ]);
  });

  it("tolerates a missing or malformed server list", () => {
    expect(normalizeIceServers(undefined)).toEqual([]);
    expect(normalizeIceServers("nonsense" as never)).toEqual([]);
  });
});

describe("RenegotiationQueue", () => {
  it("runs once for a single request", async () => {
    const run = vi.fn(async () => undefined);
    const queue = new RenegotiationQueue({ run, isStable: () => true, onError: () => undefined });

    queue.schedule();
    await vi.waitFor(() => expect(run).toHaveBeenCalledOnce());
  });

  // A busy room emits `renegotiation_required` many times a second. Running them
  // concurrently corrupts the peer connection; running them all in sequence is
  // wasted work. One trailing run against the latest state is what is wanted.
  it("collapses a burst into one run plus one trailing run", async () => {
    let resolveFirst: () => void = () => undefined;
    const run = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          if (run.mock.calls.length === 1) resolveFirst = resolve;
          else resolve();
        }),
    );
    const queue = new RenegotiationQueue({ run, isStable: () => true, onError: () => undefined });

    queue.schedule();
    await vi.waitFor(() => expect(run).toHaveBeenCalledOnce());

    queue.schedule();
    queue.schedule();
    queue.schedule();
    resolveFirst();

    await vi.waitFor(() => expect(run).toHaveBeenCalledTimes(2));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("waits for a stable signalling state before running", async () => {
    const run = vi.fn(async () => undefined);
    let stable = false;
    let waits = 0;
    const queue = new RenegotiationQueue({
      run,
      isStable: () => stable,
      onError: () => undefined,
      // Becomes stable on the third poll. Pushing a description into a
      // non-stable connection is what corrupts it, so the queue must hold.
      wait: async () => {
        waits += 1;
        if (waits >= 3) stable = true;
      },
    });

    queue.schedule();
    await vi.waitFor(() => expect(run).toHaveBeenCalledOnce());
    expect(waits).toBe(3);
  });

  // Better to attempt and fail than to deadlock: a connection wedged in a
  // non-stable state would otherwise never renegotiate again.
  it("gives up waiting and runs anyway after the stability budget", async () => {
    const run = vi.fn(async () => undefined);
    const queue = new RenegotiationQueue({
      run,
      isStable: () => false,
      onError: () => undefined,
      wait: async () => undefined,
    });

    queue.schedule();
    await vi.waitFor(() => expect(run).toHaveBeenCalledOnce());
  });

  // Renegotiation races participants leaving and ICE restarts, so a single
  // failure is expected and must not stop later attempts.
  it("reports a failure and keeps accepting work", async () => {
    const onError = vi.fn();
    let shouldFail = true;
    const run = vi.fn(async () => {
      if (shouldFail) throw new Error("peer went away");
    });
    const queue = new RenegotiationQueue({ run, isStable: () => true, onError });

    queue.schedule();
    await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());

    shouldFail = false;
    queue.schedule();
    await vi.waitFor(() => expect(run).toHaveBeenCalledTimes(2));
  });

  it("ignores work scheduled after close", async () => {
    const run = vi.fn(async () => undefined);
    const queue = new RenegotiationQueue({ run, isStable: () => true, onError: () => undefined });

    queue.close();
    queue.schedule();
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(run).not.toHaveBeenCalled();
  });
});
