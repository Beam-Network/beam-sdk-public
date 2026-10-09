import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { Beam } from "../src/index.js";
import { FakeMediaStream, FakePeerConnection, installWebrtcFakes, makeToken } from "./helpers.js";

const ANSWER_SDP = "v=0\r\no=- relay answer\r\n";

function harness(options: { location?: string | null; whipStatus?: number } = {}) {
  const requests: Array<{ url: string; method: string; contentType: string | undefined; body: string | undefined }> =
    [];

  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const headers = new Headers(init?.headers);
    requests.push({
      url,
      method: init?.method ?? "GET",
      contentType: headers.get("content-type") ?? undefined,
      body: typeof init?.body === "string" ? init.body : undefined,
    });

    if (url.includes("/token")) {
      return new Response(JSON.stringify({ access_token: makeToken(), expires_in: 120 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url.includes("/whip") || url.includes("/whep")) {
      const status = options.whipStatus ?? 201;
      if (status >= 400) return new Response("relay refused the offer", { status });
      const responseHeaders: Record<string, string> = { "content-type": "application/sdp" };
      const location = options.location === undefined ? "/resource/abc" : options.location;
      if (location !== null) responseHeaders["location"] = location;
      return new Response(ANSWER_SDP, { status, headers: responseHeaders });
    }
    if (url.includes("/resource/")) return new Response(null, { status: 204 });
    return new Response(
      JSON.stringify({
        id: "room_live",
        created_at: "2026-08-31T00:00:00Z",
        media: { url: "https://relay.test/t/tun_1/webrtc/pub_1", token: "media-token" },
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as unknown as typeof fetch;

  const beam = new Beam({
    tokenEndpoint: "https://app.test/api/beam/token",
    controlUrl: "https://control.test/v1",
    fetch: fetchImpl,
  });
  return { beam, requests };
}

describe("broadcast", () => {
  let uninstall: () => void;
  beforeEach(() => (uninstall = installWebrtcFakes()));
  afterEach(() => uninstall());

  it("publishes over WHIP with an SDP body and the room's media token", async () => {
    const { beam, requests } = harness();
    const stream = new FakeMediaStream(["audio", "video"]) as unknown as MediaStream;

    const connection = await beam.broadcast.publish({ roomId: "room_live", stream });

    const whip = requests.find((request) => request.url.includes("/whip"));
    expect(whip?.method).toBe("POST");
    // WHIP is SDP over HTTP, not JSON — getting this wrong fails at the relay.
    expect(whip?.contentType).toBe("application/sdp");
    expect(whip?.body).toContain("v=0");
    expect(FakePeerConnection.instances[0]?.getSenders()).toHaveLength(2);
    expect(FakePeerConnection.instances[0]?.remoteDescription?.sdp).toBe(ANSWER_SDP);

    await connection.close();
  });

  it("watches over WHEP with recvonly transceivers", async () => {
    const { beam, requests } = harness();

    const connection = await beam.broadcast.watch({ roomId: "room_live" });

    expect(requests.some((request) => request.url.includes("/whep"))).toBe(true);
    expect(FakePeerConnection.instances[0]?.getTransceivers().map((t) => t.direction)).toEqual([
      "recvonly",
      "recvonly",
    ]);
    await connection.close();
  });

  it("emits inbound tracks to the viewer", async () => {
    const { beam } = harness();
    const connection = await beam.broadcast.watch({ roomId: "room_live" });

    const onTrack = vi.fn();
    connection.on("track", onTrack);
    FakePeerConnection.instances[0]?.emitTrack(new FakeMediaStream(["video"]), "video");

    expect(onTrack).toHaveBeenCalledWith(expect.objectContaining({ track: expect.anything() }));
    await connection.close();
  });

  // DELETEing the resource is how WHIP/WHEP releases a slot; skipping it leaves
  // the relay holding it until the lease expires.
  it("deletes the WHIP resource on close", async () => {
    const { beam, requests } = harness();
    const stream = new FakeMediaStream(["video"]) as unknown as MediaStream;

    const connection = await beam.broadcast.publish({ roomId: "room_live", stream });
    await connection.close();

    const cleanup = requests.find((request) => request.method === "DELETE");
    expect(cleanup?.url).toBe("https://relay.test/resource/abc");
  });

  it("resolves a relative Location header against the endpoint", async () => {
    const { beam, requests } = harness({ location: "./session/9" });
    const connection = await beam.broadcast.watch({ roomId: "room_live" });
    await connection.close();

    expect(requests.find((request) => request.method === "DELETE")?.url).toBe(
      "https://relay.test/t/tun_1/webrtc/pub_1/session/9",
    );
  });

  it("closes cleanly when the relay returns no Location", async () => {
    const { beam, requests } = harness({ location: null });
    const connection = await beam.broadcast.watch({ roomId: "room_live" });
    await connection.close();

    expect(requests.some((request) => request.method === "DELETE")).toBe(false);
    expect(FakePeerConnection.instances[0]?.closed).toBe(true);
  });

  // A failed publish must not leave a live peer connection holding the camera.
  it("tears down the peer connection when signalling fails", async () => {
    const { beam } = harness({ whipStatus: 403 });
    const stream = new FakeMediaStream(["video"]) as unknown as MediaStream;

    await expect(beam.broadcast.publish({ roomId: "room_live", stream })).rejects.toThrow(/relay refused the offer/);
    expect(FakePeerConnection.instances[0]?.closed).toBe(true);
  });

  it("accepts an explicit endpoint URL without a room lookup", async () => {
    const { beam, requests } = harness();
    const stream = new FakeMediaStream(["video"]) as unknown as MediaStream;

    const connection = await beam.broadcast.publish({
      url: "https://relay.test/custom/whip",
      token: "direct-token",
      stream,
    });

    expect(requests.some((request) => request.url.includes("/rooms"))).toBe(false);
    await connection.close();
  });

  it("requires either a roomId or a url", async () => {
    const { beam } = harness();
    const stream = new FakeMediaStream(["video"]) as unknown as MediaStream;

    await expect(beam.broadcast.publish({ stream })).rejects.toThrow(/roomId.*url/);
  });

  it("close is idempotent", async () => {
    const { beam } = harness();
    const connection = await beam.broadcast.watch({ roomId: "room_live" });

    await connection.close();
    await expect(connection.close()).resolves.toBeUndefined();
  });
});
