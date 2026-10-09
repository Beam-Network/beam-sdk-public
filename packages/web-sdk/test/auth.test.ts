import { describe, expect, it, vi } from "vitest";

import { CachingTokenProvider, brokerMinter, callbackMinter, clientKeyMinter } from "../src/core/auth/index.js";
import { decodeTokenClaims, isTokenStale, normalizeTokenResponse } from "../src/core/auth/token.js";
import { BeamAuthError, BeamConfigError } from "../src/core/errors/index.js";
import { makeToken, stubFetch } from "./helpers.js";

describe("token decoding", () => {
  it("reads claims out of a bwt1 token", () => {
    const token = makeToken({ sub: "user-7", scopes: ["rooms:join"] });
    expect(decodeTokenClaims(token)).toMatchObject({ sub: "user-7", scopes: ["rooms:join"] });
  });

  it("returns undefined for an opaque token rather than throwing", () => {
    expect(decodeTokenClaims("opaque-token")).toBeUndefined();
    expect(decodeTokenClaims("bwt1.@@@notbase64@@@.sig")).toBeUndefined();
  });

  // `exp` is absolute, so it survives a response sitting in a proxy; `expires_in`
  // is relative and would over-report freshness in that case.
  it("prefers the absolute exp claim over expires_in", () => {
    const exp = Math.floor(Date.now() / 1000) + 60;
    const token = normalizeTokenResponse({ access_token: makeToken({ exp }), expires_in: 9999 }, Date.now());
    expect(token.expiresAt).toBe(exp * 1000);
  });

  it("falls back to expires_in for an opaque token", () => {
    const now = 1_000_000;
    const token = normalizeTokenResponse({ access_token: "opaque", expires_in: 90 }, now);
    expect(token.expiresAt).toBe(now + 90_000);
  });

  it("treats a token as stale once 75% of its life has elapsed", () => {
    const issuedAt = 0;
    const token = { value: "t", expiresAt: 120_000, scopes: [], endpoint: undefined };
    expect(isTokenStale(token, 60_000, issuedAt)).toBe(false);
    expect(isTokenStale(token, 90_000, issuedAt)).toBe(true);
  });
});

describe("CachingTokenProvider", () => {
  it("reuses a fresh token instead of minting again", async () => {
    const mint = vi.fn(async () => ({ access_token: makeToken(), expires_in: 120 }));
    const provider = new CachingTokenProvider({ mint });

    await provider.getToken();
    await provider.getToken();

    expect(mint).toHaveBeenCalledTimes(1);
  });

  // Opening a room and starting a transfer in the same tick must not burn two
  // rate-limited broker calls for one session.
  it("collapses concurrent callers onto a single mint", async () => {
    let resolveMint: (value: { access_token: string; expires_in: number }) => void = () => undefined;
    const mint = vi.fn(
      () =>
        new Promise<{ access_token: string; expires_in: number }>((resolve) => {
          resolveMint = resolve;
        }),
    );
    const provider = new CachingTokenProvider({ mint });

    const pending = [provider.getToken(), provider.getToken(), provider.getToken()];
    resolveMint({ access_token: makeToken(), expires_in: 120 });
    const tokens = await Promise.all(pending);

    expect(mint).toHaveBeenCalledTimes(1);
    expect(new Set(tokens.map((token) => token.value)).size).toBe(1);
  });

  it("mints again after invalidate", async () => {
    const mint = vi.fn(async () => ({ access_token: makeToken(), expires_in: 120 }));
    const provider = new CachingTokenProvider({ mint });

    await provider.getToken();
    provider.invalidate();
    expect(provider.peek()).toBeUndefined();
    await provider.getToken();

    expect(mint).toHaveBeenCalledTimes(2);
  });

  it("re-mints once the cached token goes stale", async () => {
    let now = 0;
    const mint = vi.fn(async () => ({ access_token: "opaque", expires_in: 100 }));
    const provider = new CachingTokenProvider({ mint, now: () => now });

    await provider.getToken();
    now = 50_000;
    await provider.getToken();
    expect(mint).toHaveBeenCalledTimes(1);

    now = 80_000; // past 75% of a 100s life
    await provider.getToken();
    expect(mint).toHaveBeenCalledTimes(2);
  });

  it("rejects a response with no access_token", async () => {
    const provider = new CachingTokenProvider({ mint: async () => ({}) as never });
    await expect(provider.getToken()).rejects.toThrow(BeamAuthError);
  });

  // Almost always a server clock problem, and worth saying so: the alternative
  // is an infinite mint-and-fail loop that looks like a network fault.
  it("rejects an already-expired token and names clock skew", async () => {
    const expired = makeToken({ exp: Math.floor(Date.now() / 1000) - 10 });
    const provider = new CachingTokenProvider({ mint: async () => ({ access_token: expired }) });
    await expect(provider.getToken()).rejects.toThrow(/clock skew/);
  });
});

describe("minters", () => {
  it("clientKeyMinter refuses a non-publishable key", () => {
    expect(() => clientKeyMinter("https://api.test/token", "bm_live_secret", { fetch: globalThis.fetch })).toThrow(
      BeamConfigError,
    );
  });

  it("clientKeyMinter posts the key and omits credentials", async () => {
    const { fetch, requests } = stubFetch({ "/token": { body: { access_token: makeToken(), expires_in: 120 } } });
    const mint = clientKeyMinter("https://api.test/token", "bm_pub_abc", {
      fetch,
      scopes: ["rooms:create"],
      subject: "user-7",
    });

    await mint({});

    const body = JSON.parse(requests[0]?.body ?? "{}") as Record<string, unknown>;
    expect(body).toMatchObject({ client_key: "bm_pub_abc", scopes: ["rooms:create"], subject: "user-7" });
  });

  it("brokerMinter sends requested scopes and keeps same-origin credentials", async () => {
    const { fetch, requests } = stubFetch({ "/api/beam/token": { body: { access_token: makeToken() } } });
    const mint = brokerMinter("/api/beam/token", { fetch, scopes: ["transfers:create"] });

    await mint({});

    expect(JSON.parse(requests[0]?.body ?? "{}")).toEqual({ scopes: ["transfers:create"] });
  });

  it("brokerMinter surfaces the server's error message", async () => {
    const { fetch } = stubFetch({
      "/api/beam/token": { status: 403, body: { error_description: "user lacks the rooms:create permission" } },
    });
    const mint = brokerMinter("/api/beam/token", { fetch });

    await expect(mint({})).rejects.toThrow(/user lacks the rooms:create permission/);
  });

  it("callbackMinter accepts a bare string", async () => {
    const mint = callbackMinter(async () => "raw-token");
    expect(await mint({})).toEqual({ access_token: "raw-token" });
  });
});
