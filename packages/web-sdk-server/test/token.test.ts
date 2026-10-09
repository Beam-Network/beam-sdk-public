import { describe, expect, it } from "vitest";

import { BeamTokenError, MAX_TTL_SECONDS, assertScopes, mintToken, verifyToken } from "../src/token.js";

const SECRET = "a".repeat(32);
const OTHER_SECRET = "b".repeat(32);

describe("mintToken", () => {
  it("produces a bwt1 token whose claims round-trip", () => {
    const { token, claims, expiresIn } = mintToken({
      secret: SECRET,
      scopes: ["rooms:create", "room:read"],
      subject: "user-7",
      organizationId: "org-1",
    });

    expect(token.startsWith("bwt1.")).toBe(true);
    expect(token.split(".")).toHaveLength(3);
    expect(claims).toMatchObject({ v: 1, sub: "user-7", org: "org-1", scopes: ["rooms:create", "room:read"] });
    expect(expiresIn).toBe(120);
    expect(verifyToken(token, { secret: SECRET })).toMatchObject({ sub: "user-7" });
  });

  it("gives each token a distinct id", () => {
    const first = mintToken({ secret: SECRET, scopes: ["room:read"] });
    const second = mintToken({ secret: SECRET, scopes: ["room:read"] });
    expect(first.claims.jti).not.toBe(second.claims.jti);
  });

  it("deduplicates scopes", () => {
    const { claims } = mintToken({ secret: SECRET, scopes: ["room:read", "room:read"] });
    expect(claims.scopes).toEqual(["room:read"]);
  });

  // Short-lived is the entire security argument for putting a token in a
  // browser, so the ceiling is not negotiable.
  it("clamps the lifetime to the five-minute ceiling", () => {
    const { expiresIn, claims } = mintToken({ secret: SECRET, scopes: ["room:read"], ttlSeconds: 86_400 });
    expect(expiresIn).toBe(MAX_TTL_SECONDS);
    expect(claims.exp - claims.iat).toBe(MAX_TTL_SECONDS);
  });

  it("rejects a lifetime below the floor", () => {
    expect(() => mintToken({ secret: SECRET, scopes: ["room:read"], ttlSeconds: 5 })).toThrow(/ttlSeconds/);
  });

  it("rejects an unknown scope", () => {
    expect(() => mintToken({ secret: SECRET, scopes: ["admin:everything"] as never })).toThrow(/Unknown scope/);
  });

  it("rejects an empty scope list", () => {
    expect(() => mintToken({ secret: SECRET, scopes: [] })).toThrow(/at least one scope/);
  });

  // This secret is the only thing between a stranger and a token that spends
  // the customer's credits.
  it("rejects a weak signing secret and says how to make one", () => {
    expect(() => mintToken({ secret: "short", scopes: ["room:read"] })).toThrow(BeamTokenError);
    expect(() => mintToken({ secret: "short", scopes: ["room:read"] })).toThrow(/openssl rand -hex 32/);
  });
});

describe("verifyToken", () => {
  it("rejects a token signed with a different secret", () => {
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"] });
    expect(() => verifyToken(token, { secret: OTHER_SECRET })).toThrow(/signature is invalid/);
  });

  it("rejects a tampered payload", () => {
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"] });
    const [prefix, , signature] = token.split(".");
    const forged = Buffer.from(
      JSON.stringify({ v: 1, iss: "x", aud: "beam-web-sdk", scopes: ["transfers:create"], iat: 0, exp: 9e9, jti: "x" }),
    ).toString("base64url");

    expect(() => verifyToken(`${prefix}.${forged}.${signature}`, { secret: SECRET })).toThrow(/signature is invalid/);
  });

  it("rejects a malformed token", () => {
    for (const bad of ["", "nonsense", "bwt1.only-two", "jwt1.a.b"]) {
      expect(() => verifyToken(bad, { secret: SECRET })).toThrow(BeamTokenError);
    }
  });

  it("rejects an expired token", () => {
    const past = Date.now() - 10 * 60 * 1000;
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"], now: () => past });
    expect(() => verifyToken(token, { secret: SECRET })).toThrow(/expired/);
  });

  it("tolerates modest clock drift between hosts", () => {
    const slightlyAhead = Date.now() + 10_000;
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"], now: () => slightlyAhead });
    expect(() => verifyToken(token, { secret: SECRET })).not.toThrow();
  });

  it("rejects a token issued far in the future", () => {
    const wayAhead = Date.now() + 60 * 60 * 1000;
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"], now: () => wayAhead });
    expect(() => verifyToken(token, { secret: SECRET })).toThrow(/issued in the future/);
  });

  it("enforces the audience", () => {
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"], audience: "other-app" });
    expect(() => verifyToken(token, { secret: SECRET, audience: "beam-web-sdk" })).toThrow(/audience mismatch/);
  });

  // A token lifted from one page must not work when replayed from another.
  it("enforces the origin binding when the token carries one", () => {
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"], origin: "https://app.example" });

    expect(() => verifyToken(token, { secret: SECRET, origin: "https://app.example" })).not.toThrow();
    expect(() => verifyToken(token, { secret: SECRET, origin: "https://evil.example" })).toThrow(/different origin/);
  });

  it("allows a token minted without an origin to be used anywhere", () => {
    const { token } = mintToken({ secret: SECRET, scopes: ["room:read"] });
    expect(() => verifyToken(token, { secret: SECRET, origin: "https://anywhere.example" })).not.toThrow();
  });
});

describe("assertScopes", () => {
  it("passes when every required scope is present", () => {
    const { claims } = mintToken({ secret: SECRET, scopes: ["rooms:create", "room:read"] });
    expect(() => assertScopes(claims, ["room:read"])).not.toThrow();
  });

  it("names the missing scopes", () => {
    const { claims } = mintToken({ secret: SECRET, scopes: ["room:read"] });
    expect(() => assertScopes(claims, ["transfers:create", "transfers:cancel"])).toThrow(
      /transfers:create, transfers:cancel/,
    );
  });
});
