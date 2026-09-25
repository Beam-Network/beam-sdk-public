import { describe, expect, it } from "vitest";

import { Beam } from "../src/index.js";
import { resolveConfig } from "../src/config.js";
import { BeamConfigError } from "../src/core/errors/index.js";

describe("configuration", () => {
  it("requires exactly one authentication mode", () => {
    expect(() => resolveConfig({})).toThrow(BeamConfigError);
    expect(() => resolveConfig({ clientKey: "bm_pub_abc", tokenEndpoint: "/token" })).toThrow(
      /more than one authentication mode/,
    );
  });

  it("accepts each authentication mode on its own", () => {
    expect(resolveConfig({ clientKey: "bm_pub_abc" }).auth).toEqual({ clientKey: "bm_pub_abc" });
    expect(resolveConfig({ tokenEndpoint: "/api/beam/token" }).auth).toEqual({ tokenEndpoint: "/api/beam/token" });
    const getToken = async (): Promise<string> => "token";
    expect(resolveConfig({ getToken }).auth).toEqual({ getToken });
  });

  // The whole point of the SDK's auth design. If these ever stop throwing, a
  // customer can ship a credit-spending credential to every visitor.
  describe("secret credential guard", () => {
    it.each([
      ["bm_live_0123456789abcdef", "beam-website secret key"],
      ["bm_test_0123456789abcdef", "beam-website secret key"],
      ["b1m_0123456789abcdef", "BeamCore API key"],
      ["bag_0123456789abcdef", "Beam agent credential"],
      ["btrstd1.eyJhIjoxfQ.c2ln", "coordinator Studio delegation"],
    ])("rejects %s passed as clientKey", (key) => {
      expect(() => resolveConfig({ clientKey: key })).toThrow(BeamConfigError);
      expect(() => resolveConfig({ clientKey: key })).toThrow(/publishable key/);
    });

    it("rejects an `apiKey` option even though it is not in the type", () => {
      expect(() => resolveConfig({ apiKey: "b1m_secret" } as never)).toThrow(/`apiKey` is not a supported option/);
    });

    it("names the console as the source of a publishable key", () => {
      expect(() => new Beam({ clientKey: "bm_live_abc" })).toThrow(/Beam console/);
    });
  });

  it("validates numeric options", () => {
    expect(() => resolveConfig({ clientKey: "bm_pub_a", timeoutMs: 0 })).toThrow(/timeoutMs/);
    expect(() => resolveConfig({ clientKey: "bm_pub_a", maxAttempts: 0 })).toThrow(/maxAttempts/);
    expect(() => resolveConfig({ clientKey: "bm_pub_a", maxAttempts: 1.5 })).toThrow(/maxAttempts/);
  });

  it("maps debug:true onto the debug log level", () => {
    expect(resolveConfig({ clientKey: "bm_pub_a", debug: true }).logLevel).toBe("debug");
    expect(resolveConfig({ clientKey: "bm_pub_a" }).logLevel).toBe("warn");
    // An explicit level wins over the shorthand.
    expect(resolveConfig({ clientKey: "bm_pub_a", debug: true, logLevel: "error" }).logLevel).toBe("error");
  });

  it("strips a trailing slash from controlUrl so paths do not double up", () => {
    expect(resolveConfig({ clientKey: "bm_pub_a", controlUrl: "https://x.test/api/" }).controlUrl).toBe(
      "https://x.test/api",
    );
  });

  it("constructs without performing any network call", () => {
    let called = false;
    const beam = new Beam({
      clientKey: "bm_pub_abc",
      fetch: (() => {
        called = true;
        return Promise.reject(new Error("should not be called"));
      }) as unknown as typeof fetch,
    });
    expect(beam.rooms).toBeDefined();
    expect(beam.transfers).toBeDefined();
    expect(beam.broadcast).toBeDefined();
    expect(called).toBe(false);
  });
});
