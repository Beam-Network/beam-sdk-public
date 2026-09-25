/**
 * Entry point for the script-tag build (`beam.min.js`, global `Beam`).
 *
 * This is what the Beam console's embed snippet loads. It exists so a customer
 * can paste two lines into a page and be running, without a bundler, a build
 * step, or a backend — the same shape as Adobe's `alloy` or Stripe's `Stripe()`.
 *
 * The global name, the file path, and the `configure`/`client` surface are a
 * public contract: changing any of them breaks every page that pasted the
 * snippet, so they are versioned in the CDN path rather than edited in place.
 */

import { Beam as BeamClient } from "./client.js";
import type { BeamOptions } from "./config.js";
import { BeamConfigError } from "./core/errors/index.js";

export * from "./index.js";

let singleton: BeamClient | undefined;

/**
 * Configures the page-wide Beam client.
 *
 * Calling it twice replaces the client. That is deliberate — a single-page app
 * that re-configures after a user signs in should get a client bound to the new
 * identity, not silently keep the anonymous one.
 */
export function configure(options: BeamOptions): BeamClient {
  singleton = new BeamClient(options);
  return singleton;
}

/** The configured client. Throws if `configure` has not been called. */
export function client(): BeamClient {
  if (!singleton) {
    throw new BeamConfigError(
      "Beam is not configured yet. Call Beam.configure({ clientKey: 'bm_pub_…' }) before using the SDK.",
    );
  }
  return singleton;
}

/** True once `configure` has run. Lets a snippet guard against double-loading. */
export function isConfigured(): boolean {
  return singleton !== undefined;
}

/** Discards the page-wide client. Primarily for tests and sign-out flows. */
export function reset(): void {
  singleton = undefined;
}
