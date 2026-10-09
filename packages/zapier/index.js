'use strict';

const { version: platformVersion } = require('zapier-platform-core');

const { version } = require('./package.json');
const { afterResponse, beforeRequest } = require('./api');
const authentication = require('./authentication');
const transferCompleted = require('./triggers/transfer-completed');
const findTransfer = require('./searches/find-transfer');
const startTransfer = require('./creates/start-transfer');

/**
 * The Beam integration for Zapier.
 *
 * Both directions: a Zap can start when a Beam transfer finishes, and a Zap can
 * start a transfer.
 *
 * The second one is not obvious, because creating a Beam transfer needs a
 * process that stays connected for the life of the transfer to answer route
 * re-signing callbacks, and a Zapier action cannot be that. It does not have to
 * be: Studio's worker already holds that connection, so Start a Transfer asks a
 * Studio workflow to run rather than creating the transfer itself.
 */
module.exports = {
  version,
  platformVersion,

  authentication,

  /**
   * The platform rewrites input data before an operation sees it -- coercing
   * types, trimming strings -- which makes a Zap behave differently from the
   * same request made directly. Turning it off globally keeps what a user
   * typed and what Beam receives the same thing.
   */
  flags: {
    cleanInputData: false,
  },

  beforeRequest: [beforeRequest],
  afterResponse: [afterResponse],

  triggers: {
    [transferCompleted.key]: transferCompleted,
  },

  searches: {
    [findTransfer.key]: findTransfer,
  },

  creates: {
    [startTransfer.key]: startTransfer,
  },
};
