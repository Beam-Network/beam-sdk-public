'use strict';

const { BASE_URL } = require('./api');

/**
 * API-key authentication.
 *
 * Beam keys are long-lived and scoped to one account, so there is nothing to
 * refresh and no consent screen to redirect through. OAuth would be the nicer
 * experience, but Beam's authorization server has no `client_credentials` grant
 * and no dynamic client registration, so a Zapier app cannot register itself as
 * a client today.
 */
module.exports = {
  type: 'custom',

  fields: [
    {
      key: 'api_key',
      label: 'Beam API key',
      type: 'password',
      required: true,
      helpText:
        'Create a key in the Beam console under **API keys**. It starts with `b1m_` and is shown only once, so copy it before closing the dialog.',
    },
  ],

  /**
   * The cheapest call that proves the key authenticates. It also returns the
   * key's own role and prefix, which is what the connection label is built
   * from — better than echoing the key back at the user.
   */
  test: {
    url: `${BASE_URL}/auth/me`,
    method: 'GET',
  },

  /**
   * Distinguishes two connections in the Zap editor. The key prefix is the
   * public half of the credential and is already how Beam identifies a key in
   * its own console, so it is the thing a user will recognise.
   */
  connectionLabel: '{{bundle.inputData.current_key_prefix}}',
};
