'use strict';

const {
  BASE_URL,
  TRANSFER_OUTPUT_FIELDS,
  TRANSFER_SAMPLE,
  toZapierTransfer,
} = require('../api');

/**
 * Looks up one transfer mid-Zap.
 *
 * Beam accepts either the UUID or the transfer key on this route, and the
 * transfer key is the half a user actually recognises — it is what they named
 * the transfer with — so both are offered rather than forcing a UUID.
 */
const perform = async (z, bundle) => {
  const identifier = String(bundle.inputData.transfer || '').trim();
  if (!identifier) {
    return [];
  }

  const response = await z.request({
    url: `${BASE_URL}/transfers/${encodeURIComponent(identifier)}`,
    method: 'GET',
    // A search that finds nothing is an empty result, not a failed Zap step.
    // Without this the 404 becomes an error and halts the Zap.
    skipThrowForStatus: true,
  });

  if (response.status === 404) {
    return [];
  }
  if (response.status >= 400) {
    throw new z.errors.Error(
      `Beam returned ${response.status} looking up "${identifier}".`,
      'BeamApiError',
      response.status,
    );
  }

  return [toZapierTransfer(response.data)];
};

module.exports = {
  key: 'find_transfer',
  noun: 'Transfer',

  display: {
    label: 'Find Transfer',
    description: 'Finds a Beam transfer by its ID or transfer key.',
  },

  operation: {
    inputFields: [
      {
        key: 'transfer',
        label: 'Transfer ID or key',
        type: 'string',
        required: true,
        helpText:
          'Either the transfer UUID or the transfer key you named it with.',
      },
    ],

    perform,
    sample: TRANSFER_SAMPLE,
    outputFields: TRANSFER_OUTPUT_FIELDS,
  },
};
