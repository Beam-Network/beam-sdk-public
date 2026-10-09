'use strict';

const {
  BASE_URL,
  TRANSFER_OUTPUT_FIELDS,
  TRANSFER_SAMPLE,
  toZapierTransfer,
} = require('../api');

/**
 * Fires when a Beam transfer reaches a terminal state.
 *
 * Ordering is the whole correctness story here. Zapier polls, takes the page it
 * is given, and de-duplicates on `id` — so anything that does not appear near
 * the top of the first page is never seen at all. Ordering by creation would
 * miss exactly the transfers most worth knowing about: a large one created days
 * ago that finishes today sorts below everything created since. `order_by`
 * exists on Beam's API for this reason.
 */
const perform = async (z, bundle) => {
  const response = await z.request({
    url: `${BASE_URL}/transfers`,
    method: 'GET',
    params: {
      // Filtering server-side keeps in-flight transfers out of the dedupe
      // window entirely, so a long-running transfer cannot consume the page
      // before it has anything to report.
      status: bundle.inputData.status || 'completed',
      order_by: 'completed_at',
      limit: 100,
    },
  });

  const transfers = (response.data && response.data.transfers) || [];
  return transfers.map(toZapierTransfer);
};

module.exports = {
  key: 'transfer_completed',
  noun: 'Transfer',

  display: {
    label: 'Transfer Finished',
    description:
      'Triggers when a Beam transfer finishes. Choose whether to watch successful transfers, failures, or both.',
  },

  operation: {
    type: 'polling',

    inputFields: [
      {
        key: 'status',
        label: 'Which transfers',
        type: 'string',
        required: false,
        default: 'completed',
        choices: [
          { value: 'completed', sample: 'completed', label: 'Succeeded only' },
          { value: 'failed', sample: 'failed', label: 'Failed only' },
          { value: 'cancelled', sample: 'cancelled', label: 'Cancelled only' },
        ],
        helpText:
          'Leave as **Succeeded only** to act on transfers that worked. Pick **Failed only** to build an alerting Zap.',
      },
    ],

    perform,
    sample: TRANSFER_SAMPLE,
    outputFields: TRANSFER_OUTPUT_FIELDS,
  },
};
