'use strict';

/**
 * Starts a Beam transfer by asking a Studio workflow to run one.
 *
 * Beam transfers are created over NATS, and creating one involves BeamCore
 * calling back to ask the client to re-sign expired storage routes part-way
 * through. That needs a process that stays connected for the life of the
 * transfer, which a Zapier action -- invoked for a few seconds -- cannot be.
 *
 * So this action does not create the transfer itself. It triggers a Studio
 * workflow, and Studio's worker, which already holds that connection, does the
 * part that needs to persist. The Zap gets a run id back immediately rather
 * than blocking on a transfer that may take minutes.
 *
 * What the workflow does with the posted body is the workflow's business: it
 * lands as the run's input, so `${...}` bindings inside the workflow decide
 * which file moves and where. That makes this one action serve every transfer
 * shape, instead of Zapier needing to model Beam's whole transfer schema.
 */

const perform = async (z, bundle) => {
  const hookUrl = String(bundle.inputData.hook_url || '').trim();

  if (!/^https:\/\//i.test(hookUrl)) {
    throw new z.errors.Error(
      'The Studio webhook URL must be an https:// address copied from the workflow trigger.',
      'InvalidHookUrl',
      400,
    );
  }

  const headers = { 'Content-Type': 'application/json' };
  const idempotencyKey = String(bundle.inputData.idempotency_key || '').trim();
  if (idempotencyKey) {
    // Sent always, but only honoured when the workflow's webhook trigger sets
    // coalesceWindowSeconds above zero: Studio de-duplicates inside its
    // coalescing path and nowhere else, so with the default of 0 the key is
    // accepted and ignored. Verified against a live trigger -- three POSTs with
    // one key produced three runs at 0, and one at 30.
    headers['Idempotency-Key'] = idempotencyKey;
  }

  const response = await z.request({
    url: hookUrl,
    method: 'POST',
    headers,
    body: bundle.inputData.payload || {},
    // The 404 below needs interpreting rather than surfacing raw.
    skipThrowForStatus: true,
  });

  if (response.status === 404) {
    // Studio answers 404 for a bad token and for a trigger that does not exist,
    // deliberately, so that the endpoint cannot be used to discover valid
    // workflow ids. The message says both, because the caller cannot tell.
    throw new z.errors.Error(
      'Studio did not recognise this webhook URL. Check the workflow still exists and that the URL was copied whole, including its token.',
      'HookNotFound',
      404,
    );
  }
  if (response.status >= 400) {
    throw new z.errors.Error(
      `Studio returned ${response.status} starting the workflow.`,
      'StudioError',
      response.status,
    );
  }

  const data = response.data || {};
  return {
    accepted: data.accepted !== false,
    // Null while a coalesced event waits for its window to close: Studio
    // batches a burst into one run, and the id does not exist until it flushes.
    run_id: data.runId || null,
    pending: Boolean(data.pending),
    requested_at: new Date().toISOString(),
  };
};

module.exports = {
  key: 'start_transfer',
  noun: 'Transfer',

  display: {
    label: 'Start a Transfer',
    description:
      'Runs a Beam Transfer Studio workflow, which starts the transfer it defines.',
  },

  operation: {
    inputFields: [
      {
        key: 'hook_url',
        label: 'Studio webhook URL',
        type: 'password',
        required: true,
        helpText:
          'From your Studio workflow: add a **Webhook** trigger and copy its URL. It ends in a token, so treat it as a secret — anyone with it can run the workflow. Your Studio must be reachable from the internet for Zapier to call it; if it is on a private network, use the Beam action inside Studio instead so the call goes outbound.',
      },
      {
        key: 'payload',
        label: 'Input',
        dict: true,
        required: false,
        helpText:
          'Passed to the workflow as its run input, readable inside the workflow as `${...}` bindings. Use it to say which file to move, or leave empty if the workflow already knows.',
      },
      {
        key: 'idempotency_key',
        label: 'Idempotency key',
        required: false,
        helpText:
          'Map a stable value from the trigger, such as a file id. **This only takes effect if the workflow\'s webhook trigger has a coalescing window set** — with the default of none, Studio ignores the key and a Zapier retry will start a second transfer.',
      },
    ],

    perform,

    sample: {
      accepted: true,
      run_id: 'wfr_01J9ZK3M4N5P6Q7R8S9T',
      pending: false,
      requested_at: '2026-09-07T06:45:04.509Z',
    },

    outputFields: [
      { key: 'accepted', label: 'Accepted', type: 'boolean' },
      { key: 'run_id', label: 'Workflow run ID' },
      { key: 'pending', label: 'Waiting to be batched', type: 'boolean' },
      { key: 'requested_at', label: 'Requested at', type: 'datetime' },
    ],
  },
};
