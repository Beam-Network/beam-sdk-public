import { test } from 'node:test';
import assert from 'node:assert/strict';
import { SdkPerformanceCollector, SourceSignatureHistory } from '../dist/performance.js';

test('performance v2 bounds detail and retains the exact v1 wire shape', () => {
  const metrics = new SdkPerformanceCollector();
  metrics.observe('sdk.discovery', 7);
  metrics.observe('sdk.multipart_provider', 25);
  metrics.observe('sdk.multipart_provider', 250);
  metrics.observe('signed-url:private', 1);
  metrics.observe('sdk.signing', Infinity);
  metrics.gauge('signing_limit', 64);
  metrics.gauge('object_key', 1);
  metrics.mark('first_batch_ms');
  metrics.finish();
  const current = metrics.snapshot(true);
  assert.equal(current.schema_version, 'sdk-performance/v2');
  const provider = current.measurements.find(x => x.name === 'sdk.multipart_provider');
  assert.equal(provider.histogram.length, 18);
  assert.equal(provider.histogram.reduce((a,b) => a+b, 0), 2);
  assert.equal(provider.work_ms, 275);
  assert.deepEqual(current.gauges, { signing_limit: 64 });
  assert(current.unmeasured.includes('sdk.manifest_ack'));
  assert(!JSON.stringify(current).includes('private'));
  const legacy = metrics.snapshot(false);
  assert.deepEqual(Object.keys(legacy).sort(), ['counters', 'measurements', 'schema_version']);
  assert.equal(legacy.schema_version, 'sdk-performance/v1');
  assert.deepEqual(legacy.measurements, [{name:'sdk.discovery',count:1,work_ms:7,max_ms:7}]);
});


test('renewals distinguish new sources and replay, with bounded history and exact legacy shape', () => {
  const history = new SourceSignatureHistory();
  const initial = new SdkPerformanceCollector(history);
  initial.sourceCreated(0); initial.sourceCreated(131071);
  assert.equal(initial.snapshot().counters.source_renewals, 0);
  assert.equal(initial.snapshot(false).counters.source_renewals, undefined);
  const replay = new SdkPerformanceCollector(history);
  replay.sourceCreated(131071); replay.sourceCreated(1);
  replay.observe('sdk.producer_wait', 3);
  assert.equal(replay.snapshot().counters.source_signatures, 2);
  assert.equal(replay.snapshot().counters.source_renewals, 1);
  assert.equal(replay.snapshot().measurements[0].name, 'sdk.producer_wait');
  replay.sourceCreated(131072);
  assert.equal(replay.snapshot().counters.source_renewals, undefined);
  const resumed = new SdkPerformanceCollector(new SourceSignatureHistory(false));
  resumed.sourceCreated(0);
  assert.equal(resumed.snapshot().counters.source_renewals, undefined);
});
