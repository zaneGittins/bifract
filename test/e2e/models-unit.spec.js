// Unit tests for pure helpers in web/static/models.js, evaluated in Node.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load(currentFractal = null) {
  const read = (f) => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8');
  const window = { FractalContext: { currentFractal } };
  const document = { dispatchEvent() {}, getElementById() { return null; }, querySelectorAll() { return []; }, querySelector() { return null; } };
  const localStorage = { getItem() { return null; }, setItem() {} };
  new Function('window', 'document', 'localStorage', read('timezone.js'))(window, document, localStorage);
  new Function('window', 'document', 'localStorage', 'TZ', read('timePicker.js'))(window, document, localStorage, window.TZ);
  new Function('window', 'document', read('models.js'))(window, document);
  return window.AnalyticsModels;
}

const plain = { timeRange: '24h', previewWindow: '7d', previewStart: '', previewEnd: '' };

test('the editor opens on the last 24 hours and 7 days in a plain fractal', () => {
  expect(load({ id: 'plain' })._rangeDefaults()).toEqual(plain);
  expect(load(null)._rangeDefaults()).toEqual(plain);
});

test('score preview opens on a dataset fractal range, in whole hours', () => {
  const M = load({ id: 'f1', default_range_start: '2026-10-27T08:37:53Z', default_range_end: '2026-10-30T13:49:59Z' });
  // Network models score complete hours, so the last partial hour must be inside.
  expect(M._rangeDefaults()).toEqual({
    timeRange: 'dataset', previewWindow: 'custom', previewStart: '2026-10-27T08:00:00.000Z', previewEnd: '2026-10-30T14:00:00.000Z',
  });
});

test('a dataset whose last log is on the hour keeps that hour', () => {
  const M = load({ id: 'f1', default_range_start: '2026-10-27T00:00:00Z', default_range_end: '2026-10-30T00:00:00Z' });
  expect(M._rangeDefaults().previewEnd).toBe('2026-10-30T01:00:00.000Z');
});

test('the Results tab runs over the dataset range, to the minute', () => {
  const M = load({ id: 'f1', default_range_start: '2026-10-27T08:37:53Z', default_range_end: '2026-10-30T13:49:59Z' });
  M.editor = { timeRange: 'dataset' };
  expect(M._editorTimeRange()).toEqual({ start: '2026-10-27T08:37:00.000Z', end: '2026-10-30T13:50:00.000Z' });
  expect(M._editorRanges().map(([v]) => v)).toEqual(['1h', '6h', '24h', '7d', '30d', 'dataset']);
  expect(load({ id: 'plain' })._editorRanges().map(([v]) => v)).toEqual(['1h', '6h', '24h', '7d', '30d']);
});

test('a dataset range longer than a preview allows keeps its last 90 days', () => {
  const M = load({ id: 'f1', default_range_start: '2026-01-01T00:00:00Z', default_range_end: '2026-10-30T13:49:59Z' });
  expect(M._rangeDefaults()).toEqual({
    timeRange: 'dataset', previewWindow: 'custom', previewStart: '2026-08-01T14:00:00.000Z', previewEnd: '2026-10-30T14:00:00.000Z',
  });
});

test('connection fields show the bytes field for Beacon and the duration field for Long Connection', () => {
  const M = load();
  const network = { src_field: 's', dst_field: 'd', port_field: 'p', bytes_field: 'orig_bytes', duration_field: 'dur' };
  expect(M._connectionFields('beacon', { network })).toEqual(['s', 'd', 'p', 'orig_bytes']);
  expect(M._connectionFields('long_connection', { network })).toEqual(['s', 'd', 'p', 'dur']);
  expect(M._connectionFields('beacon', {})).toEqual(['src_ip', 'dst_ip', 'dst_port', 'orig_bytes']);
  expect(M._connectionFields('long_connection', {})).toEqual(['src_ip', 'dst_ip', 'dst_port', 'duration']);
});

test('rarity and first seen read as a value, overall or per group', () => {
  const M = load();
  expect(M._shapeParts('rarity', { value_key: 'rmm' })).toEqual({ groups: [], value: 'rmm' });
  expect(M._shapeParts('rarity', { partition_key: 'user', value_key: 'rmm' })).toEqual({ groups: ['user'], value: 'rmm' });
  expect(M._shapeParts('first_seen', { key_fields: ['rmm'] })).toEqual({ groups: [], value: 'rmm' });
  expect(M._shapeParts('first_seen', { key_fields: ['user', 'host', 'rmm'] })).toEqual({ groups: ['user', 'host'], value: 'rmm' });
  expect(M._shapeParts('volume_baseline', { key_fields: ['host'] })).toBeNull();
});

test('the shape summary says what a model scores and where', () => {
  const M = load();
  expect(M._shapeSummary('rarity', { groups: [], value: 'rmm' })).toBe('Scores how rare each <code>rmm</code> is across all matching logs, counted in days.');
  expect(M._shapeSummary('rarity', { groups: ['user'], value: 'rmm' })).toBe('Scores how rare each <code>rmm</code> is for each <code>user</code>, counted in days.');
  expect(M._shapeSummary('first_seen', { groups: ['user', 'host'], value: 'rmm' })).toBe('Tracks when each <code>rmm</code> was first and last seen for each <code>user</code> + <code>host</code>.');
  expect(M._shapeSummary('first_seen', { groups: [], value: '<b>' })).toContain('&lt;b&gt;');
  expect(M._shapeSummary('first_seen', { groups: ['<i>'], value: 'v' })).toContain('&lt;i&gt;');
});

test('an ungrouped rarity model has no group column', () => {
  const M = load();
  const cols = def => M._viewColumns({ model_type: 'rarity', definition: def }).map(c => c.col);
  expect(cols({ value_key: 'rmm' })).not.toContain('partition_val');
  expect(cols({ partition_key: 'user', value_key: 'rmm' })).toContain('partition_val');
});

test('a first seen editor needs a value, and groups stay ahead of it', () => {
  const M = load();
  M.editor = { modelType: 'first_seen', keyFields: ['user', ''], network: {} };
  expect(M._validateShape()).toBe('Set a value field');
  M.editor.keyFields = ['', 'rmm'];
  expect(M._validateShape()).toBeNull();
  expect(M._composeDefinition({}).key_fields).toEqual(['rmm']);
  M.editor.keyFields = ['user', 'rmm'];
  expect(M._composeDefinition({}).key_fields).toEqual(['user', 'rmm']);
});

test('a rarity editor needs only a value', () => {
  const M = load();
  M.editor = { modelType: 'rarity', partitionKey: '', valueKey: '', keyFields: [''], network: {} };
  expect(M._validateShape()).toBe('Set a value field');
  M.editor.valueKey = 'rmm';
  expect(M._validateShape()).toBeNull();
});

test('a new group goes ahead of the first seen value, and a removed one leaves it', () => {
  const M = load();
  M.currentView = 'editor';
  M.editor = { modelType: 'first_seen', keyFields: ['rmm'] };
  M._addKeyField();
  expect(M.editor.keyFields).toEqual(['', 'rmm']);
  M.editor.keyFields = ['user', 'host', 'rmm'];
  M._removeKeyField(0);
  expect(M.editor.keyFields).toEqual(['host', 'rmm']);
  expect(M.editor.dirty).toBe(true);
});

test('other types append key fields', () => {
  const M = load();
  M.editor = { modelType: 'volume_baseline', keyFields: ['host'] };
  M._addKeyField();
  expect(M.editor.keyFields).toEqual(['host', '']);
});

test('a type switch fills blank fields and never overwrites', () => {
  const M = load();
  const go = (e, to) => { const prev = e.modelType; e.modelType = to; M._carryShape(e, prev); return e; };

  const e = { modelType: 'rarity', partitionKey: 'user', valueKey: 'rmm', keyFields: [''] };
  expect(go(e, 'first_seen').keyFields).toEqual(['user', 'rmm']);

  const edited = { modelType: 'first_seen', partitionKey: '', valueKey: 'v', keyFields: ['a', 'b', 'w'] };
  go(go(edited, 'rarity'), 'first_seen');
  expect(edited.keyFields).toEqual(['a', 'b', 'w']);
  expect(edited.valueKey).toBe('v');

  const filled = { modelType: 'rarity', partitionKey: 'g', valueKey: 'v', keyFields: [''] };
  go(go(go(filled, 'volume_baseline'), 'first_seen'), 'rarity');
  expect([filled.partitionKey, filled.valueKey]).toEqual(['g', 'v']);

  const groupOnly = { modelType: 'rarity', partitionKey: 'g', valueKey: '', keyFields: [''] };
  expect(go(groupOnly, 'first_seen').keyFields).toEqual(['g', '']);

  const fs = { modelType: 'first_seen', partitionKey: '', valueKey: '', keyFields: ['a', 'b', 'w'] };
  go(fs, 'rarity');
  expect([fs.partitionKey, fs.valueKey]).toEqual(['a', 'w']);
});

test('tlsh keeps the first seen value, not a group', () => {
  const M = load();
  const e = { modelType: 'tlsh', keyFields: ['host', 'digest'] };
  M._carryShape(e, 'first_seen');
  expect(e.keyFields).toEqual(['digest']);
  const v = { modelType: 'tlsh', keyFields: ['a', 'b'] };
  M._carryShape(v, 'volume_baseline');
  expect(v.keyFields).toEqual(['a']);
});

test('an ungrouped rarity row reads as its value, with value counts in the summary', () => {
  const M = load();
  const model = { model_type: 'rarity', definition: { value_key: 'rmm' } };
  expect(M._rowTitle({ partition_val: '', value_val: 'Atera' }, model)).toBe('Atera');
  expect(M._rowTitle({ partition_val: 'bob', value_val: 'Atera' }, { model_type: 'rarity', definition: {} })).toBe('bob / Atera');
});

const rarity = { model_type: 'rarity', definition: { partition_key: 'host', alert: { coverage_threshold: 0.9 } } };
const firstSeen = { model_type: 'first_seen', definition: { alert: { alert_on_new: true } } };
const ready = {
  state: 'ready', history_days: 30, needed_days: 28, new_per_day: 1.3,
  self_check: { half_days: 15, forecast: 20, actual: 17 },
  groups: { total: 1000, ready: 700, ready_days: 14, above_coverage: 800 },
};
const ungrouped = { ...ready, groups: undefined };
// Numbers localise, so expectations follow the runner's locale.
const thousand = (1000).toLocaleString(), rate13 = (1.3).toLocaleString(), gate = (0.9).toLocaleString();

test('a ready model labels the rate an estimate and shows its self-check', () => {
  const e = load()._estimateSummary(ready, rarity);
  expect(e.badge).toBe('');
  expect(e.text).toBe(`Estimates: about ${rate13} new alerts a day. Last 15 days: forecast 20, actual 17. 700 of ${thousand} groups have enough history. Coverage above ${gate} in 80% of groups.`);
  expect(e.title).toContain('self-check');
});

test('a young model shows its learning state and no rate, even if the server sent one', () => {
  const e = load()._estimateSummary({ ...ready, state: 'learning', history_days: 9, self_check: undefined }, rarity);
  expect(e.badge).toBe('Learning 9/28d');
  expect(e.text).toBe(`No rate until the model has 28 days of history. Estimates: 700 of ${thousand} groups have enough history. Coverage above ${gate} in 80% of groups.`);
  expect(e.text).not.toContain('a day');
  expect(e.title).not.toContain('The rate is');
});

test('a running backfill holds the rate back', () => {
  const e = load()._estimateSummary({ ...ungrouped, state: 'backfilling' }, rarity);
  expect(e.badge).toBe('');
  expect(e.text).toBe('No rate while the backfill runs.');
});

test('a backfilled model says its rate is not all alerts that fired', () => {
  const e = load()._estimateSummary({ ...ungrouped, backfilled: true }, rarity);
  expect(e.text).toContain(`about ${rate13} new alerts a day, including backfilled history.`);
  expect(e.title).toContain('Backfilled history never alerts');
});

test('a window too short for a self-check says so, and does not explain one', () => {
  const e = load()._estimateSummary({ ...ungrouped, self_check: undefined }, rarity);
  expect(e.text).toBe(`Estimates: about ${rate13} new alerts a day. The history is too short for a self-check.`);
  expect(e.title).not.toContain('self-check');
});

test('a model that cannot alert reports new values, not alerts', () => {
  const M = load();
  const noThresholds = { model_type: 'rarity', definition: { partition_key: 'host', alert: {} } };
  const quiet = { model_type: 'first_seen', definition: { alert: { alert_on_new: false } } };
  expect(M._estimateSummary(ungrouped, noThresholds).text).toContain('new values a day');
  expect(M._estimateSummary(ungrouped, quiet).text).toContain('new values a day');
  expect(M._estimateSummary(ungrouped, firstSeen).text).toContain('new alerts a day');
});

test('the rate reads plainly at both ends, and its noun follows what is shown', () => {
  const M = load();
  const line = r => M._estimateSummary({ ...ungrouped, new_per_day: r, self_check: undefined }, rarity).text.split(/ a day/)[0];
  expect(M._estimateRate(0)).toBe('about 0');
  expect(M._estimateRate(0.003)).toBe('under 0.1');
  expect(M._estimateRate(0.1)).toBe(`about ${(0.1).toLocaleString()}`);
  expect(M._estimateRate(9.96)).toBe('about 10');
  expect(M._estimateRate(1234.56)).toBe(`about ${(1235).toLocaleString()}`);
  for (const r of [0.96, 1, 1.04]) expect(line(r)).toBe('Estimates: about 1 new alert');
  expect(line(2)).toBe('Estimates: about 2 new alerts');
});

test('group counts read properly at zero, one, and just short of all', () => {
  const M = load();
  const text = groups => M._estimateSummary({ ...ungrouped, groups }, rarity).text;
  expect(text({ total: 0, ready: 0, ready_days: 14, above_coverage: 0 })).not.toContain('group');
  expect(text({ total: 1, ready: 1, ready_days: 14 })).toContain('1 of 1 group has enough history.');
  expect(text({ total: 10, ready: 5, ready_days: 14 })).not.toContain('Coverage');
  expect(text({ total: 1000, ready: 5, ready_days: 14, above_coverage: 999 })).toContain('in 99% of groups');
  expect(M._estimateSummary({ ...ready }, { model_type: 'rarity' }).text).not.toContain('undefined');
});

test('no readiness means no estimate line', () => {
  const M = load();
  expect(M._estimateSummary(undefined, rarity)).toBeNull();
  expect(M._estimateHTML(undefined, rarity)).toBe('');
});

test('the estimate line escapes what it renders', () => {
  const M = load();
  M._estimateSummary = () => ({ badge: '<b>', text: '<i>', title: '"><img>' });
  const html = M._estimateHTML(ready, rarity);
  for (const raw of ['<b>', '<i>', '"><img>']) expect(html).not.toContain(raw);
});

test('the Estimate half field shows the default, a set value, and escapes both', () => {
  const M = load();
  expect(M._estimateHalfFieldHTML({})).toContain('value="14"');
  expect(M._estimateHalfFieldHTML({ estimate_half_days: 7 })).toContain('value="7"');
  expect(M._estimateHalfFieldHTML({ estimate_half_days: '"><img>' })).not.toContain('"><img>');
  expect(M._estimateHalfLabel({})).toBe('14 days');
  expect(M._estimateHalfLabel({ alert: { estimate_half_days: 7 } })).toBe('7 days');
});

test('a bad Estimate half stops a save with its own label, only where the field shows', () => {
  const M = load();
  const check = (modelType, half) => {
    M.editor = { modelType, valueKey: 'v', keyFields: ['k'], network: { src_field: 's', dst_field: 'd' }, alertConfig: { estimate_half_days: half } };
    return M._validateShape();
  };
  for (const bad of [1, 2.5, 183, -3]) expect(check('rarity', bad)).toContain('Estimate half (days)');
  expect(check('first_seen', 1)).toContain('Estimate half (days)');
  for (const good of [undefined, 0, 2, 14, 182]) expect(check('rarity', good)).toBeNull();
  expect(check('beacon', 1)).toBeNull();
  expect(check('volume_baseline', 1)).toBeNull();
});

test('Estimate half is sent only for the types that use it', () => {
  const M = load();
  const sent = modelType => {
    M.editor = { modelType, keyFields: ['k'], network: {}, modifiers: {}, alertConfig: { estimate_half_days: 7 } };
    return M._composeDefinition({}).alert.estimate_half_days;
  };
  expect(sent('rarity')).toBe(7);
  expect(sent('first_seen')).toBe(7);
  for (const t of ['volume_baseline', 'tlsh', 'beacon', 'long_connection']) expect(sent(t)).toBeUndefined();
});

test('Group days is a sortable column on a grouped rarity model only', () => {
  const M = load();
  const col = M._viewColumns(rarity).find(c => c.col === 'model_total');
  expect(col).toMatchObject({ label: 'Group days', sortable: true });
  expect(M._viewColumns({ model_type: 'rarity', definition: {} }).some(c => c.col === 'model_total')).toBe(false);
});
