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
