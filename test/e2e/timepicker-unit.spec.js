// Unit tests for the range math in web/static/timePicker.js: calendar ranges
// and the Start/End inputs. Pure logic evaluated in Node, no page or stack.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load(zone) {
  const read = (f) => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8');
  const window = {};
  const document = { dispatchEvent() {}, getElementById() { return null; }, querySelectorAll() { return []; } };
  const localStorage = { getItem() { return null; }, setItem() {} };
  new Function('window', 'document', 'localStorage', read('timezone.js'))(window, document, localStorage);
  window.TZ._zone = zone;
  window.TZ._fmtCache.clear();
  new Function('window', 'document', 'localStorage', 'TZ', read('timePicker.js'))(window, document, localStorage, window.TZ);
  return window.TimePicker;
}

// Thursday 2026-10-29 15:30 UTC.
const NOW = Date.UTC(2026, 9, 29, 15, 30);
const iso = (y, mo, d, h = 0, mi = 0) => new Date(Date.UTC(y, mo - 1, d, h, mi)).toISOString();

test('calendar ranges start on local boundaries, weeks on Monday', () => {
  const TP = load('UTC');
  const now = new Date(NOW).toISOString();
  expect(TP.calendarRange('today', NOW)).toEqual({ start: iso(2026, 10, 29), end: now });
  expect(TP.calendarRange('yesterday', NOW)).toEqual({ start: iso(2026, 10, 28), end: iso(2026, 10, 29) });
  expect(TP.calendarRange('this_week', NOW)).toEqual({ start: iso(2026, 10, 26), end: now });
  expect(TP.calendarRange('last_week', NOW)).toEqual({ start: iso(2026, 10, 19), end: iso(2026, 10, 26) });
  expect(TP.calendarRange('this_month', NOW)).toEqual({ start: iso(2026, 10, 1), end: now });
  expect(TP.calendarRange('last_month', NOW)).toEqual({ start: iso(2026, 9, 1), end: iso(2026, 10, 1) });
});

test('calendar ranges follow the display zone, not UTC', () => {
  const TP = load('America/Denver');
  // 2026-10-29 02:00 UTC is still the 28th in Denver (UTC-6).
  const early = Date.UTC(2026, 9, 29, 2, 0);
  expect(TP.calendarRange('today', early).start).toBe(iso(2026, 10, 28, 6));
  expect(TP.calendarRange('yesterday', early)).toEqual({ start: iso(2026, 10, 27, 6), end: iso(2026, 10, 28, 6) });
  // Last month crosses the November DST change: midnight Nov 1 is UTC-6, Dec 1 is UTC-7.
  const dec = Date.UTC(2026, 11, 10, 12, 0);
  expect(TP.calendarRange('last_month', dec)).toEqual({ start: iso(2026, 11, 1, 6), end: iso(2026, 12, 1, 7) });
});

test('a week that starts on Sunday still begins the Monday before', () => {
  const TP = load('UTC');
  const sunday = Date.UTC(2026, 10, 1, 9, 0);
  expect(TP.calendarRange('this_week', sunday).start).toBe(iso(2026, 10, 26));
});

test('Start/End inputs: relative start ending now stays relative', () => {
  const TP = load('UTC');
  expect(TP.parseInputs('3d', 'now', NOW)).toEqual({ type: 'relative', relativeN: 3, relativeUnit: 'days' });
  expect(TP.parseInputs('90m', '', NOW)).toEqual({ type: 'relative', relativeN: 90, relativeUnit: 'minutes' });
  expect(TP.parseInputs(' 3W ', 'Now', NOW)).toEqual({ type: 'relative', relativeN: 3, relativeUnit: 'weeks' });
});

test('Start/End inputs: an end before now becomes a fixed range', () => {
  const TP = load('UTC');
  expect(TP.parseInputs('7d', '1d', NOW)).toEqual({
    type: 'custom', customStart: iso(2026, 10, 22, 15, 30), customEnd: iso(2026, 10, 28, 15, 30),
  });
  expect(TP.parseInputs('2026-10-20 08:00', '4h', NOW)).toEqual({
    type: 'custom', customStart: iso(2026, 10, 20, 8), customEnd: iso(2026, 10, 29, 11, 30),
  });
  expect(TP.parseInputs('2026-10-20 08:00', 'now', NOW)).toEqual({
    type: 'custom', customStart: iso(2026, 10, 20, 8), customEnd: iso(2026, 10, 29, 15, 30),
  });
});

test('Start/End inputs: rejects garbage and inverted ranges', () => {
  const TP = load('UTC');
  expect(TP.parseInputs('', 'now', NOW)).toBeNull();
  expect(TP.parseInputs('soon', 'now', NOW)).toBeNull();
  expect(TP.parseInputs('0h', 'now', NOW)).toBeNull();
  expect(TP.parseInputs('1d', '7d', NOW)).toBeNull();
  expect(TP.parseInputs('now', 'now', NOW)).toBeNull();
});

test('Start/End inputs show the applied range', () => {
  const TP = load('UTC');
  expect(TP.boundsText({ type: '24h' }, NOW)).toEqual({ start: '24h', end: 'now' });
  expect(TP.boundsText({ type: 'relative', relativeN: 3, relativeUnit: 'days' }, NOW)).toEqual({ start: '3d', end: 'now' });
  expect(TP.boundsText({ type: 'yesterday' }, NOW)).toEqual({ start: '2026-10-28 00:00', end: '2026-10-29 00:00' });
  expect(TP.boundsText({ type: 'custom', customStart: iso(2026, 10, 1, 8), customEnd: iso(2026, 10, 2) }, NOW))
    .toEqual({ start: '2026-10-01 08:00', end: '2026-10-02 00:00' });
  expect(TP.boundsText({ type: 'all' }, NOW)).toEqual({ start: '', end: 'now' });
});

test('labels and whether a range ends now', () => {
  const TP = load('UTC');
  TP.state.type = 'last_week';
  expect(TP.getLabel()).toBe('Last week');
  expect(TP.endsNow()).toBe(false);
  TP.state.type = 'this_month';
  expect(TP.getLabel()).toBe('This month');
  expect(TP.endsNow()).toBe(true);
  TP.state.type = 'custom';
  expect(TP.endsNow()).toBe(false);
  TP.state.type = '7d';
  expect(TP.endsNow()).toBe(true);
});

test('14d is a preset, and 4h still resolves for saved queries that use it', () => {
  const TP = load('UTC');
  TP.state.type = '14d';
  expect(TP.getLabel()).toBe('Last 14d');
  expect(TP.boundsText({ type: '14d' }, NOW)).toEqual({ start: '14d', end: 'now' });
  const r = TP.getTimeRange();
  expect(Date.parse(r.end) - Date.parse(r.start)).toBe(14 * 86400000);
  TP.state.type = '4h';
  expect(TP.getLabel()).toBe('Last 4h');
});

test('a midnight lost to a DST jump starts the day at the first real instant', () => {
  // Beirut skips 00:00 to 01:00 on 2026-03-29 (UTC+2 to UTC+3).
  const TP = load('Asia/Beirut');
  expect(TP.calendarRange('today', Date.UTC(2026, 2, 29, 15)).start).toBe(iso(2026, 3, 28, 22));
  expect(TP.parseInputs('2026-03-29', 'now', Date.UTC(2026, 2, 29, 15)).customStart).toBe(iso(2026, 3, 28, 22));
  // Santiago skips 00:00 to 01:00 on 2026-09-06 (UTC-4 to UTC-3).
  expect(load('America/Santiago').calendarRange('today', Date.UTC(2026, 8, 6, 15)).start).toBe(iso(2026, 9, 6, 4));
});

test('impossible dates and out of range offsets are rejected, not rolled over', () => {
  const TP = load('UTC');
  expect(TP.parseInputs('2026-02-30', 'now', NOW)).toBeNull();
  expect(TP.parseInputs('2026-10-20 25:99', 'now', NOW)).toBeNull();
  expect(TP.parseInputs('99999999999d', '', NOW)).toBeNull();
  expect(() => TP.parseInputs('9999999999w', '2026-10-29', NOW)).not.toThrow();
  expect(TP.parseInputs('9999999999w', '2026-10-29', NOW)).toBeNull();
});

test('a huge relative range from a link or storage clamps instead of throwing', () => {
  const TP = load('UTC');
  Object.assign(TP.state, { type: 'relative', relativeN: 99999999999, relativeUnit: 'days' });
  expect(TP.getTimeRange().start).toBe('2000-01-01T00:00:00.000Z');
});

test('an offset matching a preset applies as that preset', () => {
  const TP = load('UTC');
  expect(TP.parseInputs('24h', 'now', NOW)).toEqual({ type: '24h' });
  expect(TP.parseInputs('4h', 'now', NOW)).toEqual({ type: '4h' });
  expect(TP.parseInputs('14d', '', NOW)).toEqual({ type: '14d' });
  expect(TP.parseInputs('2w', '', NOW)).toEqual({ type: '14d' });
});

test('applying the inputs unedited keeps the current range', () => {
  const TP = load('UTC');
  for (const type of ['today', 'last_week', 'all', 'live']) {
    TP.state.type = type;
    const shown = TP.boundsText(TP.state, NOW);
    expect(TP.parseInputs(shown.start, shown.end, NOW)).toEqual({ type });
  }
});

test('unknown range types from a link or storage are ignored', () => {
  const TP = load('UTC');
  TP.setState({ type: '24h' }, true);
  for (const type of ['constructor', '__proto__', 'toString', 'bogus']) {
    TP.applyState({ type }, true);
    expect(TP.state.type).toBe('24h');
  }
});

test('custom ranges keep their seconds in the inputs', () => {
  const TP = load('UTC');
  const state = { type: 'custom', customStart: '2026-10-01T08:00:30.000Z', customEnd: '2026-10-01T09:15:00.000Z' };
  expect(TP.boundsText(state, NOW)).toEqual({ start: '2026-10-01 08:00:30', end: '2026-10-01 09:15:00' });
});
