// Unit tests for web/static/timezone.js. Pure logic, no page and no running
// stack: the module is evaluated in Node, which has the same Intl time zone
// database the browser does.
//
// These earn their place because the failure mode is silent. A wrong offset
// renders a plausible timestamp, and the cases that break naive implementations
// (a ClickHouse string parsed as browser-local, a wall clock entered during a
// DST transition, a zone whose offset is not a whole hour) all look correct
// until someone correlates against another tool during an incident.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function loadTZ() {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static/timezone.js'), 'utf8');
  const sandbox = {
    window: {},
    document: { dispatchEvent() {} },
    localStorage: { _d: {}, getItem(k) { return this._d[k] || null; }, setItem(k, v) { this._d[k] = v; } },
    Intl, Date, Number, Math, String, JSON, fetch: async () => ({ ok: true, json: async () => ({ success: true }) }),
  };
  new Function('window', 'document', 'localStorage', 'fetch', src)(
    sandbox.window, sandbox.document, sandbox.localStorage, sandbox.fetch);
  return sandbox.window.TZ;
}

function withZone(TZ, zone, fn) {
  TZ._zone = zone;
  TZ._fmtCache.clear();
  fn();
}

test('naive ClickHouse timestamps are read as UTC, not browser-local', () => {
  const TZ = loadTZ();
  expect(TZ.toEpoch('2026-08-25 14:23:11')).toBe(Date.UTC(2026, 7, 25, 14, 23, 11));
  expect(TZ.toEpoch('2026-08-25T14:23:11Z')).toBe(Date.UTC(2026, 7, 25, 14, 23, 11));
  expect(TZ.toEpoch('2026-08-25T10:23:11-04:00')).toBe(Date.UTC(2026, 7, 25, 14, 23, 11));
  expect(TZ.toEpoch('2026-08-25')).toBe(Date.UTC(2026, 7, 25));
  expect(Number.isNaN(TZ.toEpoch(''))).toBe(true);
  expect(Number.isNaN(TZ.toEpoch(null))).toBe(true);
});

test('UTC rendering is byte-identical to what the backend sent', () => {
  const TZ = loadTZ();
  withZone(TZ, 'UTC', () => {
    expect(TZ.format('2026-08-25 14:23:11')).toBe('2026-08-25 14:23:11');
    expect(TZ.format('2026-08-25 14:23:11', 'minute')).toBe('2026-08-25 14:23');
    expect(TZ.format('2026-08-25 14:23:11', 'full')).toBe('2026-08-25 14:23:11 UTC');
    expect(TZ.format('2026-08-25 14:23', 'datetime')).toBe('2026-08-25 14:23:00');
    expect(TZ.offsetLabel(Date.UTC(2026, 7, 25))).toBe('UTC+00:00');
  });
});

test('offsets follow DST and cross day boundaries', () => {
  const TZ = loadTZ();
  withZone(TZ, 'America/Denver', () => {
    expect(TZ.format('2026-08-25 14:23:11')).toBe('2026-08-25 08:23:11');
    expect(TZ.format('2026-08-25 14:23:11', 'full')).toBe('2026-08-25 08:23:11 MDT');
    expect(TZ.offsetLabel(Date.UTC(2026, 7, 25))).toBe('UTC-06:00');
    expect(TZ.offsetLabel(Date.UTC(2026, 0, 15))).toBe('UTC-07:00');
    expect(TZ.format('2026-08-25 03:00:00')).toBe('2026-08-24 21:00:00');
    // Midnight must be 00, not the 24 some engines produce under hour12:false.
    expect(TZ.format('2026-08-25 06:00:00', 'time')).toBe('00:00:00');
  });
});

test('sub-hour zone offsets render exactly', () => {
  const TZ = loadTZ();
  withZone(TZ, 'Asia/Kolkata', () => {
    expect(TZ.format('2026-08-25 14:23:11')).toBe('2026-08-25 19:53:11');
    expect(TZ.offsetLabel(Date.UTC(2026, 7, 25))).toBe('UTC+05:30');
  });
  withZone(TZ, 'Asia/Kathmandu', () => {
    expect(TZ.offsetLabel(Date.UTC(2026, 7, 25))).toBe('UTC+05:45');
  });
  withZone(TZ, 'Pacific/Chatham', () => {
    expect(TZ.offsetLabel(Date.UTC(2026, 6, 1))).toBe('UTC+12:45');
  });
});

test('a typed wall clock resolves to the right instant on both sides of DST', () => {
  const TZ = loadTZ();
  withZone(TZ, 'America/Denver', () => {
    expect(TZ.parseWallClock('2026-08-25 08:23:11')).toBe(Date.UTC(2026, 7, 25, 14, 23, 11));
    expect(TZ.parseWallClock('2026-01-15 08:23:11')).toBe(Date.UTC(2026, 0, 15, 15, 23, 11));
    expect(TZ.parseWallClock('2026-08-25 08:23')).toBe(Date.UTC(2026, 7, 25, 14, 23));
    // 02:30 does not exist on the spring-forward date. It must still resolve to
    // a real instant rather than NaN, so the time picker cannot be wedged.
    expect(Number.isFinite(TZ.parseWallClock('2026-03-08 02:30:00'))).toBe(true);
  });
});

// Rendering and parsing must be exact inverses, including through the hours
// either side of a transition. Fall-back hours are genuinely ambiguous (one
// wall clock, two instants), so the contract asserted is that re-rendering
// whatever we parsed reproduces what was displayed.
for (const [zone, startUTC, label] of [
  ['America/Denver', Date.UTC(2026, 2, 7), 'spring forward'],
  ['America/Denver', Date.UTC(2026, 9, 31), 'fall back'],
  ['Europe/Dublin', Date.UTC(2026, 9, 24), 'negative-DST zone'],
  ['Australia/Lord_Howe', Date.UTC(2026, 3, 4), '30-minute DST shift'],
  ['Asia/Kolkata', Date.UTC(2026, 5, 1), 'no DST'],
]) {
  test(`format/parse round-trips across ${zone} (${label})`, () => {
    const TZ = loadTZ();
    withZone(TZ, zone, () => {
      for (let i = 0; i < 72; i++) {
        const shown = TZ.format(startUTC + i * 3600000, 'datetime');
        expect(TZ.format(TZ.parseWallClock(shown), 'datetime'), `hour ${i}`).toBe(shown);
      }
    });
  });
}

test('hover text states the zone and the UTC value without leaking zone state', () => {
  const TZ = loadTZ();
  withZone(TZ, 'America/Denver', () => {
    expect(TZ.title('2026-08-25 14:23:11'))
      .toBe('2026-08-25 08:23:11 MDT (UTC-06:00)\n2026-08-25 14:23:11 UTC');
    expect(TZ.zone()).toBe('America/Denver');
  });
  withZone(TZ, 'UTC', () => {
    expect(TZ.title('2026-08-25 14:23:11')).toBe('2026-08-25 14:23:11 UTC (UTC+00:00)');
  });
});

test('unknown zones are rejected and unparseable values pass through', () => {
  const TZ = loadTZ();
  expect(TZ._valid('America/Denver')).toBe(true);
  expect(TZ._valid('Mars/Olympus')).toBe(false);
  expect(TZ._valid('')).toBe(false);
  withZone(TZ, 'America/Denver', () => {
    expect(TZ.format('not a time')).toBe('not a time');
    expect(TZ.format('')).toBe('');
  });
});
