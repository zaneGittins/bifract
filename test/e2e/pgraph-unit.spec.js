// Unit tests for web/static/pgraphView.js, the pure logic behind the pgraph view:
// what the anomalous-path view keeps, how reconnected peers group, the fact chips
// that explain a score, Table time labels, and search pivots. Evaluated in Node,
// no page and no running stack.
//
// These matter because every failure here renders plausibly: a path view that
// drops the seed, a peer filed under the wrong bridge, or a chip that pivots on
// an unescaped path all look like a working UI.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load(file, name) {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static', file), 'utf8');
  const window = {};
  new Function('window', 'document', 'localStorage', src)(window, { dispatchEvent() {} },
    { getItem() { return null; }, setItem() {} });
  return window[name];
}
const PgView = load('pgraphView.js', 'PgView');
const TZ = load('timezone.js', 'TZ');

// A small model in the _pgBuildModel shape: root -> a -> b, root -> c -> d, plus a leaf on d.
function model() {
  const procSet = new Set(['root', 'a', 'b', 'c', 'd', 'peer']);
  const parentOf = new Map([['a', 'root'], ['b', 'a'], ['c', 'root'], ['d', 'c']]);
  return {
    procSet,
    parentOf,
    anomalyByNode: new Map([['a', 0.2], ['b', 0.3], ['c', 0.1], ['d', 0.2], ['net:1.2.3.4', 0.99]]),
    leafGroups: new Map([['d', { file: [], net: [{ id: 'net:1.2.3.4', anomaly: 0.99 }], dns: [] }]]),
    interactions: new Map(),
  };
}

test.describe('anomalous path', () => {
  const hot = (a) => a >= 0.9;

  test('keeps hot activity, its ancestors and the seed, nothing else', () => {
    const m = model();
    const anchors = PgView.pathAnchors(m, hot, ['b']);
    expect([...anchors].sort()).toEqual(['b', 'd']);
    const keep = PgView.pathKeep(anchors, m.parentOf);
    expect([...keep].sort()).toEqual(['a', 'b', 'c', 'd', 'root']);
    // The leaf id itself is not a process and never enters the set.
    expect(keep.has('net:1.2.3.4')).toBe(false);
  });

  test('an interaction marks both ends; non-processes in extras are ignored', () => {
    const m = model();
    m.interactions.set('a', [{ target: 'peer', anomaly: 0.95 }]);
    const anchors = PgView.pathAnchors(m, hot, ['nope', null]);
    expect(anchors.has('a') && anchors.has('peer')).toBe(true);
    expect(anchors.has('nope')).toBe(false);
  });

  test('ancestor walk stops on a parent cycle', () => {
    const keep = PgView.pathKeep(new Set(['x']), new Map([['x', 'y'], ['y', 'x']]));
    expect([...keep].sort()).toEqual(['x', 'y']);
  });

  test('off-path siblings fold into one count, aggregates stay while any member does', () => {
    const shown = (id) => id === 'k1' || id === 'm2';
    const size = (id) => ({ h1: 4, h2: 1, m1: 1, m3: 2 })[id] || 1;
    const r = PgView.splitChildren([
      { kind: 'proc', id: 'k1' }, { kind: 'proc', id: 'h1' }, { kind: 'proc', id: 'h2' },
      { kind: 'agg', id: 'g1', members: ['m1', 'm2'] }, { kind: 'agg', id: 'g2', members: ['m3'] },
    ], shown, size);
    expect(r.vis.map(e => e.id)).toEqual(['k1', 'g1']);
    expect(r.hidden).toBe(4 + 1 + 2);
    expect(r.branches).toBe(3);
  });
});

test.describe('reconnected peers', () => {
  // home tree H; peers P1..P4. P1 and P2 hang off H, P3 only off P2, P4 is unlinked.
  const rootOf = (g) => g.split('.')[0];
  const score = (r) => ({ P1: 0.5, P2: 0.9, P3: 1, P4: NaN })[r];

  test('each peer joins the strongest bridge reaching it from nearer home', () => {
    const groups = PgView.peerGroups([
      { key: 'net:weak', type: 'net', label: 'weak', anomaly: 0.4, owners: ['H.a', 'P1.x', 'P2.x'] },
      { key: 'dns:strong', type: 'dns', label: 'strong', anomaly: 0.95, owners: ['H.b', 'P2.y'] },
      { key: 'file:chain', type: 'file', label: 'chain', anomaly: 0.99, owners: ['P2.z', 'P3.q'] },
      { key: 'net:same-tree', type: 'net', label: 'x', anomaly: 1, owners: ['H.a', 'H.b'] },
    ], rootOf, 'H', ['P1', 'P2', 'P3', 'P4'], score);
    const byKey = Object.fromEntries(groups.map(g => [g.key, g]));
    expect(byKey['dns:strong'].peers.map(p => p.root)).toEqual(['P2']);
    expect(byKey['net:weak'].peers.map(p => p.root)).toEqual(['P1']);
    // P3 is reachable only through P2, so it reads outward from there.
    expect(byKey['file:chain'].peers.map(p => p.root)).toEqual(['P3']);
    expect(byKey['file:chain'].peers[0].via).toBe('P3.q');
    expect(byKey['file:chain'].from).toEqual(['P2.z']);
    // A bridge inside one tree links nothing.
    expect(byKey['net:same-tree']).toBeUndefined();
    // Unlinked peers are not dropped.
    expect(groups[groups.length - 1].key).toBe('#other');
    expect(groups[groups.length - 1].peers.map(p => p.root)).toEqual(['P4']);
    // Strongest bridge first.
    expect(groups.slice(0, 3).map(g => g.key)).toEqual(['file:chain', 'dns:strong', 'net:weak']);
  });

  test('peers in a group sort by score, unscored last', () => {
    const groups = PgView.peerGroups([
      { key: 'k', type: 'net', label: 'k', anomaly: 1, owners: ['H.a', 'P1.a', 'P2.a', 'P4.a'] },
    ], rootOf, 'H', ['P1', 'P2', 'P4'], score);
    expect(groups[0].peers.map(p => p.root)).toEqual(['P2', 'P1', 'P4']);
  });

  test('host labels shorten to the machine name', () => {
    expect(PgView.shortHost('WKSTN-00235.corp.example.com')).toBe('WKSTN-00235');
    expect(PgView.shortHost('10.1.2.3')).toBe('10.1.2.3');
  });
});

test.describe('table time column', () => {
  test('date only when the day changes, seconds always', () => {
    TZ._zone = 'UTC'; TZ._fmtCache.clear();
    const parts = (v) => TZ.parts(v);
    const t1 = Date.UTC(2026, 8, 29, 11, 41, 7);
    expect(PgView.timeLabel(t1, null, parts, 2026)).toEqual({ date: 'Sep 29', time: '11:41:07' });
    expect(PgView.timeLabel(t1 + 6900, t1, parts, 2026)).toEqual({ date: '', time: '11:41:13' });
    expect(PgView.timeLabel(t1 + 86400000, t1, parts, 2026).date).toBe('Sep 30');
    expect(PgView.timeLabel(t1, null, parts, 2027).date).toBe('Sep 29, 2026');
    expect(PgView.timeLabel(null, null, parts, 2026)).toBeNull();
  });

  test('day boundaries follow the display zone', () => {
    TZ._zone = 'America/Denver'; TZ._fmtCache.clear();
    const parts = (v) => TZ.parts(v);
    // 03:00 UTC on Sep 30 is still Sep 29 in Denver: same day as 20:00 local.
    const a = Date.UTC(2026, 8, 30, 2, 0, 0), b = Date.UTC(2026, 8, 30, 3, 0, 0);
    expect(PgView.timeLabel(b, a, parts, 2026).date).toBe('');
    TZ._zone = 'UTC'; TZ._fmtCache.clear();
  });

  test('parent gap reads at a sensible unit', () => {
    expect(PgView.fmtDelta(6900)).toBe('+6.9s');
    expect(PgView.fmtDelta(-10 * 86400000)).toBe('-10d');
    expect(PgView.fmtDelta(260)).toBe('+260ms');
    expect(PgView.fmtDelta(NaN)).toBe('');
  });

  test('first-seen chip only when it says something', () => {
    const now = Date.UTC(2026, 9, 7), day = 86400000;
    const ctx = { windowStartMs: now - 30 * day, baselineStartMs: now - 69 * day, now };
    expect(PgView.firstSeenKind(now - 28 * day, ctx)).toBe('new');
    expect(PgView.firstSeenKind(now - 69 * day, ctx)).toBe('');      // as old as the baseline
    expect(PgView.firstSeenKind(now - 50 * day, ctx)).toBe('');
    expect(PgView.firstSeenKind(now - 40 * day, Object.assign({}, ctx, { windowStartMs: null }))).toBe('');
    expect(PgView.firstSeenKind(now - 10 * day, Object.assign({}, ctx, { windowStartMs: null }))).toBe('recent');
    expect(PgView.firstSeenKind(NaN, ctx)).toBe('');
  });
});

test.describe('score fact chips', () => {
  const now = Date.UTC(2026, 9, 7);
  const base = { basis: 'transition', edge: 0, src: 453, exec: 0, tgt: 0, tgtHosts: 1, totalHosts: 302, own: 0.98, final: 1, firstSeen: '2026-10-05' };

  test('novel, rare and inherited facts carry their tone', () => {
    const chips = PgView.whyChips(base, { now, windowStartMs: now - 7 * 86400000, targetQuery: 'dst_ip="1.2.3.4"' });
    const by = Object.fromEntries(chips.map(c => [c.label, c]));
    expect(by['First seen']).toMatchObject({ value: '2d ago', tone: 'alert' });
    expect(by['Target on']).toMatchObject({ value: '1 of 302 hosts', tone: 'alert', query: 'dst_ip="1.2.3.4"' });
    expect(by['Other host-days']).toMatchObject({ value: '0 of 453', tone: 'alert' });
    expect(by['Own'].value).toBe('0.98');
    expect(by['Inherited']).toMatchObject({ value: '+0.02', tone: 'warn' });
  });

  test('established and common facts stay quiet; capped host counts say so', () => {
    const chips = PgView.whyChips(Object.assign({}, base, { firstSeen: '2026-07-30', tgtHosts: 256, edge: 200, own: 1, final: 1 }),
      { now, windowStartMs: now - 7 * 86400000 });
    const by = Object.fromEntries(chips.map(c => [c.label, c]));
    expect(by['First seen'].tone).toBe('muted');
    expect(by['Target on']).toMatchObject({ value: '256+ of 302 hosts', tone: '', query: '' });
    expect(by['Other host-days'].tone).toBe('');
    expect(by['Inherited']).toBeUndefined();
  });

  test('no explanation, no chips', () => {
    expect(PgView.whyChips(null)).toEqual([]);
  });
});

test.describe('search pivots', () => {
  test('values are quoted for BQL, backslashes and quotes escaped', () => {
    expect(PgView.targetQuery('file', 'C:\\Users\\a "b"\\x.exe')).toBe('target_file="C:\\\\Users\\\\a \\"b\\"\\\\x.exe"');
    expect(PgView.targetQuery('net', '1.2.3.4')).toBe('dst_ip="1.2.3.4"');
    expect(PgView.targetQuery('dns', 'evil.example')).toBe('query="evil.example"');
    expect(PgView.targetQuery('inject', 'x')).toBe('');
    expect(PgView.imageQuery('C:\\w\\c.exe')).toBe('bifract_category="process_creation" image="C:\\\\w\\\\c.exe" | groupby(computer_name) | sort(_count, order=desc)');
  });

  test('hash pivots pick the strongest digest from a Sysmon list', () => {
    expect(PgView.hashQuery('hash', 'SHA256=ABC123,MD5=FF00,IMPHASH=0011')).toBe('hash=~ABC123');
    expect(PgView.hashQuery('hash', 'MD5=ff00')).toBe('hash=~ff00');
    expect(PgView.hashQuery('sha256', 'deadbeef')).toBe('sha256="deadbeef"');
    expect(PgView.hashQuery('', 'x')).toBe('');
  });

  test('pivots use the public /go/search contract', () => {
    const u = new URL(PgView.searchUrl('https://b.example', 'process_guid="{x}"', { kind: 'fractal', name: 'prod' }, '2026-10-01T00:00:00.000Z', '2026-10-02T00:00:00.000Z'));
    expect(u.pathname).toBe('/go/search');
    expect(u.searchParams.get('q')).toBe('process_guid="{x}"');
    expect(u.searchParams.get('fractal')).toBe('prod');
    expect(u.searchParams.get('from')).toBe('2026-10-01T00:00:00.000Z');
    expect(new URL(PgView.searchUrl('https://b.example', 'x', { kind: 'prism', name: 'all' }, '-24h')).searchParams.get('prism')).toBe('all');
  });
});
