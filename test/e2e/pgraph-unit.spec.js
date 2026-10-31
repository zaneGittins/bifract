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

  // A collapsed node's members share its parent node and an image that the server compared
  // normalized (case, user folders, GUIDs). Matching the file name case-insensitively keeps
  // every member; a single parent pins them, a collapsed parent falls back to the host.
  test('members of a collapsed node', () => {
    expect(PgView.membersQuery('C:\\WINDOWS\\System32\\cmd.exe', 'WS02', 'p1', null))
      .toBe('bifract_category="process_creation" image=$"\\\\cmd.exe" computer_name="WS02" parent_process_guid="p1"');
    expect(PgView.membersQuery('/usr/bin/sh', 'WS02', 'p1', { n: 40 }))
      .toBe('bifract_category="process_creation" image=$"/sh" computer_name="WS02"');
    expect(PgView.membersQuery('cmd.exe', '', undefined, null)).toBe('bifract_category="process_creation" image=$"cmd.exe"');
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

test.describe('scope', () => {
  test('a focused view offers everything; a full view offers focus only when it would narrow', () => {
    expect(PgView.scopeAction({ pathActive: true, shown: 24, total: 46, focusN: 24 })).toBe('full');
    expect(PgView.scopeAction({ pathActive: false, pathable: true, shown: 46, total: 46, focusN: 24 })).toBe('path');
    // Focus would keep every process: offering it would do nothing.
    expect(PgView.scopeAction({ pathActive: false, pathable: true, shown: 46, total: 46, focusN: 46 })).toBe(null);
    // Unscored (ptg) has no anomalous path to focus on.
    expect(PgView.scopeAction({ pathActive: false, pathable: false, shown: 46, total: 46, focusN: 0 })).toBe(null);
    // Path is on but keeps everything: nothing to show more of, and focus is already on.
    expect(PgView.scopeAction({ pathActive: true, shown: 46, total: 46, focusN: 46 })).toBe(null);
  });
});

test.describe('viewport', () => {
  test('fit frames the whole graph on both axes, centered', () => {
    const v = PgView.fitView({ w: 1000, h: 4000 }, 1200, 600, { pad: 50, max: 1.2 });
    expect(v.s).toBeCloseTo(0.125);
    expect(v.y).toBeCloseTo((600 - 4000 * v.s) / 2);
    expect(v.x).toBeCloseTo((1200 - 1000 * v.s) / 2);
    expect(PgView.fitView({ w: 100, h: 100 }, 1200, 600, { pad: 50, max: 1.2 }).s).toBe(1.2);
  });

  test('framing centers a story that fits at a readable scale', () => {
    const box = { x: 400, y: 300, w: 600, h: 400 };
    const v = PgView.frameView({ x: 400, y: 300, w: 600, h: 800 }, 1200, 600, { pad: 40, min: 0.55, max: 1 });
    expect(v.s).toBeCloseTo(520 / 800);
    // Capped at max, then the box center lands on the viewport center.
    const c = PgView.frameView(box, 2000, 2000, { pad: 40, min: 0.55, max: 1 });
    expect(c.s).toBe(1);
    expect(c.x + (box.x + box.w / 2) * c.s).toBeCloseTo(1000);
    expect(c.y + (box.y + box.h / 2) * c.s).toBeCloseTo(1000);
  });

  test('a story too tall to read pins its top-left instead of shrinking past legibility', () => {
    const box = { x: 100, y: 200, w: 800, h: 3000 };
    const v = PgView.frameView(box, 1200, 600, { pad: 40, min: 0.55, max: 1 });
    expect(v.s).toBe(0.55);
    expect(v.x + box.x * v.s).toBeCloseTo(40);
    expect(v.y + box.y * v.s).toBeCloseTo(40);
  });
});

test.describe('quiet encoding', () => {
  test('only elevated and high edges carry a score pill', () => {
    expect(PgView.showsEdgePill('high')).toBe(true);
    expect(PgView.showsEdgePill('med')).toBe(true);
    expect(PgView.showsEdgePill('low')).toBe(false);
    expect(PgView.showsEdgePill('none')).toBe(false);
  });
});

test.describe('rare stepper', () => {
  test('steps through anomalous processes top to bottom and wraps', () => {
    const order = PgView.rareOrder(['c', 'a', 'b', 'gone'], new Map([['a', 10], ['b', 20], ['c', 30]]));
    expect(order).toEqual(['a', 'b', 'c']);
    expect(PgView.stepTo(order, null, 1)).toBe('a');
    expect(PgView.stepTo(order, null, -1)).toBe('c');
    expect(PgView.stepTo(order, 'a', 1)).toBe('b');
    expect(PgView.stepTo(order, 'c', 1)).toBe('a');
    expect(PgView.stepTo(order, 'a', -1)).toBe('c');
    expect(PgView.stepTo(order, 'elsewhere', 1)).toBe('a');
    expect(PgView.stepTo([], null, 1)).toBe(null);
  });
});

test.describe('lateral leads', () => {
  const typeOf = (id) => (/^(net|dns|file):/.exec(id) || [null, 'process'])[1];
  const home = new Set(['qc', 'w']);
  const row = (parent, child, extra) => Object.assign({ parent, child, recon_role: 'lead', anomaly_score: '0.94', host: '', parent_label: '', label: '', timestamp: '' }, extra);

  test('a lead is its seed-side process, the shared artifact and the hosts on the other end', () => {
    const leads = PgView.reconLeads([
      row('qc', 'net:203.0.113.87', { label: '203.0.113.87', anomaly_score: '0.95' }),
      row('p4', 'net:203.0.113.87', { host: 'WS04', parent_label: 'qcsvc.exe', label: '203.0.113.87', timestamp: '2026-10-31 10:50:00.000' }),
      row('p3', 'net:203.0.113.87', { host: 'WS03', parent_label: 'qcsvc.exe', label: '203.0.113.87' }),
      row('w', 'x9', { host: 'WS05', label: 'C:\\t\\a.exe', anomaly_score: '1' }), // dropped & ran
    ], g => home.has(g), typeOf);
    expect(leads.map(l => l.key)).toEqual(['file:C:\\t\\a.exe', 'net:203.0.113.87']); // strongest first
    const c2 = leads[1];
    expect(c2.src).toBe('qc');
    expect(c2.type).toBe('net');
    expect(c2.anomaly).toBe(0.95);
    expect(c2.peers.map(p => `${p.host} ${p.image}`)).toEqual(['WS03 qcsvc.exe', 'WS04 qcsvc.exe']);
    expect(leads[0].src).toBe('w');
    expect(leads[0].peers.map(p => p.guid)).toEqual(['x9']);
  });

  test('a bridge without its seed side or its peers is not drawn', () => {
    expect(PgView.reconLeads([row('p3', 'net:1.2.3.4', { host: 'WS03' })], g => home.has(g), typeOf)).toEqual([]);
    expect(PgView.reconLeads([row('qc', 'net:1.2.3.4')], g => home.has(g), typeOf)).toEqual([]);
  });

  test('lane: artifact on its process row, hosts beside it, capped with +N, blocks never overlap', () => {
    const peers = (n) => Array.from({ length: n }, (_, i) => ({ guid: 'p' + i, host: 'WS' + i }));
    const leads = [
      { key: 'a', src: 's1', label: 'x', peers: peers(7) },
      { key: 'b', src: 's2', label: 'y', peers: peers(2) },
    ];
    const at = { s1: { x: 100, y: 200 }, s2: { x: 160, y: 240 } };
    const lay = PgView.leadLayout(leads, s => at[s], 600, () => 200);
    const by = (id) => lay.nodes.find(n => n.id === id);
    expect(by('pglead:a')).toMatchObject({ kind: 'art', x: 600, y: 200 });
    const cardsA = lay.nodes.filter(n => n.lead === leads[0] && (n.kind === 'peer' || n.kind === 'more'));
    expect(cardsA.map(n => n.kind)).toEqual(['peer', 'peer', 'peer', 'peer', 'peer', 'more']);
    expect(by('pgleadmore:a').more).toBe(2);
    expect(cardsA.every(n => n.x === 600 + 200 + PgView.LEAD_GAP)).toBe(true);
    const bottomA = Math.max(...cardsA.map(n => n.y));
    const cardsB = lay.nodes.filter(n => n.lead === leads[1] && n.kind === 'peer');
    expect(Math.min(...cardsB.map(n => n.y))).toBeGreaterThan(bottomA);
    expect(by('pglead:b').y).toBe(240); // the artifact stays on its process's row
    expect(lay.bottom).toBeGreaterThanOrEqual(Math.max(...cardsB.map(n => n.y)));
    expect(lay.right).toBeGreaterThan(600 + 200 + PgView.LEAD_GAP);
  });
});
