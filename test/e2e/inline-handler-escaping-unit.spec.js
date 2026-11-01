// Unit tests for values that reach inline event handlers and style attributes.
// Evaluated in Node with no page: the attribute is cut where an HTML parser
// would cut it, entity-decoded the way the browser decodes it, and then run, so
// a tag or name that breaks out of either layer shows up as an extra call.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Evaluate the given web/static files in one scope. Globals the code reads as
// bare names (BifractCharts and the like) can be passed in.
function load(files, globals = {}) {
  const src = [].concat(files).map(f => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8')).join('\n;\n');
  const window = { addEventListener() {} };
  const document = {
    addEventListener() {},
    getElementById() { return null; },
    createElement() {
      return {
        set textContent(s) { this._t = String(s); },
        get innerHTML() { return this._t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); },
      };
    },
  };
  const scope = { window, navigator: { platform: 'Linux' }, document, module: undefined, ...globals };
  new Function(...Object.keys(scope), src)(...Object.values(scope));
  return window;
}

function decodeEntities(s) {
  return s
    .replace(/&#x([0-9a-f]+);?/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);?/g, (_, d) => String.fromCodePoint(Number(d)))
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

// Attributes of every start tag in html, as an HTML parser would read them.
function tags(html) {
  const out = [];
  const tagRe = /<([a-z]+)((?:\s+[^\s=>"']+(?:="[^"]*"|='[^']*'|=[^\s>]*)?)*)\s*\/?>/gi;
  let m;
  while ((m = tagRe.exec(html))) {
    const attrs = {};
    const attrRe = /\s+([^\s=>"']+)(?:="([^"]*)"|='([^']*)'|=([^\s>]*))?/g;
    let a;
    while ((a = attrRe.exec(m[2]))) attrs[a[1].toLowerCase()] = decodeEntities(a[2] ?? a[3] ?? a[4] ?? '');
    out.push({ name: m[1].toLowerCase(), attrs });
  }
  return out;
}

// Run an inline handler and record every global call it makes.
function fire(handler) {
  const calls = [];
  const recorder = name => new Proxy({}, { get: (_, k) => (...args) => { calls.push([`${name}.${String(k)}`, ...args]); } });
  const names = ['AlertFeeds', 'AlertList', 'AlertTests', 'Alerts', 'Notebooks', 'Normalizers', 'CommentedLogs', 'InstructionLibraries', 'QueryReference', 'App'];
  const event = { stopPropagation() {}, preventDefault() {} };
  const alert = (...args) => calls.push(['alert', ...args]);
  try {
    new Function(...names, 'event', 'alert', handler)(...names.map(recorder), event, alert);
  } catch (e) {
    calls.push(['error', e.message]);
  }
  return calls;
}

const HOSTILE = [
  "');alert(1);//",
  "&#39;);alert(1);//",
  "&#x27;);alert(1);//",
  '" onmouseover="alert(1)',
  '&quot; onmouseover=&quot;alert(1)',
  'back\\slash\\\');alert(1);//',
  '</span><img src=x onerror=alert(1)>',
  'line\nbreak sep',
  'T1059.001',
];

test.describe('Inline handler escaping', () => {
  for (const value of HOSTILE) {
    test(`Utils.escapeJs survives the HTML layer: ${JSON.stringify(value)}`, () => {
      const { Utils } = load('utils.js');
      const [el] = tags(`<span onclick="App.go('${Utils.escapeJs(value)}')">x</span>`);
      expect(Object.keys(el.attrs)).toEqual(['onclick']);
      expect(fire(el.attrs.onclick)).toEqual([['App.go', value]]);
    });

    test(`alert label pills filter by the exact label: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'alertList.js']);
      w.BifractCharts = { _colorForName: () => '#123456' };
      const html = w.AlertList.labelsCell([value], 'AlertFeeds.setLabelFilter');
      const pill = tags(html).find(t => t.attrs.onclick);
      expect(Object.keys(pill.attrs).filter(k => k.startsWith('on'))).toEqual(['onclick']);
      expect(fire(pill.attrs.onclick)).toEqual([['AlertFeeds.setLabelFilter', value]]);
    });

    test(`notebook tag chips pass the exact tag: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'notebooks.js']);
      const html = w.Notebooks.renderTagsArea({ id: 'sec1', section_type: 'note', tags: [value] });
      const chip = tags(html).find(t => t.attrs.class === 'section-tag-chip');
      const remove = tags(html).find(t => t.attrs.class === 'section-tag-remove');
      expect(fire(chip.attrs.onclick)).toEqual([['Notebooks.onTagChipClick', value]]);
      expect(fire(remove.attrs.onclick)).toEqual([['Notebooks.removeTagFromSection', 'sec1', value]]);
    });

    test(`library missing-link rows open the exact page name: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'instructionLibraries.js']);
      w.InstructionLibraries.pages = [];
      const name = value.replace(/[\[\]\n]/g, '');
      const html = w.InstructionLibraries._outgoingHtml(`see [[${name}]]`);
      const row = tags(html).find(t => t.attrs.onclick);
      expect(fire(row.attrs.onclick)).toEqual([['InstructionLibraries.openWikilink', name.trim()]]);
    });

    test(`alert severity pills filter by the exact severity: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'alertList.js']);
      const html = w.AlertList.severityCell({ severity: value }, 'Alerts.setSeverityFilter');
      const pill = tags(html).find(t => t.attrs.onclick);
      expect(Object.keys(pill.attrs).filter(k => k.startsWith('on'))).toEqual(['onclick']);
      expect(fire(pill.attrs.onclick)).toEqual([['Alerts.setSeverityFilter', value.toLowerCase()]]);
      expect(tags(html).map(t => t.name)).toEqual(['td', 'span']);
    });

    test(`alert detail severity pill stays one element: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'alertDetail.js'], { TZ: { format: () => '' } });
      const html = w.AlertDetail.renderBody({ id: 'a1', name: 'n', severity: value, actions: [] });
      const pill = tags(html).find(t => (t.attrs.class || '').startsWith('severity-pill'));
      expect(Object.keys(pill.attrs)).toEqual(['class', 'style']);
      expect(tags(html).map(t => t.name)).not.toContain('img');
    });

    test(`chat presentation severity stays one element: ${JSON.stringify(value)}`, () => {
      const div = {};
      const doc = { addEventListener() {}, getElementById() { return null; }, createElement: () => div };
      const w = load(['utils.js', 'chat.js'], { document: doc });
      w.Chat.renderPresentation({ appendChild() {} }, { severity: value, summary: 's' });
      expect(tags(div.innerHTML).map(t => t.name)).toEqual(['div']);
      expect(Object.keys(tags(div.innerHTML)[0].attrs)).toEqual(['class']);
    });

    test(`alert test toggles open the exact test: ${JSON.stringify(value)}`, () => {
      const w = load(['utils.js', 'alertTests.js']);
      Object.assign(w.AlertTests, { _selected: new Set(), _open: null });
      const html = w.AlertTests.renderTest({ name: value, events: [], expectation: 'match' }, 0);
      const toggle = tags(html).find(t => t.attrs.class === 'at-test-toggle');
      expect(fire(toggle.attrs.onclick)).toEqual([['AlertTests.toggleOpen', value]]);
    });

    test(`normalizer export passes the exact name: ${JSON.stringify(value)}`, () => {
      const container = { innerHTML: '' };
      const doc = { addEventListener() {}, getElementById: id => (id === 'normalizersListContainer' ? container : null) };
      const w = load(['utils.js', 'normalizers.js'], { document: doc });
      w.Normalizers.normalizers = [{ id: 'n1', name: value, transforms: [], field_mappings: [] }];
      w.Normalizers.renderNormalizers();
      const exp = tags(container.innerHTML).find(t => (t.attrs.onclick || '').includes('exportNormalizer'));
      expect(fire(exp.attrs.onclick)).toEqual([['Normalizers.exportNormalizer', 'n1', value]]);
    });
  }
});

test.describe('Rule colours in style attributes', () => {
  const rule = color => [{ column: 'c', operator: '=', value: '1', target: 'row', color }];
  const hostile = 'red;background-image:url(https://attacker.example/x)';

  test('Utils.safeColor keeps only #rrggbb', () => {
    const { Utils } = load('utils.js');
    expect(Utils.safeColor('#A1b2C3')).toBe('#A1b2C3');
    expect(Utils.safeColor(hostile)).toBe('#8b5cf6');
    expect(Utils.safeColor('"><img>')).toBe('#8b5cf6');
    expect(Utils.safeColor(undefined, '#000000')).toBe('#000000');
  });

  for (const [file, obj] of [['dashboards.js', 'Dashboards'], ['notebooks.js', 'Notebooks']]) {
    test(`${obj} row and cell highlights ignore a hostile colour`, () => {
      const w = load(['utils.js', file]);
      expect(w[obj].getRowHighlightStyle({ c: '1' }, rule(hostile))).toBe('background-color: #8b5cf626;');
      const cell = [{ ...rule(hostile)[0], target: 'cell' }];
      expect(w[obj].getCellHighlightStyle({ c: '1' }, 'c', cell)).toBe('background-color: #8b5cf626;');
      expect(w[obj].getRowHighlightStyle({ c: '1' }, rule('#22c55e'))).toBe('background-color: #22c55e26;');
    });
  }
});

test.describe('Values that are not strings', () => {
  test('Utils.escapeHtml escapes arrays and objects instead of passing them through', () => {
    const { Utils } = load('utils.js');
    expect(Utils.escapeHtml(['<img src=x onerror=alert(1)>'])).toBe('&lt;img src=x onerror=alert(1)&gt;');
    expect(Utils.escapeHtml(42)).toBe('42');
    expect(Utils.escapeHtml(null)).toBe('');
    expect(Utils.escapeHtml(undefined)).toBe('');
  });

  test('notebook results header escapes stored chart_type, count and timing', () => {
    const w = load(['utils.js', 'notebooks.js']);
    const html = w.Notebooks.renderQueryResults({
      results: [], chart_type: '<img src=x onerror=alert(1)>', count: '<b>1</b>', execution_ms: '<i>2</i>',
    }, { id: 's1' });
    expect(tags(html).map(t => t.name)).not.toContain('img');
    expect(tags(html).map(t => t.name)).not.toContain('b');
    expect(tags(html).map(t => t.name)).not.toContain('i');
  });

  test('a single-value unit with absurd decimals still renders', () => {
    const w = load(['utils.js', 'charts.js'], { getComputedStyle: () => ({ getPropertyValue: () => '' }) });
    expect(() => w.BifractCharts.formatValue(1.5, { type: 'number', decimals: 1000 })).not.toThrow();
    expect(() => w.BifractCharts.formatValue(1.5, { type: 'percent', decimals: 'x' })).not.toThrow();
  });
});

test.describe('Attack chain links', () => {
  test('only findings that point at a section in this notebook become links', () => {
    const w = load(['utils.js', 'notebooks.js']);
    w.Notebooks.currentNotebook = { sections: [{ id: 'comment-1' }, { id: 'query-1' }] };
    const content = JSON.stringify({ executive_summary: 's', tactics: [{ tactic: 'Execution', findings: [
      { description: 'a', section_id: 'comment-1' },
      { description: 'b', section_id: 'query-1' },
      { description: 'c', section_id: 'ghost' },
    ] }] });
    const html = w.Notebooks.renderAttackChainSection({ id: 'ac', section_type: 'ai_attack_chain', content });
    const links = tags(html).filter(t => t.attrs.class === 'attack-chain-link').map(t => fire(t.attrs.onclick));
    expect(links).toEqual([[['Notebooks.scrollToSection', 'comment-1']], [['Notebooks.scrollToSection', 'query-1']]]);
  });
});

test.describe('Format panel and query reference', () => {
  test('a series colour override is only used when it is #rrggbb', () => {
    const BifractCharts = { SERIES_COLORS: ['#111111'], PIE_COLORS: ['#222222'], PALETTES: {} };
    const w = load(['utils.js', 'format-panel.js'], { BifractCharts });
    w.BifractFormat._cfg = { colors: { overrides: { a: '"><img src=x onerror=alert(1)>', b: '#abcdef' } } };
    w.BifractFormat._detectLabels = () => ['a', 'b'];
    const html = w.BifractFormat._sectionColors('series');
    expect(tags(html).map(t => t.name)).not.toContain('img');
    const inputs = tags(html).filter(t => t.attrs.class === 'rule-color-custom').map(t => t.attrs.value);
    expect(inputs).toEqual(['#111111', '#abcdef']);
  });

  test('a reference example with double quotes copies intact', () => {
    const w = load(['utils.js', 'queryReference.js']);
    w.QueryReference.expandedCards = new Set();
    const ex = 'action="denied" | join(src_ip) { action="login" }';
    const html = w.QueryReference.renderCompactFunction({
      name: 'join', category: 'x', description: 'd', syntax: 's', parameters: [], examples: [ex],
    });
    const copy = tags(html).find(t => t.attrs.class === 'ref-copy-btn');
    expect(fire(copy.attrs.onclick)).toEqual([['QueryReference.copyToQuery', ex]]);
  });
});

test.describe('Severity styling', () => {
  test('a known chat severity keeps its bubble class', () => {
    const div = {};
    const doc = { addEventListener() {}, getElementById() { return null; }, createElement: () => div };
    const w = load(['utils.js', 'chat.js'], { document: doc });
    w.Chat.renderPresentation({ appendChild() {} }, { severity: 'critical', summary: 's' });
    expect(tags(div.innerHTML)[0].attrs.class).toBe('chat-severity-bubble chat-severity-critical');
  });
});
