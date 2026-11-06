// Unit tests for how the Exchange tracks dataset imports. Pure logic, evaluated in
// Node with no page. A list read and the progress stream can cross, so the newest
// state of each dataset's latest import must win whatever order they arrive in.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load() {
  const read = (f) => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8');
  const window = {};
  const toasts = [];
  const document = { getElementById() { return null; }, querySelectorAll() { return []; } };
  new Function('window', 'navigator', 'document', read('utils.js'))(window, { platform: 'Linux' }, document);
  window.Toast = {
    success: (title) => toasts.push(`ok: ${title}`),
    error: (title) => toasts.push(`error: ${title}`),
    warning: (title) => toasts.push(`warn: ${title}`),
  };
  window.CSS = { escape: (s) => s };
  new Function('window', 'document', 'Utils', 'Toast', 'CSS', 'requestAnimationFrame', read('exchange.js'))(
    window, document, window.Utils, window.Toast, window.CSS, () => {});
  const ex = window.Exchange;
  ex.watchImports = () => {};
  return { ex, toasts, window };
}

const imp = (over) => ({
  id: 1, dataset_id: 'lab', fractal_name: 'lab', status: 'importing',
  logs_done: 10, logs_total: 100, bytes_done: 0, bytes_total: 0,
  updated_at: '2026-10-31T10:00:00Z', ...over,
});

test('an older state of the same import never replaces a newer one', () => {
  const { ex } = load();
  ex.onImport(imp({ logs_done: 50, updated_at: '2026-10-31T10:00:10Z' }));
  ex.onImport(imp({ logs_done: 20, updated_at: '2026-10-31T10:00:05Z' }));
  expect(ex.imports.get('lab').logs_done).toBe(50);
});

test('a newer import of a dataset replaces the old one, an older one does not', () => {
  const { ex } = load();
  ex.onImport(imp({ id: 2, status: 'pending' }));
  ex.onImport(imp({ id: 1, status: 'failed' }));
  expect(ex.imports.get('lab')).toMatchObject({ id: 2, status: 'pending' });
});

test('the end of an import is announced once, and not when read from the list', () => {
  const { ex, toasts } = load();
  ex.onImport(imp({ status: 'importing' }), false);
  ex.onImport(imp({ status: 'succeeded', updated_at: '2026-10-31T10:01:00Z' }), false);
  expect(toasts).toEqual([]);
  ex.imports.clear();
  ex.onImport(imp({ status: 'importing' }));
  ex.onImport(imp({ status: 'failed', updated_at: '2026-10-31T10:01:00Z' }));
  ex.onImport(imp({ status: 'failed', updated_at: '2026-10-31T10:02:00Z' }));
  expect(toasts).toEqual(['error: Could not import lab']);
});

test('progress reads in logs while importing and in bytes while downloading', () => {
  const { ex } = load();
  expect(ex.importDetail(imp({ logs_done: 1500, logs_total: 2083151 }))).toBe('Importing: 1,500 of 2,083,151 logs');
  expect(ex.importDetail(imp({ status: 'downloading', bytes_done: 1048576, bytes_total: 118236887 })))
    .toBe('Downloading: 1.0 MB of 112.8 MB');
  expect(ex.importProgress(imp({ status: 'downloading', bytes_done: 59118443, bytes_total: 118236887 })).pct).toBe(49);
});

test('an item links its website by address, and never a script URL', () => {
  const { ex } = load();
  const lab = {
    id: 'lab', kind: 'dataset', name: 'Lab', author: 'Bifract Labs', homepage: 'https://labs.bifract.io/001/',
    dataset: { logs: 1, bytes: 1, start: '2026-10-27T00:00:00Z', end: '2026-10-28T00:00:00Z' },
  };
  ex.items = [lab];
  expect(ex.mainHtml(lab)).toContain(
    '<dt>Website</dt><dd><a href="https://labs.bifract.io/001/" target="_blank" rel="noopener noreferrer">labs.bifract.io/001</a></dd>');
  expect(ex.mainHtml({ ...lab, homepage: 'javascript:alert(1)' })).not.toContain('Website');
});

// A bundle built around a dataset: its other items go into the dataset's fractal
// while the import is held, and only then do the logs arrive.
function labRig() {
  const { ex, toasts, window } = load();
  ex.load = async () => {};
  const lab = { id: 'lab', kind: 'dataset', name: 'Lab', dataset: { logs: 5, bytes: 1, start: '2026-10-27T00:00:00Z', end: '2026-10-28T00:00:00Z' } };
  const norm = { id: 'sysmon', kind: 'normalizer', name: 'Sysmon' };
  const list = { id: 'rmm', kind: 'lookup', name: 'RMM', lookup: { source: { url: 'https://x.example/l.csv', key_field: 'k' } } };
  const model = { id: 'users', kind: 'model', name: 'Users', requires: ['rmm'] };
  const rule = { id: 'rare', kind: 'detection', name: 'Rare', requires: ['users'] };
  const bundle = { id: 'hunt', kind: 'bundle', name: 'Hunt', items: ['lab', 'sysmon', 'users', 'rare'] };
  ex.items = [lab, norm, list, model, rule, bundle];
  ex.render = () => {};
  ex.canAddDatasets = () => true;
  const calls = [];
  let scope = 0;
  const posts = {};
  ex.scopeToken = () => scope;
  ex.isStale = (t) => t !== scope;
  ex.loadInstalled = async () => new Set();
  ex.post = async (token, url, body) => {
    if (token !== null && token !== scope) throw new Error('stale');
    calls.push({ url, body });
    const data = url.endsWith('/import') ? { ...imp({ id: 4, status: 'held' }), fractal_id: 'f9' } : { ...imp({ id: 4, status: 'pending', updated_at: '2026-10-31T10:01:00Z' }), fractal_id: 'f9' };
    return { resp: { ok: true, status: 200 }, data: { success: true, data } };
  };
  for (const kind of ['normalizer', 'lookup', 'model', 'detection']) {
    ex.installers[kind] = async function (step, token) {
      if (posts[step.id]) throw new Error(posts[step.id]);
      await this.post(token, `/add/${step.id}`, {});
      return { installed: true, message: '' };
    };
  }
  const globals = { switched: [], ok: true };
  return { ex, toasts, window, calls, bundle, lab, posts, globals, switchScope: () => { scope += 1; } };
}

function withContext(rig, fn) {
  rig.window.FractalContext = {
    currentFractal: { id: 'f0' },
    setCurrentFractal: async function (f) {
      rig.globals.switched.push(f.id);
      if (rig.globals.ok) {
        rig.switchScope();
        this.currentFractal = { id: f.id };
      }
      return rig.globals.ok;
    },
  };
  return fn();
}

test('a bundle with a dataset adds everything to its fractal before the import starts', async () => {
  const rig = labRig();
  rig.ex.watchImports = () => {};
  await withContext(rig, () => rig.ex.install(rig.bundle));
  expect(rig.globals.switched).toEqual(['f9']);
  expect(rig.calls.map(c => c.url)).toEqual([
    '/api/v1/exchange/datasets/lab/import', '/add/sysmon', '/add/rmm', '/add/users', '/add/rare',
    '/api/v1/exchange/datasets/imports/4/start',
  ]);
  expect(rig.calls[0].body).toEqual({ hold: true });
  expect(rig.ex.imports.get('lab').status).toBe('pending');
  expect(rig.toasts).toEqual(['ok: Added Hunt']);
});

test('a member that fails leaves the import held, and Continue finishes it', async () => {
  const rig = labRig();
  rig.posts.users = 'bad model';
  await withContext(rig, () => rig.ex.install(rig.bundle));
  expect(rig.calls.some(c => c.url.endsWith('/start'))).toBe(false);
  expect(rig.ex.imports.get('lab').status).toBe('held');
  expect(rig.toasts[0]).toContain('Did not import');
  expect(rig.ex.actionHtml(rig.bundle)).toContain('Continue');

  delete rig.posts.users;
  rig.calls.length = 0;
  rig.ex.loadInstalled = async () => new Set(['normalizer:Sysmon', 'lookup:rmm']);
  await withContext(rig, () => rig.ex.install(rig.bundle));
  expect(rig.calls.map(c => c.url)).toEqual(['/add/users', '/add/rare', '/api/v1/exchange/datasets/imports/4/start']);
});

test('no import starts if the session cannot move to the dataset\'s fractal', async () => {
  const rig = labRig();
  rig.globals.ok = false;
  await withContext(rig, () => rig.ex.install(rig.bundle));
  expect(rig.calls.map(c => c.url)).toEqual(['/api/v1/exchange/datasets/lab/import']);
  expect(rig.toasts[0]).toContain('Could not add Hunt');
});

test('a bundle with a dataset follows its import', () => {
  const { ex, bundle, lab } = labRig();
  expect(ex.actionHtml(bundle)).toContain('Add all (5)');
  expect(ex.isInstalled(bundle)).toBe(false);
  ex.imports.set('lab', imp({ status: 'importing' }));
  expect(ex.actionHtml(bundle)).toContain('Open');
  expect(ex.statusHtml(bundle)).toContain('Importing 10%');
  ex.imports.set('lab', imp({ status: 'scoring' }));
  expect(ex.statusHtml(bundle)).toContain('Scoring models');
  ex.imports.set('lab', imp({ status: 'failed', error: 'a lookup did not load' }));
  expect(ex.actionHtml(bundle)).toContain(`data-action="retry-import" data-id="lab"`);
  expect(ex.mainHtml(bundle)).toContain('a lookup did not load');
  ex.imports.set('lab', imp({ status: 'held' }));
  expect(ex.actionHtml(lab)).toContain('Start import');
  expect(ex.statusHtml(lab)).toContain('Not started');
  expect(ex.statusMark(lab)).toContain('Not started');
});

// Installed means its members are there, which only their fractal can say. A
// dataset added alone, or shared by two bundles, has an import but not the members.
test('a bundle with an imported dataset is installed only once its members are in the fractal', () => {
  const rig = labRig();
  const { ex, bundle, window } = rig;
  ex.imports.set('lab', { ...imp({ status: 'succeeded' }), fractal_id: 'f9' });
  window.FractalContext = { currentFractal: { id: 'f0' } };
  expect(ex.isInstalled(bundle)).toBe(false);
  expect(ex.statusHtml(bundle)).toContain('Imported');
  expect(ex.actionHtml(bundle)).toContain('Open');

  window.FractalContext = { currentFractal: { id: 'f9' } };
  expect(ex.isInstalled(bundle)).toBe(false);
  expect(ex.actionHtml(bundle)).toContain('Add 4 more');
  ex.installed = new Set(['normalizer:Sysmon', 'lookup:rmm', 'model:users', 'detection:Rare']);
  expect(ex.isInstalled(bundle)).toBe(true);
  expect(ex.statusHtml(bundle)).toContain('Installed');
  expect(ex.actionHtml(bundle)).toContain('Open');
});

test('adding what a lab fractal is missing adds it there, with no new import', async () => {
  const rig = labRig();
  rig.ex.imports.set('lab', { ...imp({ status: 'succeeded' }), fractal_id: 'f9' });
  rig.window.FractalContext = { currentFractal: { id: 'f9' } };
  await rig.ex.install(rig.bundle);
  expect(rig.calls.map(c => c.url)).toEqual(['/add/sysmon', '/add/rmm', '/add/users', '/add/rare']);
});

test('nothing else can start the import while a bundle is being added', async () => {
  const rig = labRig();
  let release;
  rig.ex.installers.model = async function (step, token) {
    await new Promise(r => { release = r; });
    await this.post(token, `/add/${step.id}`, {});
    return { installed: true, message: '' };
  };
  const other = { id: 'other', kind: 'bundle', name: 'Other', items: ['lab', 'rare'] };
  rig.ex.items.push(other);
  const run = withContext(rig, () => rig.ex.install(rig.bundle));
  while (!release) await new Promise(r => setTimeout(r, 1));
  expect(rig.ex.actionHtml(rig.lab)).toContain('disabled');
  expect(rig.ex.actionHtml(other)).toContain('disabled');
  await rig.ex.install(other);
  expect(rig.calls.filter(c => c.url.endsWith('/start'))).toEqual([]);
  release();
  await run;
  expect(rig.calls.filter(c => c.url.endsWith('/start')).length).toBe(1);
});

test('a dataset is not offered from a bundle, nor its members outside its fractal', () => {
  const { ex, bundle, window } = labRig();
  const add = () => ex.contentsHtml(bundle).match(/data-action="install"/g)?.length || 0;
  expect(add()).toBe(0);
  ex.imports.set('lab', { ...imp({ status: 'succeeded' }), fractal_id: 'f9' });
  window.FractalContext = { currentFractal: { id: 'f9' } };
  expect(add()).toBe(3);
});

test('an import that left with its fractal is forgotten, one added meanwhile is not', async () => {
  const { ex, window } = labRig();
  ex.imports.set('gone', imp({ dataset_id: 'gone' }));
  ex.imports.set('lab', imp({ status: 'failed' }));
  global.HttpUtils = {
    safeFetch: async () => {
      ex.imports.set('fresh', imp({ dataset_id: 'fresh', status: 'held' }));
      return { data: [imp({ status: 'failed' })] };
    },
  };
  await ex.loadImports();
  delete global.HttpUtils;
  expect([...ex.imports.keys()].sort()).toEqual(['fresh', 'lab']);
});

test('a fractal switch during a bundle install leaves the reload to the install', () => {
  const { ex, window } = labRig();
  let subscriber, loads = 0;
  global.FractalContext = { subscribe: (_, cb) => { subscriber = cb; }, shouldReload: () => true };
  window.FractalContext = global.FractalContext;
  ex.load = () => { loads += 1; };
  ex.init();
  ex._labInstall = true;
  subscriber();
  expect(loads).toBe(0);
  ex._labInstall = false;
  subscriber();
  expect(loads).toBe(1);
  delete global.FractalContext;
});

test('the confirmation says where everything goes, and leaves out shared items already here', () => {
  const { ex, bundle } = labRig();
  ex.installed = new Set(['normalizer:Sysmon', 'model:users']);
  ex._confirm = 'hunt';
  const html = ex.confirmHtml(bundle);
  expect(html).toContain('Creates fractal <strong>lab</strong>');
  expect(html).toContain('adds 3 items');
  expect(html).not.toContain('Sysmon');
  expect(html).toContain('Users');
});

test('a successful import with a warning says so', () => {
  const { ex, toasts } = load();
  ex.renderSoon = () => {};
  ex.onImport(imp({ status: 'scoring' }), false);
  ex.onImport(imp({ status: 'succeeded', warning: 'beacons was not scored.', updated_at: '2026-10-31T10:01:00Z' }));
  expect(toasts).toEqual(['warn: Imported lab']);
});
