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
  };
  window.CSS = { escape: (s) => s };
  new Function('window', 'document', 'Utils', 'Toast', 'CSS', 'requestAnimationFrame', read('exchange.js'))(
    window, document, window.Utils, window.Toast, window.CSS, () => {});
  const ex = window.Exchange;
  ex.watchImports = () => {};
  return { ex, toasts };
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
