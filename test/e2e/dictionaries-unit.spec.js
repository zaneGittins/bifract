// Unit tests for the lookups listing in web/static/dictionaries.js, evaluated in Node.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load() {
  const read = (f) => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8');
  const tbody = { innerHTML: '', closest() { return null; }, querySelectorAll() { return []; } };
  const window = {};
  const document = { getElementById: (id) => (id === 'dictsTableBody' ? tbody : null) };
  const localStorage = { getItem() { return null; }, setItem() {} };
  const navigator = { platform: 'Linux' };
  new Function('window', 'document', 'localStorage', read('timezone.js'))(window, document, localStorage);
  new Function('window', 'document', 'navigator', 'TZ', read('utils.js'))(window, document, navigator, window.TZ);
  new Function('window', 'document', 'TZ', 'Utils', read('dictionaries.js'))(window, document, window.TZ, window.Utils);
  return { D: window.Dictionaries, tbody };
}

const cells = (html) => [...html.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(m => m[1]);

test('the listing shows when each lookup was last updated, with the exact time on hover', () => {
  const { D, tbody } = load();
  const updated = new Date(Date.now() - 3 * 3600 * 1000).toISOString();
  D.filteredDictionaries = [{ id: 'd1', name: 'bad_ips', columns: [], row_count: 2, updated_at: updated }];
  D.renderDictionaryTable(D.filteredDictionaries);

  expect(cells(tbody.innerHTML)[6]).toBe('3h ago');
  const td = tbody.innerHTML.match(/<td class="dict-col-meta dict-col-updated" title="([^"]*)">/);
  expect(td, 'updated cell carries the absolute time').not.toBeNull();
  expect(td[1]).toContain(updated.slice(0, 10));
});

test('a lookup without a timestamp leaves the cell empty', () => {
  const { D, tbody } = load();
  D.filteredDictionaries = [{ id: 'd1', name: 'x', columns: [] }];
  D.renderDictionaryTable(D.filteredDictionaries);
  expect(cells(tbody.innerHTML)[6]).toBe('');
});
