// The shared results table renders for every surface, not just the Query tab.
//
// Dashboards, notebooks, and the alert and model editors call it with options
// objects that omit what the Query tab passes. An option once named valueOf
// resolved to Object.prototype.valueOf on those objects, so every one of those
// surfaces threw while the Query tab kept working.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

const FIELDS = ['computer_name', '_count'];
const ROWS = [{ computer_name: 'WS-01', _count: '12' }, { computer_name: 'WS-02', _count: '3' }];

test.beforeEach(async ({ page }) => {
  await login(page);
  await page.goto('/');
  await page.waitForFunction(() => window.QueryExecutor && typeof QueryExecutor.buildResultsTable === 'function');
});

test('builds a table from minimal options, as dashboards and notebooks call it', async ({ page }) => {
  const out = await page.evaluate(({ fields, rows }) => {
    const res = {};
    for (const [name, opts] of [['none', undefined], ['empty', {}], ['dashboard', { features: { sort: false } }]]) {
      try {
        const built = QueryExecutor.buildResultsTable(fields, rows, opts);
        res[name] = /<table/.test(built.html) ? 'ok' : 'no table';
      } catch (e) {
        res[name] = e.message;
      }
    }
    return res;
  }, { fields: FIELDS, rows: ROWS });
  expect(out).toEqual({ none: 'ok', empty: 'ok', dashboard: 'ok' });
});

test('renders into an element, as the alert and model editors call it', async ({ page }) => {
  const out = await page.evaluate(({ fields, rows }) => {
    const el = document.createElement('div');
    document.body.appendChild(el);
    try {
      QueryExecutor.renderResultsToElement(rows, el, fields, {});
    } catch (e) {
      return e.message;
    }
    return el.querySelectorAll('tbody tr').length;
  }, { fields: FIELDS, rows: ROWS });
  expect(out).toBe(ROWS.length);
});

test('marks an all-numeric column numeric and reads nested values through cellValue', async ({ page }) => {
  const out = await page.evaluate(() => {
    const numeric = [...QueryExecutor._computeNumericFields(['computer_name', '_count'], [{ computer_name: 'a', _count: '1' }])];
    const nested = [...QueryExecutor._computeNumericFields(['n'], [{ x: { n: 5 } }], (row) => row.x.n)];
    return { numeric, nested };
  });
  expect(out).toEqual({ numeric: ['_count'], nested: ['n'] });
});
