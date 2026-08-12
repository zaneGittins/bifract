// Results-header controls on the Query tab: which of Fields / wrap / export apply
// to the current output type, and that each export format really downloads.
//
// None of this is visible to a static check. Every button is present and wired in
// the markup no matter what the query rendered, so only a real page shows that
// wrap is gone on a pie chart, that the fields rail retires on an aggregation, or
// that PNG is offered exactly when there is a canvas to encode.
const { test, expect } = require('@playwright/test');
const fs = require('fs');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function openSearch(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();

  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  await page.locator('.fractal-listing-table tbody tr td').first().click();
  await page.locator('#fractalSearchTabBtn').click();
  await page.locator('#queryInput').waitFor({ timeout: 15000 });

  // Widen to the full retention so the assertions do not depend on how recently
  // the fixture was ingested.
  await page.locator('#timePickerBtn').click();
  await page.locator('#timePickerPanel .tp-preset[data-value="all"]').click();
}

async function runQuery(page, query) {
  await page.locator('#queryInput').fill(query);
  await page.locator('#executeBtn').click();
  // The export menu appears only once a run finishes with rows.
  await expect(page.locator('#exportMenuWrap')).toBeVisible({ timeout: 30000 });
}

async function download(page, itemId) {
  await page.locator('#exportMenuBtn').click();
  const [dl] = await Promise.all([
    page.waitForEvent('download'),
    page.locator(`#${itemId}`).click(),
  ]);
  return dl;
}

// Each test runs a real ClickHouse query, so the budget has to cover the scan
// and not just the render.
test.describe.configure({ timeout: 90000 });

test.describe('results-header controls follow the output type', () => {
  test('raw results offer fields, wrap and the row formats', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | limit(20)');

    await expect(page.locator('#outputTypeLabel')).toHaveText('Table');
    await expect(page.locator('#fieldsRailToggle')).toBeVisible();
    await expect(page.locator('#wrapToggleBtn')).toBeVisible();

    await page.locator('#exportMenuBtn').click();
    await expect(page.locator('#exportCsvItem')).toBeVisible();
    await expect(page.locator('#exportJsonlItem')).toBeVisible();
    // No canvas behind a table, so no image to export.
    await expect(page.locator('#exportPngItem')).toBeHidden();
  });

  test('a chart retires wrap and the fields rail, and offers PNG', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | groupBy(event_type) | piechart()');

    await expect(page.locator('#outputTypeLabel')).toHaveText('Pie Chart');
    await expect(page.locator('#wrapToggleBtn')).toBeHidden();
    await expect(page.locator('#fieldsRailToggle')).toBeHidden();

    await page.locator('#exportMenuBtn').click();
    await expect(page.locator('#exportPngItem')).toBeVisible();
    await expect(page.locator('#exportCsvItem')).toBeVisible();
  });

  test('an aggregation table keeps wrap but retires the fields rail', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | groupBy(event_type)');

    await expect(page.locator('#outputTypeLabel')).toHaveText('Table');
    await expect(page.locator('#wrapToggleBtn')).toBeVisible();
    // Field statistics describe raw events; an aggregation has none.
    await expect(page.locator('#fieldsRailToggle')).toBeHidden();
  });
});

test.describe('exports produce real files', () => {
  test('CSV carries a header and one line per row', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | limit(5)');

    const dl = await download(page, 'exportCsvItem');
    expect(dl.suggestedFilename()).toMatch(/^bifract-results-.*\.csv$/);
    const body = fs.readFileSync(await dl.path(), 'utf8');
    const lines = body.trim().split('\n');
    expect(lines[0]).toContain('"timestamp"');
    expect(lines.length).toBeGreaterThan(1);
  });

  test('JSONL emits one parseable object per line', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | limit(5)');

    const dl = await download(page, 'exportJsonlItem');
    expect(dl.suggestedFilename()).toMatch(/^bifract-results-.*\.jsonl$/);
    const lines = fs.readFileSync(await dl.path(), 'utf8').trim().split('\n');
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      const row = JSON.parse(line);
      expect(typeof row).toBe('object');
      // _all_fields is merged up, never emitted as an opaque nested blob.
      expect(row._all_fields).toBeUndefined();
    }
  });

  test('PNG is a real image with the theme background composited in', async ({ page }) => {
    await openSearch(page);
    await runQuery(page, '* | groupBy(event_type) | piechart()');
    // The canvas has to be painted before it can be encoded.
    await expect(page.locator('.pie-chart-wrapper canvas')).toBeVisible({ timeout: 30000 });

    const dl = await download(page, 'exportPngItem');
    expect(dl.suggestedFilename()).toMatch(/^bifract-results-.*\.png$/);
    const bytes = fs.readFileSync(await dl.path());
    expect(bytes.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))).toBeTruthy();
    // A transparent-only encode of an empty canvas would be a few hundred bytes.
    expect(bytes.length).toBeGreaterThan(2000);
  });
});
