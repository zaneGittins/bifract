// Fractals listing: table structure assertions.
//
// The prism row shipped with five <td> against a four-<th> header. Nothing
// errors on that: the browser silently invents a fifth column, so the header
// band stops short of the rows and every column is re-proportioned. Only
// counting cells against the header catches it.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function openFractals(page) {
  const res = await page.request.post('/api/v1/auth/login', {
    data: { username: USER, password: PASS },
  });
  expect(res.ok(), 'login request failed').toBeTruthy();
  await page.goto('/');
  await page.locator('#fractalListingTabBtn').click();
  await expect(page.locator('.fractal-listing-table table')).toBeVisible();
  await expect(page.locator('.fractal-listing-table tbody tr').first()).toBeVisible({ timeout: 15000 });
}

test.describe('Fractals listing', () => {
  test('every row has exactly as many cells as the header has columns', async ({ page }) => {
    await openFractals(page);
    const cols = await page.locator('.fractal-listing-table thead th').count();
    expect(cols).toBeGreaterThan(0);

    const counts = await page.locator('.fractal-listing-table tbody tr').evaluateAll(
      rows => rows.map(r => ({
        cells: r.querySelectorAll('td').length,
        label: (r.querySelector('td')?.textContent || '').trim().slice(0, 40),
      })));
    expect(counts.length).toBeGreaterThan(0);

    for (const row of counts) {
      expect(row.cells, `row "${row.label}" has ${row.cells} cells, header has ${cols}`).toBe(cols);
    }
  });

  // A prism is the variant that regressed, so assert it specifically rather
  // than relying on one happening to be present in the generic check above.
  test('the prism row matches the header column count', async ({ page }) => {
    await openFractals(page);
    const prism = page.locator('.fractal-listing-table tbody tr', { has: page.locator('.prism-badge') }).first();
    test.skip(await prism.count() === 0, 'no prism configured');

    const cols = await page.locator('.fractal-listing-table thead th').count();
    expect(await prism.locator('td').count(), 'prism row cell count drifted from the header').toBe(cols);
  });

  // The symptom the mismatch produced: a header band narrower than the rows.
  test('the header band spans the full width of the rows', async ({ page }) => {
    await openFractals(page);
    const headRight = await page.locator('.fractal-listing-table thead tr')
      .evaluate(el => el.getBoundingClientRect().right);
    const rowRight = await page.locator('.fractal-listing-table tbody tr').first()
      .evaluate(el => el.getBoundingClientRect().right);
    expect(Math.abs(headRight - rowRight),
      `header stops at ${headRight} but rows end at ${rowRight}`).toBeLessThan(2);
  });

  test('no horizontal scroll at a laptop width', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await openFractals(page);
    const overflow = await page.evaluate(() =>
      document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(overflow, 'body scrolls horizontally').toBeLessThanOrEqual(0);
  });
});
