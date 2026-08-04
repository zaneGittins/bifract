// ATT&CK coverage map: rendered-page assertions.
//
// The grid is ~700 buttons built once and then only recoloured, and the whole
// point of the view is that a gap looks different from low coverage. Both of
// those fail silently in static checks: the DOM is valid either way.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function openCoverage(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();

  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  await page.locator('.fractal-listing-table tbody tr td').first().click();
  await page.locator('#fractalAlertsTabBtn').click();
  await page.locator('.alerts-sub-tab[data-subtab="coverage"]').click();
  await expect(page.locator('#attackCoverageView')).toBeVisible();
  await expect(page.locator('#atkMatrix .atk-column').first()).toBeVisible({ timeout: 15000 });
}

test.describe('ATT&CK coverage', () => {
  test('renders every tactic column in kill-chain order', async ({ page }) => {
    await openCoverage(page);

    const names = await page.locator('#atkMatrix .atk-col-name').allTextContents();
    expect(names.length).toBeGreaterThanOrEqual(14);
    expect(names[0]).toBe('Reconnaissance');
    expect(names[names.length - 1]).toBe('Impact');

    // Every column must carry a coverage ratio, not a placeholder.
    const counts = await page.locator('#atkMatrix .atk-col-count').allTextContents();
    for (const c of counts) expect(c).toMatch(/^\d+\/\d+ · \d+%$/);
  });

  test('covered cells are filled and gaps are not', async ({ page }) => {
    await openCoverage(page);

    const covered = page.locator('#atkMatrix .atk-cell[data-heat]');
    await expect(covered.first()).toBeVisible();
    expect(await covered.count()).toBeGreaterThan(0);

    // A covered cell shows its rule count; a gap shows nothing. Colour is never
    // the only signal.
    const badge = await covered.first().locator('.atk-badge').textContent();
    expect(Number(badge)).toBeGreaterThan(0);

    const gap = page.locator('#atkMatrix .atk-cell:not([data-heat]):not([data-sev])').first();
    expect(await gap.locator('.atk-badge').textContent()).toBe('');
    expect(await gap.evaluate(el => getComputedStyle(el).borderStyle)).toBe('dashed');
  });

  test('summary strip reports coverage against the matrix', async ({ page }) => {
    await openCoverage(page);

    const stats = page.locator('#atkSummary .atk-stat');
    expect(await stats.count()).toBe(5);
    const headline = await stats.first().locator('.atk-stat-value').textContent();
    expect(headline).toMatch(/^\d+\/\d+$/);
    const [covered, total] = headline.split('/').map(Number);
    expect(total).toBeGreaterThan(100);
    expect(covered).toBeLessThanOrEqual(total);
  });

  test('gaps-only filter hides covered techniques', async ({ page }) => {
    await openCoverage(page);

    const visibleCovered = () => page.locator('#atkMatrix .atk-cell[data-heat]:not(.atk-dim)').count();
    expect(await visibleCovered()).toBeGreaterThan(0);

    await page.locator('#atkCoverage').selectOption('gaps');
    await expect.poll(visibleCovered).toBe(0);

    await page.locator('#atkCoverage').selectOption('covered');
    await expect.poll(visibleCovered).toBeGreaterThan(0);
  });

  test('search narrows to the matching technique', async ({ page }) => {
    await openCoverage(page);

    await page.locator('#atkSearch').fill('T1543');
    const undimmed = page.locator('#atkMatrix .atk-cell:not(.atk-dim)');
    await expect.poll(() => undimmed.count()).toBeGreaterThan(0);
    for (const text of await undimmed.locator('.atk-cell-meta').allTextContents()) {
      expect(text).toContain('T1543');
    }
  });

  test('clicking a covered cell opens the drawer with its rules', async ({ page }) => {
    await openCoverage(page);

    await page.locator('#atkMatrix .atk-cell[data-heat]').first().click();
    await expect(page.locator('#atkDrawer')).toHaveClass(/open/);
    await expect(page.locator('#atkDrawerId')).toHaveText(/^T\d+/);
    await expect(page.locator('#atkDrawerBody .atk-rule').first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#atkDrawerBody a.atk-link')).toHaveAttribute('href', /attack\.mitre\.org/);

    await page.keyboard.press('Escape');
    await expect(page.locator('#atkDrawer')).not.toHaveClass(/open/);
  });

  test('the matrix scrolls inside its own box, not the page', async ({ page }) => {
    await openCoverage(page);

    const pageOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(pageOverflow).toBeLessThanOrEqual(1);
  });

  test('switching sub-tabs leaves no coverage view behind', async ({ page }) => {
    await openCoverage(page);

    await page.locator('.alerts-sub-tab[data-subtab="manual"]').click();
    await expect(page.locator('#attackCoverageView')).toBeHidden();
    await expect(page.locator('#alertsView')).toBeVisible();

    await page.locator('.alerts-sub-tab[data-subtab="coverage"]').click();
    await expect(page.locator('#attackCoverageView')).toBeVisible();
    await expect(page.locator('#alertsView')).toBeHidden();
  });
});
