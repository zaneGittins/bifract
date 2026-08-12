// `| mitre()` ATT&CK matrix on the Query tab: rendered-page assertions.
//
// The grid is ~700 buttons whose only signal is colour plus a count badge, and
// the tag-to-technique resolution (sub-technique rollup, revoked IDs, tactic-only
// tags) happens in the browser. Both are invisible to static checks: the DOM is
// valid whether or not a single cell lit up.
//
// Requires the sample detections ingested by the accompanying fixture, or any
// events carrying attack.* tags in the selected fractal.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';
const QUERY = process.env.BIFRACT_E2E_MITRE_QUERY || '* | mitre(tags=rule_tags, by=computer_name)';

async function runMitreQuery(page, query = QUERY) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();

  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  await page.locator('.fractal-listing-table tbody tr td').first().click();
  await page.locator('#fractalSearchTabBtn').click();

  const input = page.locator('#queryInput');
  await input.waitFor({ timeout: 15000 });
  await input.fill(query);
  await page.locator('#executeBtn').click();

  await expect(page.locator('.mtr-host .atk-matrix .atk-column').first()).toBeVisible({ timeout: 30000 });
}

// Each test runs a real ClickHouse aggregation over the fractal, so the budget
// has to exceed the query, not just the render: the default 30s per-test cap
// fires before the locator's own wait can, turning a slow box into a failure.
test.describe.configure({ timeout: 90000 });

test.describe('ATT&CK matrix over query results', () => {
  test('renders the kill chain with observed techniques heated', async ({ page }) => {
    await runMitreQuery(page);

    const names = await page.locator('.mtr-host .atk-col-name').allTextContents();
    expect(names.length).toBeGreaterThanOrEqual(14);
    expect(names[0]).toBe('Reconnaissance');
    expect(names[names.length - 1]).toBe('Impact');

    // Colour is never the only signal: an observed cell also carries a count.
    const observed = page.locator('.mtr-host .atk-cell[data-heat]');
    await expect(observed.first()).toBeVisible();
    const badge = await observed.first().locator('.atk-badge').textContent();
    expect(badge.trim().length).toBeGreaterThan(0);

    // An untouched technique stays an outline with no number.
    const quiet = page.locator('.mtr-host .atk-cell:not([data-heat])').first();
    expect((await quiet.locator('.atk-badge').textContent()).trim()).toBe('');

    await expect(page.locator('#outputTypeLabel')).toHaveText('ATT&CK Matrix');
  });

  test('summary reports techniques, tactics and the top technique', async ({ page }) => {
    await runMitreQuery(page);

    const summary = page.locator('.mtr-host .atk-summary');
    await expect(summary).toContainText('Techniques');
    await expect(summary).toContainText('Tactics');
    await expect(summary).toContainText('Top technique');
    await expect(summary).toContainText('ATT&CK v');

    // Tactics reads "n/m" over the whole matrix, never a bare count.
    const tactics = await summary.locator('.atk-stat', { hasText: 'Tactics' }).first().textContent();
    expect(tactics).toMatch(/\d+\/\d+/);
  });

  test('observed-only hides untouched techniques and columns', async ({ page }) => {
    await runMitreQuery(page);

    const allCells = await page.locator('.mtr-host .atk-cell').count();
    await page.locator('.mtr-host [data-mtr-scope]').selectOption('observed');

    const visible = await page.locator('.mtr-host .atk-cell:not(.mtr-hidden)').count();
    expect(visible).toBeGreaterThan(0);
    expect(visible).toBeLessThan(allCells);
    // Every surviving cell is one that actually fired.
    const heated = await page.locator('.mtr-host .atk-cell:not(.mtr-hidden)[data-heat]').count();
    expect(heated).toBe(visible);
  });

  test('clicking a technique opens the detail drawer with its breakdown', async ({ page }) => {
    await runMitreQuery(page);

    await page.locator('.mtr-host .atk-cell[data-heat]').first().click();
    const drawer = page.locator('#mtrDrawerHost .atk-drawer');
    await expect(drawer).toHaveClass(/open/);
    await expect(drawer.locator('[data-id]')).toHaveText(/^T\d{4}/);
    await expect(drawer.locator('.mtr-metric-value')).toBeVisible();
    await expect(drawer.locator('.atk-drawer-body')).toContainText('attack.');
    // by=computer_name was requested, so the drawer must break the hits down.
    await expect(drawer.locator('.atk-drawer-body')).toContainText('By computer_name');

    await page.keyboard.press('Escape');
    await expect(drawer).not.toHaveClass(/open/);
  });

  test('drill-down replaces mitre() with a filter on the same field', async ({ page }) => {
    await runMitreQuery(page);

    await page.locator('.mtr-host .atk-cell[data-heat]').first().click();
    await page.locator('#mtrDrawerHost [data-drill]').click();

    await expect(page.locator('#queryInput')).toHaveValue(/rule_tags=~attack\./);
    // The matrix must give way to the event table it drilled into.
    await expect(page.locator('#resultsTable')).toBeVisible({ timeout: 30000 });
  });

  test('exports what was observed as a Navigator layer', async ({ page }) => {
    await runMitreQuery(page);

    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.locator('.mtr-host [data-mtr-export]').click(),
    ]);
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    const layer = JSON.parse(Buffer.concat(chunks).toString());

    expect(layer.domain).toBe('enterprise-attack');
    expect(layer.versions.layer).toBe('4.5');
    expect(layer.techniques.length).toBeGreaterThan(0);
    // Navigator scores a technique by how much of it we saw; zero would be a gap.
    for (const t of layer.techniques) {
      expect(t.techniqueID).toMatch(/^T\d{4}(\.\d{3})?$/);
      expect(t.score).toBeGreaterThan(0);
    }
    expect(layer.gradient.maxValue).toBeGreaterThanOrEqual(
      Math.max(...layer.techniques.map(t => t.score)));
  });

  test('a truncated tail is reported, never presented as a total', async ({ page }) => {
    await runMitreQuery(page, '* | mitre(tags=rule_tags, limit=3)');
    await expect(page.locator('.mtr-host [data-mtr-status]')).toContainText('lower bounds');
  });

  test('a result set with no ATT&CK tags explains itself', async ({ page }) => {
    await runMitreQuery(page, 'message="bifract-mitre-sample-untagged" | mitre(tags=rule_tags)');
    await expect(page.locator('.mtr-host [data-mtr-status]')).toContainText('No ATT&CK tags found');
  });
});
