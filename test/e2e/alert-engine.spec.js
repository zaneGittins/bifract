// System > Alerts: engine health.
//
// The page used to derive avg, p95, max, a histogram and a "slowest" table from
// one overwritten column. These assert the replacements are wired to real data
// and that the two things a rendered page catches still hold: the mode toggle
// swaps the table, and the disabled strip names names.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

async function openAlertEngine(page) {
  await login(page);
  await page.goto('/');
  await page.locator('#mainPerformanceTabBtn').click();
  await expect(page.locator('#performanceView')).toBeVisible();
  await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="alerts"]').click();
  await expect(page.locator('#perfPaneAlerts')).toBeVisible();
  // The tiles fill from the summary endpoint on the first poll.
  await expect(page.locator('#aeTileEvaluating')).not.toHaveText('--');
}

test.describe('Alert engine health', () => {
  test('renders the tiles, both charts and the table', async ({ page }) => {
    await openAlertEngine(page);

    for (const id of ['aeTileEvaluating', 'aeTileFires', 'aeTileDisabled', 'aeTileActions']) {
      await expect(page.locator('#' + id)).not.toHaveText('--');
    }
    // Either a drawn chart or its placeholder: both mean the endpoint answered.
    await expect(page.locator('#aeLatencyWrap svg, #aeLatencyWrap .perf-chart-placeholder')).toBeVisible();
    await expect(page.locator('#aeFiresWrap svg, #aeFiresWrap .perf-chart-placeholder')).toBeVisible();
    await expect(page.locator('#aeRowsTable table, #aeRowsTable .empty-state')).toBeVisible();
  });

  test('Recent fires swaps the table and hides the disabled strip', async ({ page }) => {
    await openAlertEngine(page);

    await page.locator('#aeModes .act-mode[data-mode="fires"]').click();
    await expect(page.locator('#aeModes .act-mode[data-mode="fires"]')).toHaveClass(/active/);
    await expect(page.locator('#aeDisabledStrip')).toBeHidden();
    await expect(page.locator('#aeModeHint')).toContainText('execution log');

    await page.locator('#aeModes .act-mode[data-mode="alerts"]').click();
    await expect(page.locator('#aeModes .act-mode[data-mode="alerts"]')).toHaveClass(/active/);
    await expect(page.locator('#aeModeHint')).toContainText('furthest behind');
  });

  // The tile count and the strip have to describe the same set. The old query
  // counted disabled_reason inside a WHERE enabled = true, so it could only find
  // alerts that had been re-enabled without their reason cleared.
  test('the auto-disabled count agrees with the strip, and every entry gives a reason', async ({ page }) => {
    await openAlertEngine(page);

    const counted = Number(await page.locator('#aeTileDisabled').textContent());
    const pills = page.locator('#aeDisabledStrip .act-failpill');
    const listed = await pills.count();

    if (counted === 0) {
      await expect(page.locator('#aeDisabledStrip')).toBeHidden();
      return;
    }
    expect(listed, 'the tile counts auto-disabled alerts the strip cannot name').toBeGreaterThan(0);
    for (let i = 0; i < listed; i++) {
      await expect(pills.nth(i).locator('.ae-pill-name')).not.toHaveText('');
      await expect(pills.nth(i).locator('.ae-pill-why')).not.toHaveText('');
    }
  });

  test('severity never rides on colour alone', async ({ page }) => {
    await openAlertEngine(page);
    const cell = page.locator('#aeRowsTable tbody .act-source').first();
    test.skip(await cell.count() === 0, 'no alerts configured');
    // A swatch plus its label, so the row survives a colourblind reader.
    await expect(cell.locator('.act-dot')).toBeVisible();
    await expect(cell).toHaveText(/Critical|High|Medium|Low/);
  });

  test('the search box filters server-side', async ({ page }) => {
    await openAlertEngine(page);
    test.skip(await page.locator('#aeRowsTable tbody tr').count() === 0, 'no alerts configured');

    const before = Number(await page.locator('#aeRowCount').textContent());
    await page.locator('#aeSearch').fill('zzz-no-such-alert-zzz');
    await expect.poll(async () => Number(await page.locator('#aeRowCount').textContent()),
      { message: 'the filter did not reach the server' }).toBe(0);
    await page.locator('#aeSearch').fill('');
    await expect.poll(async () => Number(await page.locator('#aeRowCount').textContent())).toBe(before);
  });
});

test.describe('Storage & Ingest', () => {
  // logs_hot moved off the Alerts tab: it is storage health, not alerting health.
  test('the hot table stats render under Storage', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="storage"]').click();
    await expect(page.locator('#perfPaneStorage')).toBeVisible();
    await expect(page.locator('#hotMetricPartitions')).toBeVisible();
    await expect(page.locator('#perfPaneAlerts #hotMetricPartitions')).toHaveCount(0);
  });
});
