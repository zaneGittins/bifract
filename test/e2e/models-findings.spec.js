// Model data viewer: findings first, then every row.
//
// The viewer opens on what the model's alert would raise, says how much that is
// above the table, and explains each row with fact chips instead of a column
// dump. All of it is wiring between /data?view=findings, /stats and the DOM,
// which static checks cannot see.
const { test, expect } = require('@playwright/test');
const { login, listFractals, selectFractal } = require('./fixtures');

// The first model with scored rows whose type has an alert, or null. A tlsh
// index raises no alert, so it has no Findings view to test.
async function populatedModel(page) {
  for (const fractal of await listFractals(page)) {
    const scope = { 'X-Bifract-Scope': `fractal:${fractal.id}` };
    const res = await page.request.get('/api/v1/models', { headers: scope });
    if (!res.ok()) continue;
    for (const m of (await res.json())?.data || []) {
      if (m.model_type === 'tlsh' || m.status !== 'active') continue;
      const detail = await page.request.get(`/api/v1/models/${m.id}`, { headers: scope });
      if (detail.ok() && (await detail.json())?.data?.row_count > 0) return { fractal, model: m };
    }
  }
  return null;
}

test.describe('Model findings view', () => {
  test('opens on Findings with a summary, and rows explain themselves', async ({ page }) => {
    await login(page);
    const found = await populatedModel(page);
    test.skip(!found, 'no fractal holds a populated model with an alert');
    const { fractal, model } = found;

    await selectFractal(page, fractal.id);
    await page.goto(`/#f/${fractal.id}/models/${model.id}`);

    const tabs = page.locator('#modelsViewTabs');
    await expect(tabs.locator('.mv-tab[data-view="findings"]')).toHaveClass(/active/, { timeout: 20000 });
    await expect(tabs.locator('.mv-tab[data-view="all"]')).not.toHaveClass(/active/);

    // The summary leads with what would alert, and the tab carries the same count.
    const summary = page.locator('#modelsStats .mv-sum-facts');
    await expect(summary).toBeVisible({ timeout: 20000 });
    await expect(summary).toContainText(/would alert|new this week/);
    const findingsCount = tabs.locator('.mv-tab[data-view="findings"] .mv-tab-n');
    await expect(findingsCount).toHaveText(/^[\d.,KMB]+$/);

    // Findings render as rows or as an explained empty state, never a bare table.
    const wrap = page.locator('#modelsDataTableWrap');
    await expect(wrap.locator('tbody tr, .es').first()).toBeVisible({ timeout: 20000 });
    if (await wrap.locator('.es').count()) {
      await expect(wrap.locator('.es-title')).toHaveText(/Nothing would alert|Nothing new this week|No alert thresholds set/);
    }

    // Column headers carry the label only; the stored name moved to the tooltip.
    await tabs.locator('.mv-tab[data-view="all"]').click();
    await expect(tabs.locator('.mv-tab[data-view="all"]')).toHaveClass(/active/);
    const firstRow = wrap.locator('tbody tr').first();
    await expect(firstRow).toBeVisible({ timeout: 20000 });
    await expect(wrap.locator('th .mv-h-src')).toHaveCount(0);
    await expect(wrap.locator('th.mv-why')).toHaveText('Why');
    await expect(firstRow.locator('td.mv-why .fact-chip').first()).toBeVisible();

    // The drawer leads with the facts and folds the raw columns away.
    await firstRow.locator('td').first().click();
    const drawer = page.locator('#modelsRowDrawer');
    await expect(drawer).toBeVisible();
    await expect(drawer.locator('.fact-chips .fact-chip').first()).toBeVisible();
    const allCols = drawer.locator('details.mv-all-cols');
    await expect(allCols).not.toHaveAttribute('open', '');
    await allCols.locator('summary').click();
    await expect(allCols.locator('dl dt').first()).toBeVisible();
  });
});
