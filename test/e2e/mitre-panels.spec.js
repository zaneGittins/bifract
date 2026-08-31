// `| mitre()` as an embedded panel: dashboard widget and notebook section.
//
// The query page and the panels are separate render paths (queryExecutor,
// dashboards, notebooks), each mounting the matrix itself. A panel that silently
// falls back to a table still looks like a working dashboard, so these drive the
// real widgets end to end.
const { test, expect } = require('@playwright/test');
const { login, populatedFractal, selectFractal, openFractal } = require('./fixtures');

const QUERY = '* | mitre(tags=rule_tags, by=computer_name)';

// A panel can only be asserted against events carrying attack.* tags, so the
// fixture is probed and the test skips rather than waiting 30s for a cell that
// can never render. Scope is session state (the fractal selector POSTs
// /select), not a header, so the fractal is selected the way the UI does it
// before anything is created under it.
async function mitreFractal(page) {
  await login(page);
  const fractal = await populatedFractal(page, QUERY);
  if (fractal) await selectFractal(page, fractal.id);
  return fractal;
}

// Each test runs a real ClickHouse aggregation over the fractal, so the budget
// has to exceed the query, not just the render: the default 30s per-test cap
// fires before the locator's own wait can, turning a slow box into a failure.
test.describe.configure({ timeout: 90000 });

test.describe('ATT&CK matrix panels', () => {
  test('renders inside a dashboard widget, observed-only by default', async ({ page }) => {
    const fractal = await mitreFractal(page);
    test.skip(!fractal, 'no fractal on this stack has events carrying attack.* tags');

    const dash = await (await page.request.post('/api/v1/dashboards', {
      data: { name: `e2e mitre ${Date.now()}`, time_range_type: 'last24h' },
    })).json();
    expect(dash.success, `dashboard create failed: ${JSON.stringify(dash)}`).toBeTruthy();
    const dashboardId = dash.data.id;

    const widget = await (await page.request.post(`/api/v1/dashboards/${dashboardId}/widgets`, {
      data: { title: 'ATT&CK', query_content: QUERY, chart_type: 'mitre', width: 12, height: 8 },
    })).json();
    expect(widget.success, `widget create failed: ${JSON.stringify(widget)}`).toBeTruthy();

    await openFractal(page, 'fractalDashboardsTabBtn', fractal.name);
    await page.evaluate(id => window.Dashboards.openDashboard(id), dashboardId);

    // Observed-only drops whole columns, so the assertion targets a surviving one.
    const host = page.locator('.mtr-host').first();
    await expect(host.locator('.atk-column:not(.mtr-hidden)').first()).toBeVisible({ timeout: 30000 });

    // A panel opens on what fired: nothing untouched should be on screen.
    await expect(host.locator('[data-mtr-scope]')).toHaveValue('observed');
    const visible = await host.locator('.atk-cell:not(.mtr-hidden)').count();
    const heated = await host.locator('.atk-cell:not(.mtr-hidden)[data-heat]').count();
    expect(visible).toBeGreaterThan(0);
    expect(heated).toBe(visible);
    // The hunting controls belong to the query page, not a wallboard tile.
    await expect(host.locator('[data-mtr-search]')).toHaveCount(0);

    // The drawer still works from a panel, minus the drill-down a panel cannot do.
    await host.locator('.atk-cell[data-heat]').first().click();
    await expect(page.locator('#mtrDrawerHost .atk-drawer')).toHaveClass(/open/);
    await expect(page.locator('#mtrDrawerHost [data-drill]')).toHaveCount(0);
    await page.keyboard.press('Escape');

    await page.request.delete(`/api/v1/dashboards/${dashboardId}`);
  });

  test('renders inside a notebook section', async ({ page }) => {
    const fractal = await mitreFractal(page);
    test.skip(!fractal, 'no fractal on this stack has events carrying attack.* tags');

    const nb = await (await page.request.post('/api/v1/notebooks', {
      data: { name: `e2e mitre ${Date.now()}`, description: 'matrix panel', time_range_type: '24h' },
    })).json();
    expect(nb.success, `notebook create failed: ${JSON.stringify(nb)}`).toBeTruthy();
    const notebookId = nb.data.id;

    const section = await (await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
      data: { section_type: 'query', title: 'ATT&CK', content: QUERY, order_index: 0 },
    })).json();
    expect(section.success, `section create failed: ${JSON.stringify(section)}`).toBeTruthy();

    await openFractal(page, 'fractalNotebooksTabBtn', fractal.name);
    await page.evaluate(id => window.Notebooks.openNotebook(id), notebookId);

    const run = page.locator('.execute-query-btn').first();
    await run.waitFor({ timeout: 20000 });
    await run.click();

    const host = page.locator('.notebook-section .mtr-host').first();
    await expect(host.locator('.atk-column:not(.mtr-hidden)').first()).toBeVisible({ timeout: 30000 });
    await expect(host.locator('.atk-cell[data-heat]').first()).toBeVisible();

    await page.request.delete(`/api/v1/notebooks/${notebookId}`);
  });
});
