// Alerts tabs share the list-page layout: no page title, actions on the toolbar
// row, labelled buttons, one table style and one empty state.
//
// Each tab used to style these its own way: four empty states, icon-only gear
// buttons, uppercase strips, and "+" prefixed actions.
const { test, expect } = require('@playwright/test');
const { login, openFractal } = require('./fixtures');
const { expectNoPageTitle } = require('./layout');

const TABS = [
  { tab: 'manual', name: 'Rules', view: '#alertsView' },
  { tab: 'feeds', name: 'Feeds', view: '#feedAlertsView' },
  { tab: 'actions', name: 'Actions', view: '#actionsManageView' },
  { tab: 'coverage', name: 'Coverage', view: '#attackCoverageView' },
  { tab: 'policies', name: 'Policies', view: '#alertPoliciesView' },
  { tab: 'changes', name: 'Changes', view: '#alertChangesView' },
];

async function openAlerts(page) {
  await page.setViewportSize({ width: 1440, height: 900 });
  await login(page);
  await openFractal(page, 'alerts');
  await expect(page.locator('#alertsSubTabs')).toBeVisible();
}

async function openTab(page, t) {
  await page.locator(`#alertsSubTabs .alerts-sub-tab[data-subtab="${t.tab}"]`).click();
  const view = page.locator(t.view);
  await expect(view).toBeVisible();
  // Let the tab's own fetch land so empty states and tables are on screen.
  await expect(view.locator('.loading:visible')).toHaveCount(0, { timeout: 15000 });
  return view;
}

test.describe('Alerts layout', () => {
  for (const t of TABS) {
    test(`${t.name}: no title, labelled actions, no case-forced text`, async ({ page }) => {
      await openAlerts(page);
      const view = await openTab(page, t);
      await expectNoPageTitle(view, t.name);

      const toolbarButtons = await view.locator('.admin-toolbar button:visible').allTextContents();
      expect(toolbarButtons.filter(s => !s.trim()), 'icon-only toolbar buttons').toEqual([]);
      const labels = await view.locator('button:visible').allTextContents();
      expect(labels.filter(s => s.trim().startsWith('+')), 'buttons carry a "+" prefix').toEqual([]);

      const forced = await view.evaluate(root => [...root.querySelectorAll('*')]
        .filter(el => el.offsetParent && el.children.length === 0 && el.textContent.trim().length > 2
          && ['uppercase', 'lowercase', 'capitalize'].includes(getComputedStyle(el).textTransform))
        .map(el => el.textContent.trim()));
      expect(forced, 'text forced into another case').toEqual([]);
    });

    test(`${t.name}: one empty state and one table style`, async ({ page }) => {
      await openAlerts(page);
      const view = await openTab(page, t);
      expect(await view.locator('.es:visible, .empty-state:visible, .ap-empty:visible').count(),
        'a legacy empty state is showing').toBe(0);

      const th = view.locator('table th:visible');
      if (await th.count()) {
        const styles = await th.evaluateAll(els => els.map(el => {
          const cs = getComputedStyle(el);
          return `${cs.fontSize} ${cs.fontWeight} ${cs.textTransform}`;
        }));
        expect(new Set(styles)).toEqual(new Set(['12px 500 none']));
      }
    });
  }

  test('primary actions sit flush right on the toolbar row', async ({ page }) => {
    await openAlerts(page);
    const bar = await page.locator('#alertsSubTabs').boundingBox();
    for (const t of TABS.filter(x => ['manual', 'feeds', 'actions', 'policies'].includes(x.tab))) {
      const view = await openTab(page, t);
      const btn = view.locator('.admin-toolbar:visible .btn-primary:visible').last();
      await expect(btn, `${t.name} has no primary action on its toolbar`).toBeVisible();
      const box = await btn.boundingBox();
      expect(Math.abs(bar.x + bar.width - (box.x + box.width)), `${t.name} action is not flush right`).toBeLessThanOrEqual(1);
    }
  });

  test('Manage feeds opens the feed sources panel with no forced case', async ({ page }) => {
    await openAlerts(page);
    const view = await openTab(page, TABS.find(x => x.tab === 'feeds'));
    await view.locator('#feedManageToggleBtn').click();
    const panel = view.locator('#feedManageView');
    await expect(panel).toBeVisible();
    const forced = await panel.evaluate(root => [...root.querySelectorAll('*')]
      .filter(el => el.offsetParent && el.children.length === 0 && getComputedStyle(el).textTransform !== 'none')
      .map(el => el.textContent.trim()));
    expect(forced).toEqual([]);
  });

  test('Coverage opens on summary tiles', async ({ page }) => {
    await openAlerts(page);
    const view = await openTab(page, TABS.find(x => x.tab === 'coverage'));
    await expect(view.locator('.perf-metrics-grid .perf-metric-card').first()).toBeVisible();
  });
});
