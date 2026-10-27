// Settings page layout: one tab bar, one content column, actions on the toolbar.
//
// The sub-tabs used to each lay themselves out differently, so the content
// edges jumped as you moved between them and the tab labels sat indented from
// the content below. These assert the shared geometry on the rendered page.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal } = require('./fixtures');
const { expectNoPageTitle, expectActionFlushRight, firstBlockEdges } = require('./layout');

const TABS = ['users', 'groups', 'apikeys', 'context', 'platform', 'security', 'danger'];
const WITH_ACTION = ['users', 'groups', 'apikeys', 'context'];
const TABLE_TABS = WITH_ACTION;
const FORM_TABS = ['platform', 'security', 'danger'];

async function openSettings(page) {
  await login(page);
  await page.goto('/');
  await openNav(page, 'settings');
  await expect(page.locator('#settingsView')).toBeVisible();
}

async function openTab(page, tab) {
  await page.locator(`#settingsSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).click();
  const panel = page.locator('#settingsView .settings-sub-panel:visible');
  await expect(panel).toHaveCount(1);
  return panel;
}

// Left edge of the rendered label text, not the button box, so tab padding
// cannot hide an indent.
const textLeft = loc => loc.evaluate(el => {
  const range = document.createRange();
  range.selectNodeContents(el);
  return range.getBoundingClientRect().left;
});

test.describe('Settings layout', () => {
  test('every tab shares the content edges, and the tab labels start on them', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await openSettings(page);

    const bar = await page.locator('#settingsSubTabs').boundingBox();
    const firstLabel = await textLeft(page.locator('#settingsSubTabs .alerts-sub-tab').first());
    expect(Math.abs(firstLabel - bar.x), 'first tab label is indented from the content edge').toBeLessThanOrEqual(1);

    for (const tab of TABS) {
      const edges = await firstBlockEdges(await openTab(page, tab));
      expect(Math.abs(edges.left - bar.x), `${tab} content left edge`).toBeLessThanOrEqual(1);
    }
  });

  // Lists use the whole width like every other page; forms keep a readable
  // column so a control stays near its label. The tab bar spans the page either way.
  test('on a wide screen, tables fill the width and forms keep a readable column', async ({ page }) => {
    await page.setViewportSize({ width: 2560, height: 1080 });
    await openSettings(page);
    const view = await page.locator('#settingsView').boundingBox();
    const bar = await page.locator('#settingsSubTabs').boundingBox();
    expect(Math.abs(bar.x + bar.width - (view.x + view.width)), 'tab bar stops short of the page').toBeLessThanOrEqual(1);

    for (const tab of TABLE_TABS) {
      const edges = await firstBlockEdges(await openTab(page, tab));
      expect(Math.abs(edges.right - (bar.x + bar.width)), `${tab} stops short of the tab bar`).toBeLessThanOrEqual(1);
    }
    for (const tab of FORM_TABS) {
      const panel = await openTab(page, tab);
      const form = await panel.locator('.sp-content').boundingBox();
      expect(Math.abs(form.x - bar.x), `${tab} form left edge`).toBeLessThanOrEqual(1);
      expect(form.width, `${tab} form stretches across the screen`).toBeLessThanOrEqual(961);
    }
  });

  test('no tab repeats its name as a heading', async ({ page }) => {
    await openSettings(page);
    for (const tab of TABS) {
      const panel = await openTab(page, tab);
      const name = await page.locator(`#settingsSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).textContent();
      await expectNoPageTitle(panel, name.trim());
    }
  });

  test('the primary action sits on the toolbar row, flush right', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await openSettings(page);
    const bar = await page.locator('#settingsSubTabs').boundingBox();
    for (const tab of WITH_ACTION) {
      await expectActionFlushRight(await openTab(page, tab), bar.x + bar.width, tab);
    }
  });

  test('settings has a single level of navigation', async ({ page }) => {
    await openSettings(page);
    await openTab(page, 'platform');
    expect(await page.locator('#settingsView nav, #settingsView .sp-rail').count()).toBe(0);
  });

  test('old General deep links open Platform', async ({ page }) => {
    await login(page);
    for (const old of ['settings', 'limits']) {
      await page.goto(`/#settings/${old}`);
      await expect(page.locator('#settingsSubTabs .alerts-sub-tab.active')).toHaveAttribute('data-subtab', 'platform');
      await expect(page.locator('#settingsSubTabPlatform')).toBeVisible();
    }
  });

  test('users show status and last sign-in in separate columns', async ({ page }) => {
    await openSettings(page);
    await openTab(page, 'users');
    const headers = page.locator('#usersListSettings thead th');
    await expect(headers).toContainText(['User', 'Role', 'Status', 'Last sign-in']);
  });

  test('danger zone actions are outlined, not filled', async ({ page }) => {
    await openSettings(page);
    await openTab(page, 'danger');
    const btn = page.locator('#endpointBaselinesClearBtn');
    await expect(btn).toBeVisible();
    const bg = await btn.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(bg, 'danger action is a solid fill').toMatch(/rgba\(0, 0, 0, 0\)|transparent/);
  });

  test('settings does not scroll sideways at a laptop width', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 800 });
    await openSettings(page);
    for (const tab of TABS) {
      await openTab(page, tab);
      const overflow = await page.evaluate(() =>
        document.documentElement.scrollWidth - document.documentElement.clientWidth);
      expect(overflow, `${tab} scrolls horizontally`).toBeLessThanOrEqual(0);
    }
  });
});

// The tab bar class is shared, and Alerts sub-views each added their own inset,
// so the content shifted under the tabs from one view to the next.
test('alerts sub-views start on the tab bar edge', async ({ page }) => {
  await login(page);
  await openFractal(page, 'alerts');
  const bar = await page.locator('#alertsSubTabs').boundingBox();
  for (const [tab, content] of [['manual', '#alertsList'], ['feeds', '#feedAlertsList'], ['actions', '#unifiedActionsList']]) {
    await page.locator(`#alertsSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).click();
    const box = await page.locator(content).boundingBox();
    expect(box, `${tab} content did not render`).toBeTruthy();
    expect(Math.abs(box.x - bar.x), `${tab} content is inset from the tabs`).toBeLessThanOrEqual(1);
  }
});
