// Settings page layout: one tab bar, one content column, one page header.
//
// The sub-tabs used to each lay themselves out differently, so the content
// edges jumped as you moved between them and the tab labels sat indented from
// the content below. These assert the shared geometry on the rendered page.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal } = require('./fixtures');

const TABS = ['users', 'groups', 'apikeys', 'context', 'platform', 'security', 'danger'];
const WITH_ACTION = ['users', 'groups', 'apikeys', 'context'];

async function openSettings(page) {
  await login(page);
  await page.goto('/');
  await openNav(page, 'settings');
  await expect(page.locator('#settingsView')).toBeVisible();
}

async function openTab(page, tab) {
  await page.locator(`#settingsSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).click();
  const head = page.locator('#settingsView .settings-sub-panel:visible .admin-page-head').first();
  await expect(head).toBeVisible();
  return head;
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
      const head = await (await openTab(page, tab)).boundingBox();
      expect(Math.abs(head.x - bar.x), `${tab} header left edge`).toBeLessThanOrEqual(1);
      expect(Math.abs(head.x + head.width - (bar.x + bar.width)), `${tab} header right edge`).toBeLessThanOrEqual(1);
    }
  });

  test('every tab has a title and a description', async ({ page }) => {
    await openSettings(page);
    for (const tab of TABS) {
      const head = await openTab(page, tab);
      await expect(head.locator('h2')).not.toBeEmpty();
      await expect(head.locator('p').first()).not.toBeEmpty();
    }
  });

  test('the primary action sits on the title row, flush right', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await openSettings(page);
    for (const tab of WITH_ACTION) {
      const head = await openTab(page, tab);
      const h = await head.boundingBox();
      const btn = await head.locator('.btn-primary').boundingBox();
      expect(btn, `${tab} has no primary action in its header`).toBeTruthy();
      expect(h.x + h.width - (btn.x + btn.width), `${tab} action is not flush right`).toBeLessThanOrEqual(1);
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
