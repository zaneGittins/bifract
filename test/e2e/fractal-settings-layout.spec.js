// Fractal settings share the Settings layout: no page header, setting rows whose
// controls line up, and a red Danger zone tab set apart on the right.
const { test, expect } = require('@playwright/test');
const { login, openFractal } = require('./fixtures');
const { expectNoPageTitle, firstBlockEdges } = require('./layout');

const TABS = ['overview', 'access', 'lifecycle', 'danger'];

async function openManage(page) {
  await page.setViewportSize({ width: 1440, height: 900 });
  await login(page);
  await openFractal(page, 'manage');
  await expect(page.locator('#manageSubTabs')).toBeVisible();
}

async function openTab(page, tab) {
  await page.locator(`#manageSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).click();
  const panel = page.locator('#fractalOnlyManageSection .manage-sub-panel:visible');
  await expect(panel).toHaveCount(1);
  return panel;
}

test.describe('Fractal settings layout', () => {
  test('every tab starts on the tab bar edges without repeating its name', async ({ page }) => {
    await openManage(page);
    const bar = await page.locator('#manageSubTabs').boundingBox();
    for (const tab of TABS) {
      const panel = await openTab(page, tab);
      const name = await page.locator(`#manageSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).textContent();
      await expectNoPageTitle(panel, name.trim());
      const edges = await firstBlockEdges(panel);
      expect(Math.abs(edges.left - bar.x), `${tab} content left edge`).toBeLessThanOrEqual(1);
    }
  });

  test('on a wide screen the tab bar spans the page and forms keep a readable column', async ({ page }) => {
    await openManage(page);
    await page.setViewportSize({ width: 2560, height: 1080 });
    const section = await page.locator('#fractalOnlyManageSection').boundingBox();
    const bar = await page.locator('#manageSubTabs').boundingBox();
    expect(Math.abs(bar.x + bar.width - (section.x + section.width)), 'tab bar stops short of the page').toBeLessThanOrEqual(1);
    for (const tab of ['overview', 'lifecycle']) {
      const form = await (await openTab(page, tab)).locator('.sp-content').boundingBox();
      expect(form.width, `${tab} form stretches across the screen`).toBeLessThanOrEqual(961);
    }
  });

  test('Danger zone sits apart on the right, in red', async ({ page }) => {
    await openManage(page);
    const danger = page.locator('#manageSubTabs .alerts-sub-tab[data-subtab="danger"]');
    await expect(danger).toHaveClass(/alerts-sub-tab-danger/);
    const bar = await page.locator('#manageSubTabs').boundingBox();
    const box = await danger.boundingBox();
    expect(bar.x + bar.width - (box.x + box.width), 'Danger zone is not on the right').toBeLessThanOrEqual(1);
  });

  test('no case-forced headings and no "+" buttons', async ({ page }) => {
    await openManage(page);
    for (const tab of TABS) {
      await openTab(page, tab);
      const panel = page.locator('#fractalOnlyManageSection .manage-sub-panel:visible');
      const transforms = await panel.locator('h2, h3').evaluateAll(els =>
        els.filter(e => e.offsetParent).map(e => `${e.textContent.trim()}: ${getComputedStyle(e).textTransform}`));
      for (const t of transforms) expect(t, `${tab} heading is case-transformed`).toMatch(/: none$/);
      const labels = await panel.locator('button:visible').allTextContents();
      expect(labels.filter(t => t.trim().startsWith('+')), `${tab} buttons carry "+"`).toEqual([]);
    }
  });

  test('lifecycle controls share one width and one right edge', async ({ page }) => {
    await openManage(page);
    await openTab(page, 'lifecycle');
    const boxes = await page.locator('#manageSubTabLifecycle .sp-row-control > :is(select, input, .quota-input-group)')
      .evaluateAll(els => els.filter(e => e.offsetParent).map(e => {
        const r = e.getBoundingClientRect();
        return { w: Math.round(r.width), r: Math.round(r.right) };
      }));
    expect(boxes.length, 'lifecycle controls are not in setting rows').toBeGreaterThanOrEqual(2);
    expect(new Set(boxes.map(b => b.w)).size, `widths ${boxes.map(b => b.w)}`).toBe(1);
    expect(new Set(boxes.map(b => b.r)).size, `right edges ${boxes.map(b => b.r)}`).toBe(1);
  });
});
