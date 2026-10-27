// List pages share one layout: a toolbar with search on the left and actions on
// the right, and one table style. The sidebar names the page, so no title.
//
// Each page used to style its own header, buttons and table, and Normalizers
// borrowed another page's table class, so removing that page's styles left its
// columns unaligned and unpadded. These assert the shared rules.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal } = require('./fixtures');
const { expectNoPageTitle, expectActionFlushRight } = require('./layout');

// key: how to reach it; view: the page's root.
const PAGES = [
  { key: 'fractalListing', name: 'Fractals', view: '#fractalListingTabContent', fractal: false },
  { key: 'notebooks', name: 'Notebooks', view: '#notebookListing', fractal: true },
  { key: 'dashboards', name: 'Dashboards', view: '#dashboardListing', fractal: true },
  { key: 'dictionaries', name: 'Lookups', view: '#dictListing', fractal: true },
  { key: 'models', name: 'Analytics', view: '#modelsView', fractal: true },
  { key: 'ingest', name: 'Ingest', view: '#ingestTokenListing', fractal: true },
  { key: 'normalizers', name: 'Normalizers', view: '#normalizersView', fractal: false },
];

async function open(page, p) {
  await page.setViewportSize({ width: 1440, height: 900 });
  await login(page);
  if (p.fractal) {
    await openFractal(page, p.key);
  } else {
    await page.goto('/');
    await openNav(page, p.key);
  }
  const toolbar = page.locator(`${p.view} .admin-toolbar`).first();
  await expect(toolbar).toBeVisible();
  return toolbar;
}

test.describe('List pages', () => {
  for (const p of PAGES) {
    test(`${p.key}: actions flush right on the toolbar, no page title`, async ({ page }) => {
      const toolbar = await open(page, p);
      const view = page.locator(p.view);
      await expectNoPageTitle(view, p.name);
      const viewBox = await view.boundingBox();
      await expectActionFlushRight(view, viewBox.x + viewBox.width, p.key);
      const bar = await toolbar.boundingBox();
      expect(Math.abs(bar.x - viewBox.x), 'toolbar is inset from the page').toBeLessThanOrEqual(1);

      const labels = await view.locator('button:visible').allTextContents();
      expect(labels.filter(t => t.trim().startsWith('+')), 'buttons still carry a "+" prefix').toEqual([]);
    });

    test(`${p.key}: table headers use the shared style`, async ({ page }) => {
      await open(page, p);
      const th = page.locator(`${p.view} table th:visible`);
      test.skip(await th.count() === 0, 'no rows on this stack');
      const styles = await th.evaluateAll(els => els.map(el => {
        const cs = getComputedStyle(el);
        return `${cs.fontSize} ${cs.fontWeight} ${cs.textTransform}`;
      }));
      expect(new Set(styles)).toEqual(new Set(['12px 500 none']));
    });
  }

  test('no page scrolls sideways at phone width', async ({ page }) => {
    for (const p of PAGES) {
      await open(page, p);
      await page.setViewportSize({ width: 390, height: 844 });
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
      expect(overflow, `${p.key} scrolls sideways`).toBeLessThanOrEqual(0);
    }
  });

  // Revealing admin-only elements once wiped their inline styles, which also
  // un-hid views the router had hidden, such as the editor under this list.
  // Only a direct load shows it: the reveal ran after the first route.
  test('normalizers: the editor stays hidden under the list', async ({ page }) => {
    await login(page);
    await page.goto('/#normalizers');
    await expect(page.locator('#normalizersListContainer')).toBeVisible();
    await expect(page.locator('#normalizerEditorView')).toBeHidden();
  });

  test('normalizers: cells are padded and headers sit over their columns', async ({ page }) => {
    await open(page, PAGES.find(p => p.key === 'normalizers'));
    const row = page.locator('#normalizersListContainer tbody tr').first();
    await expect(row).toBeVisible();
    const cells = await row.locator('td').evaluateAll(els => els.map(el => {
      const r = el.getBoundingClientRect();
      return { left: r.left, pad: parseFloat(getComputedStyle(el).paddingLeft) };
    }));
    const heads = await page.locator('#normalizersListContainer thead th').evaluateAll(els =>
      els.map(el => el.getBoundingClientRect().left));
    for (const [i, c] of cells.entries()) {
      expect(c.pad, `cell ${i} has no padding`).toBeGreaterThanOrEqual(8);
      expect(Math.abs(c.left - heads[i]), `header ${i} is not over its column`).toBeLessThanOrEqual(1);
    }
    const headAlign = await page.locator('#normalizersListContainer thead th').first().evaluate(el => getComputedStyle(el).textAlign);
    expect(headAlign).toMatch(/left|start/);
  });
});
