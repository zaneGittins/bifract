// List pages share one layout: a titled header with the primary action on the
// right, a toolbar, and one table style.
//
// Each page used to style its own header, buttons and table, and Normalizers
// borrowed another page's table class, so removing that page's styles left its
// columns unaligned and unpadded. These assert the shared rules.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal } = require('./fixtures');

// key: how to reach it; view: the page's root.
const PAGES = [
  { key: 'fractalListing', view: '#fractalListingTabContent', fractal: false },
  { key: 'notebooks', view: '#notebookListing', fractal: true },
  { key: 'dashboards', view: '#dashboardListing', fractal: true },
  { key: 'dictionaries', view: '#dictListing', fractal: true },
  { key: 'models', view: '#modelsView', fractal: true },
  { key: 'ingest', view: '#ingestTokenListing', fractal: true },
  { key: 'normalizers', view: '#normalizersView', fractal: false },
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
  const head = page.locator(`${p.view} .admin-page-head`).first();
  await expect(head).toBeVisible();
  return head;
}

test.describe('List pages', () => {
  for (const p of PAGES) {
    test(`${p.key}: titled header, action flush right, content on one edge`, async ({ page }) => {
      const head = await open(page, p);
      await expect(head.locator('h2')).not.toBeEmpty();
      await expect(head.locator('p').first()).not.toBeEmpty();

      const h = await head.boundingBox();
      const btn = await head.locator('.btn-primary').last().boundingBox();
      expect(btn, 'no primary action in the header').toBeTruthy();
      expect(h.x + h.width - (btn.x + btn.width), 'primary action is not flush right').toBeLessThanOrEqual(1);

      // Short lists (Normalizers) have no search, so no toolbar.
      const toolbar = page.locator(`${p.view} .admin-toolbar`).first();
      if (await toolbar.count()) {
        const bar = await toolbar.boundingBox();
        expect(Math.abs(bar.x - h.x), 'toolbar is inset from the header').toBeLessThanOrEqual(1);
      }

      const labels = await page.locator(`${p.view} button:visible`).allTextContents();
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
