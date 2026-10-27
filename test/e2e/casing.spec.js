// Text reads as written: no CSS forcing it into upper, lower or title case.
//
// Editors, drawers and side panels each uppercased their own labels, so the
// same kind of label looked different from one screen to the next. The app's
// navigation group labels are the one deliberate exception and are not checked.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal, listFractals } = require('./fixtures');

// Visible leaf text inside `scope` whose case CSS changes.
async function forcedCase(page, scope) {
  return page.locator(scope).first().evaluate(root => [...root.querySelectorAll('*')]
    .filter(el => el.offsetParent && el.children.length === 0 && el.textContent.trim().length > 1
      && getComputedStyle(el).textTransform !== 'none')
    .map(el => el.textContent.trim()));
}

const SCREENS = [
  { name: 'alert editor', scope: '#alertEditorView', open: async p => { await openFractal(p, 'alerts'); await p.locator('#createAlertBtn').click(); } },
  { name: 'model editor', scope: '#modelsView', open: async p => { await openFractal(p, 'models'); await p.locator('#modelsNewBtn').click(); } },
  { name: 'normalizer editor', scope: '#normalizerEditorView', open: async p => { await p.goto('/'); await openNav(p, 'normalizers'); await p.locator('#normalizerCreateBtn').click(); } },
  { name: 'action picker', scope: '#actionDrawer', open: async p => { await openFractal(p, 'alerts'); await p.locator('#alertsSubTabs [data-subtab="actions"]').click(); await p.locator('#actionsManageView .btn-primary').click(); } },
  { name: 'schema', scope: '#schemaFieldsView', open: async p => { await p.goto('/'); await openNav(p, 'schema'); await p.locator('#schemaFieldsView tbody tr').first().waitFor(); } },
  { name: 'api reference', scope: '#apiExplorerView', open: async p => { await p.goto('/'); await openNav(p, 'api'); } },
  { name: 'instructions', scope: '#librariesView', open: async p => { await openFractal(p, 'library'); } },
  { name: 'evidence', scope: '#commentedView', open: async p => { await openFractal(p, 'notebooks'); await p.locator('[data-investigation-subtab="evidence"]').click(); } },
];

test.describe('Text case', () => {
  for (const s of SCREENS) {
    test(`${s.name}: no forced case`, async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await login(page);
      await s.open(page);
      await expect(page.locator(s.scope).first()).toBeVisible();
      await page.waitForTimeout(500);
      expect(await forcedCase(page, s.scope)).toEqual([]);
    });
  }

  test('schema field drawer: no forced case', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await openNav(page, 'schema');
    await page.locator('#schemaFieldsView tbody tr').first().click();
    await expect(page.locator('#schemaDrawer')).toBeVisible();
    expect(await forcedCase(page, '#schemaDrawer')).toEqual([]);
  });
});

test('normalizer editor keeps Discard and Save at the right of its top bar', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await login(page);
  await page.goto('/');
  await openNav(page, 'normalizers');
  await page.locator('#normalizerCreateBtn').click();
  const bar = await page.locator('#normalizerTopbar').boundingBox();
  const save = await page.locator('#normalizerSaveBtn').boundingBox();
  const pad = await page.locator('#normalizerTopbar').evaluate(el => parseFloat(getComputedStyle(el).paddingRight));
  expect(Math.abs(bar.x + bar.width - pad - (save.x + save.width))).toBeLessThanOrEqual(1);
});

test('an empty state never draws its own frame inside a framed panel', async ({ page }) => {
  await login(page);
  // Recall's sidebar entry only shows once the archive is provisioned; the view
  // and its empty state render either way.
  const [f] = await listFractals(page);
  await page.goto(`/#f/${f.id}/recall`);
  const empty = page.locator('#recallResults .empty-state');
  await expect(empty).toBeVisible();
  expect(await empty.evaluate(el => getComputedStyle(el).borderTopStyle)).toBe('none');
});
