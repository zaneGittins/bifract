// Admin panels and the normalizer list: rendered-page assertions.
//
// These cover UI affordances that static checks pass on while the page is
// visibly wrong: a name that looks clickable but has no handler, a button that
// renders left because its lone child sits under `justify-content: space-between`,
// and content width that jumps as you move between sub-tabs.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', {
    data: { username: USER, password: PASS },
  });
  expect(res.ok(), 'login request failed').toBeTruthy();
  const body = await res.json();
  expect(body.success, `login rejected: ${JSON.stringify(body)}`).toBeTruthy();
}

async function openAdmin(page, subtab) {
  await login(page);
  await page.goto('/');
  await page.locator('#mainSettingsTabBtn').click();
  await expect(page.locator('#settingsView')).toBeVisible();
  if (subtab) {
    await page.locator(`#settingsSubTabs .alerts-sub-tab[data-subtab="${subtab}"]`).click();
  }
}

async function openNormalizers(page) {
  await login(page);
  await page.goto('/');
  await page.locator('#mainNormalizersTabBtn').click();
  await expect(page.locator('#normalizersView')).toBeVisible();
}

// The toolbar button must sit at the right edge. The old markup put a single
// child under `justify-content: space-between`, which renders it hard left.
async function expectActionsRightAligned(page, toolbar) {
  const bar = await toolbar.boundingBox();
  const btn = await toolbar.locator('.btn-primary').boundingBox();
  expect(bar && btn, 'toolbar or its primary button did not render').toBeTruthy();
  const gapRight = bar.x + bar.width - (btn.x + btn.width);
  const gapLeft = btn.x - bar.x;
  expect(gapRight, 'primary action is not flush right').toBeLessThan(24);
  expect(gapLeft, 'primary action is hugging the left edge').toBeGreaterThan(gapRight);
}

test.describe('Normalizer list', () => {
  test('the name is a real control that opens the editor', async ({ page }) => {
    await openNormalizers(page);
    const name = page.locator('#normalizersListContainer .table-name-link').first();
    test.skip(await name.count() === 0, 'no normalizers configured');

    // A <button>, not a styled <td>: it must be keyboard reachable.
    expect(await name.evaluate(el => el.tagName)).toBe('BUTTON');
    await expect(name).toHaveCSS('cursor', 'pointer');

    const label = (await name.textContent()).trim();
    await name.click();
    await expect(page.locator('#normalizerEditorView')).toBeVisible();
    await expect(page.locator('#normalizersView')).toBeHidden();
    // The editor must open the normalizer that was clicked, not a blank form.
    await expect(page.locator('#normalizerName')).toHaveValue(label);
    await expect(page.locator('#normalizerEditorTitle')).not.toContainText('Create');
  });

  test('hover colour shifts so the name reads as clickable', async ({ page }) => {
    await openNormalizers(page);
    const name = page.locator('#normalizersListContainer .table-name-link').first();
    test.skip(await name.count() === 0, 'no normalizers configured');

    const rest = await name.evaluate(el => getComputedStyle(el).color);
    await name.hover();
    const hovered = await name.evaluate(el => getComputedStyle(el).color);
    expect(hovered, 'name gives no hover feedback').not.toBe(rest);
  });

  test('opening via keyboard works', async ({ page }) => {
    await openNormalizers(page);
    const name = page.locator('#normalizersListContainer .table-name-link').first();
    test.skip(await name.count() === 0, 'no normalizers configured');

    await name.focus();
    expect(await name.evaluate(el => el === document.activeElement)).toBeTruthy();
    await page.keyboard.press('Enter');
    await expect(page.locator('#normalizerEditorView')).toBeVisible();
  });

  test('toolbar actions are right-aligned without an inline style', async ({ page }) => {
    await openNormalizers(page);
    const toolbar = page.locator('#normalizersView .admin-toolbar');
    await expect(toolbar).toBeVisible();
    await expectActionsRightAligned(page, toolbar);
    expect(await toolbar.getAttribute('style'), 'alignment patched inline again').toBeFalsy();
  });
});

test.describe('Admin panels', () => {
  // `scope` is the list region only: the group detail panel keeps a legitimate
  // .section-header (a real "Members" title plus its button), which is exactly
  // what that class is for.
  for (const { subtab, panel, scope } of [
    { subtab: 'users', panel: '#settingsSubTabUsers', scope: '#settingsSubTabUsers' },
    { subtab: 'groups', panel: '#settingsSubTabGroups', scope: '#groupsListView' },
    { subtab: 'context', panel: '#settingsSubTabContext', scope: '#settingsSubTabContext' },
  ]) {
    test(`${subtab}: primary action sits right, free of notebook chrome`, async ({ page }) => {
      await openAdmin(page, subtab);
      const toolbar = page.locator(`${panel} .admin-toolbar`).first();
      await expect(toolbar).toBeVisible();
      await expectActionsRightAligned(page, toolbar);

      // The old .section-header carried the notebook cell-header bar into admin.
      expect(await page.locator(`${scope} > .section-header`).count(),
        'notebook cell-header chrome is back in admin').toBe(0);
      expect(await toolbar.getAttribute('style'), 'alignment patched inline again').toBeFalsy();
    });
  }

  // A table capped short of the full-width nav/sub-tab bar reads as truncated,
  // with dead space to its right. The tables must reach the same right edge as
  // the chrome above them. Settings is deliberately exempt: it is a form, and
  // .sp-content caps it to a readable measure.
  test('table panels fill the width rather than stranding space on the right', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await openAdmin(page, 'users');

    const chromeRight = await page.locator('#settingsSubTabs')
      .evaluate(el => el.getBoundingClientRect().right);

    for (const { subtab, table } of [
      { subtab: 'users', table: '#usersListSettings table' },
      { subtab: 'groups', table: '#groupsList table' },
    ]) {
      await page.locator(`#settingsSubTabs .alerts-sub-tab[data-subtab="${subtab}"]`).click();
      const el = page.locator(table);
      if (await el.count() === 0) continue;
      const right = await el.evaluate(n => n.getBoundingClientRect().right);
      expect(chromeRight - right,
        `${subtab} table stops short of the chrome (${right} vs ${chromeRight})`).toBeLessThan(24);
    }
  });

  test('users search filters the table and reports an empty result', async ({ page }) => {
    await openAdmin(page, 'users');
    await expect(page.locator('#usersListSettings table')).toBeVisible();
    const before = await page.locator('#usersListSettings tbody tr').count();
    expect(before).toBeGreaterThan(0);

    await page.locator('#usersSearch').fill('zzz-no-such-user');
    await expect(page.locator('#usersListSettings')).toContainText('No users match');

    await page.locator('#usersSearch').fill('');
    await expect(page.locator('#usersListSettings tbody tr')).toHaveCount(before);
  });

  test('groups search filters the list', async ({ page }) => {
    await openAdmin(page, 'groups');
    const search = page.locator('#groupsSearch');
    await expect(search).toBeVisible();

    const rows = page.locator('#groupsList tbody tr');
    test.skip(await rows.count() === 0, 'no groups configured');
    const before = await rows.count();

    await search.fill('zzz-no-such-group');
    await expect(page.locator('#groupsList')).toContainText('No groups match');

    await search.fill('');
    await expect(page.locator('#groupsList tbody tr')).toHaveCount(before);
  });

  test('groups description sits above the toolbar', async ({ page }) => {
    await openAdmin(page, 'groups');
    const desc = page.locator('#groupsListView .admin-panel-desc');
    await expect(desc).toBeVisible();
    const descBox = await desc.boundingBox();
    const barBox = await page.locator('#groupsListView .admin-toolbar').boundingBox();
    expect(descBox.y, 'description must precede the toolbar').toBeLessThan(barBox.y);
    expect(await desc.getAttribute('style'), 'spacing patched inline again').toBeFalsy();
  });

  test('admin panels do not scroll sideways at a laptop width', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    for (const subtab of ['users', 'groups', 'context', 'settings']) {
      await openAdmin(page, subtab);
      const overflow = await page.evaluate(() =>
        document.documentElement.scrollWidth - document.documentElement.clientWidth);
      expect(overflow, `${subtab} scrolls horizontally`).toBeLessThanOrEqual(0);
    }
  });
});
