// The shared destructive-action dialog. Every request it would send is intercepted,
// so nothing on the stack is actually cleared.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal, listFractals } = require('./fixtures');

async function openDangerZone(page) {
  await login(page);
  await page.goto('/');
  await openNav(page, 'settings');
  await page.locator('#settingsSubTabs .alerts-sub-tab[data-subtab="settings"]').click();
  await page.locator('#settingsSectionRail [data-section="spSectionDanger"]').click();
}

const dialog = page => page.locator('.modal.danger-confirm');

test('a typed phrase gates the action and must match exactly', async ({ page }) => {
  await openDangerZone(page);
  const trigger = page.locator('#endpointBaselinesClearBtn');
  await trigger.click();

  const d = dialog(page);
  await expect(d).toBeVisible();
  await expect(d.locator('.danger-confirm-title')).toHaveText('Clear Endpoint Analytics Baselines');
  const ok = d.locator('.danger-confirm-ok');
  const input = d.locator('#dangerConfirmInput');
  await expect(input).toBeFocused();
  await expect(ok).toBeDisabled();

  await input.fill('clear baselines');
  await expect(ok).toBeDisabled();
  await input.fill('CLEAR BASELINES');
  await expect(ok).toBeEnabled();

  await page.keyboard.press('Escape');
  await expect(d).toBeHidden();
  await expect(trigger).toBeFocused();
});

test('a failure stays in the dialog and a retry can succeed', async ({ page }) => {
  let calls = 0;
  await page.route('**/api/v1/system/endpoint-analysis/clear', route => {
    calls++;
    return calls === 1
      ? route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"ClickHouse unavailable"}' })
      : route.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true}' });
  });
  await openDangerZone(page);
  await page.locator('#endpointBaselinesClearBtn').click();

  const d = dialog(page);
  await d.locator('#dangerConfirmInput').fill('CLEAR BASELINES');
  await page.keyboard.press('Enter');
  await expect(d.locator('.danger-confirm-error')).toHaveText('ClickHouse unavailable');
  await expect(d).toBeVisible();

  await d.locator('.danger-confirm-ok').click();
  await expect(d).toBeHidden();
  expect(calls).toBe(2);
});

test('a reversible action confirms without a phrase', async ({ page }) => {
  await page.route('**/api/v1/system/archive/spool/clear', route =>
    route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"intercepted"}' }));
  await openDangerZone(page);
  const trigger = page.locator('#archiveClearSpoolBtn');
  test.skip(await trigger.isDisabled(), 'archive is enabled, so the spool cannot be cleared here');
  await trigger.click();

  const d = dialog(page);
  await expect(d).toBeVisible();
  await expect(d.locator('#dangerConfirmInput')).toBeHidden();
  await expect(d.locator('.danger-confirm-cancel')).toBeFocused();
  await expect(d.locator('.danger-confirm-ok')).toBeEnabled();

  await d.locator('.danger-confirm-cancel').click();
  await expect(d).toBeHidden();
});

test('a fractal dialog refuses to act once the selected fractal changes', async ({ page }) => {
  const fractals = await listFractals(page);
  test.skip(fractals.length < 2, 'needs two fractals');
  const [a, b] = fractals;
  let calls = 0;
  await page.route('**/api/v1/logs?*', route => { calls++; return route.abort(); });

  await openFractal(page, 'manage', a.name);
  await page.locator('.alerts-sub-tab[data-subtab="danger"][onclick*="switchSubTab"]').click();
  await page.locator('#manageClearFractalLogsBtn').click();

  const d = dialog(page);
  await d.locator('#dangerConfirmInput').fill(a.name);
  await page.evaluate(f => window.FractalContext.setCurrentFractal(f), b);
  await d.locator('.danger-confirm-ok').click();

  await expect(d.locator('.danger-confirm-error')).toContainText('selected fractal changed');
  expect(calls).toBe(0);
});

test('Tab stays inside the dialog', async ({ page }) => {
  await openDangerZone(page);
  await page.locator('#endpointBaselinesClearBtn').click();
  const d = dialog(page);
  await d.locator('#dangerConfirmInput').fill('CLEAR BASELINES');
  for (let i = 0; i < 6; i++) {
    await page.keyboard.press(i % 2 ? 'Shift+Tab' : 'Tab');
    expect(await page.evaluate(() => !!document.activeElement.closest('.modal.danger-confirm'))).toBe(true);
  }
  await page.keyboard.press('Escape');
});
