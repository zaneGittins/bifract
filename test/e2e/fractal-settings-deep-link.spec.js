// Fractal settings opened by URL must show the stored fractal, not the id and
// name the router knows from the link. Showing a stub here also seeds the
// Lifecycle controls with "Never" and no quota, which a save would then write.
const { test, expect } = require('@playwright/test');
const { login, listFractals } = require('./fixtures');

const GIB = 1024 ** 3;

async function createConfiguredFractal(page) {
  await login(page);
  const res = await page.request.post('/api/v1/fractals', {
    data: { name: `e2e-deep-link-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, description: 'deep link settings test' },
  });
  expect(res.ok(), 'could not create a fractal').toBeTruthy();
  const { id } = (await res.json()).data;
  expect((await page.request.put(`/api/v1/fractals/${id}/retention`, { data: { retention_days: 30 } })).ok()).toBeTruthy();
  expect((await page.request.put(`/api/v1/fractals/${id}/disk-quota`, { data: { quota_bytes: 5 * GIB, action: 'rollover' } })).ok()).toBeTruthy();
  const stored = await page.request.get(`/api/v1/fractals/${id}`);
  return (await stored.json()).data;
}

async function expectStoredValues(page, fractal) {
  await expect(page.locator('#manageSubTabs')).toBeVisible({ timeout: 15000 });
  await expect(page.locator('#manageFractalCreatedBy')).toHaveText(fractal.created_by);
  await expect(page.locator('#manageFractalDescription')).toHaveText('deep link settings test');
  await expect(page.locator('#manageFractalRetentionSelect')).toHaveValue('30');
  await expect(page.locator('#manageFractalQuotaInput')).toHaveValue('5');
  await expect(page.locator('#manageFractalQuotaAction')).toHaveValue('rollover');
}

test.describe('Fractal settings by URL', () => {
  let fractal;
  test.beforeEach(async ({ page }) => { fractal = await createConfiguredFractal(page); });
  test.afterEach(async ({ page }) => { await page.request.delete(`/api/v1/fractals/${fractal.id}`); });

  test('a deep link shows the stored fractal', async ({ page }) => {
    await page.goto(`/#f/${fractal.id}/manage`);
    await expectStoredValues(page, fractal);
  });

  test('a reload keeps the stored fractal', async ({ page }) => {
    await page.goto(`/#f/${fractal.id}/manage`);
    await expect(page.locator('#manageSubTabs')).toBeVisible({ timeout: 15000 });
    await page.reload();
    await expectStoredValues(page, fractal);
  });

  test('a tab-only link restores the stored fractal', async ({ page }) => {
    await page.goto('/');
    await page.evaluate((f) => localStorage.setItem('bifract_current_context',
      JSON.stringify({ id: f.id, name: f.name, type: 'fractal' })), fractal);
    await page.goto('/#manage');
    await page.reload();
    await expectStoredValues(page, fractal);
  });
});

test('a deep link shows the stored log count', async ({ page }) => {
  await login(page);
  const counted = (await listFractals(page)).find(f => f.log_count > 0);
  test.skip(!counted, 'no fractal on this stack has a stored log count yet');

  await page.goto(`/#f/${counted.id}/manage`);
  await expect(page.locator('#manageFractalLogCount')).toHaveText(Number(counted.log_count).toLocaleString('en-US'), { timeout: 15000 });
});
