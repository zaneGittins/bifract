// Switching fractal or prism must never show one scope's results under another.
// The landing sample is mocked and tagged with the scope each request carries,
// and /select can be held open, so the window between asking for a switch and
// the server answering can be inspected.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

const rowsOf = (fractal) => `ROWS-fractal:${fractal.id}`;

async function mockLanding(page) {
  const loads = [];
  await page.route('**/api/v1/logs/recent*', (route) => {
    const scope = route.request().headers()['x-bifract-scope'];
    loads.push(scope);
    const results = [{ timestamp: new Date().toISOString(), norm_log: JSON.stringify({ msg: `ROWS-${scope}` }), log_id: `id-${loads.length}`, fractal_id: '' }];
    route.fulfill({ json: { success: true, results, count: 1, field_order: ['timestamp', 'norm_log', 'log_id'] } });
  });
  await page.route('**/api/v1/logs/histogram*', (route) => route.fulfill({ json: { success: true, histogram: [] } }));
  return loads;
}

// Hold the next /select for this fractal until release('ok' | 'fail').
async function holdSelect(page, fractalID) {
  let release;
  const released = new Promise((resolve) => { release = resolve; });
  let seen;
  const requested = new Promise((resolve) => { seen = resolve; });
  await page.route(`**/api/v1/fractals/${fractalID}/select`, async (route) => {
    seen();
    if (await released === 'fail') await route.fulfill({ status: 500, json: { success: false, error: 'held' } });
    else await route.continue();
  });
  return { requested, release };
}

const created = [];

async function createFractals(page, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const res = await page.request.post('/api/v1/fractals', {
      data: { name: `e2e-scope-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, description: 'scope switch test' },
    });
    expect(res.ok(), 'could not create a fractal').toBeTruthy();
    const fractal = (await res.json()).data;
    created.push(fractal.id);
    out.push(fractal);
  }
  return out;
}

// Open the first fractal's search tab with its landing sample on screen.
async function openScopes(page, n) {
  await login(page);
  const fractals = await createFractals(page, n);
  const loads = await mockLanding(page);
  await page.goto(`/#f/${fractals[0].id}/search`);
  await expect(page.locator('#resultsTable')).toContainText(rowsOf(fractals[0]));
  return { fractals, loads };
}

const currentScope = (page) => page.evaluate(() => FractalContext.currentFractal.id);

test.afterEach(async ({ page }) => {
  while (created.length) await page.request.delete(`/api/v1/fractals/${created.pop()}`);
});

test.describe('scope switch', () => {
  test('hides the previous scope\'s results until the switch lands', async ({ page }) => {
    const { fractals: [a, b] } = await openScopes(page, 2);
    const held = await holdSelect(page, b.id);

    await page.evaluate((id) => { FractalSelector.selectFractal(id); }, b.id);
    await held.requested;
    await expect(page.locator('#resultsTable')).toBeHidden();

    held.release('ok');
    await expect(page.locator('#resultsTable')).toContainText(rowsOf(b));
    await expect(page.locator('#resultsTable')).toBeVisible();
    await expect(page.locator('#resultsTable')).not.toContainText(rowsOf(a));
  });

  test('a refused switch shows the current results again without reloading them', async ({ page }) => {
    const { fractals: [a, b], loads } = await openScopes(page, 2);
    const held = await holdSelect(page, b.id);

    await page.evaluate((id) => { FractalSelector.selectFractal(id); }, b.id);
    await held.requested;
    held.release('fail');

    await expect(page.locator('#resultsTable')).toBeVisible();
    await expect(page.locator('#resultsTable')).toContainText(rowsOf(a));
    expect(await currentScope(page)).toBe(a.id);
    expect(loads).toEqual([`fractal:${a.id}`]);
  });

  test('overlapping switches settle on the last one asked for', async ({ page }) => {
    const { fractals: [, b, c] } = await openScopes(page, 3);
    const held = await holdSelect(page, b.id);

    // Back/Forward and the listing switch without the selector's busy guard.
    await page.evaluate((f) => { window.__slowSwitch = FractalContext.setCurrentFractal(f); }, b);
    await held.requested;
    await page.evaluate((f) => FractalContext.setCurrentFractal(f), c);
    await expect(page.locator('#resultsTable')).toContainText(rowsOf(c));

    held.release('ok');
    expect(await page.evaluate(() => window.__slowSwitch)).toBe(false);
    expect(await currentScope(page)).toBe(c.id);
    await expect(page.locator('#resultsTable')).toContainText(rowsOf(c));
    await expect(page.locator('#resultsTable')).toBeVisible();
  });

  test('a share link waiting on a switch does not run in another scope', async ({ page }) => {
    const { fractals: [, b, c] } = await openScopes(page, 3);

    await page.evaluate((id) => {
      QueryExecutor.pendingShareData = { query: 'shared_marker="1"', timeRangeValue: '24h', contextId: id, isPrismShare: false };
      QueryExecutor.isProcessingSharedQuery = true;
    }, b.id);
    await page.evaluate((f) => FractalContext.setCurrentFractal(f), c);

    await expect(page.locator('#resultsTable')).toContainText(rowsOf(c));
    await expect(page.locator('#queryInput')).toHaveValue('');
  });
});
