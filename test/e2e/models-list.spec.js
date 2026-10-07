// Analytics models: the listing as a health dashboard, and template-led creation.
//
// A model whose source stopped, or one still learning, used to read exactly like
// a healthy one. The State, Findings and Last alert columns are the fix, and only
// a rendered page shows whether every row actually carries them. Creates nothing:
// the template test fills the editor and leaves without saving.
const { test, expect } = require('@playwright/test');
const { login, listFractals, openFractal } = require('./fixtures');

// The first fractal that holds at least one model, or null.
async function fractalWithModels(page) {
  for (const f of await listFractals(page)) {
    const res = await page.request.get('/api/v1/models', { headers: { 'X-Bifract-Scope': `fractal:${f.id}` } });
    if (!res.ok()) continue;
    const body = await res.json();
    if ((body?.data || []).length) return f;
  }
  return null;
}

async function openModels(page, fractalName) {
  await openFractal(page, 'models', fractalName);
  await expect(page.locator('#modelsView .models-filters')).toBeVisible({ timeout: 15000 });
}

test.describe('Models listing', () => {
  test('every row carries a state, findings and last alert', async ({ page }) => {
    await login(page);
    const fractal = await fractalWithModels(page);
    test.skip(!fractal, 'no fractal has models');
    await openModels(page, fractal.name);

    const rows = page.locator('.models-table tbody tr');
    await expect(rows.first()).toBeVisible({ timeout: 15000 });

    const headers = (await page.locator('.models-table thead th').allTextContents()).map(h => h.trim());
    for (const want of ['State', 'Findings', 'Last alert']) {
      expect(headers.some(h => h.startsWith(want)), `missing ${want} column in ${headers}`).toBeTruthy();
    }

    // Summaries the server had not read yet render as Checking and the listing
    // polls until they land.
    await expect(page.locator('.models-table .model-state-cell .badge-checking')).toHaveCount(0, { timeout: 30000 });

    const cells = await rows.evaluateAll(trs => trs.map(tr => ({
      name: tr.querySelector('.model-name-link')?.textContent.trim(),
      count: tr.querySelectorAll('td').length,
      state: tr.querySelector('.model-state-cell .model-badge')?.textContent.trim() || '',
      findings: tr.querySelector('.model-findings-cell')?.textContent.trim() || '',
      sparkBars: tr.querySelectorAll('.model-findings-cell .model-spark rect').length,
      lastAlert: tr.querySelector('.model-last-alert-cell')?.textContent.trim() || '',
    })));
    for (const c of cells) {
      expect(c.count, `${c.name}: cells vs header`).toBe(headers.length);
      expect(c.state, `${c.name}: state`).toMatch(/^(Error|Not updating|Not started|Behind|Rebuilding|Backfill|Stale|Learning|Healthy|Unknown)/);
      expect(c.findings, `${c.name}: findings`).toMatch(/^(\d[\d,.KMB]*|n\/a)$/);
      expect([0, 7], `${c.name}: sparkline is one bar per day`).toContain(c.sparkBars);
      expect(c.lastAlert, `${c.name}: last alert`).toMatch(/^(Never|n\/a|today|\d+(s|m|h|d|mo) ago)$/);
    }
  });

  test('a template fills the editor and scores it', async ({ page }) => {
    await login(page);
    const fractal = await fractalWithModels(page);
    test.skip(!fractal, 'no fractal has models');
    await openModels(page, fractal.name);

    await page.locator('#modelsNewBtn').click();
    const gallery = page.locator('#modelTemplateGallery');
    await expect(gallery).toBeVisible();
    expect(await gallery.locator('.me-tpl-card').count()).toBeGreaterThanOrEqual(5);

    await gallery.locator('.me-tpl-card[data-template="new_programs"]').click();

    await expect(page.locator('#modelName')).toHaveValue('new_programs_per_host');
    await expect(page.locator('#modelQueryInput')).toHaveValue(/bifract_category=process_creation/);
    await expect(page.locator('#modelTypeTagValue')).toHaveText('First / Last Seen');
    await expect(page.locator('#modelTypeCards .me-type-card.active')).toHaveAttribute('data-type', 'first_seen');
    await expect(page.locator('#keyField0')).toHaveValue('computer_name');
    await expect(page.locator('#keyField1')).toHaveValue('image');
    await expect(page.locator('#modelDesc')).not.toHaveValue('');

    // Choosing a template runs the score preview.
    await expect(page.locator('#modelResultTabs .ert-tab[data-mode="scores"]')).toHaveClass(/active/);
    await expect(page.locator('#modelScorePreview .score-preview, #modelScorePreview .query-error, #modelScorePreview .es'))
      .toBeVisible({ timeout: 45000 });
  });

  test('a custom preview range is offered beside the presets', async ({ page }) => {
    await login(page);
    const fractal = await fractalWithModels(page);
    test.skip(!fractal, 'no fractal has models');
    await openModels(page, fractal.name);

    await page.locator('#modelsNewBtn').click();
    await page.locator('.me-tpl-card[data-template="outbound_ports"]').click();
    const range = page.locator('#modelPreviewRange');
    await expect(range).toBeHidden();
    await page.locator('#modelPreviewWindow').selectOption('custom');
    await expect(range).toBeVisible();
    await expect(page.locator('#modelPreviewStart')).toHaveValue(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/);
  });

  // The Description textarea sat outside every rule that themes rail inputs and
  // rendered with the browser's light default in the dark theme.
  test('the description field follows the theme', async ({ page }) => {
    await login(page);
    const fractal = await fractalWithModels(page);
    test.skip(!fractal, 'no fractal has models');
    await openModels(page, fractal.name);
    await page.locator('#modelsNewBtn').click();

    const [desc, input] = await Promise.all([
      page.locator('#modelDesc').evaluate(el => getComputedStyle(el).backgroundColor),
      page.locator('#shapePartKey').evaluate(el => getComputedStyle(el).backgroundColor),
    ]);
    expect(desc).toBe(input);
  });
});
