// The command palette: one Cmd/Ctrl+K surface whose leading rows follow the page.
//
// Query used to open its own history popover on the shortcut, which left it the
// one page the palette could not navigate from. These pin the merged behaviour:
// Query's rows lead, everything else is still a keystroke away, and Enter only
// ever loads a query; running it is the separate, deliberate Cmd/Ctrl+Enter.
const { test, expect } = require('@playwright/test');
const { login, listFractals, scopeHeader } = require('./fixtures');

const SHORTCUT = process.platform === 'darwin' ? 'Meta+k' : 'Control+k';
const MOD = process.platform === 'darwin' ? 'Meta' : 'Control';

async function openQuery(page) {
  await login(page);
  const [fractal] = await listFractals(page);
  await page.goto(`/#f/${fractal.id}/search`);
  await expect(page.locator('#sidebar a[data-nav="search"]')).toHaveAttribute('aria-current', 'page', { timeout: 15000 });
  return fractal;
}

// A history row only this test owns, so parallel specs cannot change what it finds.
async function recordHistory(page, text) {
  const res = await page.request.post('/api/v1/query-history', {
    headers: await scopeHeader(page),
    data: { query_text: text, time_range: '24h', status: 'ok' },
  });
  expect(res.ok(), 'recording history failed').toBeTruthy();
}

test.describe('command palette', () => {
  test('on Query, recent queries lead and the first is selected', async ({ page }) => {
    await openQuery(page);
    await recordHistory(page, `source=palette-lead-${Date.now()}`);

    await page.keyboard.press(SHORTCUT);
    await expect(page.locator('#palette')).toBeVisible();
    await expect(page.locator('.pal-group').first()).toContainText('Recent queries');
    await expect(page.locator('.pal-item').first()).toHaveClass(/active/);
    await expect(page.locator('#palettePreview')).toContainText('Recent query');
  });

  test('Enter loads a query without running it', async ({ page }) => {
    await openQuery(page);
    const text = `source=palette-load-${Date.now()}`;
    await recordHistory(page, text);

    await page.keyboard.press(SHORTCUT);
    await page.locator('#paletteInput').fill(text);
    await expect(page.locator('.pal-item.active')).toContainText(text);
    // A run starts a tick after the editor fills, so wait for one rather than
    // checking straight away.
    const ran = page.waitForRequest(r => r.method() === 'POST' && /\/api\/v1\/query(\/stream)?$/.test(new URL(r.url()).pathname), { timeout: 2000 })
      .then(() => true, () => false);
    await page.keyboard.press('Enter');

    await expect(page.locator('#palette')).toBeHidden();
    await expect(page.locator('#queryInput')).toHaveValue(text);
    expect(await ran, 'Enter ran the query').toBe(false);
  });

  test('Query can still reach any page', async ({ page }) => {
    const fractal = await openQuery(page);
    await page.keyboard.press(SHORTCUT);
    await page.locator('#paletteInput').fill('coverage');
    await expect(page.locator('.pal-item.active')).toContainText('Alerts > Coverage');
    await page.keyboard.press('Enter');
    await expect(page).toHaveURL(new RegExp(`#f/${fractal.id}/alerts/coverage$`));
  });

  test('the best match is selected, not the first section', async ({ page }) => {
    await openQuery(page);
    // Holds c-o-v-e-r-a-g-e in order, so it matches loosely and sorts first.
    const tag = `agent=e2e-${Date.now()}`;
    await recordHistory(page, `src=cloud verb=read ${tag}`);
    // Fail the server's history search, which would drop the loose match, so
    // the rows stay as they are in the moment before it lands.
    await page.route(url => url.pathname === '/api/v1/query-history' && url.searchParams.has('search'), route => route.abort());
    await page.keyboard.press(SHORTCUT);
    await expect(page.locator('.pal-group').first()).toContainText('Recent queries');
    await page.locator('#paletteInput').fill('coverage');
    await expect(page.locator('.pal-item', { hasText: tag })).toBeVisible();
    await expect(page.locator('.pal-item.active')).toContainText('Alerts > Coverage');
  });

  test('names are shown as text, never as markup', async ({ page }) => {
    await openQuery(page);
    const name = `<img src=x onerror="window.__palXss=1"> ${Date.now()}`;
    const res = await page.request.post('/api/v1/saved-queries', {
      headers: await scopeHeader(page),
      data: { name, query_text: 'source="<b>x</b>"', visibility: 'personal', description: '<script>window.__palXss=1</script>' },
    });
    const saved = (await res.json()).data;
    try {
      await page.keyboard.press(SHORTCUT);
      await page.locator('#paletteInput').fill(name);
      await expect(page.locator('.pal-item.active')).toContainText(name);
      await expect(page.locator('#palettePreview .pal-pv-note')).toContainText('<script>');
      expect(await page.locator('#palette img, #palette script, #palette b').count()).toBe(0);
      expect(await page.evaluate(() => window.__palXss)).toBeUndefined();
    } finally {
      await page.request.delete(`/api/v1/saved-queries/${saved.id}`, { headers: await scopeHeader(page) });
    }
  });

  test('a prefix narrows to one section and Backspace widens it again', async ({ page }) => {
    await openQuery(page);
    await page.keyboard.press(SHORTCUT);
    await page.keyboard.type('#');

    await expect(page.locator('#paletteScope')).toBeVisible();
    await expect(page.locator('#paletteScope')).toContainText('Queries');
    await expect(page.locator('#paletteInput')).toHaveValue('');
    await expect(page.locator('.pal-group', { hasText: 'Actions' })).toHaveCount(0);

    await page.keyboard.press('Backspace');
    await expect(page.locator('#paletteScope')).toBeHidden();
    await expect(page.locator('.pal-group', { hasText: 'Actions' })).toHaveCount(1);
  });

  test('the Queries button opens a saved-first browse view', async ({ page }) => {
    await openQuery(page);
    const name = `palette browse ${Date.now()}`;
    const res = await page.request.post('/api/v1/saved-queries', {
      headers: await scopeHeader(page),
      data: { name, query_text: 'source=palette-browse', description: 'browse description', tags: ['browse-tag'], visibility: 'personal' },
    });
    const saved = (await res.json()).data;
    try {
      await page.locator('#queriesMenuBtn').click();
      await expect(page.locator('#palette')).toBeVisible();
      await expect(page.locator('#paletteInput')).toBeFocused();
      await expect(page.locator('#paletteTabs [role="tab"][aria-selected="true"]')).toContainText('Saved');
      // Browsing has no side preview: rows carry the detail themselves.
      await expect(page.locator('#palettePreview')).toBeHidden();

      await page.locator('#paletteInput').fill(name);
      const row = page.locator('.pal-item.active');
      await expect(row).toContainText(name);
      await expect(row).toContainText('browse description');
      await expect(row).toContainText('source=palette-browse');
      await expect(row).toContainText('browse-tag');
      await expect(row.locator('button', { hasText: 'Edit' })).toBeVisible();
    } finally {
      await page.request.delete(`/api/v1/saved-queries/${saved.id}`, { headers: await scopeHeader(page) });
    }
  });

  test('Tab switches browse tabs and Backspace leaves browsing', async ({ page }) => {
    await openQuery(page);
    await recordHistory(page, `source=palette-tabs-${Date.now()}`);
    await page.locator('#queriesMenuBtn').click();
    await expect(page.locator('#paletteTabs [aria-selected="true"]')).toContainText('Saved');

    await page.keyboard.press('Tab');
    await expect(page.locator('#paletteTabs [aria-selected="true"]')).toContainText('Recent');
    await expect(page.locator('.pal-item').first()).toContainText('source=palette-');
    await page.keyboard.press('Shift+Tab');
    await expect(page.locator('#paletteTabs [aria-selected="true"]')).toContainText('Saved');

    await page.keyboard.press('Backspace');
    await expect(page.locator('#paletteTabs')).toBeHidden();
    await expect(page.locator('#palettePreview')).toBeVisible();
  });

  test('a row button acts on its own row', async ({ page }) => {
    await openQuery(page);
    const text = `source=palette-rowbtn-${Date.now()}`;
    await recordHistory(page, text);
    await page.locator('#queriesMenuBtn').click();
    await page.keyboard.press('Tab');
    const row = page.locator('.pal-item', { hasText: text });
    await row.hover();
    await row.locator('button', { hasText: 'Save as' }).click();
    const form = page.locator('#palettePreview .pal-panel');
    await expect(form).toBeVisible();
    await expect(form.locator('.pal-code')).toContainText(text);
  });

  test('the Queries button no longer advertises the palette shortcut', async ({ page }) => {
    await openQuery(page);
    await expect(page.locator('#queriesMenuBtn kbd, #queriesMenuBtn .toolbar-menu-kbd')).toHaveCount(0);
  });

  test('Cmd/Ctrl+S saves the editor query from the palette', async ({ page }) => {
    await openQuery(page);
    const name = `palette save ${Date.now()}`;
    await page.locator('#queryInput').fill('source=palette-save');

    await page.keyboard.press(SHORTCUT);
    await page.keyboard.press(`${MOD}+s`);
    const form = page.locator('#palettePreview .pal-panel');
    await expect(form).toBeVisible();
    await expect(form.locator('input[name="name"]')).toBeFocused();
    await form.locator('input[name="name"]').fill(name);
    await page.keyboard.press('Enter');

    await expect(form).toBeHidden();
    const res = await page.request.get(`/api/v1/saved-queries?search=${encodeURIComponent(name)}`, { headers: await scopeHeader(page) });
    const saved = ((await res.json()).data || []).find(s => s.name === name);
    expect(saved, 'the query was not saved').toBeTruthy();
    expect(saved.query_text).toBe('source=palette-save');
    await page.request.delete(`/api/v1/saved-queries/${saved.id}`, { headers: await scopeHeader(page) });
  });

  test('Enter on Cancel cancels the form instead of saving it', async ({ page }) => {
    await openQuery(page);
    await page.locator('#queryInput').fill('source=palette-cancel');
    await page.keyboard.press(SHORTCUT);
    await page.keyboard.press(`${MOD}+s`);
    const form = page.locator('#palettePreview .pal-panel');
    await form.locator('input[name="name"]').fill(`palette cancel ${Date.now()}`);
    let posted = false;
    page.on('request', r => { if (r.method() === 'POST' && new URL(r.url()).pathname === '/api/v1/saved-queries') posted = true; });
    await form.locator('[data-panel="cancel"]').focus();
    await page.keyboard.press('Enter');

    await expect(form).toHaveCount(0);
    expect(posted, 'Enter on Cancel saved the query').toBe(false);
  });

  test('Escape closes a form before the palette', async ({ page }) => {
    await openQuery(page);
    await page.locator('#queryInput').fill('source=palette-escape');
    await page.keyboard.press(SHORTCUT);
    await page.keyboard.press(`${MOD}+s`);
    await expect(page.locator('#palettePreview .pal-panel')).toBeVisible();

    await page.keyboard.press('Escape');
    await expect(page.locator('#palettePreview .pal-panel')).toHaveCount(0);
    await expect(page.locator('#paletteInput')).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(page.locator('#palette')).toBeHidden();
  });
});

test.describe('command palette, narrow window', () => {
  test.use({ viewport: { width: 900, height: 800 } });

  test('the preview stacks under the results', async ({ page }) => {
    await openQuery(page);
    await recordHistory(page, `source=palette-narrow-${Date.now()}`);
    await page.keyboard.press(SHORTCUT);
    await expect(page.locator('.pal-item.active')).toBeVisible();

    const list = await page.locator('#paletteList').boundingBox();
    const preview = await page.locator('#palettePreview').boundingBox();
    expect(preview.y).toBeGreaterThanOrEqual(list.y + list.height - 1);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
});
