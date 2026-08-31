// The star gutter on the Query tab's results table.
//
// The risky part of this feature is structural and invisible to a static check:
// the gutter is a real column, so a mistake shifts every data cell one place to
// the right, breaks the colgroup-to-header alignment that column resizing keys
// off, and misreads which field a right-click landed on. Only a rendered table
// shows that. The reveal-on-hover behaviour and the "absent until the scope uses
// notebooks" rule are likewise not checkable from the markup alone.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
  const body = await res.json();
  expect(body.success, `login rejected: ${JSON.stringify(body)}`).toBeTruthy();
}

// The scope the page is currently on, for API calls made outside the page's own
// fetch (which is what stamps the scope header).
async function currentScope(page) {
  const id = await page.evaluate(() => window.FractalContext && FractalContext.currentFractal && FractalContext.currentFractal.id);
  const isPrism = await page.evaluate(() => window.FractalContext && FractalContext.isPrism && FractalContext.isPrism());
  return { header: { 'X-Bifract-Scope': `${isPrism ? 'prism' : 'fractal'}:${id}` } };
}

// Pick a notebook to capture into, for the tests that are not about what happens
// when nobody has picked one.
async function ensureActiveNotebook(page) {
  const { header } = await currentScope(page);

  const list = await page.request.get('/api/v1/notebooks?limit=1', { headers: header });
  const notebooks = (await list.json()).data;
  let id = Array.isArray(notebooks) && notebooks.length ? notebooks[0].id : null;

  if (!id) {
    const made = await page.request.post('/api/v1/notebooks', {
      headers: header,
      data: { name: `e2e-star-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
    });
    expect(made.ok(), 'could not create a notebook').toBeTruthy();
    id = (await made.json()).data.id;
  }

  const set = await page.request.put('/api/v1/notebooks/active', { headers: header, data: { notebook_id: id } });
  expect(set.ok(), 'could not set the active notebook').toBeTruthy();
  return id;
}

// The first fractal that holds at least one log, so the table has rows to star.
async function openSearchOnPopulatedFractal(page) {
  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });

  const rows = page.locator('.fractal-listing-table tbody tr');
  const count = await rows.count();
  for (let i = 0; i < count; i++) {
    await rows.nth(i).locator('td').first().click();
    await page.locator('#fractalSearchTabBtn').click();
    await page.locator('#queryInput').waitFor({ timeout: 15000 });

    await page.locator('#timePickerBtn').click();
    await page.locator('#timePickerPanel .tp-preset[data-value="all"]').click();
    await page.locator('#queryInput').fill('*');
    await page.locator('#executeBtn').click();

    const firstRow = page.locator('#resultsTable .result-row').first();
    if (await firstRow.isVisible({ timeout: 20000 }).catch(() => false)) return true;

    await page.goto('/');
    await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  }
  return false;
}

test.describe('star gutter', () => {
  test('is a real column that keeps the table aligned', async ({ page }) => {
    await login(page);
    expect(await openSearchOnPopulatedFractal(page), 'no fractal with logs').toBeTruthy();

    const table = page.locator('#resultsTable table.results-table');
    await expect(table).toBeVisible();

    const hasGutter = await table.evaluate(t => t.classList.contains('has-gutter'));
    test.skip(!hasGutter, 'scope has no notebooks or comments, so the gutter is correctly absent');

    // One <col> per <th>, or resizing drags the wrong column.
    const counts = await table.evaluate(t => ({
      cols: t.querySelectorAll('colgroup col').length,
      ths: t.querySelectorAll('thead th').length,
      tds: t.querySelector('tbody tr').children.length,
      gutterTh: t.querySelectorAll('thead th.sg-gutter').length,
      gutterFirst: t.querySelector('thead th').classList.contains('sg-gutter'),
      cellFirst: t.querySelector('tbody tr').children[0].classList.contains('sg-gutter'),
    }));
    expect(counts.cols).toBe(counts.ths);
    expect(counts.tds).toBe(counts.ths);
    expect(counts.gutterTh).toBe(1);
    expect(counts.gutterFirst).toBeTruthy();
    expect(counts.cellFirst).toBeTruthy();

    // Not a data column: never sortable, reorderable or resizable.
    const gutterTh = table.locator('thead th.sg-gutter');
    expect(await gutterTh.evaluate(th => th.hasAttribute('data-field'))).toBeFalsy();
    expect(await gutterTh.evaluate(th => th.classList.contains('sortable'))).toBeFalsy();
    expect(await gutterTh.locator('.column-resizer').count()).toBe(0);
  });

  test('reveals on hover and stays put', async ({ page }) => {
    await login(page);
    expect(await openSearchOnPopulatedFractal(page), 'no fractal with logs').toBeTruthy();

    const table = page.locator('#resultsTable table.results-table');
    const hasGutter = await table.evaluate(t => t.classList.contains('has-gutter'));
    test.skip(!hasGutter, 'scope has no notebooks or comments');

    const row = page.locator('#resultsTable .result-row').first();
    const star = row.locator('.sg-star');
    test.skip(await star.count() === 0, 'first row is aggregated and has no log to star');

    // Transparent at rest, so a table nobody stars reads as it did before.
    expect(await star.evaluate(el => getComputedStyle(el).opacity)).toBe('0');

    // Revealing it must not move the row's text.
    const before = await row.locator('td').nth(1).boundingBox();
    await row.hover();
    await expect.poll(async () => star.evaluate(el => getComputedStyle(el).opacity)).toBe('1');
    const after = await row.locator('td').nth(1).boundingBox();
    expect(Math.abs(after.x - before.x)).toBeLessThan(1);
  });

  test('stars and unstars a row', async ({ page }) => {
    await login(page);
    expect(await openSearchOnPopulatedFractal(page), 'no fractal with logs').toBeTruthy();

    const table = page.locator('#resultsTable table.results-table');
    const hasGutter = await table.evaluate(t => t.classList.contains('has-gutter'));
    test.skip(!hasGutter, 'scope has no notebooks or comments');

    await ensureActiveNotebook(page);
    await page.reload();
    await page.locator('#queryInput').waitFor({ timeout: 15000 });
    await page.locator('#executeBtn').click();
    await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 20000 });

    const row = page.locator('#resultsTable .result-row').first();
    const star = row.locator('.sg-star');
    test.skip(await star.count() === 0, 'first row is aggregated');
    test.skip(await star.evaluate(el => el.getAttribute('aria-pressed')) === 'true', 'row already starred');

    const logId = await star.getAttribute('data-log-id');
    await row.hover();
    await star.click();

    // Starring writes a comment, which is what makes the row visible to the
    // comments tab and to comments(). The star is meaningless if that is missing.
    await expect.poll(async () => {
      const res = await page.request.get(`/api/v1/logs/${logId}/comments`);
      const body = await res.json();
      return (body.data || []).length;
    }, { timeout: 10000 }).toBeGreaterThan(0);

    await expect(row).toHaveClass(/starred/);
    await expect(star).toHaveAttribute('aria-pressed', 'true');
    // Persistent once starred, not only while hovered.
    await page.locator('#queryInput').hover();
    expect(await star.evaluate(el => getComputedStyle(el).opacity)).toBe('1');

    // And it comes back off, or a mis-click is unrecoverable.
    await row.hover();
    await star.click();
    await expect(star).toHaveAttribute('aria-pressed', 'false', { timeout: 10000 });
    await expect(row).not.toHaveClass(/starred/);
    await expect.poll(async () => {
      const res = await page.request.get(`/api/v1/logs/${logId}/comments`);
      const body = await res.json();
      return (body.data || []).length;
    }, { timeout: 10000 }).toBe(0);
  });

  test('is absent in a scope that has never used notebooks or comments', async ({ page }) => {
    await login(page);

    const created = await page.request.post('/api/v1/fractals', {
      data: { name: `e2e-gutter-${Date.now()}`, description: 'star gutter scope test' },
    });
    expect(created.ok(), 'could not create a fractal').toBeTruthy();
    const fractalId = (await created.json()).data.id;

    try {
      const state = await page.request.get('/api/v1/notebooks/active', {
        headers: { 'X-Bifract-Scope': `fractal:${fractalId}` },
      });
      const body = await state.json();
      expect(body.data.has_notebooks).toBeFalsy();
      expect(body.data.has_comments).toBeFalsy();
      expect(body.data.notebook_id).toBe('');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractalId}`);
    }
  });

  // A star drawn only while the rail is open is a star that vanishes on the next
  // page load, which is what happened: the pinned set was read by the rail's own
  // load path, and that only ran when the rail was showing.
  test('a starred row is still starred after a reload with the rail closed', async ({ page }) => {
    await login(page);
    expect(await openSearchOnPopulatedFractal(page), 'no fractal with logs').toBeTruthy();

    const table = page.locator('#resultsTable table.results-table');
    const hasGutter = await table.evaluate(t => t.classList.contains('has-gutter'));
    test.skip(!hasGutter, 'scope has no notebooks or comments');

    const notebookId = await ensureActiveNotebook(page);
    await page.reload();
    await page.locator('#queryInput').waitFor({ timeout: 15000 });
    await page.locator('#executeBtn').click();
    await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 20000 });

    const row = page.locator('#resultsTable .result-row').first();
    const star = row.locator('.sg-star');
    test.skip(await star.count() === 0, 'first row is aggregated');
    test.skip(await star.getAttribute('aria-pressed') === 'true', 'row already starred');

    const logId = await star.getAttribute('data-log-id');
    await row.hover();
    await star.click();
    await expect(star).toHaveAttribute('aria-pressed', 'true', { timeout: 10000 });

    const { header } = await currentScope(page);
    try {
      await page.reload();
      await page.locator('#queryInput').waitFor({ timeout: 15000 });
      await page.locator('#executeBtn').click();
      await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 20000 });

      // The rail is closed here, which is the normal state. Its markup still
      // exists; what matters is that nothing opened the panel.
      expect(await page.evaluate(() => !!(window.RailPanel && RailPanel.isPaneVisible('notebook')))).toBeFalsy();
      const after = page.locator(`#resultsTable .sg-star[data-log-id="${logId}"]`);
      await expect(after).toHaveAttribute('aria-pressed', 'true', { timeout: 10000 });
      await expect(after.locator('xpath=ancestor::tr[1]')).toHaveClass(/starred/);
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/evidence/${logId}`, { headers: header });
    }
  });

  // Capture must never stop to ask for a name at the moment someone found
  // something, which is the whole reason the scratch notebook exists.
  test('starring with no notebook chosen opens one', async ({ page }) => {
    await login(page);
    expect(await openSearchOnPopulatedFractal(page), 'no fractal with logs').toBeTruthy();

    const { header } = await currentScope(page);
    await page.request.put('/api/v1/notebooks/active', { headers: header, data: { notebook_id: '' } });
    await page.reload();
    await page.locator('#queryInput').waitFor({ timeout: 15000 });
    await page.locator('#executeBtn').click();
    await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 20000 });

    const row = page.locator('#resultsTable .result-row').first();
    const star = row.locator('.sg-star');
    test.skip(await star.count() === 0, 'first row is aggregated');

    const logId = await star.getAttribute('data-log-id');
    await row.hover();
    await star.click();

    // A notebook appears, it becomes the capture target, and the event lands in
    // it. Anything less and the click is silently lost.
    await expect(page.locator('#nbrName')).toHaveText('Untitled notebook', { timeout: 10000 });
    await expect(row).toHaveClass(/starred/, { timeout: 10000 });

    const active = await page.request.get('/api/v1/notebooks/active', { headers: header });
    const notebookId = (await active.json()).data.notebook_id;
    expect(notebookId).toBeTruthy();

    // The scratch notebook is reused across sessions, so assert the starred
    // event is in it rather than that it is the only thing there.
    const summary = await page.request.get(`/api/v1/notebooks/${notebookId}/summary`, { headers: header });
    const sections = (await summary.json()).data.sections;
    const filed = sections.filter(s => s.section_type === 'comment_context' && (s.content || '').includes(logId));
    expect(filed.length).toBe(1);

    // The second star with nothing chosen reuses that notebook rather than
    // leaving a pile of empty ones behind.
    await page.request.put('/api/v1/notebooks/active', { headers: header, data: { notebook_id: '' } });
    const again = await page.request.post('/api/v1/notebooks/active', { headers: header });
    expect((await again.json()).data.notebook_id).toBe(notebookId);

    // Unfile before dropping the notebook, so the empty star comment goes too.
    await page.request.delete(`/api/v1/notebooks/${notebookId}/evidence/${logId}`, { headers: header });
    await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers: header });
  });
});