// The notebook rail as the annotation surface.
//
// The rail is where captures are made, so it has to be where they are written
// up: a comment typed days later during cleanup is a comment nobody types. None
// of this is checkable statically. Editing evidence writes to the comment, not
// to the section, so a wrong target would look identical in the markup and lose
// the text on the next read. Dragging must rewrite notebook order and must not
// be offered against the computed chronological order, where a drop would be
// silently discarded.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
  expect((await res.json()).success, 'login rejected').toBeTruthy();
}

async function scopeHeader(page) {
  const id = await page.evaluate(() => FractalContext.currentFractal && FractalContext.currentFractal.id);
  const isPrism = await page.evaluate(() => FractalContext.isPrism && FractalContext.isPrism());
  return { 'X-Bifract-Scope': `${isPrism ? 'prism' : 'fractal'}:${id}` };
}

// Land on a fractal with logs, in a notebook of this test's own making so the
// assertions do not depend on what else is in the fixture.
async function openRailWithCaptures(page, count) {
  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });

  const rows = page.locator('.fractal-listing-table tbody tr');
  let found = false;
  for (let i = 0; i < await rows.count(); i++) {
    await rows.nth(i).locator('td').first().click();
    await page.locator('#fractalSearchTabBtn').click();
    await page.locator('#queryInput').waitFor({ timeout: 15000 });
    await page.locator('#timePickerBtn').click();
    await page.locator('#timePickerPanel .tp-preset[data-value="all"]').click();
    await page.locator('#queryInput').fill('*');
    await page.locator('#executeBtn').click();
    if (await page.locator('#resultsTable .result-row').first().isVisible({ timeout: 20000 }).catch(() => false)) {
      found = true;
      break;
    }
    await page.goto('/');
    await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  }
  expect(found, 'no fractal with logs').toBeTruthy();

  const headers = await scopeHeader(page);
  const made = await page.request.post('/api/v1/notebooks', {
    headers,
    data: { name: `e2e-rail-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
  });
  expect(made.ok(), 'could not create a notebook').toBeTruthy();
  const notebookId = (await made.json()).data.id;
  await page.request.put('/api/v1/notebooks/active', { headers, data: { notebook_id: notebookId } });

  await page.reload();
  await page.locator('#queryInput').waitFor({ timeout: 15000 });
  await page.locator('#executeBtn').click();
  await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 20000 });

  // The rail no longer forces itself open on every capture, so open it the same
  // way the reveal path does.
  await page.evaluate(() => RailPanel.open('notebook'));
  await page.locator('#nbrName').waitFor({ timeout: 15000 });

  const starred = [];
  const resultRows = page.locator('#resultsTable .result-row');
  for (let i = 0; starred.length < count && i < await resultRows.count(); i++) {
    const star = resultRows.nth(i).locator('.sg-star');
    if (await star.count() === 0) continue;
    if (await star.getAttribute('aria-pressed') === 'true') continue;
    await resultRows.nth(i).hover();
    await star.click();
    starred.push(await star.getAttribute('data-log-id'));
    await expect(resultRows.nth(i)).toHaveClass(/starred/, { timeout: 10000 });
  }
  expect(starred.length, 'not enough rows to star').toBe(count);

  await expect(page.locator('#nbrList .nbr-row')).toHaveCount(count, { timeout: 10000 });
  return { notebookId, headers, starred };
}

test.describe('notebook rail', () => {
  // Each case scans for a populated fractal, makes its own notebook and stars
  // real rows before it asserts anything, which does not fit the default budget.
  test.describe.configure({ timeout: 120000 });

  test('writes a comment and tags onto the captured event', async ({ page }) => {
    await login(page);
    const { notebookId, headers, starred } = await openRailWithCaptures(page, 1);

    try {
      await page.locator('#nbrList .nbr-row').first().hover();
      await page.locator('#nbrList .nbr-row [data-act="edit"]').first().click();
      const editor = page.locator('#nbrList .nbr-editor');
      await expect(editor).toBeVisible();

      await editor.locator('.nbr-editor-title').fill('beaconing host');
      await editor.locator('.nbr-editor-text').fill('outbound every 60s');
      await editor.locator('.nbr-editor-tags').fill('lateral, c2');
      await editor.locator('[data-act="save"]').click();
      await expect(editor).toHaveCount(0, { timeout: 10000 });

      // The comment is the record. Writing in the rail has to reach it, or the
      // notebook and the comments tab disagree again.
      await expect.poll(async () => {
        const res = await page.request.get(`/api/v1/logs/${starred[0]}/comments`, { headers });
        const list = (await res.json()).data || [];
        return list.length ? { text: list[0].text, tags: (list[0].tags || []).slice().sort() } : null;
      }, { timeout: 10000 }).toEqual({ text: 'outbound every 60s', tags: ['c2', 'lateral'] });

      // And the outline shows what was written, not a stale copy of it.
      await expect(page.locator('#nbrList .nbr-row-title').first()).toHaveText('beaconing host');
      await expect(page.locator('#nbrList .nbr-tag')).toHaveCount(2);
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/evidence/${starred[0]}`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  test('reorders by dragging, and only in notebook order', async ({ page }) => {
    await login(page);
    const { notebookId, headers, starred } = await openRailWithCaptures(page, 3);

    try {
      // The rail opens on the chronology, which is computed and so cannot be
      // dragged. Reordering is a property of notebook order.
      await expect(page.locator('#nbrOrderLabel')).toHaveText('Event order');
      await expect(page.locator('#nbrList .nbr-row').first()).not.toHaveAttribute('draggable', 'true');
      await page.locator('#nbrOrderToggle').click();
      await expect(page.locator('#nbrOrderLabel')).toHaveText('Notebook order');

      const rows = page.locator('#nbrList .nbr-row');
      await expect(rows.first()).toHaveAttribute('draggable', 'true');

      const before = await rows.allTextContents();
      await rows.nth(0).dragTo(rows.nth(2));

      // The moved row lands after its target and the change survives a reload,
      // which is what says the order was written rather than only repainted.
      await expect.poll(async () => (await page.locator('#nbrList .nbr-row').allTextContents())[0],
        { timeout: 10000 }).toBe(before[1]);

      await page.reload();
      await page.locator('#queryInput').waitFor({ timeout: 15000 });
      await page.evaluate(() => RailPanel.open('notebook'));
      await page.locator('#nbrName').waitFor({ timeout: 15000 });
      await page.locator('#nbrOrderToggle').click();
      await expect(page.locator('#nbrOrderLabel')).toHaveText('Notebook order');
      await expect.poll(async () => (await page.locator('#nbrList .nbr-row').allTextContents())[0],
        { timeout: 15000 }).toBe(before[1]);
    } finally {
      for (const logId of starred) {
        await page.request.delete(`/api/v1/notebooks/${notebookId}/evidence/${logId}`, { headers });
      }
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  test('removes a capture from the rail, clearing its star', async ({ page }) => {
    await login(page);
    const { notebookId, headers, starred } = await openRailWithCaptures(page, 1);

    try {
      const row = page.locator('#nbrList .nbr-row').first();
      await row.hover();
      await row.locator('[data-act="delete"]').click();

      await expect(page.locator('#nbrList .nbr-row')).toHaveCount(0, { timeout: 10000 });

      // Removing an event that carried no written comment takes the star
      // comment with it, rather than leaving one nothing points at.
      await expect.poll(async () => {
        const res = await page.request.get(`/api/v1/logs/${starred[0]}/comments`, { headers });
        return ((await res.json()).data || []).length;
      }, { timeout: 10000 }).toBe(0);

      const star = page.locator(`#resultsTable .sg-star[data-log-id="${starred[0]}"]`);
      await expect(star).toHaveAttribute('aria-pressed', 'false', { timeout: 10000 });
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });
});
