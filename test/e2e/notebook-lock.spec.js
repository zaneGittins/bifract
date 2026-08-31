// Locking a notebook.
//
// A locked notebook is the record of an investigation, so what matters is that
// the page stops offering edits and the server stops accepting them. Neither is
// visible statically: the read-only view is a class the stylesheet keys off, and
// the interesting case is the one nobody clicks through by hand, where a star is
// aimed at a notebook that was locked out from under it.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
  expect((await res.json()).success, 'login rejected').toBeTruthy();
}

async function openFirstFractal(page) {
  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  await page.locator('.fractal-listing-table tbody tr td').first().click();
  await page.locator('#fractalSearchTabBtn').waitFor({ timeout: 15000 });
  const id = await page.evaluate(() => FractalContext.currentFractal && FractalContext.currentFractal.id);
  return { 'X-Bifract-Scope': `fractal:${id}` };
}

// Open a notebook of this test's own making in the editor.
async function openNotebook(page, headers, name) {
  const made = await page.request.post('/api/v1/notebooks', {
    headers,
    data: { name, time_range_type: '24h', max_results_per_section: 100 },
  });
  expect(made.ok(), 'could not create a notebook').toBeTruthy();
  const id = (await made.json()).data.id;

  await page.request.post(`/api/v1/notebooks/${id}/sections`, {
    headers,
    data: { section_type: 'markdown', title: 'Hypothesis', content: 'macro dropped a loader', append: true },
  });

  await page.locator('#fractalNotebooksTabBtn').click();
  await page.evaluate(nb => Notebooks.openNotebook(nb), id);
  await expect(page.locator('#notebookEditor')).toBeVisible({ timeout: 15000 });
  return id;
}

test.describe('notebook lock', () => {
  test.describe.configure({ timeout: 120000 });

  test('locks the editor read-only and unlocks it again', async ({ page }) => {
    page.on('dialog', d => d.accept());
    await login(page);
    const headers = await openFirstFractal(page);
    const notebookId = await openNotebook(page, headers, `e2e-lock-${Date.now()}`);

    try {
      const editor = page.locator('#notebookEditor');
      await expect(editor).not.toHaveClass(/notebook-locked/);
      await expect(page.locator('#notebookLockBadge')).toBeHidden();
      await expect(page.locator('#addSectionBtn')).toBeVisible();

      await page.locator('#notebookLockBtn').click();

      // The badge names who sealed it, and the controls that would change it go
      // away rather than sitting there rejecting clicks.
      await expect(editor).toHaveClass(/notebook-locked/, { timeout: 10000 });
      await expect(page.locator('#notebookLockBadge')).toContainText('Locked by');
      await expect(page.locator('#addSectionBtn')).toBeHidden();
      await expect(page.locator('#runAllSectionsBtn')).toBeHidden();
      await expect(page.locator('#saveNotebookBtn')).toBeHidden();
      await expect(page.locator('#notebookLockBtn')).toContainText('Unlock');

      // Content is still readable: locked is read-only, not hidden.
      await expect(page.locator('#notebookSections')).toContainText('macro dropped a loader');

      // And the server agrees, which is the half that actually protects it.
      const rejected = await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
        headers,
        data: { section_type: 'markdown', content: 'tampered', append: true },
      });
      expect(rejected.status()).toBe(409);

      // Reopening has to show the same state, or there is no way back: the
      // detail endpoint used to drop locked_at, so the button read "Lock" on a
      // locked notebook and nothing could unlock it.
      await page.reload();
      await page.locator('#fractalNotebooksTabBtn').click();
      await page.evaluate(nb => Notebooks.openNotebook(nb), notebookId);
      await expect(page.locator('#notebookEditor')).toHaveClass(/notebook-locked/, { timeout: 15000 });
      await expect(page.locator('#notebookLockBtn')).toContainText('Unlock');
      await expect(page.locator('#notebookLockBadge')).toContainText('Locked by');

      await page.locator('#notebookLockBtn').click();
      await expect(editor).not.toHaveClass(/notebook-locked/, { timeout: 10000 });
      await expect(page.locator('#addSectionBtn')).toBeVisible();
      const accepted = await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
        headers,
        data: { section_type: 'markdown', content: 'editable again', append: true },
      });
      expect(accepted.ok()).toBeTruthy();
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  test('refuses to lock a notebook whose queries never ran', async ({ page }) => {
    page.on('dialog', d => d.accept());
    await login(page);
    const headers = await openFirstFractal(page);
    const notebookId = await openNotebook(page, headers, `e2e-lock-pending-${Date.now()}`);

    try {
      await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
        headers,
        data: { section_type: 'query', title: 'Step 1', content: '*', append: true },
      });

      // Sealing a query that never ran would freeze a permanently blank section.
      const refused = await page.request.post(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      expect(refused.status()).toBe(409);
      expect((await refused.json()).error).toContain('never been run');

      const sections = (await (await page.request.get(`/api/v1/notebooks/${notebookId}`, { headers })).json()).data.sections;
      const query = sections.find(s => s.section_type === 'query');
      await page.request.put(`/api/v1/notebooks/${notebookId}/sections/${query.id}/results`, {
        headers,
        data: {
          last_executed_at: '2026-01-01T12:00:00Z',
          last_results: JSON.stringify({ results: [{ host: 'ws-01' }], count: 1, field_order: ['host'] }),
        },
      });

      const locked = await page.request.post(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      expect(locked.ok()).toBeTruthy();

      // The export says the notebook is frozen and when its rows were read, and
      // the results table is valid markdown rather than a run-together line.
      const md = await (await page.request.get(`/api/v1/notebooks/${notebookId}/export?format=md`, { headers })).text();
      expect(md).toContain('Locked ');
      expect(md).toContain('Results as of 2026-01-01 12:00 UTC');
      expect(md).toContain('| host |\n| --- |\n| ws-01 |');
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  // Notebooks.init() runs on every visit to the tab, and it used to re-add a
  // listener to every static button each time, so one click on Lock (or Save,
  // or Run All) fired as many requests as the tab had been opened.
  test('one click sends one request after repeated tab visits', async ({ page }) => {
    const locks = [];
    const dialogs = [];
    page.on('dialog', d => { dialogs.push(1); d.accept(); });
    page.on('request', r => { if (r.url().includes('/lock')) locks.push(r.method()); });

    await login(page);
    const headers = await openFirstFractal(page);

    const made = await page.request.post('/api/v1/notebooks', {
      headers,
      data: { name: `e2e-lock-once-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
    });
    const notebookId = (await made.json()).data.id;
    await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
      headers,
      data: { section_type: 'markdown', content: 'note', append: true },
    });

    try {
      for (let i = 0; i < 4; i++) {
        await page.locator('#fractalNotebooksTabBtn').click();
        await page.locator('#fractalSearchTabBtn').click();
      }
      await page.locator('#fractalNotebooksTabBtn').click();
      await page.evaluate(nb => Notebooks.openNotebook(nb), notebookId);
      await expect(page.locator('#notebookEditor')).toBeVisible({ timeout: 15000 });

      // A notebook takes the whole pane: the sub-tab switcher belongs to the
      // listing it switches between.
      await expect(page.locator('#investigationSubTabs')).toBeHidden();

      await page.locator('#notebookLockBtn').click();
      await expect(page.locator('#notebookLockBtn')).toContainText('Unlock', { timeout: 10000 });

      expect(dialogs.length).toBe(1);
      expect(locks).toEqual(['POST']);
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  test('a locked notebook stops being the capture target', async ({ page }) => {
    page.on('dialog', d => d.accept());
    await login(page);
    const headers = await openFirstFractal(page);

    const made = await page.request.post('/api/v1/notebooks', {
      headers,
      data: { name: `e2e-lock-target-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
    });
    const notebookId = (await made.json()).data.id;

    try {
      await page.request.put('/api/v1/notebooks/active', { headers, data: { notebook_id: notebookId } });
      expect((await (await page.request.get('/api/v1/notebooks/active', { headers })).json()).data.notebook_id)
        .toBe(notebookId);

      await page.request.post(`/api/v1/notebooks/${notebookId}/lock`, { headers });

      // Locking clears it for everyone, so nobody's next star lands on a
      // refusal they never chose.
      expect((await (await page.request.get('/api/v1/notebooks/active', { headers })).json()).data.notebook_id).toBe('');

      // It also cannot be chosen again while locked.
      const reselect = await page.request.put('/api/v1/notebooks/active', { headers, data: { notebook_id: notebookId } });
      expect(reselect.status()).toBe(409);

      // Filing into it directly is refused too, which is the path a star takes.
      const star = await page.request.post('/api/v1/comments', {
        headers,
        data: {
          log_id: Date.now().toString(16).padStart(32, 'a'),
          log_timestamp: new Date().toISOString(),
          text: '',
          notebook_id: notebookId,
        },
      });
      expect(star.status()).toBe(409);
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}/lock`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });
});
