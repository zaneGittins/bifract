// The single destination for collected work, and what leaves it.
//
// Notebooks and the annotations inside them were two top-level tabs for one
// object entered once per investigation, which is most of what made the feature
// feel scattered. The merge is only real if the sub-views actually switch and
// the unfiled bucket answers the question it exists for: what did someone
// annotate and never collect.
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

test.describe('investigation tab', () => {
  test.describe.configure({ timeout: 120000 });

  test('is one tab with notebooks and evidence inside it', async ({ page }) => {
    await login(page);
    await openFirstFractal(page);

    // The Comments tab is gone, not renamed.
    await expect(page.locator('#fractalCommentsTabBtn')).toHaveCount(0);

    await page.locator('#fractalNotebooksTabBtn').click();
    await expect(page.locator('#investigationSubTabs')).toBeVisible();
    await expect(page.locator('#notebooksView')).toBeVisible();
    await expect(page.locator('#commentedView')).toBeHidden();

    await page.locator('[data-investigation-subtab="evidence"]').click();
    await expect(page.locator('#commentedView')).toBeVisible();
    await expect(page.locator('#notebooksView')).toBeHidden();
    await expect(page.locator('[data-investigation-subtab="evidence"]')).toHaveClass(/active/);

    await page.locator('[data-investigation-subtab="notebooks"]').click();
    await expect(page.locator('#notebooksView')).toBeVisible();
  });

  test('separates what was collected from what was only annotated', async ({ page }) => {
    await login(page);
    const headers = await openFirstFractal(page);

    // A synthetic log id with an explicit timestamp: comment creation only looks
    // the log up in ClickHouse when no timestamp is given, and what is under
    // test here is filing, not ingestion.
    const logId = Date.now().toString(16).padStart(32, 'a');
    const logTimestamp = new Date().toISOString();

    const made = await page.request.post('/api/v1/notebooks', {
      headers,
      data: { name: `e2e-filing-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
    });
    const notebookId = (await made.json()).data.id;

    const filed = await page.request.post('/api/v1/comments', {
      headers,
      data: { log_id: logId, log_timestamp: logTimestamp, text: 'collected into the notebook', notebook_id: notebookId },
    });
    const filedId = (await filed.json()).data.id;
    const loose = await page.request.post('/api/v1/comments', {
      headers,
      data: { log_id: logId, log_timestamp: logTimestamp, text: 'never collected' },
    });
    const looseId = (await loose.json()).data.id;

    try {
      const unfiled = await page.request.get('/api/v1/comments/flat?filed=unfiled', { headers });
      const unfiledIds = ((await unfiled.json()).data || []).map(c => c.id);
      expect(unfiledIds).toContain(looseId);
      expect(unfiledIds).not.toContain(filedId);

      // A filed comment names where it went, which is what makes the evidence
      // view navigable rather than a flat list.
      const all = await page.request.get('/api/v1/comments/flat', { headers });
      const row = ((await all.json()).data || []).find(c => c.id === filedId);
      expect((row.notebooks || []).map(n => n.id)).toContain(notebookId);
    } finally {
      await page.request.delete(`/api/v1/comments/${looseId}`, { headers });
      await page.request.delete(`/api/v1/comments/${filedId}`, { headers });
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

  test('exports a report a case system can take', async ({ page }) => {
    await login(page);
    const headers = await openFirstFractal(page);

    const made = await page.request.post('/api/v1/notebooks', {
      headers,
      data: { name: `e2e-report-${Date.now()}`, time_range_type: '24h', max_results_per_section: 100 },
    });
    const notebookId = (await made.json()).data.id;

    try {
      await page.request.put(`/api/v1/notebooks/${notebookId}`, {
        headers,
        data: {
          name: 'e2e-report', description: 'report check', time_range_type: '24h',
          max_results_per_section: 100, timezone: 'UTC',
          external_ref_url: 'https://cases.example.com/CASE-42', external_ref_label: 'CASE-42',
        },
      });
      await page.request.post(`/api/v1/notebooks/${notebookId}/sections`, {
        headers,
        data: { section_type: 'markdown', title: 'Hypothesis', content: 'macro dropped a loader', tags: ['theory'], append: true },
      });

      const res = await page.request.get(`/api/v1/notebooks/${notebookId}/export?format=md&order=time`, { headers });
      expect(res.headers()['content-type']).toContain('text/markdown');
      const body = await res.text();
      expect(body).toContain('# e2e-report');
      expect(body).toContain('[CASE-42](https://cases.example.com/CASE-42)');
      expect(body).toContain('macro dropped a loader');

      // Tag scoping is how one slice of an investigation goes to someone else.
      const sliced = await page.request.get(`/api/v1/notebooks/${notebookId}/export?format=md&tags=nothing-has-this`, { headers });
      expect(await sliced.text()).toContain('Nothing to export');

      // YAML stays the round-trip format and must not have moved.
      const yaml = await page.request.get(`/api/v1/notebooks/${notebookId}/export`, { headers });
      expect(await yaml.text()).toContain('kind: Notebook');
    } finally {
      await page.request.delete(`/api/v1/notebooks/${notebookId}`, { headers });
    }
  });

});
