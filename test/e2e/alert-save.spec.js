// Saving from the alert editor.
//
// Save used to run the whole query over the preview range before writing anything, so
// a heavy query left the button looking dead, a loaded ClickHouse refused valid alerts,
// and a server refusal surfaced as a browser error. These pin what replaced that.
//
// Each test runs in a fractal of its own, and deleting it takes the alerts with it.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

async function openEditor(page) {
  await login(page);
  const res = await page.request.post('/api/v1/fractals', {
    data: { name: `e2e-save-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, description: 'alert save test' },
  });
  expect(res.ok(), 'could not create a fractal').toBeTruthy();
  const fractal = (await res.json()).data;

  await page.goto(`/#f/${fractal.id}/alerts`);
  await expect(page.locator('#alertsView')).toBeVisible({ timeout: 15000 });
  await page.evaluate(() => Alerts.showAlertEditor());
  await expect(page.locator('#saveAlertBtn')).toBeVisible();
  return { fractal, scope: { 'X-Bifract-Scope': `fractal:${fractal.id}` } };
}

async function fill(page, name, query = 'process_name="cmd.exe"') {
  await page.fill('#editorAlertName', name);
  await page.fill('#editorQueryInput', query);
}

const alertNames = async (page, scope) => {
  const res = await page.request.get('/api/v1/alerts?limit=100', { headers: scope });
  return ((await res.json()).data || []).map((a) => a.name);
};

test.describe('Saving an alert', () => {
  test('does not wait on the query running', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      let ran = 0;
      await page.route('**/api/v1/query', (route) => { ran += 1; return route.abort(); });

      await fill(page, 'no execution');
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-success')).toContainText('created');
      expect(ran).toBe(0);
      expect(await alertNames(page, scope)).toContain('no execution');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('refuses a query that cannot run, and writes nothing', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      await fill(page, 'bad query', 'a=1 | modelLookup(model="missing", key=[a])');
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-error')).toContainText('model "missing" not found');
      expect(await alertNames(page, scope)).not.toContain('bad query');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('refuses a command the engine cannot run', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      await fill(page, 'search only', 'pgr(start="g1")');
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-error')).toContainText('only works in a search');
      expect(await alertNames(page, scope)).not.toContain('search only');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('keeps comment lines in the query', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      const query = '// why this fires\nprocess_name="cmd.exe"';
      await fill(page, 'commented', query);
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-success')).toContainText('created');
      const res = await page.request.get('/api/v1/alerts?limit=100', { headers: scope });
      const saved = (await res.json()).data.find((a) => a.name === 'commented');
      expect(saved.query_string).toBe(query);
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('a refusal from the server shows its reason', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      const taken = await page.request.post('/api/v1/alerts', {
        headers: scope,
        data: { name: 'taken', query_string: 'a=1', alert_type: 'event', severity: 'low' },
      });
      expect(taken.ok(), 'could not seed an alert').toBeTruthy();

      await fill(page, 'taken');
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-error')).toContainText('already exists');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('a proxy error page is not shown as the reason', async ({ page }) => {
    const { fractal } = await openEditor(page);
    try {
      await page.route('**/api/v1/alerts', (route) => (
        route.request().method() === 'POST'
          ? route.fulfill({ status: 504, contentType: 'text/html', body: '<html><body>504 Gateway Time-out</body></html>' })
          : route.continue()
      ));

      await fill(page, 'behind a proxy');
      await page.click('#saveAlertBtn');

      const toast = page.locator('.toast-error');
      await expect(toast).toContainText('Request failed (504)');
      await expect(toast).not.toContainText('<html>');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('the draft status settles once autosave finishes', async ({ page }) => {
    const { fractal } = await openEditor(page);
    try {
      const autosaved = page.waitForResponse((r) => r.url().endsWith('/alert-drafts') && r.request().method() === 'PUT');
      await fill(page, 'status');
      await autosaved;
      await expect(page.locator('#alertEditorStatus')).toContainText('Draft saved');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('an autosave still in flight leaves no draft behind', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      let release;
      const held = new Promise((resolve) => { release = resolve; });
      let autosaving;
      const autosaveStarted = new Promise((resolve) => { autosaving = resolve; });
      await page.route('**/api/v1/alert-drafts', async (route) => {
        if (route.request().method() !== 'PUT') return route.continue();
        autosaving();
        await held;
        return route.continue();
      });

      await fill(page, 'drafted');
      await autosaveStarted;
      await page.click('#saveAlertBtn');
      await expect(page.locator('.toast-success')).toContainText('created');
      const autosaved = page.waitForResponse((r) => r.url().endsWith('/alert-drafts') && r.request().method() === 'PUT');
      release();
      await autosaved;

      await expect.poll(async () => {
        const res = await page.request.get('/api/v1/alert-drafts', { headers: scope });
        return ((await res.json()).data || []).length;
      }, { timeout: 5000 }).toBe(0);
      await expect(page.locator('#alertsDraftsBtn')).toBeHidden();
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('shows it is working and ignores further clicks', async ({ page }) => {
    const { fractal, scope } = await openEditor(page);
    try {
      let posts = 0;
      let release;
      const held = new Promise((resolve) => { release = resolve; });
      await page.route('**/api/v1/alerts', async (route) => {
        if (route.request().method() !== 'POST') return route.continue();
        posts += 1;
        await held;
        return route.continue();
      });

      await fill(page, 'one save');
      const button = page.locator('#saveAlertBtn');
      await button.click();
      await expect(button).toBeDisabled();
      await button.dispatchEvent('click');
      await expect.poll(() => posts).toBe(1);

      release();
      await expect(page.locator('.toast-success')).toContainText('created');
      expect(posts).toBe(1);
      await expect(button).toBeEnabled();
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('the button comes back after a failed save', async ({ page }) => {
    const { fractal } = await openEditor(page);
    try {
      await page.route('**/api/v1/alerts', (route) => (
        route.request().method() === 'POST' ? route.abort() : route.continue()
      ));

      await fill(page, 'retry me');
      await page.click('#saveAlertBtn');

      await expect(page.locator('.toast-error')).toBeVisible();
      await expect(page.locator('#saveAlertBtn')).toBeEnabled();
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });
});
