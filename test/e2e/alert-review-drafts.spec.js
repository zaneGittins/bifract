// Alerts under review: importing to a draft, and withdrawing back to one.
//
// With review on, Import YAML or Sigma either did nothing useful or opened a proposal
// straight away, and Withdraw closed a proposal for good. These pin the replacements:
// an import lands in the author's drafts unless they choose to propose it, and a
// withdrawn proposal comes back as a draft with its content intact.
//
// Each test runs in a fractal of its own, so turning review on never reaches another
// spec, and deleting the fractal takes every alert, draft and proposal with it.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

const definition = (name) => ({
  name, description: 'e2e', query_string: 'process_name="cmd.exe"', alert_type: 'event', severity: 'low',
  labels: [], references: [],
  webhook_action_ids: [], fractal_action_ids: [], dictionary_action_ids: [], email_action_ids: [],
});

async function createFractal(page) {
  await login(page);
  const res = await page.request.post('/api/v1/fractals', {
    data: { name: `e2e-review-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, description: 'alert review drafts test' },
  });
  expect(res.ok(), 'could not create a fractal').toBeTruthy();
  const fractal = (await res.json()).data;
  return { fractal, scope: { 'X-Bifract-Scope': `fractal:${fractal.id}` } };
}

async function enableReview(page, scope) {
  const res = await page.request.put('/api/v1/alert-gate', {
    headers: scope,
    data: { enabled: true, min_approvals: 1, allow_self_approval: true },
  });
  expect(res.ok(), 'could not turn review on').toBeTruthy();
}

async function apiData(page, method, path, scope, data) {
  const res = await page.request.fetch(`/api/v1${path}`, { method, headers: scope, data });
  const body = await res.json().catch(() => ({}));
  return { status: res.status(), data: body.data, error: body.error };
}

async function openAlerts(page, fractal) {
  await page.goto(`/#f/${fractal.id}/alerts`);
  await expect(page.locator('#alertsView')).toBeVisible({ timeout: 15000 });
}

test.describe('Alert review drafts', () => {
  test('a Sigma import saves to the author\'s drafts, not the review queue', async ({ page }) => {
    const { fractal, scope } = await createFractal(page);
    try {
      await enableReview(page, scope);
      const title = `E2E Draft Import ${Date.now()}`;
      const sigma = [
        `title: ${title}`,
        'id: 6f1b3c2a-1d4e-4c8b-9a7f-0e2d5b7c9a11',
        'status: test',
        'level: high',
        'logsource:',
        '  category: process_creation',
        '  product: windows',
        'detection:',
        '  selection:',
        '    CommandLine|contains: DownloadString',
        '  condition: selection',
      ].join('\n');

      await openAlerts(page, fractal);
      await page.locator('.alerts-tools .kebab-btn').click();
      await page.locator('.alerts-tools .kebab-item', { hasText: 'Import YAML or Sigma' }).click();

      const modal = page.locator('#importYamlModal');
      await expect(modal).toBeVisible();
      // Review is on, so the modal asks where the rule goes, and defaults to a draft.
      await expect(page.locator('#importDestination')).toBeVisible();
      await expect(page.locator('#importDestination [data-dest="draft"]')).toHaveClass(/active/);
      await expect(page.locator('#importYamlBtn')).toHaveText('Save draft');
      await expect(page.locator('#importProposalGroup')).toBeHidden();

      // Proposing is a separate, deliberate choice that asks for a summary.
      await page.locator('#importDestination [data-dest="propose"]').click();
      await expect(page.locator('#importProposalGroup')).toBeVisible();
      await expect(page.locator('#importYamlBtn')).toHaveText('Open proposal');
      await page.locator('#importDestination [data-dest="draft"]').click();
      await expect(page.locator('#importProposalGroup')).toBeHidden();

      await page.locator('#yamlContent').fill(sigma);
      await expect(page.locator('#sigmaDetectedInfo')).toBeVisible();
      await page.locator('#importYamlBtn').click();

      await expect(modal).toBeHidden({ timeout: 10000 });
      await expect(page.locator('#alertImportError')).toBeHidden();

      const drafts = await apiData(page, 'GET', '/alert-drafts', scope);
      expect(drafts.status).toBe(200);
      const draft = drafts.data.find(d => d.content?.name === title);
      expect(draft, 'the import is not among the drafts').toBeTruthy();
      expect(draft.status).toBe('draft');
      expect(draft.kind).toBe('create');
      expect(draft.content.query_string).toBeTruthy();

      const open = await apiData(page, 'GET', '/alert-changes?open=true', scope);
      expect(open.data || [], 'an import to drafts must not open a proposal').toEqual([]);

      await expect(page.locator('#alertsDraftsBtn')).toBeVisible();
      await expect(page.locator('#alertsDraftsCount')).toHaveText('1');
      await page.locator('#alertsDraftsBtn').click();
      await expect(page.locator('#alertsDraftsPanel .adr-name')).toHaveText(title);

      // From a draft, submitting for review is its own step.
      const submitted = await apiData(page, 'POST', `/alert-drafts/${draft.id}/submit`, scope);
      expect(submitted.status, submitted.error).toBe(200);
      expect(submitted.data.status).toBe('open');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('withdrawing a proposal returns it to the author\'s drafts', async ({ page }) => {
    const { fractal, scope } = await createFractal(page);
    try {
      await enableReview(page, scope);
      const name = `E2E Withdrawn ${Date.now()}`;
      const proposed = await apiData(page, 'POST', '/alert-changes', scope, {
        kind: 'create', title: name, summary: 'catch cmd launches', content: definition(name),
      });
      expect(proposed.status, proposed.error).toBe(200);
      const id = proposed.data.id;

      await openAlerts(page, fractal);
      await page.locator('#alertsSubTabs .alerts-sub-tab[data-subtab="changes"]').click();
      const row = page.locator(`#alertChangesView .ac-row[data-id="${id}"]`);
      await row.click();
      const withdraw = page.locator('#alertChangeDrawer .ac-drawer-actions button', { hasText: 'Withdraw' });
      await expect(withdraw).toBeVisible();

      page.once('dialog', dialog => {
        expect(dialog.message()).toContain('drafts');
        dialog.accept();
      });
      await withdraw.click();
      await expect(page.locator('.toast-title', { hasText: 'Moved to your drafts' })).toBeVisible();
      await expect(row).toHaveCount(0);

      const drafts = await apiData(page, 'GET', '/alert-drafts', scope);
      const draft = drafts.data.find(d => d.id === id);
      expect(draft, 'the withdrawn proposal is not among the drafts').toBeTruthy();
      expect(draft.content.query_string).toBe('process_name="cmd.exe"');
      expect(draft.content.name).toBe(name);
      expect(draft.summary).toBe('catch cmd launches');

      const open = await apiData(page, 'GET', '/alert-changes?open=true', scope);
      expect((open.data || []).some(cr => cr.id === id)).toBeFalsy();

      await page.locator('#alertsSubTabs .alerts-sub-tab[data-subtab="manual"]').click();
      await expect(page.locator('#alertsDraftsCount')).toHaveText('1');

      // And it can go straight back into review.
      const resubmitted = await apiData(page, 'POST', `/alert-drafts/${id}/submit`, scope);
      expect(resubmitted.status, resubmitted.error).toBe(200);
      expect(resubmitted.data.status).toBe('open');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });

  test('a withdrawn edit becomes a draft of its alert, once', async ({ page }) => {
    const { fractal, scope } = await createFractal(page);
    try {
      // The alert exists before review is turned on, which is the only way to write
      // one directly.
      const name = `E2E Edited ${Date.now()}`;
      const created = await apiData(page, 'POST', '/alerts', scope, definition(name));
      expect(created.status, created.error).toBe(200);
      const alertId = created.data.id;
      await enableReview(page, scope);

      const edit = { ...definition(name), query_string: 'process_name="powershell.exe"' };
      const proposed = await apiData(page, 'POST', '/alert-changes', scope, {
        kind: 'update', alert_id: alertId, title: name, summary: 'widen it', content: edit,
      });
      expect(proposed.status, proposed.error).toBe(200);

      const withdrawn = await apiData(page, 'POST', `/alert-changes/${proposed.data.id}/discard`, scope);
      expect(withdrawn.status, withdrawn.error).toBe(200);
      expect(withdrawn.data.status).toBe('draft');
      expect(withdrawn.data.alert_id).toBe(alertId);

      const draft = await apiData(page, 'GET', `/alerts/${alertId}/draft`, scope);
      expect(draft.data?.id).toBe(proposed.data.id);
      expect(draft.data.content.query_string).toBe('process_name="powershell.exe"');

      // One draft per author per alert: a second withdrawal must not overwrite it.
      const again = await apiData(page, 'POST', '/alert-changes', scope, {
        kind: 'update', alert_id: alertId, title: name, summary: 'second try', content: edit,
      });
      expect(again.status, again.error).toBe(200);
      const refused = await apiData(page, 'POST', `/alert-changes/${again.data.id}/discard`, scope);
      expect(refused.status).toBe(409);
      const stillOpen = await apiData(page, 'GET', `/alert-changes/${again.data.id}`, scope);
      expect(stillOpen.data.status).toBe('open');
    } finally {
      await page.request.delete(`/api/v1/fractals/${fractal.id}`);
    }
  });
});
