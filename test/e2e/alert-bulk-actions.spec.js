// Adding and removing actions across many alerts at once.
//
// The picker shows each action's state across the selection (on all, some or none),
// the review step shows what will actually change, and Undo puts every alert back as
// it was rather than flipping the whole set. The same edit is reachable from an
// action's "Used by" count, and with review on it becomes one proposal.
//
// Each test runs in a fractal of its own, so its alerts, actions and review setting
// never reach another spec.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

async function setup(page, count) {
  await login(page);
  const res = await page.request.post('/api/v1/fractals', {
    data: { name: `e2e-bulk-${Date.now()}-${Math.floor(Math.random() * 1e6)}`, description: 'bulk action edits' },
  });
  expect(res.ok(), 'could not create a fractal').toBeTruthy();
  const fractal = (await res.json()).data;
  const scope = { 'X-Bifract-Scope': `fractal:${fractal.id}` };

  const api = async (method, path, data) => {
    const r = await page.request.fetch(`/api/v1${path}`, { method, headers: scope, data });
    const body = await r.json().catch(() => ({}));
    expect(r.ok(), `${method} ${path}: ${body.error}`).toBeTruthy();
    return body.data;
  };

  const slack = await api('POST', '/webhooks', { name: 'Slack e2e', url: 'https://hooks.example.com/slack', method: 'POST', enabled: true });
  const pager = await api('POST', '/webhooks', { name: 'Pager e2e', url: 'https://hooks.example.com/pager', method: 'POST', enabled: true });
  const alerts = [];
  for (let i = 0; i < count; i++) {
    alerts.push(await api('POST', '/alerts', {
      name: `Bulk rule ${String(i).padStart(2, '0')}`, description: 'e2e', query_string: `event_id="${4000 + i}"`,
      alert_type: 'event', severity: 'low', labels: [], references: [], enabled: true,
      // Every third alert already sends to Slack, so the picker starts mixed.
      webhook_action_ids: i % 3 === 0 ? [slack.id] : [],
    }));
  }
  return { fractal, api, slack, pager, alerts };
}

async function webhooksOf(api, id) {
  return ((await api('GET', `/alerts/${id}`)).webhook_actions || []).map(w => w.id).sort();
}

async function openAlerts(page, fractal) {
  await page.goto(`/#f/${fractal.id}/alerts`);
  await expect(page.locator('.alerts-table')).toBeVisible({ timeout: 15000 });
}

async function teardown(page, fractal) {
  await page.request.delete(`/api/v1/fractals/${fractal.id}`);
}

test.describe('Bulk action edits', () => {
  test('selects every matching alert, adds an action where missing, and undoes exactly that', async ({ page }) => {
    const { fractal, api, slack, alerts } = await setup(page, 30);
    try {
      await openAlerts(page, fractal);

      // A page, then everything matching the filter.
      await page.click('#abPageCheck');
      await expect(page.locator('.ab-count')).toHaveText('25 selected');
      await page.getByRole('button', { name: 'Select all 30 matching alerts' }).click();
      await expect(page.locator('.ab-count')).toHaveText('30 selected');

      // Slack is on 10 of 30: mixed. One click puts it on all.
      await page.click('#abActionsBtn');
      const row = page.locator('.ab-picker-row', { hasText: 'Slack e2e' });
      await expect(row).toHaveAttribute('aria-checked', 'mixed');
      await expect(row.locator('.ab-picker-note')).toHaveText('10 of 30');
      await row.click();
      await expect(row).toHaveAttribute('aria-checked', 'true');
      await expect(row.locator('.ab-picker-note')).toHaveText('+20');
      await page.locator('.ab-picker-foot').getByRole('button', { name: 'Review' }).click();

      const review = page.locator('#abReviewModal');
      await expect(review.locator('.ab-review-lead')).toHaveText('20 alerts will change, 10 already match');
      await expect(review.locator('.ab-line-add')).toContainText('Slack e2e');
      await review.getByRole('button', { name: 'Apply to 20 alerts' }).click();
      await expect(review).toBeHidden();

      for (const a of alerts) expect(await webhooksOf(api, a.id)).toEqual([slack.id]);

      // Undo removes Slack from the 20 it was added to, and only those.
      await page.locator('.toast-action', { hasText: 'Undo' }).click();
      await expect(page.locator('.toast-title', { hasText: 'Change undone' })).toBeVisible();
      for (const [i, a] of alerts.entries()) {
        expect(await webhooksOf(api, a.id)).toEqual(i % 3 === 0 ? [slack.id] : []);
      }
    } finally {
      await teardown(page, fractal);
    }
  });

  test('selection follows the filter, and shift-click selects a run', async ({ page }) => {
    const { fractal } = await setup(page, 8);
    try {
      await openAlerts(page, fractal);
      const boxes = page.locator('.ab-row-check');
      await boxes.nth(1).click();
      await boxes.nth(4).click({ modifiers: ['Shift'] });
      await expect(page.locator('.ab-count')).toHaveText('4 selected');
      await expect(page.locator('#abPageCheck')).toHaveJSProperty('indeterminate', true);

      // Filtering an alert away drops it from the selection, so it is never acted on unseen.
      await page.fill('#alertSearchInput', 'Bulk rule 01');
      await page.locator('#alertSearchInput').dispatchEvent('input');
      await expect(page.locator('.ab-count')).toHaveText('1 selected');
    } finally {
      await teardown(page, fractal);
    }
  });

  test('an action\'s Used by count opens the alerts it runs on, and saves the difference', async ({ page }) => {
    const { fractal, api, pager, alerts } = await setup(page, 6);
    try {
      await page.goto(`/#f/${fractal.id}/alerts/actions`);
      const used = page.locator('tr', { hasText: 'Pager e2e' }).locator('.ab-used-by');
      await expect(used).toHaveText('No alerts');
      await used.click();

      const modal = page.locator('#abUsageModal');
      await modal.locator('.ab-picker-search').fill('Bulk rule 0');
      await modal.getByRole('button', { name: 'Check all shown', exact: true }).click();
      await modal.locator('.ab-usage-row', { hasText: 'Bulk rule 05' }).locator('input').uncheck();
      await expect(modal.locator('.ab-usage-diff')).toHaveText('+5');
      await modal.getByRole('button', { name: 'Review' }).click();
      await page.locator('#abReviewModal').getByRole('button', { name: 'Apply to 5 alerts' }).click();

      await expect(modal).toBeHidden();
      await expect(used).toHaveText('5 alerts');
      expect(await webhooksOf(api, alerts[5].id)).not.toContain(pager.id);
      expect(await webhooksOf(api, alerts[1].id)).toContain(pager.id);
    } finally {
      await teardown(page, fractal);
    }
  });

  test('with review on, the edit opens one proposal and applies nothing', async ({ page }) => {
    const { fractal, api, pager, alerts } = await setup(page, 4);
    try {
      await api('PUT', '/alert-gate', { enabled: true, min_approvals: 1, allow_self_approval: true });
      await openAlerts(page, fractal);
      await page.click('#abPageCheck');
      await page.click('#abActionsBtn');
      await page.locator('.ab-picker-row', { hasText: 'Pager e2e' }).click();
      await page.locator('.ab-picker-foot').getByRole('button', { name: 'Review' }).click();

      const review = page.locator('#abReviewModal');
      await expect(review.locator('.ab-review-gate')).toBeVisible();
      await review.locator('#abReviewSummary').fill('page on every rule');
      await review.getByRole('button', { name: 'Propose change' }).click();
      await expect(page.locator('.toast-title', { hasText: 'Proposal opened' })).toBeVisible();

      const open = await api('GET', '/alert-changes?open=true');
      expect(open).toHaveLength(1);
      expect(open[0].kind).toBe('actions');
      expect(open[0].summary).toBe('page on every rule');
      for (const a of alerts) expect(await webhooksOf(api, a.id)).not.toContain(pager.id);
    } finally {
      await teardown(page, fractal);
    }
  });
});
