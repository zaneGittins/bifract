// Schema Fields tab: rendered-page assertions.
//
// Every check here targets a failure mode that static analysis passed on. The
// bulk-bar case is the concrete one: markup, JS syntax and DOM wiring were all
// valid while the bar was permanently visible, because a `display: flex` class
// rule outranks the UA stylesheet's `display: none` for [hidden]. Only a real
// layout engine can catch that.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', {
    data: { username: USER, password: PASS },
  });
  expect(res.ok(), 'login request failed').toBeTruthy();
  const body = await res.json();
  expect(body.success, `login rejected: ${JSON.stringify(body)}`).toBeTruthy();
}

async function openSchemaTab(page) {
  await login(page);
  await page.goto('/');
  await page.locator('#mainSchemaTabBtn').click();
  await expect(page.locator('#schemaFieldsView')).toBeVisible();
  // Wait for a real data row, not merely any row: the empty-state row is also a
  // <tr>, and immediately after a restart the table can legitimately still be
  // showing it while ClickHouse warms up.
  await expect(page.locator('#schemaTbody tr .schema-f').first()).toBeVisible({ timeout: 15000 });
}

test.describe('Schema Fields', () => {
  test('loads the unified field table', async ({ page }) => {
    await openSchemaTab(page);
    const rows = page.locator('#schemaTbody tr');
    expect(await rows.count()).toBeGreaterThan(0);
    // Every row must carry a verdict: a blank cell would read as an error.
    const verdicts = await page.locator('#schemaTbody tr .schema-v').count();
    expect(verdicts).toBe(await rows.count());
    await expect(page.locator('#schemaResultCount')).toContainText('fields');
  });

  test('bulk bar stays hidden until a row is selected, and Clear dismisses it', async ({ page }) => {
    await openSchemaTab(page);
    const bar = page.locator('#schemaBulkBar');

    // Regression: [hidden] was defeated by `display: flex`, so the bar showed
    // from page load and Clear appeared to do nothing.
    await expect(bar).toBeHidden();

    const label = page.locator('#schemaTbody .schema-box').first();
    test.skip(await label.count() === 0, 'no selectable field in this dataset');
    // Click the label: the input is visually hidden by design, which is exactly
    // how a real user (and a screen reader) interacts with it.
    await label.click();

    await expect(bar).toBeVisible();
    await expect(page.locator('#schemaBulkN')).toContainText('1 field selected');
    await expect(page.locator('#schemaBulkImpact')).toContainText('Capacity');

    await page.locator('#schemaBulkClear').click();
    await expect(bar).toBeHidden();
    await expect(page.locator('#schemaTbody .schema-box input:checked')).toHaveCount(0);
  });

  test('capacity strip appears only once data has loaded', async ({ page }) => {
    await openSchemaTab(page);
    await expect(page.locator('#schemaCapacity')).toBeVisible();
    await expect(page.locator('#schemaCapUsed')).not.toBeEmpty();
    // The warning is hidden unless something has actually overflowed.
    const warn = page.locator('#schemaCapWarn');
    if (await warn.isVisible()) {
      await expect(warn).toContainText('out of capacity');
    }
  });

  test('custom checkbox is visible and themed, never a bare native control', async ({ page }, testInfo) => {
    await openSchemaTab(page);
    const box = page.locator('#schemaTbody .schema-box').first();
    test.skip(await box.count() === 0, 'no selectable field in this dataset');

    // The native input is visually removed; the styled span is what renders.
    const native = box.locator('input');
    await expect(native).toHaveCSS('opacity', '0');
    const span = box.locator('span');
    await expect(span).toBeVisible();

    const unchecked = await span.evaluate(el => getComputedStyle(el).backgroundColor);
    await box.click();
    expect(await native.isChecked(), 'clicking the label must toggle the input').toBeTruthy();
    const checked = await span.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(checked, 'checked state must change the box fill').not.toBe(unchecked);

    // A white box on the dark theme was the original complaint.
    if (testInfo.project.name === 'dark') {
      const [r, g, b] = unchecked.match(/\d+/g).map(Number);
      expect(r + g + b, `unchecked box is near-white on dark: ${unchecked}`).toBeLessThan(300);
    }
    await box.click();
    expect(await native.isChecked()).toBeFalsy();
  });

  // The visually-hidden-input pattern is only correct if the control stays
  // reachable and its focus ring lands on the box a user can actually see.
  test('checkbox is keyboard reachable with a visible focus ring', async ({ page }) => {
    await openSchemaTab(page);
    const native = page.locator('#schemaTbody .schema-box input').first();
    test.skip(await native.count() === 0, 'no selectable field in this dataset');

    await native.focus();
    expect(await native.evaluate(el => el === document.activeElement)).toBeTruthy();

    // :focus-visible is not satisfied by a programmatic focus(), so assert the
    // rule is authored rather than sampling a computed style that will not apply.
    const hasRing = await page.evaluate(() => [...document.styleSheets]
      .filter(sh => (sh.href || '').includes('schemaFields'))
      .flatMap(sh => [...sh.cssRules])
      .some(r => r.selectorText
        && r.selectorText.includes('.schema-box input:focus-visible')
        && /outline/.test(r.style.cssText)));
    expect(hasRing, 'checkbox needs a focus-visible ring on the styled box').toBeTruthy();

    const span = page.locator('#schemaTbody .schema-box span').first();

    // The hidden input must sit on top of its own control, not drift elsewhere.
    const inputBox = await native.boundingBox();
    const spanBox = await span.boundingBox();
    if (inputBox && spanBox) {
      expect(Math.abs(inputBox.x - spanBox.x), 'hidden input drifted from its control').toBeLessThan(40);
      expect(Math.abs(inputBox.y - spanBox.y), 'hidden input drifted from its control').toBeLessThan(40);
    }

    await page.keyboard.press('Space');
    expect(await native.isChecked(), 'Space must toggle the checkbox').toBeTruthy();
    await page.keyboard.press('Space');
  });

  test('filters narrow the table and show an active state', async ({ page }) => {
    await openSchemaTab(page);
    const before = await page.locator('#schemaTbody tr').count();

    await page.locator('#schemaFVerdict').selectOption('keep');
    await expect(page.locator('#schemaWrapVerdict')).toHaveClass(/active/);
    const after = await page.locator('#schemaTbody tr').count();
    expect(after).toBeLessThanOrEqual(before);

    await page.locator('#schemaFVerdict').selectOption('');
    await expect(page.locator('#schemaWrapVerdict')).not.toHaveClass(/active/);

    await page.locator('#schemaQ').fill('zzz-no-such-field');
    await expect(page.locator('#schemaTbody')).toContainText('No fields match');
    await page.locator('#schemaQ').fill('');
  });

  test('row click opens the detail drawer with its justification', async ({ page }) => {
    await openSchemaTab(page);
    const drawer = page.locator('#schemaDrawer');
    await expect(drawer).not.toHaveClass(/open/);

    await page.locator('#schemaTbody tr .schema-f').first().click();
    await expect(drawer).toHaveClass(/open/);
    await expect(page.locator('#schemaDrawerName')).not.toBeEmpty();
    await expect(drawer.locator('.schema-why')).toBeVisible();
    await expect(drawer).toContainText('What references this field');

    await page.keyboard.press('Escape');
    await expect(drawer).not.toHaveClass(/open/);
  });

  test('Add field opens in the drawer, not inline or as a modal', async ({ page }) => {
    await openSchemaTab(page);
    const drawer = page.locator('#schemaDrawer');
    await page.locator('#schemaFieldAddBtn').click();
    await expect(drawer).toHaveClass(/open/);
    await expect(page.locator('#schemaFieldName')).toBeVisible();
    await expect(page.locator('#schemaFieldIndexType')).toBeVisible();

    // The table must remain in place behind the drawer.
    await expect(page.locator('#schemaTbody tr').first()).toBeVisible();

    await page.locator('#schemaFieldCancelBtn').click();
    await expect(drawer).not.toHaveClass(/open/);
  });

  // The reset is destructive and cross-cutting, so it belongs with the other
  // irreversible actions in Admin > Settings > Danger Zone, not on the tab you
  // use for routine schema work.
  test('destructive reset is not on the Schema tab', async ({ page }) => {
    await openSchemaTab(page);
    await expect(page.locator('#schemaFieldsView')).not.toContainText('rebuild schema from scratch');
    await expect(page.locator('#schemaFieldsView .schema-danger')).toHaveCount(0);
    // Its button must live outside the schema view entirely.
    const inView = await page.locator('#schemaFieldsView #schemaFieldResetBtn').count();
    expect(inView, 'reset button still inside the schema tab').toBe(0);
    expect(await page.locator('#schemaFieldResetBtn').count(),
      'reset button should exist somewhere in the app').toBe(1);
  });

  test('page does not scroll sideways at a laptop width', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await openSchemaTab(page);
    const overflow = await page.evaluate(() =>
      document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(overflow, 'body scrolls horizontally').toBeLessThanOrEqual(0);
  });
});
