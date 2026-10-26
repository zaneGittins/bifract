// System page layout: one header row, one heading style, one table style.
//
// Each sub-tab used to style its own headings and tables, the tab underline
// stopped short of the controls beside it, and the range picker showed on tabs
// it does not drive. These assert the shared rules on the rendered page.
const { test, expect } = require('@playwright/test');
const { login, openNav } = require('./fixtures');

const TABS = ['overview', 'storage', 'activity', 'alerts', 'archive'];
const RANGE_TABS = ['overview', 'activity', 'alerts'];

async function openSystem(page) {
  await page.setViewportSize({ width: 1440, height: 900 });
  await login(page);
  await page.goto('/');
  await openNav(page, 'performance');
  await expect(page.locator('#performanceView')).toBeVisible();
}

async function openTab(page, tab) {
  await page.locator(`#perfSubTabs .alerts-sub-tab[data-subtab="${tab}"]`).click();
  const pane = page.locator('#performanceView .perf-pane:visible');
  await expect(pane).toHaveCount(1);
  return pane;
}

test.describe('System layout', () => {
  test('the header rule runs under the tabs and the controls alike', async ({ page }) => {
    await openSystem(page);
    const head = page.locator('#performanceView .performance-header');
    expect(await head.evaluate(el => getComputedStyle(el).borderBottomWidth)).toBe('1px');

    const h = await head.boundingBox();
    const pane = await (await openTab(page, 'overview')).boundingBox();
    expect(Math.abs(h.x + h.width - (pane.x + pane.width)), 'rule stops short of the right edge').toBeLessThanOrEqual(1);

    // The active underline sits on the rule, not above it.
    const tab = await page.locator('#perfSubTabs .alerts-sub-tab.active').boundingBox();
    expect(Math.abs(tab.y + tab.height - (h.y + h.height)), 'tab underline floats above the rule').toBeLessThanOrEqual(1);
  });

  test('every tab opens with its stat tiles at the same height', async ({ page }) => {
    await openSystem(page);
    const tops = [];
    for (const tab of TABS) {
      const first = (await openTab(page, tab)).locator(':scope > :visible').first();
      await expect(first, `${tab} does not open with stat tiles`).toHaveClass(/perf-metrics-grid/);
      tops.push((await first.boundingBox()).y);
    }
    expect(Math.max(...tops) - Math.min(...tops), `tile rows start at ${tops.join(', ')}`).toBeLessThanOrEqual(1);
  });

  test('the range picker shows only on tabs it drives', async ({ page }) => {
    await openSystem(page);
    for (const tab of TABS) {
      await openTab(page, tab);
      const range = page.locator('#perfTimeRange');
      if (RANGE_TABS.includes(tab)) await expect(range, `${tab} lost its range`).toBeVisible();
      else await expect(range, `${tab} shows a range it ignores`).toBeHidden();
    }
  });

  test('headings, tile labels and table headers share one style', async ({ page }) => {
    await openSystem(page);
    for (const tab of TABS) {
      const pane = await openTab(page, tab);
      const styles = await pane.evaluate(root => {
        const vis = el => el.offsetParent !== null;
        const pick = sel => [...root.querySelectorAll(sel)].filter(vis).map(el => {
          const cs = getComputedStyle(el);
          return { text: el.textContent.trim(), size: cs.fontSize, transform: cs.textTransform, family: cs.fontFamily };
        });
        return { h3: pick('h3'), labels: pick('.perf-metric-label'), th: pick('th'), hints: pick('.perf-section-hint') };
      });
      const sizes = new Set(styles.h3.map(s => s.size));
      expect(sizes.size, `${tab} headings use sizes ${[...sizes]}`).toBeLessThanOrEqual(1);
      for (const group of ['h3', 'labels', 'th', 'hints']) {
        for (const s of styles[group]) {
          expect(s.transform, `${tab} ${group} "${s.text}" is case-transformed`).toBe('none');
        }
      }
      const families = new Set(styles.th.map(s => s.family));
      expect(families.size, `${tab} table headers mix fonts`).toBeLessThanOrEqual(1);
    }
  });

  test('the archive setup hint points at the current settings tab', async ({ page }) => {
    await openSystem(page);
    await openTab(page, 'archive');
    const hint = page.locator('#archiveHint');
    test.skip(!(await hint.isVisible()), 'archive is provisioned on this stack');
    await expect(hint).not.toContainText('Admin');
    await expect(hint).toContainText('Settings > Platform > Archive');
  });
});
