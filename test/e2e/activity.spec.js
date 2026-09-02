// System > Activity: the merged query stream.
//
// Two defects here passed every static check while the page was visibly wrong.
// Init runs twice (DOMContentLoaded and the app's own startup), so listeners
// bound twice and each chip click toggled its filter on and straight back off.
// And the drawer's <pre> carried .query-highlight, which is the query editor's
// absolutely-positioned transparent overlay layer, so it covered the drawer
// header. Both need a rendered page to catch.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

async function openActivity(page) {
  await login(page);
  await page.goto('/');
  await page.locator('#mainPerformanceTabBtn').click();
  await expect(page.locator('#performanceView')).toBeVisible();
  await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="activity"]').click();
  await expect(page.locator('#perfPaneActivity')).toBeVisible();
  // Rows arrive on the first poll; everything below asserts against real ones.
  await expect(page.locator('#actStreamTable table tbody tr').first()).toBeVisible();
}

test.describe('Activity: query stream', () => {
  test('renders the headline, both charts and the stream', async ({ page }) => {
    await openActivity(page);

    await expect(page.locator('#actTileRunning')).not.toHaveText('--');
    // Both charts are inline SVG, drawn from the summary endpoint.
    await expect(page.locator('#actSmalls .act-small')).toHaveCount(4);
    await expect(page.locator('#actLatencyWrap svg, #actLatencyWrap .perf-chart-placeholder')).toBeVisible();

    const count = Number(await page.locator('#actStreamCount').textContent());
    expect(count, 'the stream reported no rows').toBeGreaterThan(0);
    await expect(page.locator('#actLiveText')).toHaveText('Live');
  });

  test('a class chip narrows the stream to that class', async ({ page }) => {
    await openActivity(page);

    const chip = page.locator('#actChips .act-chip[data-value="alert"]');
    await chip.click();
    await expect(chip).toHaveClass(/active/);

    // The regression: with listeners bound twice the chip toggled straight back
    // off, so the filter never reached the server and the rows never changed.
    await expect.poll(async () => {
      const sources = await page.locator('#actStreamTable tbody .act-source').allTextContents();
      return sources.length > 0 && sources.every(s => s.trim() === 'Alert');
    }, { message: 'the class chip did not filter the stream' }).toBe(true);

    await chip.click();
    await expect(chip).not.toHaveClass(/active/);
  });

  test('a state chip filters, and clicking it again clears it', async ({ page }) => {
    await openActivity(page);

    const running = page.locator('#actChips .act-chip[data-value="running"]');
    await running.click();
    await expect(running).toHaveClass(/active/);
    await expect.poll(async () => {
      const states = await page.locator('#actStreamTable tbody .act-state').allTextContents();
      return states.every(s => /Running|Stopping/.test(s));
    }, { message: 'the Running chip did not filter the stream' }).toBe(true);

    await running.click();
    await expect(running).not.toHaveClass(/active/);
    await expect(page.locator('#actChips .act-chip[data-value=""]')).toBeVisible();
  });

  test('Cost patterns swaps the table and hides the live controls', async ({ page }) => {
    await openActivity(page);

    await page.locator('#actModes .act-mode[data-mode="patterns"]').click();
    await expect(page.locator('#actModes .act-mode[data-mode="patterns"]')).toHaveClass(/active/);
    await expect(page.locator('#actChips')).toBeHidden();
    await expect(page.locator('#actLive')).toBeHidden();
    await expect(page.locator('#actStreamTable th').filter({ hasText: 'Runs' })).toBeVisible();

    await page.locator('#actModes .act-mode[data-mode="live"]').click();
    await expect(page.locator('#actChips')).toBeVisible();
    await expect(page.locator('#actStreamTable th').filter({ hasText: 'State' })).toBeVisible();
  });

  test('the live tail pauses while the pointer is over the table', async ({ page }) => {
    await openActivity(page);
    await page.locator('#actStreamTable table').hover();
    await expect(page.locator('#actLiveText')).toHaveText('Paused');
    await page.locator('#actModes').hover();
    await expect(page.locator('#actLiveText')).toHaveText('Live');
  });
});

test.describe('Activity: query detail drawer', () => {
  test('opens with its header, the SQL and the per-shard profile', async ({ page }) => {
    await openActivity(page);
    await page.locator('#actStreamTable tbody tr').first().click();

    const drawer = page.locator('#actDrawer');
    await expect(drawer).toHaveClass(/open/);

    // The .query-highlight regression: the header and meta chips were painted
    // over by an absolutely-positioned overlay, so assert they are really on
    // screen rather than merely present in the DOM.
    const head = drawer.locator('.act-drawer-head');
    await expect(head).toBeVisible();
    const headBox = await head.boundingBox();
    expect(headBox.height, 'the drawer header collapsed').toBeGreaterThan(20);

    const pre = page.locator('#actDrawerQuery');
    const preBox = await pre.boundingBox();
    expect(preBox.y, 'the SQL is painted over the drawer header').toBeGreaterThan(headBox.y + headBox.height - 1);

    await expect(page.locator('#actDrawerMeta span').first()).toBeVisible();
    await expect(pre).not.toHaveText('');

    await page.keyboard.press('Escape');
    await expect(drawer).not.toHaveClass(/open/);
  });

  // A search carries its own BQL in the query tag, so the drawer shows what the
  // analyst wrote above the SQL it compiled to.
  test('a search shows its BQL above the translated SQL', async ({ page }) => {
    await openActivity(page);

    // Find a row whose source is a BQL-issuing one; system and ingest carry none.
    const row = page.locator('#actStreamTable tbody tr').filter({
      has: page.locator('.act-source', { hasText: /Search|Dashboard|Notebook|Recall|Assistant/ }),
    }).first();
    test.skip(await row.count() === 0, 'no BQL-issuing query in the current window');
    await row.click();

    const block = page.locator('#actDrawerBqlBlock');
    test.skip(!(await block.isVisible()), 'the selected query predates BQL tagging');

    await expect(page.locator('#actDrawerBql')).not.toHaveText('');
    await expect(page.locator('#actDrawerSqlLabel')).toBeVisible();

    // BQL above, SQL below: the order is the point.
    const bql = await page.locator('#actDrawerBql').boundingBox();
    const sql = await page.locator('#actDrawerQuery').boundingBox();
    expect(bql.y, 'the SQL is above the BQL').toBeLessThan(sql.y);
  });

  test('the SQL is syntax highlighted, and the highlight is readable', async ({ page }) => {
    await openActivity(page);
    await page.locator('#actStreamTable tbody tr').first().click();

    const keyword = page.locator('#actDrawerQuery .sql-keyword').first();
    test.skip(await keyword.count() === 0, 'the first row carries no SQL keywords');
    await expect(keyword).toBeVisible();
    // Transparent text is exactly what the overlay-class bug produced.
    const color = await keyword.evaluate(el => getComputedStyle(el).color);
    expect(color, 'highlighted SQL is invisible').not.toMatch(/rgba\(0, 0, 0, 0\)|transparent/);
  });
});

test.describe('Activity: failures', () => {
  // The failure pills set state=error, which builds a single-branch query. That
  // lost the UNION ALL which had been widening Int32 exception_code to Int64, so
  // the request failed the moment a real failure existed and the pill read dead.
  test('a failure pill filters the stream to the error rows', async ({ page }) => {
    await openActivity(page);
    const pill = page.locator('#actFailStrip .act-failpill').first();
    test.skip(await pill.count() === 0, 'no failures in the current range');

    await pill.click();
    await expect(page.locator('#actChips .act-chip[data-value="error"]')).toHaveClass(/active/);

    // The stream has to actually answer, with rows rather than an empty state.
    await expect.poll(async () => {
      const states = await page.locator('#actStreamTable tbody .act-state').allTextContents();
      return states.length > 0 && states.every(s => /Error|Killed/.test(s.trim()));
    }, { message: 'the failure pill did not bring back the error rows' }).toBe(true);
  });
});

test.describe('Activity: latency chart', () => {
  test('hovering reads a quantile out as a sentence', async ({ page }) => {
    await openActivity(page);
    const hit = page.locator('#actLatencyWrap svg rect[data-i]').first();
    test.skip(await hit.count() === 0, 'not enough latency samples to chart');

    await hit.hover({ force: true });
    const tip = page.locator('#actTip');
    await expect(tip).toHaveClass(/open/);
    // "p95" is a label; the tooltip has to say what it means.
    await expect(tip).toContainText(/95% of searches finished within/);
    await expect(tip).toContainText(/Half of searches finished within/);
  });
});

test.describe('Storage & Ingest', () => {
  test('leads with the ingest chart and four tiles, not a wall of stats', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="storage"]').click();
    await expect(page.locator('#perfPaneStorage')).toBeVisible();

    await expect(page.locator('#perfPaneStorage .perf-metric-card')).toHaveCount(4);
    await expect(page.locator('#metricLogStorage')).not.toHaveText('--');
    await expect(page.locator('#perfIngestChart')).toBeVisible();
    // The hot table is one line, not four cards.
    await expect(page.locator('#hotStrip')).toBeVisible();
  });

  // .perf-section-hint lowercases its text, which is right for prose and wrong
  // for values: it rendered "1.4 TB" as "1.4 tb".
  test('unit suffixes keep their case', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="storage"]').click();
    const summary = page.locator('#ingestSummary');
    await expect(summary).not.toHaveText('');
    await expect(summary).toHaveCSS('text-transform', 'none');
    expect(await summary.textContent(), 'unit suffix was lowercased').not.toMatch(/\d\s?(tb|gb|mb|kb)\b/);
  });

  // Two regressions guarded here. The axis and stacked tooltip formatters were
  // hard-coded to bytes, so the Rows measure rendered counts as byte sizes; and
  // the chart's _metric marker was compared but never stored, so the instance was
  // destroyed and rebuilt on every poll instead of updating in place.
  test('the chart formats the selected measure and reuses its instance', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="storage"]').click();
    await expect(page.locator('#perfIngestChart')).toBeVisible();

    await page.locator('#ingestModes .act-mode[data-metric="rows"]').click();
    const rows = await page.evaluate(() => {
      const c = window.Performance.ingestChart;
      const tip = c.options.plugins.tooltip.callbacks;
      return {
        metric: c._metric,
        tick: c.options.scales.y.ticks.callback(1400000),
        label: tip.label ? tip.label({ dataset: { label: 'x' }, parsed: { y: 1400000 } }) : null,
      };
    });
    expect(rows.metric, '_metric was never stored').toBe('rows');
    expect(rows.tick, 'row counts rendered as byte sizes').not.toMatch(/\b[KMGT]?B\b/);
    if (rows.label) expect(rows.label).not.toMatch(/\b[KMGT]?B\b/);

    // A re-render with nothing changed must update in place.
    const before = await page.evaluate(() => window.Performance.ingestChart.id);
    await page.evaluate(() => window.Performance.renderIngestChart());
    const after = await page.evaluate(() => window.Performance.ingestChart.id);
    expect(after, 'the chart was rebuilt instead of updated').toBe(before);

    // Bytes still format as bytes.
    await page.locator('#ingestModes .act-mode[data-metric="raw"]').click();
    const raw = await page.evaluate(() =>
      window.Performance.ingestChart.options.scales.y.ticks.callback(1400000000));
    expect(raw).toMatch(/\b[KMGT]?B\b/);
  });

  test('the ingest chart can switch measure', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="storage"]').click();
    await expect(page.locator('#perfIngestChart')).toBeVisible();

    const before = await page.locator('#ingestSummary').textContent();
    await page.locator('#ingestModes .act-mode[data-metric="rows"]').click();
    await expect(page.locator('#ingestModes .act-mode[data-metric="rows"]')).toHaveClass(/active/);
    await expect.poll(async () => page.locator('#ingestSummary').textContent(),
      { message: 'switching measure did not change the summary' }).not.toBe(before);
  });
});

test.describe('Overview: background operations', () => {
  test('merges and mutations render on the Overview tab', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.locator('#mainPerformanceTabBtn').click();
    await page.locator('#perfSubTabs .alerts-sub-tab[data-subtab="overview"]').click();
    await expect(page.locator('#perfPaneOverview')).toBeVisible();

    // Either a table of running operations or the empty state, never a stuck
    // placeholder: both mean the endpoint answered.
    await expect(page.locator('#actBackground table, #actBackground .empty-state')).toBeVisible();
    await expect(page.locator('#actBgSummary')).toContainText(/merge/);
  });
});
