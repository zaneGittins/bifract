// Live is a time range, not a mode: it is picked in the time picker, the Run
// button becomes Stop while it polls, and leaving it (Stop, or any other range)
// ends the polling. These assert only that wiring, never on rows arriving, so
// they do not depend on anything ingesting while the suite runs.
const { test, expect } = require('@playwright/test');
const { login, openSearchByClick } = require('./fixtures');

async function pickLive(page) {
  await page.locator('#timePickerBtn').click();
  await page.locator('#timePickerPanel .tp-preset[data-value="live"]').click();
}

const liveActive = (page) => page.evaluate(() => !!(window.LiveSearch && LiveSearch.active));

test.describe('live search', () => {
  test('picking Live polls until Stop', async ({ page }) => {
    await login(page);
    test.skip(!(await openSearchByClick(page)), 'no fractal on this stack holds logs');
    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));

    await pickLive(page);
    await expect(page.locator('#timePickerLabel')).toHaveText('Live');
    await expect(page.locator('#timePickerBtn')).toHaveClass(/is-live-running/);
    await expect(page.locator('#executeBtn .btn-text')).toHaveText('Stop');
    await expect(page).toHaveURL(/[?&]tr=live\b/);
    await expect.poll(() => liveActive(page)).toBe(true);

    await page.locator('#executeBtn').click();
    await expect(page.locator('#executeBtn .btn-text')).toHaveText('Run');
    await expect(page.locator('#timePickerBtn')).not.toHaveClass(/is-live-running/);
    expect(await liveActive(page)).toBe(false);
    expect(errors, 'page errors').toEqual([]);
  });

  test('another range ends Live', async ({ page }) => {
    await login(page);
    test.skip(!(await openSearchByClick(page)), 'no fractal on this stack holds logs');

    await pickLive(page);
    await expect.poll(() => liveActive(page)).toBe(true);
    await page.locator('#timePickerBtn').click();
    await page.locator('#timePickerPanel .tp-preset[data-value="1h"]').click();
    await expect(page.locator('#timePickerLabel')).toHaveText('Last 1h');
    await expect(page.locator('#timePickerBtn')).not.toHaveClass(/is-live/);
    expect(await liveActive(page)).toBe(false);
  });

  test('an aggregation re-runs instead of streaming', async ({ page }) => {
    await login(page);
    test.skip(!(await openSearchByClick(page)), 'no fractal on this stack holds logs');

    await page.locator('#queryInput').fill('* | groupBy(fractal_id)');
    await pickLive(page);
    await expect.poll(() => page.evaluate(() => LiveSearch.mode)).toBe('rerun');
    await expect(page.locator('#executeBtn .btn-text')).toHaveText('Stop');
    await page.locator('#executeBtn').click();
    expect(await liveActive(page)).toBe(false);
  });
});

// The poll endpoint is answered from a script here, so these cover the client's
// own logic (de-duplication over the re-read overlap, pausing while reading, and
// which errors end Live) without depending on anything being ingested. Each reply
// is used once in order and the last repeats, so a test moves to its next step
// only by pushing it: nothing arrives between two of its assertions.
async function scriptedPolls(page, replies) {
  let n = 0;
  await page.route('**/api/v1/query/live', (route) => {
    const reply = replies[Math.min(n++, replies.length - 1)];
    if (reply.status) return route.fulfill({ status: reply.status, json: reply.body || {} });
    return route.fulfill({ json: reply });
  });
  return replies;
}

const t0 = Date.parse('2026-10-27T12:00:00Z');
const iso = (ms) => new Date(ms).toISOString();
const row = (id, sec) => ({
  timestamp: iso(t0 + sec * 1000).replace('T', ' ').replace('Z', ''),
  _ingest_timestamp: iso(t0 + sec * 1000).replace('T', ' ').replace('Z', ''),
  log_id: id, fractal_id: 'f', norm_log: `{"msg":"${id}"}`,
});
// One poll: rows plus the histogram window it re-read, ending at sec.
const poll = (sec, rows) => ({
  success: true, results: rows, field_order: ['timestamp', 'norm_log'], cursor: iso(t0 + sec * 1000),
  histogram: { bucket_start: iso(t0 + (sec - 120) * 1000), bucket_seconds: 10, buckets: new Array(12).fill(1) },
});
const shownIds = (page) => page.evaluate(() => QueryExecutor.currentResults.map((r) => r.log_id));

test.describe('live search client', () => {
  test('rows re-read by the overlap are shown once, and a press holds new rows', async ({ page }) => {
    await login(page);
    test.skip(!(await openSearchByClick(page)), 'no fractal on this stack holds logs');
    const replies = await scriptedPolls(page, [
      poll(200, [row('b', 190), row('a', 180)]),
      poll(202, [row('c', 201), row('b', 190), row('a', 180)]),
    ]);

    await pickLive(page);
    await expect.poll(() => shownIds(page)).toEqual(['c', 'b', 'a']);

    // Hold the mouse on the table: the next arrival waits instead of shifting rows.
    const box = await page.locator('#resultsTable').boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + 40);
    await page.mouse.down();
    await expect(page.locator('#timePickerLabel')).toHaveText('Live, paused');
    replies.push(poll(204, [row('d', 203), row('c', 201), row('b', 190)]));
    await expect.poll(() => page.evaluate(() => LiveSearch._pending.length)).toBe(1);
    expect(await shownIds(page)).toEqual(['c', 'b', 'a']);

    await page.mouse.up();
    await expect.poll(() => shownIds(page)).toEqual(['d', 'c', 'b', 'a']);
    await expect(page.locator('#timePickerLabel')).toHaveText('Live');
    await page.locator('#executeBtn').click();
  });

  test('a gateway error is retried, a query error ends Live', async ({ page }) => {
    await login(page);
    test.skip(!(await openSearchByClick(page)), 'no fractal on this stack holds logs');
    const replies = await scriptedPolls(page, [{ status: 503 }, poll(200, [row('a', 190)])]);

    await pickLive(page);
    await expect.poll(() => shownIds(page), { timeout: 15000 }).toEqual(['a']);
    expect(await liveActive(page)).toBe(true);
    replies.push({ status: 500, body: { success: false, error: 'Live search failed', error_type: 'execution' } });
    await expect.poll(() => liveActive(page), { timeout: 15000 }).toBe(false);
    await expect(page.locator('#executeBtn .btn-text')).toHaveText('Run');
  });
});
