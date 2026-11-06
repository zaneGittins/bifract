// The stream outlives the answer: it stays open after the done frame to drain
// histogram chunks, and a server fault can end it with no terminal frame at all.
// Neither may leave the page showing a query still running, or wipe an answer.
// The stream is stubbed, so frame order is exact and no fixture data is needed.
const { test, expect } = require('@playwright/test');
const { login, openFractal } = require('./fixtures');

const META = { type: 'meta', sql: 'SELECT 1', field_order: ['timestamp', 'marker'], time_start: '2026-01-01T00:00:00Z', time_end: '2026-01-02T00:00:00Z' };
const HIST_META = { bucket_start: '2026-01-01T00:00:00Z', bucket_seconds: 3600, bucket_count: 4 };
const HIST = { type: 'histogram', buckets: [0, 0, 1, 1], bucket_start: '2026-01-01T00:00:00Z', bucket_seconds: 3600, bucket_count: 4, covered_from: 2, complete: false };
const ROWS = { type: 'rows', data: [{ timestamp: '2026-01-01 00:00:00', log_id: 'a', marker: 'stream-end-row' }] };
const DONE = { type: 'done', count: 1, execution_ms: 5, histogram_pending: true };
const INTERRUPTED = 'The query ended unexpectedly';

// Answers every search with frames, leaving the stream open for the test to end.
async function stubStream(page, frames) {
  await page.evaluate((frames) => {
    const realFetch = window.fetch;
    window.fetch = (url, opts) => {
      if (!String(url).includes('/query/stream')) return realFetch(url, opts);
      const body = new ReadableStream({
        start(controller) {
          const enc = new TextEncoder();
          for (const f of frames) controller.enqueue(enc.encode(JSON.stringify(f) + '\n'));
          window.closeStream = () => controller.close();
          window.dropStream = () => controller.error(new TypeError('network error'));
          // As a real fetch does, so an aborted run sees AbortError from its read.
          opts.signal.addEventListener('abort', () => {
            try { controller.error(new DOMException('aborted', 'AbortError')); } catch (e) { /* already closed */ }
          });
        },
      });
      return Promise.resolve(new Response(body, { headers: { 'Content-Type': 'application/x-ndjson' } }));
    };
  }, frames);
}

async function run(page) {
  await page.evaluate(() => { QueryExecutor.execute(); });
  await page.waitForFunction(() => typeof window.closeStream === 'function');
}

async function runAndOutwaitIndicator(page) {
  await run(page);
  await page.waitForTimeout(await page.evaluate(() => LoadingChrome.DELAY_MS + 200));
}

// The timeline is a canvas, so its label is read from the last paint.
async function spyOnTimeline(page) {
  await page.evaluate(() => {
    const render = Timeline.renderFromHistogram.bind(Timeline);
    Timeline.renderFromHistogram = (buckets, range, opts) => {
      window.timelineLabel = opts && opts.pendingLabel;
      return render(buckets, range, opts);
    };
  });
}

const pendingLabel = (page) => page.evaluate(() => window.timelineLabel);
const table = (page) => page.locator('#resultsTable');
const button = (page) => page.locator('#executeBtn .btn-text');

test.describe('search stream end', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
    await openFractal(page, 'search');
    await page.locator('#queryInput').waitFor({ timeout: 15000 });
    await page.locator('#queryInput').fill('stream_end_marker="x"');
    await spyOnTimeline(page);
  });

  for (const streaming of [false, true]) {
    test(`rows stay on screen while the histogram drains (streaming=${streaming})`, async ({ page }) => {
      await stubStream(page, [{ ...META, streaming }, ROWS, DONE]);
      await runAndOutwaitIndicator(page);
      await expect(table(page)).toContainText('stream-end-row');
      await expect(button(page)).toHaveText('Cancel');

      await page.evaluate(() => window.closeStream());
      await expect(button(page)).toHaveText('Run');
      await expect(table(page)).toContainText('stream-end-row');
      await expect(table(page).locator('.cancel-query-btn')).toHaveCount(0);
    });
  }

  test('an empty result is not repainted as still searching', async ({ page }) => {
    await stubStream(page, [{ ...META, streaming: true }, { ...DONE, count: 0 }]);
    await runAndOutwaitIndicator(page);
    await expect(table(page).locator('.results-empty')).toBeVisible();
    await page.evaluate(() => window.closeStream());
    await expect(button(page)).toHaveText('Run');
    await expect(table(page).locator('.results-empty')).toBeVisible();
  });

  test('an error frame is not repainted as still searching', async ({ page }) => {
    await stubStream(page, [{ ...META, streaming: true }, { type: 'error', error: 'scan failed', error_type: 'execution' }]);
    await runAndOutwaitIndicator(page);
    await expect(table(page).locator('.results-error')).toContainText('scan failed');
  });

  for (const end of ['closeStream', 'dropStream']) {
    test(`${end} before the answer reports an error`, async ({ page }) => {
      await stubStream(page, [{ ...META, streaming: true }]);
      await runAndOutwaitIndicator(page);
      await page.evaluate((end) => window[end](), end);
      await expect(button(page)).toHaveText('Run');
      await expect(table(page).locator('.results-error')).toContainText(INTERRUPTED);
    });

    test(`${end} after the answer keeps the rows and ends the timeline`, async ({ page }) => {
      await stubStream(page, [{ ...META, streaming: true, histogram_meta: HIST_META }, HIST, ROWS, DONE]);
      await run(page);
      await expect(table(page)).toContainText('stream-end-row');
      await expect.poll(() => pendingLabel(page)).toBe('scanning…');
      await page.evaluate((end) => window[end](), end);
      await expect(button(page)).toHaveText('Run');
      await expect(table(page)).toContainText('stream-end-row');
      await expect.poll(() => pendingLabel(page)).toBe('not scanned');
    });
  }

  test('a render queued before a dropped stream cannot repaint over the error', async ({ page }) => {
    await stubStream(page, [{ ...META, streaming: true }, ROWS]);
    await page.evaluate(() => {
      const raf = window.requestAnimationFrame;
      window.heldFrames = [];
      window.requestAnimationFrame = (cb) => window.heldFrames.push(cb);
      window.releaseFrames = () => { window.requestAnimationFrame = raf; window.heldFrames.forEach((cb) => cb(0)); };
    });
    await run(page);
    await page.waitForFunction(() => window.heldFrames.length > 0);
    await page.evaluate(() => window.closeStream());
    await expect(table(page).locator('.results-error')).toContainText(INTERRUPTED);
    await page.evaluate(() => window.releaseFrames());
    await expect(table(page).locator('.results-error')).toContainText(INTERRUPTED);
  });

  test('Cancel during the drain keeps the answer', async ({ page }) => {
    await stubStream(page, [{ ...META, streaming: true, histogram_meta: HIST_META }, HIST, ROWS, DONE]);
    await runAndOutwaitIndicator(page);
    await page.locator('#executeBtn').click();
    await expect(button(page)).toHaveText('Run');
    await expect(table(page)).toContainText('stream-end-row');
    await expect(page.locator('#resultsCount')).not.toContainText('cancelled');
    await expect.poll(() => pendingLabel(page)).toBe('not scanned');
  });

  test('the recent logs sample cannot clear a newer run', async ({ page }) => {
    await stubStream(page, [{ ...META, streaming: true }]);
    await page.evaluate(() => {
      const stubbed = window.fetch;
      window.fetch = (url, opts) => (String(url).includes('/logs/recent')
        ? new Promise((_, reject) => opts.signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))))
        : stubbed(url, opts));
      QueryExecutor.currentRequest = new AbortController();
      QueryExecutor._fetchRecentLogs(QueryExecutor.getElements());
    });
    await run(page);
    await page.waitForTimeout(100);
    expect(await page.evaluate(() => !!QueryExecutor.currentRequest)).toBe(true);
    await page.evaluate(() => window.closeStream());
    await expect(button(page)).toHaveText('Run');
  });

  test('the alert editor reports an unfinished stream and keeps a finished one', async ({ page }) => {
    const outcome = (frames) => page.evaluate(async (frames) => {
      const enc = new TextEncoder();
      let sent = false;
      const body = new ReadableStream({
        pull(c) {
          if (sent) return c.error(new TypeError('network error'));
          sent = true;
          frames.forEach((f) => c.enqueue(enc.encode(JSON.stringify(f) + '\n')));
        },
      });
      const div = document.createElement('div');
      try {
        await Alerts._consumeAlertStream(new Response(body), div, null, null, null);
        return 'resolved';
      } catch (e) { return e.message; }
    }, frames);
    expect(await outcome([META])).toContain(INTERRUPTED);
    expect(await outcome([META, ROWS, DONE])).toBe('resolved');
  });
});
