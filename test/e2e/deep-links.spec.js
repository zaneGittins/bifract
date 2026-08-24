// /go/search: the documented entry URL external tools build by hand.
//
// The server half is unit tested in pkg/deeplink. What only a browser can prove
// is the other half: that the redirect it emits is actually honoured by the SPA's
// share-link reader, with the time range, the @variables and the fractal switch
// all arriving intact. Those three used to be dropped on whichever load path the
// page happened to take, which no static check catches.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

async function login(page) {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
}

async function anyFractal(page) {
  const res = await page.request.get('/api/v1/fractals');
  expect(res.ok(), 'fractal listing failed').toBeTruthy();
  const fractals = (await res.json())?.data?.fractals || [];
  return fractals.find(f => !f.is_system) || fractals[0] || null;
}

test.describe('deep links', () => {
  test('a hand-built link runs the query in the right fractal and window', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));

    // Plain percent-encoding and a fractal name: what an EDR rule or a webhook
    // template can produce without a base64 encoder.
    await page.goto(`/go/search?q=${encodeURIComponent('* | limit(5)')}&fractal=${encodeURIComponent(fractal.name)}&from=-90d`);

    await expect(page).toHaveURL(/\/\?.*\bq=/);
    await expect(page.locator('#queryInput')).toHaveValue('* | limit(5)');
    await expect(page.locator('#timePickerLabel')).toHaveText(/90d/i);
    await expect(page.locator('#currentFractalName')).toContainText(fractal.name, { timeout: 15000 });
    await expect(page.locator('#resultsTable table tr').first()).toBeVisible({ timeout: 30000 });

    expect(errors, 'page errors during deep link load').toEqual([]);
  });

  test('the link survives a reload and stays in the address bar', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    await page.goto(`/go/search?q=${encodeURIComponent('* | limit(5)')}&fractal=${encodeURIComponent(fractal.name)}&from=-90d`);
    await expect(page.locator('#resultsTable table tr').first()).toBeVisible({ timeout: 30000 });

    // The parameters used to be stripped a second after execution, so copying
    // the URL out of the browser or refreshing lost the query entirely.
    const landed = page.url();
    expect(landed).toMatch(/\bq=/);

    await page.goto(landed);
    await expect(page.locator('#queryInput')).toHaveValue('* | limit(5)');
    await expect(page.locator('#resultsTable table tr').first()).toBeVisible({ timeout: 30000 });
  });

  test('an absolute window arrives as a custom range', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    const to = new Date();
    const from = new Date(to.getTime() - 90 * 24 * 3600 * 1000);
    await page.goto(`/go/search?q=${encodeURIComponent('* | limit(5)')}&fractal=${encodeURIComponent(fractal.name)}` +
      `&from=${from.toISOString()}&to=${to.toISOString()}`);

    // ts/te reached the picker: the deferred load path used to null them out.
    await expect(page).toHaveURL(/\bts=/);
    await expect(page).toHaveURL(/\bte=/);
    await expect(page.locator('#queryInput')).toHaveValue('* | limit(5)');
    await expect(page.locator('#timePickerLabel')).not.toHaveText(/last/i);
  });

  test('var. parameters bind to the query @variables', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    await page.goto(`/go/search?q=${encodeURIComponent('bifract_category=@category | limit(5)')}` +
      `&fractal=${encodeURIComponent(fractal.name)}&from=-90d&var.category=process_creation`);

    await expect(page.locator('#queryInput')).toHaveValue('bifract_category=@category | limit(5)');
    // The tray surfaces the auto-detected @category holding the link's value.
    const pill = page.locator('#searchVariables .variable-value-input').first();
    await expect(pill).toHaveValue('process_creation', { timeout: 15000 });
  });

  test('a malformed link explains itself instead of dropping the user on an empty search', async ({ page }) => {
    await login(page);
    const res = await page.goto('/go/search?q=x&from=yesterday');
    expect(res.status()).toBe(400);
    await expect(page.locator('body')).toContainText(/time range/i);
    await expect(page.locator('a[href="/"]')).toBeVisible();
  });

  // A bare "//host" is obviously hostile, but "/\n/host" reads as a path and only
  // becomes protocol-relative once the URL parser strips the newline.
  for (const hostile of ['//example.com', '/%0A/example.com', '/%09/example.com', '/\\/example.com']) {
    test(`login next=${hostile} cannot be turned into an open redirect`, async ({ browser }) => {
      // A context per case: a stale page redirecting itself races the next goto.
      const context = await browser.newContext();
      const page = await context.newPage();
      // Record the attempt, then block it. Blocking alone is not an assertion:
      // an aborted navigation never commits, so page.url() would still read as
      // the old same-origin page and the test would pass while wide open.
      // Match only the redirect target, not the CDN assets the app legitimately
      // loads, or every third-party request would read as an escape.
      const offsite = [];
      await page.route('**/*', route => {
        const url = route.request().url();
        if (new URL(url).hostname !== 'example.com') return route.continue();
        offsite.push(url);
        return route.abort();
      });

      // Raw, not re-encoded: the attack needs %0A to reach URLSearchParams as an
      // escape it will decode into a real newline, which is what the URL parser
      // then strips. Encoding it again would only ever produce a literal "%0A".
      await page.goto(`/login.html?next=${hostile}`);
      await page.fill('#loginUsername', USER);
      await page.fill('#loginPassword', PASS);
      await page.click('#loginBtn');

      await expect(page).not.toHaveURL(/login\.html/, { timeout: 20000 });
      expect(offsite, `next=${hostile} sent the browser off-origin`).toEqual([]);
      expect(new URL(page.url()).hostname).toBe('localhost');

      await context.close();
    });
  }

  test('an unauthenticated link bounces through login and comes back', async ({ browser }) => {
    // A fresh context: no session cookie, which is the state an analyst clicking
    // a link from chat two days later is actually in.
    const context = await browser.newContext();
    const page = await context.newPage();

    const target = `/go/search?q=${encodeURIComponent('* | limit(5)')}&from=-90d`;
    await page.goto(target);
    await expect(page).toHaveURL(/login\.html\?next=/);

    await page.fill('#loginUsername', USER);
    await page.fill('#loginPassword', PASS);
    await page.click('#loginBtn');

    // Back to the deep link, not to a bare "/".
    await expect(page).toHaveURL(/\/\?.*\bq=/, { timeout: 20000 });
    await expect(page.locator('#queryInput')).toHaveValue('* | limit(5)', { timeout: 20000 });

    await context.close();
  });
});

// The address bar mirrors the executed search, and each distinct query is its own
// history entry. Both are invisible to a static check: the page renders correctly
// either way, and only a real browser has a history stack to walk.
test.describe('query URL mirroring', () => {
  async function openSearch(page, fractal) {
    await page.goto(`/go/search?q=${encodeURIComponent('* | limit(5)')}&fractal=${encodeURIComponent(fractal.name)}&from=-90d`);
    await expect(page.locator('#resultsTable table tr').first()).toBeVisible({ timeout: 30000 });
  }

  // The URL is written when the query runs, so waiting on it is both the natural
  // settle point and the assertion: a run that never reached the address bar has
  // not produced the history entry the next step depends on.
  async function expectUrlQuery(page, query) {
    await expect.poll(() => {
      const q = new URL(page.url()).searchParams.get('q');
      if (!q) return null;
      try { return decodeURIComponent(Buffer.from(q, 'base64').toString()); } catch (e) { return null; }
    }, { timeout: 15000 }).toBe(query);
  }

  async function runQuery(page, query) {
    await page.locator('#queryInput').fill(query);
    await page.locator('#executeBtn').click();
    await expectUrlQuery(page, query);
  }

  test('running a query writes it into the URL', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');
    await openSearch(page, fractal);

    // The q param is base64(encodeURIComponent(query)), the form the reader wants.
    await runQuery(page, 'bifract_category=* | limit(7)');
  });

  test('back returns to the previous query and re-runs it', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');
    await openSearch(page, fractal);

    await runQuery(page, 'bifract_category=* | limit(7)');
    await runQuery(page, 'bifract_category=* | limit(9)');

    await page.goBack();
    await expect(page.locator('#queryInput')).toHaveValue('bifract_category=* | limit(7)', { timeout: 15000 });
    // Restored, not just rewritten: the results below must belong to it.
    await expect(page.locator('#resultsTable table tr').first()).toBeVisible({ timeout: 30000 });

    await page.goForward();
    await expect(page.locator('#queryInput')).toHaveValue('bifract_category=* | limit(9)', { timeout: 15000 });
  });

  test('re-running the same query does not stack history entries', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');
    await openSearch(page, fractal);

    await runQuery(page, 'bifract_category=* | limit(7)');
    const afterFirst = await page.evaluate(() => history.length);

    // Three identical runs describe one state, so Back must not have to chew
    // through them to reach the query before it.
    for (let i = 0; i < 3; i++) {
      await page.locator('#executeBtn').click();
      await page.waitForTimeout(600);
    }
    expect(await page.evaluate(() => history.length)).toBe(afterFirst);
  });

  test('arriving on a deep link does not stack a duplicate entry', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    await openSearch(page, fractal);
    await runQuery(page, 'bifract_category=* | limit(7)');

    // One Back must reach the link's own query. If the execute the link triggers
    // pushed its own entry, this would land on a second copy of limit(7).
    await page.goBack();
    await expect(page.locator('#queryInput')).toHaveValue('* | limit(5)', { timeout: 15000 });
  });

  test('leaving the search tab drops the query from the URL', async ({ page }) => {
    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');
    await openSearch(page, fractal);
    await runQuery(page, 'bifract_category=* | limit(7)');

    await page.locator('#fractalCommentsTabBtn').click();
    await expect(page).not.toHaveURL(/[?&]q=/, { timeout: 10000 });
  });
});

// The Share button hands out the public /go/search form, so what a colleague
// receives is readable and hand-editable rather than an opaque base64 blob.
test.describe('share button', () => {
  test('copies a readable /go/search link that works when opened', async ({ page, context, browserName }) => {
    test.skip(browserName !== 'chromium', 'clipboard permissions are chromium-only here');
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    await page.goto(`/go/search?q=${encodeURIComponent('bifract_category=@category | limit(5)')}` +
      `&fractal=${encodeURIComponent(fractal.name)}&from=-90d&var.category=process_creation`);
    await expect(page.locator('#queryInput')).toHaveValue('bifract_category=@category | limit(5)');

    await page.locator('#shareMenuBtn').click();
    await page.locator('#shareQueryBtn').click();
    const copied = await page.evaluate(() => navigator.clipboard.readText());

    const url = new URL(copied);
    expect(url.pathname).toBe('/go/search');
    // Plain text, not base64: the whole point of the external form.
    expect(url.searchParams.get('q')).toBe('bifract_category=@category | limit(5)');
    expect(url.searchParams.get('fractal')).toBe(fractal.name);
    expect(url.searchParams.get('from')).toBe('-90d');
    expect(url.searchParams.get('var.category')).toBe('process_creation');

    // And it round-trips: what was copied reproduces the search it came from.
    await page.goto(copied);
    await expect(page.locator('#queryInput')).toHaveValue('bifract_category=@category | limit(5)');
    await expect(page.locator('#searchVariables .variable-value-input').first())
      .toHaveValue('process_creation', { timeout: 15000 });
  });

  test('an absolute range survives the round trip as absolute', async ({ page, context, browserName }) => {
    test.skip(browserName !== 'chromium', 'clipboard permissions are chromium-only here');
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await login(page);
    const fractal = await anyFractal(page);
    test.skip(!fractal, 'no fractal on this stack');

    const to = new Date();
    const from = new Date(to.getTime() - 90 * 24 * 3600 * 1000);
    await page.goto(`/go/search?q=${encodeURIComponent('* | limit(5)')}&fractal=${encodeURIComponent(fractal.name)}` +
      `&from=${from.toISOString()}&to=${to.toISOString()}`);
    await expect(page).toHaveURL(/\bts=/, { timeout: 15000 });

    await page.locator('#shareMenuBtn').click();
    await page.locator('#shareQueryBtn').click();
    const url = new URL(await page.evaluate(() => navigator.clipboard.readText()));

    // A window pinned to an incident must not degrade into "last 24h" on share.
    expect(url.searchParams.get('from')).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(url.searchParams.get('to')).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(Date.parse(url.searchParams.get('from'))).toBeCloseTo(from.getTime(), -4);
  });
});
