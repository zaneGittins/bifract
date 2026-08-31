// Shared setup for the browser tests.
//
// Every spec used to carry its own copy of login(), and the ones that needed a
// fractal with data in it took whichever fractal came back first. That encodes
// one machine's layout: on a box where the first fractal is empty, or where a
// reset moved the data, those specs fail with a timeout that reads like a
// product bug. Finding a populated fractal is a fixture concern, so it lives
// here and every spec asks the same question the same way.
const { expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

// A window wide enough that fixture age never decides whether a test runs.
const WIDE_START = '2000-01-01T00:00:00Z';
const WIDE_END = '2100-01-01T00:00:00Z';
// The same idea for the UI, where the range has to be expressible as a link.
const SEARCH_WINDOW = '-90d';

// Sign in only when the context is not already signed in.
//
// Every spec calls this, and with several workers that was a hundred logins a
// run, which is enough to trip the server's failed-login throttle and make the
// whole suite fail in milliseconds for reasons that have nothing to do with the
// tests. Contexts start from the shared session saved by auth.setup.js, so this
// is usually a single cheap GET.
async function login(page) {
  const probe = await page.request.get('/api/v1/auth/user');
  if (probe.ok()) return;

  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
  const body = await res.json();
  expect(body.success, `login rejected: ${JSON.stringify(body)}`).toBeTruthy();
}

async function listFractals(page) {
  const res = await page.request.get('/api/v1/fractals');
  expect(res.ok(), 'fractal listing failed').toBeTruthy();
  const body = await res.json();
  return body?.data?.fractals || body?.data || [];
}

// Run a query against one fractal and report the row count. Probing beats the
// cached log_count on the listing, which is refreshed on a timer and reads zero
// on a freshly seeded box.
async function queryCount(page, fractalID, query) {
  const res = await page.request.post('/api/v1/query', {
    headers: { 'X-Bifract-Scope': `fractal:${fractalID}` },
    data: {
      query,
      query_type: 'bql',
      start: WIDE_START,
      end: WIDE_END,
      fractal_id: fractalID,
      max_results: 1,
    },
  });
  if (!res.ok()) return 0;
  const body = await res.json();
  return body?.count || (body?.results || []).length;
}

// The first non-system fractal that actually holds logs, or null.
async function populatedFractal(page, query = '* | limit(1)') {
  const fractals = await listFractals(page);
  const candidates = [...fractals.filter(f => !f.is_system), ...fractals.filter(f => f.is_system)];
  for (const fractal of candidates) {
    if (await queryCount(page, fractal.id, query) > 0) return fractal;
  }
  return null;
}

// Select a fractal the way the UI does. Scope is session state set by /select,
// not a header, so fixtures creating things under a fractal go through it.
async function selectFractal(page, fractalID) {
  const res = await page.request.post(`/api/v1/fractals/${fractalID}/select`);
  expect(res.ok(), 'fractal select failed').toBeTruthy();
}

// The scope header for API calls made outside the page's own fetch, which is
// what stamps it.
async function scopeHeader(page) {
  const id = await page.evaluate(() => window.FractalContext?.currentFractal?.id);
  const isPrism = await page.evaluate(() => !!window.FractalContext?.isPrism?.());
  return { 'X-Bifract-Scope': `${isPrism ? 'prism' : 'fractal'}:${id}` };
}

// Open a fractal in the UI, by name when given, and switch to a tab.
async function openFractal(page, tabButtonId, fractalName) {
  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  let row = page.locator('.fractal-listing-table tbody tr').first();
  if (fractalName) {
    const named = page.locator(`.fractal-listing-table tbody tr:has-text("${fractalName}")`).first();
    if (await named.count()) row = named;
  }
  await row.locator('td').first().click();
  if (tabButtonId) await page.locator(`#${tabButtonId}`).click();
}

// Land on the search tab of a fractal that has logs, with results on screen.
// Returns the fractal, or null when the stack holds no data at all.
//
// Entering through /go/search rather than clicking the fractal, the tab, the
// time picker and the run button does the same thing in one navigation, and it
// puts the window in the URL: a reload then comes back with the same range,
// where the click-through path silently reverted to the default and stopped
// matching fixture data as it aged.
async function openSearchOnPopulatedFractal(page, query = '*') {
  const fractal = await populatedFractal(page);
  if (!fractal) return null;

  await page.goto(`/go/search?q=${encodeURIComponent(query)}` +
    `&fractal=${encodeURIComponent(fractal.name)}&from=${SEARCH_WINDOW}`);
  await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 30000 });
  return fractal;
}

// Reach the search tab by clicking through the app, the way a person arrives
// without a link. Specs about capture use this deliberately: the share-link
// entry does not currently re-read the per-user capture state when it lands on
// the fractal the session is already on, and a fixture that took that path
// would hide it rather than let a test find it.
async function openSearchByClick(page) {
  const fractal = await populatedFractal(page);
  if (!fractal) return null;

  await openFractal(page, 'fractalSearchTabBtn', fractal.name);
  await page.locator('#queryInput').waitFor({ timeout: 15000 });
  await page.locator('#timePickerBtn').click();
  await page.locator('#timePickerPanel .tp-preset[data-value="all"]').click();
  await page.locator('#queryInput').fill('*');
  await page.locator('#executeBtn').click();
  await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 30000 });
  return fractal;
}

// Re-run the wide-open search after a reload. A reload drops the range the
// picker was set to, so running again without re-pinning it silently searches
// the default window and finds nothing once the fixture is a day old.
async function rerunAllTime(page) {
  await page.locator('#queryInput').waitFor({ timeout: 15000 });
  await page.locator('#timePickerBtn').click();
  await page.locator('#timePickerPanel .tp-preset[data-value="all"]').click();
  await page.locator('#queryInput').fill('*');
  await page.locator('#executeBtn').click();
  await page.locator('#resultsTable .result-row').first().waitFor({ timeout: 30000 });
}

module.exports = {
  USER, PASS, WIDE_START, WIDE_END, SEARCH_WINDOW,
  login, listFractals, queryCount, populatedFractal,
  selectFractal, scopeHeader, openFractal, openSearchOnPopulatedFractal, openSearchByClick, rerunAllTime,
};
