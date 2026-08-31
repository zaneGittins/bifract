// Display timezone, end to end: the zone lives on the account, the switcher is
// the time bar clock, and changing it relabels what is already on screen
// instead of re-running anything.
//
// The assertions that matter are the ones a unit test cannot make: that the
// stored value really comes back from Postgres rather than from the browser
// mirror, and that flipping the zone issues no new query.
const { test, expect } = require('@playwright/test');
const { login: signIn } = require('./fixtures');

// These cases turn on the account's display zone, so signing in and setting it
// is one step.
async function login(page, zone) {
  await signIn(page);
  await page.request.patch('/api/v1/auth/preferences', { data: { display_timezone: zone } });
}

async function openSearch(page) {
  await page.goto('/');
  await page.locator('.fractal-listing-table tbody tr').first().waitFor({ timeout: 15000 });
  await page.locator('.fractal-listing-table tbody tr td').first().click();
  await page.locator('#fractalSearchTabBtn').click();
  await page.locator('#queryInput').waitFor({ timeout: 15000 });
  await page.locator('.results-table .timestamp-cell').first().waitFor({ timeout: 20000 });
}

async function pickZone(page, query) {
  await page.locator('#tzClockBtn').click();
  await expect(page.locator('#tzPicker')).toBeVisible();
  await page.locator('#tzSearch').fill(query);
  await page.locator('#tzList .tz-row').first().click();
  await expect(page.locator('#tzPicker')).toBeHidden();
}

test('the account zone survives a browser with no cached copy', async ({ page }) => {
  await login(page, 'America/Denver');
  await page.goto('/');
  await page.evaluate(() => localStorage.removeItem('bifract-timezone'));
  await page.reload();
  // Only Postgres can answer once the mirror is gone.
  await expect(page.locator('#tzClockZone')).toHaveText('MDT', { timeout: 15000 });
});

test('the server rejects a zone it cannot resolve', async ({ page }) => {
  await login(page, 'UTC');
  for (const bad of ['Mars/Olympus', 'Local', '']) {
    const res = await page.request.patch('/api/v1/auth/preferences', { data: { display_timezone: bad } });
    expect(res.status(), `accepted ${JSON.stringify(bad)}`).toBe(400);
  }
  const after = await (await page.request.get('/api/v1/auth/user')).json();
  expect(after.user.display_timezone).toBe('UTC');
});

test('switching zones relabels the results in place and issues no query', async ({ page }) => {
  await login(page, 'UTC');
  await openSearch(page);

  const cell = page.locator('.results-table .timestamp-cell').first();
  const utcText = (await cell.textContent()).trim();
  expect(utcText).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
  await expect(page.locator('#tzClockZone')).toHaveText('UTC');

  let queried = 0;
  page.on('request', r => {
    if (/\/api\/v1\/(query|logs\/recent|logs\/histogram)/.test(r.url())) queried++;
  });

  await pickZone(page, 'Denver');
  await expect(page.locator('#tzClockZone')).toHaveText('MDT');

  const denverText = (await cell.textContent()).trim();
  const delta = (Date.parse(utcText.replace(' ', 'T') + 'Z') - Date.parse(denverText.replace(' ', 'T') + 'Z')) / 3600000;
  expect(delta, 'MDT is six hours behind UTC').toBe(6);

  // The hover text has to make the displayed value reconcilable against a raw log.
  await expect(cell.locator('span')).toHaveAttribute('title', `${denverText} MDT (UTC-06:00)\n${utcText} UTC`);

  expect(queried, 'a zone change must not re-run the search').toBe(0);
});

test('the stored log keeps its UTC value while the column is relabelled', async ({ page }) => {
  // Read the row under UTC first: what the stored payload must still say is
  // this row's UTC time, whatever format the source wrote it in. Asserting a
  // literal "Z" instead only tested how the fixture happened to be generated.
  await login(page, 'UTC');
  await openSearch(page);
  const cell = page.locator('.results-table .timestamp-cell').first();
  const utcText = (await cell.textContent()).trim();

  await pickZone(page, 'Denver');
  const shown = (await cell.textContent()).trim();
  expect(shown, 'the column must relabel when the zone changes').not.toBe(utcText);

  // norm_log is the stored payload; relabelling the column must not rewrite it.
  // Sources write the separator either way, so both forms count as the same
  // instant: what matters is that the UTC value is still what is stored.
  const raw = await page.locator('.results-table .raw-log-col').first().textContent();
  const storedUTC = raw.includes(utcText) || raw.includes(utcText.replace(' ', 'T'));
  expect(storedUTC, `stored payload does not carry ${utcText}: ${raw.slice(0, 200)}`).toBeTruthy();
  expect(raw).not.toContain(shown);
});
