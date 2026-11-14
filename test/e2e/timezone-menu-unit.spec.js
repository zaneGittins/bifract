// The user menu's Timezone item and the time bar clock open the same picker.
// Runs the real auth.js menu and timeBar.js against a stubbed page, no stack.
const { test, expect } = require('@playwright/test');
const path = require('path');

const STATIC = path.join(__dirname, '..', '..', 'web', 'static');

test.use({ storageState: { cookies: [], origins: [] } });

const PAGE = `
<div id="userInfo"></div>
<div class="tz-picker-wrap">
  <button type="button" id="tzClockBtn"><span id="tzClockZone"></span> <span id="tzClockTime"></span></button>
  <div id="tzPicker" style="display:none">
    <span id="tzPickerOffset"></span>
    <input type="text" id="tzSearch">
    <div id="tzList"></div>
  </div>
</div>`;

// localStorage needs a real origin, so the page is served from a routed one.
async function load(page) {
  const saved = [];
  await page.route('http://bifract.test/**', route => {
    const req = route.request();
    if (req.method() === 'PATCH') {
      saved.push(req.postDataJSON());
      return route.fulfill({ json: { success: true } });
    }
    return route.fulfill({ contentType: 'text/html', body: PAGE });
  });
  await page.goto('http://bifract.test/');
  for (const f of ['utils.js', 'timezone.js', 'auth.js', 'timeBar.js']) {
    await page.addScriptTag({ path: path.join(STATIC, f) });
  }
  await page.evaluate(() => {
    Auth.updateRBACVisibility = () => {};
    Auth.currentUser = { username: 'analyst', display_name: 'Analyst' };
    Auth.showLoggedInUI();
    TimeBar.init();
    // app.js keeps the menu hint in step with the zone.
    document.addEventListener(TZ.EVENT, () => Auth.updateTimezoneHint());
  });
  return saved;
}

async function chooseFromMenu(page, query) {
  await page.locator('#userClickable').click();
  await page.locator('.user-menu-item', { hasText: 'Timezone' }).click();
  await expect(page.locator('#tzPicker')).toBeVisible();
  await expect(page.locator('#tzSearch')).toBeFocused();
  await page.locator('#tzSearch').fill(query);
  await page.locator('#tzList .tz-row').first().click();
  await expect(page.locator('#tzPicker')).toBeHidden();
}

test('the user menu Timezone item opens the picker and applies the zone', async ({ page }) => {
  const saved = await load(page);
  await chooseFromMenu(page, 'Denver');

  await expect(page.locator('#tzClockZone')).toHaveText(/^M[SD]T$/);
  await expect(page.locator('#tzMenuHint')).toHaveText(/^M[SD]T$/);
  expect(await page.evaluate(() => localStorage.getItem('bifract-timezone'))).toBe('America/Denver');
  expect(saved).toEqual([{ display_timezone: 'America/Denver' }]);
});

test('a zone picked from the clock shows in the user menu, and back', async ({ page }) => {
  await load(page);
  await page.locator('#tzClockBtn').click();
  await page.locator('#tzSearch').fill('Tokyo');
  await page.locator('#tzList .tz-row').first().click();
  await expect(page.locator('#tzMenuHint')).toHaveText('GMT+9');

  await chooseFromMenu(page, 'UTC');
  await expect(page.locator('#tzClockZone')).toHaveText('UTC');
  await expect(page.locator('#tzMenuHint')).toHaveText('UTC');
});

test('a click outside still closes the picker', async ({ page }) => {
  await load(page);
  await page.locator('#tzClockBtn').click();
  await expect(page.locator('#tzPicker')).toBeVisible();
  await page.mouse.click(400, 400);
  await expect(page.locator('#tzPicker')).toBeHidden();
});
