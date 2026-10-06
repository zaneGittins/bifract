// Sidebar navigation and the Go to palette.
//
// The sidebar replaced two header tab rows, so these pin what the rows used to
// guarantee (every page reachable, the URL names where you are, a new tab opens
// the same page) plus what only the sidebar does: per-page collapse memory,
// scope-gated items, and a palette that never leaks keys to the page behind it.
const { test, expect } = require('@playwright/test');
const { login, listFractals, openNav } = require('./fixtures');

const nav = (page, key) => page.locator(`#sidebar a[data-nav="${key}"]`);
const collapsed = page => page.evaluate(() => document.documentElement.classList.contains('sb-collapsed'));

async function openPage(page, tab) {
  await login(page);
  const [fractal] = await listFractals(page);
  await page.goto(`/#f/${fractal.id}/${tab}`);
  await expect(nav(page, tab)).toHaveAttribute('aria-current', 'page', { timeout: 15000 });
  return fractal;
}

test.describe('sidebar', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.removeItem('bifract-sidebar'));
  });

  test('items link to the current scope and mark the active page', async ({ page }) => {
    const fractal = await openPage(page, 'alerts');
    await expect(nav(page, 'dashboards')).toHaveAttribute('href', `#f/${fractal.id}/dashboards`);

    await nav(page, 'dashboards').click();
    await expect(page).toHaveURL(new RegExp(`#f/${fractal.id}/dashboards$`));
    await expect(nav(page, 'dashboards')).toHaveAttribute('aria-current', 'page');
    await expect(nav(page, 'alerts')).not.toHaveAttribute('aria-current', 'page');
  });

  test('a modified click opens the page in a new tab', async ({ page, context }) => {
    const fractal = await openPage(page, 'alerts');
    const [popup] = await Promise.all([
      context.waitForEvent('page'),
      nav(page, 'notebooks').click({ modifiers: ['Control'] }),
    ]);
    await expect(popup).toHaveURL(new RegExp(`#f/${fractal.id}/notebooks$`));
    await expect(popup.locator('#sidebar a[data-nav="notebooks"]')).toHaveAttribute('aria-current', 'page', { timeout: 15000 });
    // The original tab did not navigate.
    await expect(nav(page, 'alerts')).toHaveAttribute('aria-current', 'page');
  });

  test('Query starts collapsed, other pages expanded, and each remembers its own choice', async ({ page }) => {
    await openPage(page, 'search');
    expect(await collapsed(page)).toBe(true);

    await nav(page, 'alerts').click();
    expect(await collapsed(page)).toBe(false);

    await page.locator('#sidebarToggle').click();
    expect(await collapsed(page)).toBe(true);

    await nav(page, 'search').click();
    expect(await collapsed(page)).toBe(true);
    await page.locator('#sidebarToggle').click();
    expect(await collapsed(page)).toBe(false);

    // A reload applies the saved state before first paint.
    await page.reload();
    await expect(nav(page, 'search')).toHaveAttribute('aria-current', 'page', { timeout: 15000 });
    expect(await collapsed(page)).toBe(false);
  });

  test('the listing has no scoped items and admin pages keep the selected fractal', async ({ page }) => {
    const fractal = await openPage(page, 'alerts');

    await openNav(page, 'settings');
    await expect(page).toHaveURL(/#settings/);
    await expect(nav(page, 'alerts')).toHaveAttribute('href', `#f/${fractal.id}/alerts`);

    await page.locator('#sidebar a[data-home]').click();
    await expect(page).toHaveURL(/#fractalListing$/);
    await expect(nav(page, 'fractalListing')).toHaveAttribute('aria-current', 'page');
    await expect(nav(page, 'search')).toBeHidden();
    await expect(page.locator('#fractalSelectorText')).toHaveText('Select a fractal');
  });

  test('the scope chip switches fractal and moves the URL with it', async ({ page }) => {
    await login(page);
    const fractals = await listFractals(page);
    test.skip(fractals.length < 2, 'needs two fractals');
    await openPage(page, 'search');

    const target = fractals[1];
    await page.locator('#searchView [data-scope-chip]').click();
    await page.locator(`#fractalSelectorMenu [data-scope-id="${target.id}"]`).click();
    await expect(page).toHaveURL(new RegExp(`#f/${target.id}/search$`));
    await expect(page.locator('#searchView .scope-chip-name')).toHaveText(target.name);
  });
});

test.describe('go to palette', () => {
  test('opens with the shortcut and jumps to a sub-page', async ({ page }) => {
    const fractal = await openPage(page, 'dashboards');
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+k' : 'Control+k');
    await expect(page.locator('#goToPalette')).toBeVisible();

    await page.locator('#goToInput').fill('coverage');
    await expect(page.locator('.goto-item.active')).toContainText('Alerts > Coverage');
    await page.keyboard.press('Enter');

    await expect(page.locator('#goToPalette')).toBeHidden();
    await expect(page).toHaveURL(new RegExp(`#f/${fractal.id}/alerts/coverage$`));
    await expect(page.locator('#alertsSubTabs .alerts-sub-tab.active')).toHaveText('Coverage');
  });

  test('Escape closes only the palette', async ({ page }) => {
    await openPage(page, 'alerts');
    await page.locator('#notificationBellBtn').click();
    await expect(page.locator('#notificationDropdown')).toBeVisible();

    // The shortcut, not a click: a click elsewhere closes the dropdown by itself.
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+k' : 'Control+k');
    await expect(page.locator('#goToInput')).toBeFocused();
    await page.keyboard.press('Escape');

    await expect(page.locator('#goToPalette')).toBeHidden();
    await expect(page.locator('#notificationDropdown')).toBeVisible();
  });

  test('the shortcut opens the query palette on Query', async ({ page }) => {
    await openPage(page, 'search');
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+k' : 'Control+k');
    await expect(page.locator('#queryPalette')).toBeVisible();
    await expect(page.locator('#goToPalette')).toBeHidden();
  });
});

test.describe('phone layout', () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test('the sidebar is a drawer that closes on navigation', async ({ page }) => {
    await openPage(page, 'alerts');
    await expect(nav(page, 'dashboards')).toBeHidden();
    await expect(page.locator('#sidebarMobileTitle')).toHaveText('Alerts');

    await page.locator('#sidebarOpen').click();
    await nav(page, 'dashboards').click();
    await expect(page.locator('#sidebarMobileTitle')).toHaveText('Dashboards');
    await expect(nav(page, 'dashboards')).toBeHidden();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
});
