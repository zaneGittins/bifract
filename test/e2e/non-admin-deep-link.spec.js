// A non-admin opening a fractal or prism by URL must see its name. The router
// only knows the id from the link, and the admin-only read endpoints refuse them.
const { randomUUID } = require('crypto');
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

const PASSWORD = 'e2e-analyst-password-1';

async function create(page, path, data) {
  const res = await page.request.post(path, { data });
  expect(res.ok(), `POST ${path} failed`).toBeTruthy();
  return (await res.json()).data;
}

async function expectNameShown(browser, baseURL, username, hash, name) {
  const context = await browser.newContext({ baseURL, storageState: { cookies: [], origins: [] } });
  const page = await context.newPage();
  try {
    const res = await page.request.post('/api/v1/auth/login', { data: { username, password: PASSWORD } });
    expect(res.ok(), 'analyst login failed').toBeTruthy();

    await page.goto(`/#${hash}/search`);
    await expect(page.locator('#fractalSelectorText')).toHaveText(name, { timeout: 15000 });
    await expect(page.locator('#currentFractalName')).toHaveText(name);
  } finally {
    await context.close();
  }
}

test.describe('Scope opened by URL as a non-admin', () => {
  const cleanup = [];
  let username;

  test.beforeEach(async ({ page }) => {
    await login(page);
    username = `e2e-analyst-${randomUUID()}`;
    const { invite_token: token } = await create(page, '/api/v1/auth/register', { username, role: 'user' });
    cleanup.push(`/api/v1/users?username=${encodeURIComponent(username)}`);
    await create(page, '/api/v1/auth/invite/accept', { token, password: PASSWORD });
  });

  test.afterEach(async ({ page }) => {
    while (cleanup.length) {
      const path = cleanup.pop();
      expect((await page.request.delete(path)).ok(), `DELETE ${path} failed`).toBeTruthy();
    }
  });

  test('a fractal link shows the fractal name', async ({ page, browser, baseURL }) => {
    const fractal = await create(page, '/api/v1/fractals', { name: `e2e-nonadmin-${username}` });
    cleanup.push(`/api/v1/fractals/${fractal.id}`);
    await create(page, `/api/v1/fractals/${fractal.id}/permissions`, { username, role: 'analyst' });

    await expectNameShown(browser, baseURL, username, `f/${fractal.id}`, fractal.name);
  });

  test('a prism link shows the prism name', async ({ page, browser, baseURL }) => {
    // The prism is the user's only grant: no fractal grant to fall back on.
    const prism = await create(page, '/api/v1/prisms', { name: `e2e-nonadmin-${username}` });
    cleanup.push(`/api/v1/prisms/${prism.id}`);
    await create(page, `/api/v1/prisms/${prism.id}/permissions`, { username, role: 'analyst' });

    await expectNameShown(browser, baseURL, username, `p/${prism.id}`, prism.name);
  });
});
