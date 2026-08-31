// One sign-in for the whole run, saved and reused by every project.
//
// Each spec used to authenticate for itself, which with several workers meant a
// hundred logins per run and a throttled server part-way through.
const { test: setup, expect } = require('@playwright/test');
const path = require('path');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';

const STATE = path.join(__dirname, '.auth', 'session.json');

setup('sign in once', async ({ page }) => {
  const res = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
  expect(res.ok(), 'login request failed').toBeTruthy();
  const body = await res.json();
  expect(body.success, `login rejected: ${JSON.stringify(body)}`).toBeTruthy();

  await page.request.storageState({ path: STATE });
});

module.exports = { STATE };
