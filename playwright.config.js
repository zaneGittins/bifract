// Development-only browser tests.
//
// These exist because several UI defects have shipped that static checks cannot
// catch: markup, JS syntax, and DOM wiring all validated cleanly while the page
// was visibly broken (a `display: flex` rule silently defeating the `hidden`
// attribute, for one). Asserting against a real rendered page closes that gap.
//
// Nothing here is packaged. The Go binary serves web/ directly, the runtime
// image is FROM scratch and copies only the compiled binaries plus web/, and
// node_modules is excluded from both git and the Docker build context.
//
// Requires a running stack: docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
const { defineConfig, devices } = require('@playwright/test');

const BASE_URL = process.env.BIFRACT_E2E_URL || 'http://localhost:8080';

module.exports = defineConfig({
  testDir: './test/e2e',
  // The stack is shared and stateful, so tests that mutate schema config must
  // not interleave. Correctness beats wall-clock here.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: process.env.CI ? 'list' : [['list'], ['html', { open: 'never' }]],
  timeout: 30000,
  expect: { timeout: 7000 },
  use: {
    baseURL: BASE_URL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    ignoreHTTPSErrors: true,
  },
  projects: [
    { name: 'dark', use: { ...devices['Desktop Chrome'], colorScheme: 'dark' } },
    { name: 'light', use: { ...devices['Desktop Chrome'], colorScheme: 'light' } },
  ],
});
