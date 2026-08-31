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
const path = require('path');

// One signed-in session, written by auth.setup.js and reused by every project.
const SESSION = path.join(__dirname, 'test', 'e2e', '.auth', 'session.json');

const BASE_URL = process.env.BIFRACT_E2E_URL || 'http://localhost:8080';

// Specs that mutate state the whole server shares: schema field definitions,
// normalizers, and the signed-in account's display timezone. Every other spec
// reads that state, so these run in their own phase rather than alongside.
const SHARED_STATE = /(schema-fields|admin-ui|timezone)\.spec\.js/;

// Specs whose assertions are about colour, and so mean something different in
// each theme. Everything else asserts behaviour, which does not change with the
// palette, so running the whole suite twice only ever doubled the wall clock.
const THEME_SENSITIVE = /(schema-fields|admin-ui)\.spec\.js/;

// The notebook a capture files into is stored per user, and every spec signs in
// as the same account, so these two cannot run at the same time: whichever sets
// it last wins and the other stars into a notebook it is not watching. They are
// chained rather than merged so a failure still names the spec it came from.
const CAPTURE_A = /star-gutter\.spec\.js/;
const CAPTURE_B = /notebook-rail\.spec\.js/;

const chrome = (colorScheme) => ({ ...devices['Desktop Chrome'], colorScheme, storageState: SESSION });

module.exports = defineConfig({
  testDir: './test/e2e',
  // Files run in parallel, tests within a file stay in order: several specs
  // build on what the previous test in the same file left behind.
  fullyParallel: false,
  workers: Number(process.env.BIFRACT_E2E_WORKERS || 3),
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: process.env.CI ? 'list' : [['list'], ['html', { open: 'never' }]],
  // Several specs run real ClickHouse aggregations, and a few workers sharing
  // one dev stack make those slower than a single serial run ever did. The
  // budget only costs wall clock on a test that is already failing.
  timeout: 60000,
  expect: { timeout: 7000 },
  use: {
    baseURL: BASE_URL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    ignoreHTTPSErrors: true,
  },
  // Three phases, each parallel inside itself. The shared-state specs finish
  // before anything that reads what they changed starts.
  projects: [
    {
      name: 'setup',
      testMatch: /auth\.setup\.js/,
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'shared-state',
      testMatch: SHARED_STATE,
      use: chrome('dark'),
      dependencies: ['setup'],
    },
    {
      // Only the cases that read a computed colour mean anything twice. Running
      // the rest of these files again in light doubled their cost and asserted
      // nothing new.
      name: 'shared-state-light',
      testMatch: THEME_SENSITIVE,
      grep: /themed|hover colour/,
      use: chrome('light'),
      dependencies: ['shared-state'],
    },
    {
      name: 'capture-gutter',
      testMatch: CAPTURE_A,
      use: chrome('dark'),
      dependencies: ['shared-state-light'],
    },
    {
      name: 'capture-rail',
      testMatch: CAPTURE_B,
      use: chrome('dark'),
      dependencies: ['capture-gutter'],
    },
    {
      name: 'app',
      testIgnore: new RegExp([SHARED_STATE, CAPTURE_A, CAPTURE_B].map(r => r.source).join('|')),
      use: chrome('dark'),
      dependencies: ['shared-state-light'],
    },
  ],
});
