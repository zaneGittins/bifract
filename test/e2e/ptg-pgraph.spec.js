// `ptg() | pgraph()`: the unscored process map on the Query tab.
//
// pgraph() was written against pgr()'s scored edge rows. ptg() now projects the
// same edge shape with no anomaly_score and no leaf/reconnection edges, so the
// renderer has to degrade cleanly: nodes still place and the tree still reads,
// while every anomaly affordance (pills, legend, IOC copy) drops out. None of
// that is visible to a static check -- the DOM is valid either way.
//
// Self-seeding: it finds a fractal holding process_creation lineage and a start
// guid whose tree has more than one node, and skips when the stack has none.
const { test, expect } = require('@playwright/test');

const USER = process.env.BIFRACT_E2E_USER || 'admin';
const PASS = process.env.BIFRACT_E2E_PASS || 'bifractbifract';
// Must match the tr=30d the browser runs with: a seed found in a wider window may
// have no lineage inside the one the page queries.
const WINDOW = {
  start: new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString(),
  end: new Date().toISOString(),
};

async function runQuery(page, query, fractalId) {
  const res = await page.request.post('/api/v1/query', {
    data: { query, fractal_id: fractalId, start: WINDOW.start, end: WINDOW.end },
  });
  expect(res.ok(), `query request failed: ${query}`).toBeTruthy();
  return res.json();
}

// A usable seed must have its OWN process_creation row (ptg seeds the recursion on
// process_guid), so a top parent_process_guid is only a candidate until ptg() returns
// a tree for it.
async function findSeed(page) {
  const listRes = await page.request.get('/api/v1/fractals');
  expect(listRes.ok(), 'fractal listing failed').toBeTruthy();
  const fractals = (await listRes.json())?.data?.fractals || [];
  for (const f of fractals) {
    const parents = await runQuery(page,
      'bifract_category="process_creation" | groupby(parent_process_guid) | sort(_count desc) | limit(5)', f.id);
    for (const row of parents.results || []) {
      const guid = row.parent_process_guid;
      if (!guid) continue;
      const tree = await runQuery(page, `ptg(start="${guid}") | pgraph()`, f.id);
      if ((tree.count || 0) >= 2) return { fractal: f, guid };
    }
  }
  return null;
}

test.describe.configure({ timeout: 120000 });

test.describe('ptg() | pgraph()', () => {
  test('renders an unscored process map with the anomaly chrome suppressed', async ({ page }) => {
    const login = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
    expect(login.ok(), 'login request failed').toBeTruthy();

    const seed = await findSeed(page);
    test.skip(!seed, 'no process_creation lineage in any fractal on this stack');

    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));

    // Share-link form: it selects the fractal, sets the range, and runs the query,
    // so the test drives the real render path without touching the pickers.
    const query = `ptg(start="${seed.guid}") | pgraph()`;
    const q = Buffer.from(encodeURIComponent(query)).toString('base64');
    await page.goto(`/?q=${encodeURIComponent(q)}&tr=30d&f=${seed.fractal.id}`);

    const nodes = page.locator('.pg-graph .pg-node');
    await expect(nodes.first()).toBeVisible({ timeout: 60000 });
    expect(await nodes.count(), 'expected a multi-node tree').toBeGreaterThan(1);

    // The seed is centered and ring-highlighted, exactly as with pgr().
    await expect(page.locator('.pg-graph .pg-node.pg-focus')).toHaveCount(1);

    // Unscored: no pills on nodes or edges, no anomaly legend, no IOC copy (a ptg
    // tree has no file/network/DNS nodes to extract).
    await expect(page.locator('.pg-anom')).toHaveCount(0);
    await expect(page.locator('#pgCopyIocBtn')).toHaveCount(0);
    await page.locator('#pgLegendBtn').click();
    const legend = page.locator('.pg-legend');
    await expect(legend).toBeVisible();
    await expect(legend).not.toContainText('Anomaly');
    await expect(legend).toContainText('Process creation only');
    await expect(legend).toContainText('Spawned');

    await expect(page.locator('#outputTypeLabel')).toHaveText('Process Tree');

    // The indented outline shares the model, so it must fill in too.
    await page.locator('.pg-view-btn[data-view="table"]').click();
    await expect(page.locator('.pg-tree .pg-row').first()).toBeVisible();
    await expect(page.locator('.pg-tree .pg-anom-spacer')).toHaveCount(0);
    await expect(page.locator('.pg-tree-head .pg-th-score')).toHaveCount(0);

    expect(errors, 'page errors during render').toEqual([]);
  });
});
