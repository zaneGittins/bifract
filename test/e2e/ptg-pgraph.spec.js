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
      'bifract_category="process_creation" | groupby(parent_process_guid) | sort(_count, order=desc) | limit(5)', f.id);
    for (const row of parents.results || []) {
      const guid = row.parent_process_guid;
      if (!guid) continue;
      const tree = await runQuery(page, `ptg(start="${guid}") | pgraph()`, f.id);
      if ((tree.count || 0) >= 2) return { fractal: f, guid };
    }
  }
  return null;
}

// Time cells whose text does not fit their box (the column used to clip "Sep 29 11:41"
// to "29 11:41"). Empty when every time is fully readable.
async function clippedTimeCells(page) {
  return page.evaluate(() => Array.from(document.querySelectorAll('.pg-tree .pg-gutter, .pg-tree .pg-time'))
    .filter(el => el.scrollWidth > el.clientWidth + 0.5)
    .map(el => el.textContent.trim()));
}

// A pgr() seed with a scored tree of some size, preferring one that pulled in reconnected
// peers so the Reconnected section is exercised too.
async function findPgrSeed(page) {
  const listRes = await page.request.get('/api/v1/fractals');
  expect(listRes.ok(), 'fractal listing failed').toBeTruthy();
  const fractals = (await listRes.json())?.data?.fractals || [];
  // LOLBin launches tend to score and to share rare infrastructure; busy parents are the fallback.
  const candidates = [
    'bifract_category="process_creation" image=~mshta,certutil,rundll32,regsvr32 | groupby(process_guid) | limit(5)',
    'bifract_category="process_creation" | groupby(parent_process_guid) | sort(_count, order=desc) | limit(5)',
  ];
  let fallback = null;
  for (const f of fractals) for (const cq of candidates) {
    const parents = await runQuery(page, cq, f.id);
    for (const row of parents.results || []) {
      const guid = row.process_guid || row.parent_process_guid;
      if (!guid) continue;
      const res = await runQuery(page, `pgr(start="${guid}")`, f.id);
      const rows = res.results || [];
      const procs = new Set(rows.filter(r => !/^(net|dns|file):/.test(r.child)).map(r => r.child));
      if (procs.size < 8 || !rows.some(r => r.anomaly_score !== '' && !isNaN(parseFloat(r.anomaly_score)))) continue;
      const seed = { fractal: f, guid, peers: rows.some(r => String(r.event_type).startsWith('reconnect')) };
      if (seed.peers) return seed;
      fallback = fallback || seed;
    }
  }
  return fallback;
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

    // No scores, so no anomalous path to narrow to: the Path | Full toggle is absent.
    await expect(page.locator('.pg-view-btn[data-mode]')).toHaveCount(0);

    // The indented outline shares the model, so it must fill in too.
    await page.locator('.pg-view-btn[data-view="table"]').click();
    await expect(page.locator('.pg-tree .pg-row').first()).toBeVisible();
    await expect(page.locator('.pg-tree .pg-anom-spacer')).toHaveCount(0);
    await expect(page.locator('.pg-tree-head .pg-th-score')).toHaveCount(0);
    expect(await clippedTimeCells(page), 'Time column cells clipped').toEqual([]);

    expect(errors, 'page errors during render').toEqual([]);
  });
});

test.describe('pgr() | pgraph()', () => {
  test('anomalous path, fact chips, drawer actions, table time and keyboard', async ({ page }) => {
    const login = await page.request.post('/api/v1/auth/login', { data: { username: USER, password: PASS } });
    expect(login.ok(), 'login request failed').toBeTruthy();

    const seed = await findPgrSeed(page);
    test.skip(!seed, 'no scored pgr() tree in any fractal on this stack');

    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));
    // Pivots open a new tab; record them instead so the test stays on the graph.
    await page.addInitScript(() => { window.__opened = []; window.open = (u) => { window.__opened.push(String(u)); return null; }; });

    const query = `pgr(start="${seed.guid}") | pgraph()`;
    const q = Buffer.from(encodeURIComponent(query)).toString('base64');
    const url = `/?q=${encodeURIComponent(q)}&tr=30d&f=${seed.fractal.id}`;
    await page.goto(url);
    await expect(page.locator('.pg-graph .pg-node').first()).toBeVisible({ timeout: 60000 });

    // Path | Full: Path never hides anything silently, Full shows everything, and the choice
    // survives a reload.
    const stats = page.locator('.pg-stats-right');
    await page.locator('.pg-view-btn[data-mode="path"]').click();
    await expect(page.locator('.pg-view-btn[data-mode="path"]')).toHaveClass(/active/);
    await expect(page.locator('.pg-graph .pg-node.pg-focus'), 'Path view keeps the seed').toHaveCount(1);
    if (/Showing/.test(await stats.innerText())) {
      expect(await page.locator('.pg-graph .pg-node-hidden[data-hid]').count(), 'hidden processes need a +N marker').toBeGreaterThan(0);
    }
    await page.reload();
    await expect(page.locator('.pg-graph .pg-node').first()).toBeVisible({ timeout: 60000 });
    await expect(page.locator('.pg-view-btn[data-mode="path"]')).toHaveClass(/active/);
    await page.locator('.pg-view-btn[data-mode="full"]').click();
    await expect(stats).not.toContainText('Showing');
    await expect(page.locator('.pg-graph .pg-node-hidden[data-hid]')).toHaveCount(0);

    // An explained edge score shows its facts on hover.
    const pill = page.locator('.pg-graph .pg-elabel[data-ew]').first();
    if (await pill.count()) {
      await pill.hover();
      await expect(page.locator('.pg-graph .pg-tip')).toBeVisible();
      expect(await page.locator('.pg-graph .pg-tip .fact-chip').count()).toBeGreaterThan(0);
    }

    // Process drawer: facts as chips, actions that pivot in a new tab, Esc to close.
    await page.locator('.pg-graph .pg-node.pg-focus .pg-hex').click();
    const drawer = page.locator('.pg-graph .pg-drawer.open');
    await expect(drawer).toBeVisible();
    if (await drawer.locator('.pg-why').count()) {
      expect(await drawer.locator('.pg-why .fact-chip').count()).toBeGreaterThan(0);
      await expect(drawer.locator('.pg-why-facts')).toHaveCount(0);
    }
    await expect(drawer.locator('.pg-act[data-act="proc"]')).toBeVisible();
    await drawer.locator('.pg-act[data-act="proc"]').click();
    const opened = await page.evaluate(() => window.__opened);
    expect(opened.length).toBe(1);
    const pivot = new URL(opened[0]);
    expect(pivot.pathname).toBe('/go/search');
    expect(pivot.searchParams.get('q')).toBe(`process_guid="${seed.guid}"`);
    expect(pivot.searchParams.get('fractal')).toBeTruthy();
    await page.keyboard.press('Escape');
    await expect(page.locator('.pg-graph .pg-drawer.open')).toHaveCount(0);

    // Hide branch is reversible from the stats bar.
    await page.locator('.pg-graph .pg-node.pg-focus .pg-hex').click();
    await page.locator('.pg-graph .pg-drawer.open .pg-act[data-act="hide"]').click();
    await expect(page.locator('.pg-graph .pg-node.pg-focus')).toHaveCount(0);
    await expect(stats).toContainText('Showing');
    await page.locator('.pg-stat-hidden').click();
    await expect(page.locator('.pg-graph .pg-node.pg-focus')).toHaveCount(1);
    await expect(page.locator('.pg-stat-hidden')).toHaveCount(0);

    // Table: every time is readable at full width and in a narrow viewport.
    await page.locator('.pg-view-btn[data-view="table"]').click();
    await expect(page.locator('.pg-tree .pg-row.pg-proc').first()).toBeVisible();
    expect(await clippedTimeCells(page), 'Time column cells clipped').toEqual([]);
    const vp = page.viewportSize();
    await page.setViewportSize({ width: 900, height: vp.height });
    await page.waitForTimeout(150);
    expect(await clippedTimeCells(page), 'Time column cells clipped at 900px').toEqual([]);
    await page.setViewportSize(vp);

    // Keyboard: j/k move the cursor, Enter opens the drawer on a process row, Esc closes it.
    await page.locator('.pg-tree .pg-tree-scroll').focus();
    await page.keyboard.press('j');
    await expect(page.locator('.pg-tree .pg-row.pg-kbsel')).toHaveCount(1);
    const first = await page.locator('.pg-tree .pg-row.pg-kbsel').getAttribute('data-guid');
    await page.keyboard.press('j');
    await page.keyboard.press('k');
    expect(await page.locator('.pg-tree .pg-row.pg-kbsel').getAttribute('data-guid')).toBe(first);
    if (first) {
      await page.keyboard.press('Enter');
      await expect(page.locator('.pg-tree .pg-drawer.open')).toBeVisible();
      await expect(page.locator('.pg-tree .pg-drawer.open')).toHaveAttribute('data-guid', first);
      await page.keyboard.press('Escape');
      await expect(page.locator('.pg-tree .pg-drawer.open')).toHaveCount(0);
    }

    // Reconnected peers sit in their own section, grouped by bridge, collapsed to one line
    // that expands to the peer's tree.
    if (seed.peers) {
      await expect(page.locator('.pg-tree .pg-sec-row')).toHaveCount(1);
      expect(await page.locator('.pg-tree .pg-bridge-row').count()).toBeGreaterThan(0);
      const peer = page.locator('.pg-tree .pg-peer-row').first();
      const wasOpen = (await peer.getAttribute('aria-expanded')) === 'true';
      const root = await peer.getAttribute('data-peerroot');
      await peer.click();
      await expect(page.locator(`.pg-tree .pg-peer-row[data-peerroot="${root}"]`)).toHaveAttribute('aria-expanded', String(!wasOpen));
      await page.locator('.pg-view-btn[data-view="graph"]').click();
      await expect(page.locator('.pg-graph .pg-node-sec')).toHaveCount(1);
      // Peers stack below the seeded tree rather than widening the canvas.
      const secY = await page.locator('.pg-graph .pg-node-sec').evaluate(el => parseFloat(el.style.top));
      const focusY = await page.locator('.pg-graph .pg-node.pg-focus').evaluate(el => parseFloat(el.style.top));
      expect(secY).toBeGreaterThan(focusY);
    }

    expect(errors, 'page errors during render').toEqual([]);
  });
});
