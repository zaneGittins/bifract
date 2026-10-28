// graph() labels each node from its own row. Results arrive newest first, so a
// process's row usually comes after a child's row has already named it as a
// parent; the parent must still get its labels rather than show its ID.
const { test, expect } = require('@playwright/test');
const path = require('path');

const CHARTS_JS = path.join(__dirname, '..', '..', 'web', 'static', 'charts.js');

test.use({ storageState: { cookies: [], origins: [] } });

test('a node seen first as a parent keeps the labels from its own row', async ({ page }) => {
  await page.setContent('<div></div>');
  await page.addScriptTag({ path: CHARTS_JS });

  const nodes = await page.evaluate(() => {
    const rows = [
      { guid: 'c', parent: 'b', image: 'cmd.exe' },
      { guid: 'b', parent: 'a', image: 'powershell.exe' },
      { guid: 'd', parent: 'null', image: 'orphan.exe' },
    ];
    return [...BifractCharts.graphNodeDetails(rows, 'guid', 'parent', ['image'])];
  });

  expect(nodes).toEqual([
    ['c', { image: 'cmd.exe' }],
    ['b', { image: 'powershell.exe' }],
    ['a', {}],
    ['d', { image: 'orphan.exe' }],
  ]);
});
