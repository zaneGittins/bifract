// Shared layout assertions for the browser tests.
//
// Pages carry no title of their own: the tab or sidebar item already names them,
// so a heading that repeats it is noise. Actions sit on the toolbar row, flush
// with the right edge of the content.
const { expect } = require('@playwright/test');

// No page header, and no heading that only repeats the page's name.
async function expectNoPageTitle(scope, name) {
  expect(await scope.locator('.admin-page-head:visible').count(), `${name} has a page header`).toBe(0);
  const headings = await scope.locator('h1:visible, h2:visible, h3:visible').allTextContents();
  const repeats = headings.filter(t => t.trim().toLowerCase() === name.toLowerCase());
  expect(repeats, `${name} repeats its name as a heading`).toEqual([]);
}

// The primary action is on a toolbar row and flush with the given right edge.
async function expectActionFlushRight(scope, right, name) {
  const btn = scope.locator('.admin-toolbar:visible .btn-primary:visible').last();
  await expect(btn, `${name} has no primary action on its toolbar`).toBeVisible();
  const box = await btn.boundingBox();
  expect(Math.abs(right - (box.x + box.width)), `${name} primary action is not flush right`).toBeLessThanOrEqual(1);
}

// Left and right edges of the first visible block in a panel.
async function firstBlockEdges(scope) {
  return scope.evaluate(el => {
    const kid = [...el.children].find(c => c.offsetParent && c.getBoundingClientRect().height > 0);
    const r = (kid || el).getBoundingClientRect();
    return { left: r.left, right: r.right };
  });
}

module.exports = { expectNoPageTitle, expectActionFlushRight, firstBlockEdges };
