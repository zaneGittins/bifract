// These helpers fill attributes as well as text. Escaping through a div's innerHTML
// leaves quotes alone, so a stored name could break out of an attribute.
const { test, expect } = require('@playwright/test');
const { login } = require('./fixtures');

test('every escape helper encodes quotes', async ({ page }) => {
  await login(page);
  await page.goto('/');
  const out = await page.evaluate(() => {
    const raw = `"'<>`;
    return [
      IngestTokens.esc(raw),
      Autocomplete._escapeHtml(raw),
      FieldStats.escHtml(raw),
      InstructionLibraries.esc(raw),
      BifractWorldMap._escape(raw),
    ];
  });
  for (const got of out) expect(got).toBe('&quot;&#039;&lt;&gt;');
});
