// Every dialog shares one layout: a sentence-case title level with its close
// button, labels in one style, checkboxes inline with their labels, and Cancel
// then the primary action at the right.
//
// They used to differ on all of these, and a form-wide input rule stretched
// checkboxes into 100%-wide boxes.
const { test, expect } = require('@playwright/test');
const { login, openNav, openFractal, listFractals } = require('./fixtures');

// Words that keep their capital mid-title.
const PROPER = new Set(['API', 'YAML', 'Sigma', 'Iceberg', 'URL']);

const DIALOGS = [
  { name: 'create fractal', open: async p => { await p.goto('/'); await p.locator('#createFractalBtn').click(); } },
  // The default and system fractals cannot be renamed, so their Edit is hidden.
  { name: 'edit fractal', open: async p => {
    const f = (await listFractals(p)).find(x => !x.is_default && !x.is_system);
    test.skip(!f, 'no editable fractal on this stack');
    await openFractal(p, 'manage', f.name);
    await p.locator('#manageEditFractalBtnInline').click();
  } },
  { name: 'add user', open: async p => { await p.goto('/'); await openNav(p, 'settings'); await p.locator('#addUserBtnSettings').click(); } },
  { name: 'create group', open: async p => { await p.goto('/#settings/groups'); await p.locator('#addGroupBtn').click(); } },
  { name: 'create API key', open: async p => { await p.goto('/#settings/apikeys'); await p.locator('#apiKeysAdminNew').click(); } },
  { name: 'context link', open: async p => { await p.goto('/#settings/context'); await p.locator('#contextLinkCreateBtn').click(); } },
  { name: 'grant access', open: async p => { await openFractal(p, 'manage'); await p.locator('#manageSubTabs [data-subtab="access"]').click(); await p.locator('#grantPermissionBtn').click(); } },
  { name: 'fractal API key', open: async p => { await openFractal(p, 'manage'); await p.locator('#manageSubTabs [data-subtab="access"]').click(); await p.locator('#createAPIKeyInlineBtn').click(); } },
  { name: 'ingest token', open: async p => { await openFractal(p, 'ingest'); await p.locator('#createIngestTokenBtn').click(); } },
  { name: 'lookup', open: async p => { await openFractal(p, 'dictionaries'); await p.locator('#dictCreateBtn').click(); } },
  { name: 'dashboard', open: async p => { await openFractal(p, 'dashboards'); await p.locator('#createDashboardBtn').click(); } },
  { name: 'notebook', open: async p => { await openFractal(p, 'notebooks'); await p.locator('#createNotebookBtn').click(); } },
  { name: 'import alert', open: async p => { await openFractal(p, 'alerts'); await p.locator('#alertsView .btn-menu').click(); await p.locator('.kebab-item', { hasText: 'Import YAML or Sigma' }).click(); } },
];

// The dialog that is open: the last visible modal content on the page.
const openDialog = page => page.locator(':is(.modal, .modal-overlay):visible .modal-content').last();

test.describe('Dialogs', () => {
  for (const d of DIALOGS) {
    test(`${d.name}: one dialog layout`, async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await login(page);
      await d.open(page);
      const dialog = openDialog(page);
      await expect(dialog).toBeVisible();

      const g = await dialog.evaluate((el, proper) => {
        const vis = e => e && e.offsetParent && e.getBoundingClientRect().height > 0;
        const mid = r => r.top + r.height / 2;
        const title = el.querySelector('.modal-header h2, .modal-header h3');
        const close = el.querySelector('.modal-header button');
        const words = (title?.textContent || '').trim().split(/\s+/).slice(1);
        const shouty = words.filter(w => /^[A-Z][a-z]/.test(w) && !proper.includes(w.replace(/\W/g, '')));
        const labels = [...el.querySelectorAll('label')].filter(vis);
        const caps = labels.filter(l => getComputedStyle(l).textTransform !== 'none').map(l => l.textContent.trim());
        const boxes = [...el.querySelectorAll('input[type="checkbox"]')].filter(vis).map(c => {
          const lab = c.closest('label');
          const cr = c.getBoundingClientRect();
          const lr = lab ? lab.getBoundingClientRect() : cr;
          return { w: Math.round(cr.width), off: Math.abs(mid(cr) - mid(lr)) };
        });
        const row = [...el.querySelectorAll('.form-actions, .modal-footer, .modal-buttons')].filter(vis).pop();
        const btns = row ? [...row.querySelectorAll('button')].filter(vis) : [];
        const body = el.querySelector('.modal-body') || el;
        const field = [...body.querySelectorAll('input:not([type="checkbox"]), select, textarea')].find(vis);
        return {
          title: title?.textContent.trim(),
          shouty,
          titleOff: title && close ? Math.abs(mid(title.getBoundingClientRect()) - mid(close.getBoundingClientRect())) : 0,
          caps,
          boxes,
          lastIsPrimary: btns.length ? btns[btns.length - 1].classList.contains('btn-primary') : true,
          rightGap: btns.length ? row.getBoundingClientRect().right - parseFloat(getComputedStyle(row).paddingRight) - btns[btns.length - 1].getBoundingClientRect().right : 0,
          inset: field ? field.getBoundingClientRect().left - el.getBoundingClientRect().left : 20,
        };
      }, [...PROPER]);

      expect(g.shouty, `title "${g.title}" is not sentence case`).toEqual([]);
      expect(g.titleOff, 'title is not level with the close button').toBeLessThanOrEqual(2);
      expect(g.caps, 'labels forced into another case').toEqual([]);
      for (const b of g.boxes) {
        expect(b.w, 'checkbox is stretched').toBeLessThanOrEqual(18);
        expect(b.off, 'checkbox is not level with its label').toBeLessThanOrEqual(3);
      }
      expect(g.lastIsPrimary, 'the primary action is not last').toBe(true);
      expect(g.rightGap, 'actions are not at the right').toBeLessThanOrEqual(1);
      expect(g.inset, 'fields sit behind stacked padding').toBeLessThanOrEqual(26);
    });
  }
});

test.describe('Standalone pages load the app fonts', () => {
  for (const path of ['/login.html', '/shared/not-a-real-token']) {
    test(path, async ({ browser }) => {
      // Signed out, as a first visit is: a saved session shows a different panel.
      const context = await browser.newContext({ storageState: { cookies: [], origins: [] } });
      const page = await context.newPage();
      await page.goto(path);
      await page.waitForLoadState('networkidle');
      const loaded = await page.waitForFunction(() =>
        [...document.fonts].some(f => f.family.replace(/"/g, '') === 'Inter' && f.status === 'loaded'),
        null, { timeout: 5000 }).then(() => true, () => false);
      expect(loaded, 'Inter never loaded, so text renders in a fallback font').toBe(true);
      // Headings included: a leftover rule once gave the sign-in title a Courier stack.
      const offFont = await page.evaluate(() => [...document.querySelectorAll('h1, h2, label, button')]
        .filter(el => el.offsetParent && el.textContent.trim())
        .map(el => getComputedStyle(el).fontFamily)
        .filter(f => !/Inter/.test(f)));
      expect(offFont, 'text set in a font outside the app stack').toEqual([]);
      await context.close();
    });
  }
});
