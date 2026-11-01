// Unit tests for how the alert drawer renders references. Pure rendering, evaluated
// in Node with no page. Rules from feeds or older versions may carry a javascript:
// reference, and only http(s) may become a link.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load() {
  const read = (f) => fs.readFileSync(path.join(__dirname, '../../web/static', f), 'utf8');
  const window = {};
  const document = { getElementById() { return null; }, querySelectorAll() { return []; } };
  new Function('window', 'navigator', 'document', read('utils.js'))(window, { platform: 'Linux' }, document);
  const TZ = { format: () => '', MONTHS: [] };
  new Function('window', 'document', 'Utils', 'TZ', read('alertDetail.js'))(window, document, window.Utils, TZ);
  return window.AlertDetail;
}

function referencesHtml(refs) {
  const html = load().renderBody({ name: 'r', query_string: 'a=1', references: refs });
  return html.slice(html.indexOf('alert-references'));
}

test('http and https references are links', () => {
  const html = referencesHtml(['https://attack.mitre.org/x', 'HTTP://example.com/y']);
  expect(html).toContain('<a href="https://attack.mitre.org/x"');
  expect(html).toContain('<a href="HTTP://example.com/y"');
});

test('script and other schemes render as text, not links', () => {
  for (const ref of ['javascript:alert(1)', ' javascript:alert(1)', 'JaVaScRiPt:alert(1)', 'data:text/html,x', 'vbscript:x', '//evil.example']) {
    const html = referencesHtml([ref]);
    expect(html, ref).not.toContain('<a ');
    expect(html, ref).toContain(`<li>${ref}</li>`);
  }
});
