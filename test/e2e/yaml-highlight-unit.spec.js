// Unit tests for Utils.highlightYAML. Pure logic, evaluated in Node with no page.
// The highlighted text is what an analyst reads before adding a rule, so it must
// say exactly what the file says: the visible text has to survive untouched.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function loadUtils() {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static/utils.js'), 'utf8');
  const window = {};
  new Function('window', 'navigator', 'document', src)(window, { platform: 'Linux' }, {});
  return window.Utils;
}

// Strip tags and unescape, giving the text a reader sees.
function visible(html) {
  return html.replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'").replace(/&amp;/g, '&');
}

// The class wrapping the first occurrence of text, or null.
function classOf(html, text) {
  const re = /<span class="yaml-([a-z]+)">([^<]*)<\/span>/g;
  let m;
  while ((m = re.exec(html))) if (visible(m[2]) === text) return m[1];
  return null;
}

const RULE = `# Native rule
name: Repeated LSASS Process Access
description: >-
  One process: three accesses. # not a comment
  Second line
severity: high
enabled: true
throttle: 300
queryString: 'target_image=~lsass.exe | groupBy(image)'
quoted: "a # b"
tags:
  - attack.t1003.001
  - name: nested
detection:
  selection:
    Image|endswith: '\\certutil.exe'
    CommandLine|contains:
      - 'urlcache'
url: https://example.com/a#frag # trailing
`;

test('the visible text is exactly the input', () => {
  const Utils = loadUtils();
  expect(visible(Utils.highlightYAML(RULE))).toBe(RULE);
  expect(visible(Utils.highlightYAML('<script>alert(1)</script>: "&"'))).toBe('<script>alert(1)</script>: "&"');
});

test('input is escaped, never interpreted as markup', () => {
  const Utils = loadUtils();
  const html = Utils.highlightYAML('key: <img src=x onerror=alert(1)>');
  expect(html).not.toContain('<img');
  expect(html).toContain('&lt;img');
});

test('tokens are classified', () => {
  const Utils = loadUtils();
  const html = Utils.highlightYAML(RULE);
  expect(classOf(html, '# Native rule')).toBe('comment');
  expect(classOf(html, 'name')).toBe('key');
  expect(classOf(html, 'Image|endswith')).toBe('key');
  expect(classOf(html, 'Repeated LSASS Process Access')).toBe('string');
  expect(classOf(html, "'target_image=~lsass.exe | groupBy(image)'")).toBe('string');
  expect(classOf(html, '"a # b"')).toBe('string');
  expect(classOf(html, 'true')).toBe('keyword');
  expect(classOf(html, '300')).toBe('number');
  expect(classOf(html, '>-')).toBe('keyword');
  expect(classOf(html, '# trailing')).toBe('comment');
  expect(classOf(html, 'https://example.com/a#frag')).toBe('string');
});

test('block scalar lines are text, not keys or comments', () => {
  const Utils = loadUtils();
  const html = Utils.highlightYAML(RULE);
  expect(classOf(html, '  One process: three accesses. # not a comment')).toBe('string');
  expect(classOf(html, 'One process')).toBeNull();
  // The block ends where indentation returns to the key's level.
  expect(classOf(html, 'severity')).toBe('key');
});

test('CRLF line endings and a tab after the colon still find keys', () => {
  const Utils = loadUtils();
  const crlf = 'list:\r\n  - a\r\nname: b\r\n';
  const html = Utils.highlightYAML(crlf);
  expect(visible(html)).toBe(crlf);
  expect(classOf(html, 'list')).toBe('key');
  expect(classOf(Utils.highlightYAML('a:\tb'), 'a')).toBe('key');
});

test('a long line without a colon highlights in linear time', () => {
  const Utils = loadUtils();
  const start = Date.now();
  Utils.highlightYAML('a' + ' '.repeat(200000) + 'b');
  expect(Date.now() - start).toBeLessThan(500);
});
