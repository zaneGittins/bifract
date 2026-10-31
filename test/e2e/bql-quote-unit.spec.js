// Unit tests for Utils.bqlQuote. Pure logic, evaluated in Node with no page.
// The BQL lexer reads a backslash as an escape, so a Windows path that is not
// escaped silently becomes a different value and the filter matches nothing.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function loadUtils() {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static/utils.js'), 'utf8');
  const window = {};
  new Function('window', 'navigator', 'document', src)(window, { platform: 'Linux' }, {});
  return window.Utils;
}

// Mirrors pkg/parser lexer readString: a backslash takes the next character literally.
function lexString(quoted) {
  let out = '';
  for (let i = 1; i < quoted.length - 1; i++) {
    if (quoted[i] === '\\') i++;
    out += quoted[i];
  }
  return out;
}

const VALUES = [
  'C:\\Windows\\System32\\cmd.exe',
  'ends with backslash\\',
  'say "hi"',
  'mixed \\"both\\"',
  'plain',
];

test('bqlQuote round-trips through the lexer', () => {
  const Utils = loadUtils();
  for (const v of VALUES) {
    const q = Utils.bqlQuote(v);
    expect(q.startsWith('"') && q.endsWith('"')).toBe(true);
    expect(lexString(q)).toBe(v);
  }
});

test('bqlEscape is bqlQuote without the quotes', () => {
  const Utils = loadUtils();
  for (const v of VALUES) expect('"' + Utils.bqlEscape(v) + '"').toBe(Utils.bqlQuote(v));
});
