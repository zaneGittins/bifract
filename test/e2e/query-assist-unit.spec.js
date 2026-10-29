// Unit tests for the Ask AI diff. Pure logic, evaluated in Node with no page.
// The diff is how an analyst sees what the AI changed before loading it, so a
// stage it kept must read as kept and nothing may be lost or invented.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function loadAssist() {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static/queryAssist.js'), 'utf8');
  const window = {};
  new Function('window', src)(window);
  return window.QueryAssist;
}

const QA = loadAssist();

test('a pipeline splits into one stage per pipe', () => {
  expect(QA.stages('event_id=1 | groupBy(host) | sort(_count)')).toEqual(['event_id=1', '| groupBy(host)', '| sort(_count)']);
});

test('an alternation inside a regex stays in its stage', () => {
  expect(QA.stages('image=/powershell|pwsh/i | groupBy(user)')).toEqual(['image=/powershell|pwsh/i', '| groupBy(user)']);
});

test('a pipe inside a quoted value stays in its stage', () => {
  expect(QA.stages('msg="a | b" | groupBy(host)')).toEqual(['msg="a | b"', '| groupBy(host)']);
  expect(QA.stages('msg="say \\" | " | count()')).toEqual(['msg="say \\" | "', '| count()']);
});

test('a multi-line query keeps its own lines', () => {
  expect(QA.stages('event_id=1\n| groupBy(host)')).toEqual(['event_id=1', '| groupBy(host)']);
});

test('kept stages read as kept, changes as removed and added', () => {
  const before = QA.stages('event_id=4625 | groupBy(src_ip)');
  const after = QA.stages('event_id=4625 | status="failed" | groupBy(src_ip)');
  expect(QA.diff(before, after)).toEqual([
    { op: ' ', line: 'event_id=4625' },
    { op: '+', line: '| status="failed"' },
    { op: ' ', line: '| groupBy(src_ip)' },
  ]);
});

test('every line of both queries appears exactly once', () => {
  const before = QA.stages('a=1 | b() | c() | d()');
  const after = QA.stages('a=2 | c() | e() | d()');
  const d = QA.diff(before, after);
  expect(d.filter(x => x.op !== '+').map(x => x.line)).toEqual(before);
  expect(d.filter(x => x.op !== '-').map(x => x.line)).toEqual(after);
});
