// The chat renders a reply twice: once as it streams, and again from the stored
// rows when the conversation is reopened. They were different renderers, so a
// turn changed shape the moment you navigated away and came back. These tests
// hold the two paths to the same DOM.
//
// The page is assembled here rather than driven through the app because the
// live path needs a scripted stream, which a real reply cannot provide.
const { test, expect } = require('@playwright/test');
const path = require('path');

const CHAT_JS = path.join(__dirname, '..', '..', 'web', 'static', 'chat.js');
const CHAT_CSS = path.join(__dirname, '..', '..', 'web', 'static', 'chat.css');

const PAGE = `
  <div class="chat-thread-scroll"><div id="chatMessages"></div></div>
  <input id="chatTimeRange" value="24h">
  <div id="chatStatusIndicator"><span id="chatStatusText"></span></div>
`;

async function loadChat(page) {
  await page.setContent(PAGE);
  await page.addScriptTag({ path: CHAT_JS });
  await page.addStyleTag({ path: CHAT_CSS });
  await page.evaluate(() => {
    window.Utils = {
      escapeHtml: (s) => String(s ?? '').replace(/[&<>"']/g,
        (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
    };
    window.BifractCharts = { renderFromPreprocessed: () => null };
  });
}

// One reply: prose, a think step, a query with results, more prose.
const TURN = {
  events: [
    { type: 'token', content: 'Checking the logins.\n\n' },
    { type: 'tool_call', tool_name: 'think', tool_call_id: 'c1', tool_args: { reasoning: 'Narrow to failures first.' } },
    { type: 'tool_call', tool_name: 'query_logs', tool_call_id: 'c2', tool_args: { query: 'event="login"' } },
    { type: 'think', tool_name: 'think', tool_call_id: 'c1', tool_args: { reasoning: 'Narrow to failures first.' } },
    { type: 'tool_result', tool_name: 'think', tool_call_id: 'c1', tool_result: { ok: true } },
    { type: 'tool_result', tool_name: 'query_logs', tool_call_id: 'c2', tool_result: { results: [{ host: 'a', n: 2 }], field_order: ['host', 'n'] } },
    { type: 'token', content: '\n\n**One host** stands out.' },
    { type: 'done' },
  ],
  rows: [
    {
      role: 'assistant', content: 'Checking the logins.\n\n', tool_calls: [
        { id: 'c1', function: { name: 'think', arguments: '{"reasoning":"Narrow to failures first."}' } },
        { id: 'c2', function: { name: 'query_logs', arguments: '{"query":"event=\\"login\\""}' } },
      ],
    },
    { role: 'tool', content: '', tool_results: [{ tool_call_id: 'c1', tool_name: 'think', result: { ok: true } }] },
    { role: 'tool', content: '', tool_results: [{ tool_call_id: 'c2', tool_name: 'query_logs', result: { results: [{ host: 'a', n: 2 }], field_order: ['host', 'n'] } }] },
    { role: 'assistant', content: '\n\n**One host** stands out.', tool_calls: [] },
  ],
};

// The same reply, finished with a presentation instead of prose.
const PRESENTED = {
  events: [
    { type: 'token', content: 'Working on it.' },
    { type: 'tool_call', tool_name: 'query_logs', tool_call_id: 'c1', tool_args: { query: 'event="login"' } },
    { type: 'tool_result', tool_name: 'query_logs', tool_call_id: 'c1', tool_result: { results: [{ host: 'a' }], field_order: ['host'] } },
    { type: 'tool_call', tool_name: 'present_results', tool_call_id: 'c2', tool_args: {} },
    { type: 'present', tool_name: 'present_results', tool_args: { severity: 'warning', summary: 'One host failed 40 logins.', findings: [{ label: 'host', value: 'a' }] } },
    { type: 'done' },
  ],
  rows: [
    { role: 'assistant', content: 'Working on it.', tool_calls: [{ id: 'c1', function: { name: 'query_logs', arguments: '{"query":"event=\\"login\\""}' } }] },
    { role: 'tool', content: '', tool_results: [{ tool_call_id: 'c1', tool_name: 'query_logs', result: { results: [{ host: 'a' }], field_order: ['host'] } }] },
    {
      role: 'assistant', content: 'One host failed 40 logins.', tool_calls: [
        { id: 'c2', function: { name: 'present_results', arguments: JSON.stringify({ severity: 'warning', summary: 'One host failed 40 logins.', findings: [{ label: 'host', value: 'a' }] }) } },
      ],
    },
  ],
};

// A write the model proposed, answered by the user. The reply arrives on two
// streams: the first ends on the card, the second carries the result and the
// rest of the answer.
const CONFIRMED = {
  events: [
    { type: 'token', content: 'That host looks bad.' },
    { type: 'tool_call', tool_name: 'add_comment', tool_call_id: 'c1', tool_args: { log_id: 'l1', text: 'suspicious' } },
    { type: 'tool_confirm', tool_name: 'add_comment', pending_id: 'p1', tool_args: { log_id: 'l1', text: 'suspicious' } },
    { type: 'done' },
  ],
  // Answering runs the tool and resumes the turn on its own stream.
  resumed: [
    { type: 'tool_result', tool_name: 'add_comment', tool_call_id: 'c1', tool_result: { id: 'k1' } },
    { type: 'token', content: 'Commented.' },
    { type: 'done' },
  ],
  rows: [
    { role: 'assistant', content: 'That host looks bad.', tool_calls: [{ id: 'c1', function: { name: 'add_comment', arguments: '{"log_id":"l1","text":"suspicious"}' } }] },
    { role: 'tool', content: '', tool_results: [{ tool_call_id: 'c1', tool_name: 'add_comment', result: { id: 'k1' } }] },
    { role: 'assistant', content: 'Commented.', tool_calls: [] },
  ],
  offers: [{ id: 'p1', tool_call_id: 'c1', tool_name: 'add_comment', arguments: { log_id: 'l1', text: 'suspicious' }, status: 'approve' }],
};

// Replays a turn the way _streamToAssistant does, then the way loadMessages does.
async function renderBothWays(page, turn) {
  return page.evaluate(({ events, rows, offers }) => {
    const msgs = document.getElementById('chatMessages');

    msgs.innerHTML = '';
    Chat.appendUserMessage('who failed to log in?', true);
    msgs.appendChild(Chat.createSeparator());
    const bubble = Chat.createAssistantBubble();
    msgs.appendChild(bubble);
    const contentEl = bubble.querySelector('.chat-msg-content');
    let hasContent = false;
    for (const event of events) hasContent = Chat._handleSSEEvent(contentEl, event, hasContent);
    Chat._finalizeStreamingText(contentEl);
    Chat.clearBubbleLoading(contentEl);
    const live = msgs.innerHTML;

    msgs.innerHTML = '';
    Chat.renderHistory([{ role: 'user', content: 'who failed to log in?' }, ...rows], offers || []);
    return { live, reloaded: msgs.innerHTML };
  }, turn);
}

test.describe('chat rendering', () => {
  test.beforeEach(({ page }) => loadChat(page));

  test('a streamed turn and the same turn reloaded render identically', async ({ page }) => {
    const { live, reloaded } = await renderBothWays(page, TURN);
    expect(reloaded).toBe(live);
  });

  test('a turn ending in a presentation reloads identically', async ({ page }) => {
    const { live, reloaded } = await renderBothWays(page, PRESENTED);
    expect(reloaded).toBe(live);
  });

  // The card is the record of a write the user authorised. Reopening the
  // conversation used to show a bare trace step in its place, with nothing to
  // say the action had been put to anyone.
  test('an answered approval comes back as the card that was answered', async ({ page }) => {
    const { live, reloaded } = await page.evaluate(({ events, resumed, rows, offers }) => {
      const msgs = document.getElementById('chatMessages');

      msgs.innerHTML = '';
      Chat.appendUserMessage('comment on that host', true);
      msgs.appendChild(Chat.createSeparator());
      const bubble = Chat.createAssistantBubble();
      msgs.appendChild(bubble);
      const contentEl = bubble.querySelector('.chat-msg-content');
      let hasContent = false;
      for (const event of events) hasContent = Chat._handleSSEEvent(contentEl, event, hasContent);
      Chat._finalizeStreamingText(contentEl);
      // The user approves, which marks the card and resumes the turn.
      Chat._markConfirmAnswered(contentEl.querySelector('.chat-confirm'), 'approve');
      for (const event of resumed) hasContent = Chat._handleSSEEvent(contentEl, event, hasContent);
      Chat._finalizeStreamingText(contentEl);
      Chat.clearBubbleLoading(contentEl);
      const live = msgs.innerHTML;

      msgs.innerHTML = '';
      Chat.renderHistory([{ role: 'user', content: 'comment on that host' }, ...rows], offers);
      return { live, reloaded: msgs.innerHTML };
    }, CONFIRMED);
    expect(reloaded).toBe(live);
  });

  test('an approval left unanswered comes back answerable', async ({ page }) => {
    await page.evaluate(({ rows, offers }) => {
      Chat.renderHistory([{ role: 'user', content: 'q' }, rows[0]], [{ ...offers[0], status: 'open' }]);
    }, CONFIRMED);
    await expect(page.locator('.chat-confirm:not(.is-answered) .chat-confirm-approve')).toHaveCount(1);
  });

  test('an approval the user never answered comes back superseded, not actionable', async ({ page }) => {
    await page.evaluate(({ rows, offers }) => {
      Chat.renderHistory([{ role: 'user', content: 'q' }, rows[0]], [{ ...offers[0], status: 'superseded' }]);
    }, CONFIRMED);
    await expect(page.locator('.chat-confirm-actions')).toHaveCount(0);
    await expect(page.locator('.chat-confirm-outcome')).toHaveText('Superseded');
  });

  test('one turn is one bubble however many tool rounds it took', async ({ page }) => {
    await page.evaluate((rows) => {
      Chat.renderHistory([{ role: 'user', content: 'q' }, ...rows]);
    }, TURN.rows);
    await expect(page.locator('.chat-message-assistant')).toHaveCount(1);
    await expect(page.locator('.chat-separator')).toHaveCount(1);
  });

  test('a reloaded turn keeps its tool results', async ({ page }) => {
    await page.evaluate((rows) => Chat.renderHistory(rows), TURN.rows);
    await expect(page.locator('.chat-trace-result .chat-mini-table')).toHaveCount(1);
    await expect(page.locator('.chat-trace-step[data-type="think"]')).toHaveCount(1);
  });

  // A round that batches a think with a real call announces them in one order
  // and dispatches them in another. Pairing a result with the step that went in
  // last filed query results under "Thinking".
  test('a result lands under the call it answers, not the step before it', async ({ page }) => {
    await page.evaluate((rows) => Chat.renderHistory(rows), TURN.rows);
    await expect(page.locator('.chat-trace-step[data-type="think"] .chat-trace-result')).toHaveCount(0);
    await expect(page.locator('.chat-trace-step[data-type="query_logs"] .chat-mini-table')).toHaveCount(1);
  });

  test('trace steps follow the order the model called them in', async ({ page }) => {
    await page.evaluate((rows) => Chat.renderHistory(rows), TURN.rows);
    expect(await page.locator('.chat-trace-step').evaluateAll((els) => els.map((e) => e.dataset.type)))
      .toEqual(['think', 'query_logs']);
  });

  // Markdown carries its own block structure. Under the inherited pre-wrap the
  // newlines between its tags rendered as extra blank lines, so an answer grew
  // taller the moment it stopped being plain streamed text.
  test('rendered markdown does not pick up pre-wrap spacing', async ({ page }) => {
    await page.evaluate(() => {
      Chat.renderHistory([{ role: 'assistant', content: 'First line.\n\nSecond line.', tool_calls: [] }]);
    });
    const text = page.locator('.chat-msg-text.chat-markdown');
    await expect(text).toHaveCSS('white-space', 'normal');
    expect(await text.evaluate((el) => el.getBoundingClientRect().height))
      .toBeLessThan(await page.evaluate(() => 4 * parseFloat(getComputedStyle(document.body).fontSize) * 1.6));
  });
});
