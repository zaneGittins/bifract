// Unit tests for the "Send test" kebab item on the alert actions list, evaluated in Node.
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

function load(responses) {
  const src = fs.readFileSync(path.join(__dirname, '../../web/static/alerts.js'), 'utf8');
  const window = {};
  const document = { getElementById() { return null; }, addEventListener() {}, querySelectorAll() { return []; } };
  const toasts = [];
  const Toast = { show: (msg, kind) => toasts.push({ msg, kind }) };
  const fetch = async (url) => ({ json: async () => responses[url] });
  new Function('window', 'document', 'Toast', 'fetch', src)(window, document, Toast, fetch);
  return { Alerts: window.Alerts, toasts };
}

const webhookResponse = (result) => ({
  success: true,
  data: { rendered_body: '{}', content_type: 'application/json', sent: true, result },
});

test('a delivered webhook test reports success', async () => {
  const { Alerts, toasts } = load({ '/api/v1/webhooks/w1/test': webhookResponse({ success: true, status_code: 200 }) });
  await Alerts.testUnifiedAction('w1', 'webhook');
  expect(toasts).toEqual([{ msg: 'Test sent successfully', kind: 'success' }]);
});

test('a failed webhook test reports the delivery error', async () => {
  const { Alerts, toasts } = load({ '/api/v1/webhooks/w1/test': webhookResponse({ success: false, status_code: 500, error: 'HTTP 500' }) });
  await Alerts.testUnifiedAction('w1', 'webhook');
  expect(toasts).toEqual([{ msg: 'Test failed: HTTP 500', kind: 'error' }]);
});

test('a webhook template that does not render reports the render error', async () => {
  const resp = { success: true, data: { content_type: 'application/json', sent: false, render_error: 'bad template' } };
  const { Alerts, toasts } = load({ '/api/v1/webhooks/w1/test': resp });
  await Alerts.testUnifiedAction('w1', 'webhook');
  expect(toasts).toEqual([{ msg: 'Test failed: bad template', kind: 'error' }]);
});

test('email tests keep their flat result', async () => {
  const { Alerts, toasts } = load({ '/api/v1/email-actions/e1/test': { success: true, data: { success: true } } });
  await Alerts.testUnifiedAction('e1', 'email');
  expect(toasts).toEqual([{ msg: 'Test sent successfully', kind: 'success' }]);
});
