package alerts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testPayload() WebhookPayload {
	return WebhookPayload{
		AlertName:   "Suspicious Login",
		AlertID:     "abc-123",
		Description: "Repeated failures",
		Severity:    "high",
		Labels:      []string{"attack.credential_access"},
		TriggeredAt: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
		QueryString: `event_id=4625`,
		MatchCount:  2,
		Results: []map[string]interface{}{
			{"host.name": "web-01", "user": "alice"},
			{"host.name": "web-02", "user": "bob"},
		},
	}
}

// Existing actions carry no body_mode, and must keep sending the envelope.
func TestRenderEnvelopeIsDefault(t *testing.T) {
	for _, mode := range []string{"", "envelope", "ENVELOPE", "bogus"} {
		body, contentType, err := RenderWebhookBody(WebhookAction{BodyMode: mode}, testPayload())
		if err != nil {
			t.Fatalf("mode %q: unexpected error: %v", mode, err)
		}
		if contentType != "application/json" {
			t.Errorf("mode %q: content type = %q, want application/json", mode, contentType)
		}
		var decoded WebhookPayload
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("mode %q: envelope is not valid JSON: %v", mode, err)
		}
		if decoded.AlertName != "Suspicious Login" || decoded.MatchCount != 2 {
			t.Errorf("mode %q: envelope lost fields: %+v", mode, decoded)
		}
	}
}

// The shape that motivated templating: one HEC event per result row.
func TestRenderSplunkHECShape(t *testing.T) {
	webhook := WebhookAction{
		BodyMode: BodyModeTemplate,
		BodyTemplate: `{{- range .Results}}
{"time":{{unixSeconds $.TriggeredAt}},"sourcetype":"bifract:alert","event":{{toJSON .}}}
{{- end}}`,
	}

	body, _, err := RenderWebhookBody(webhook, testPayload())
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d event lines, want 2: %q", len(lines), body)
	}
	for i, line := range lines {
		var event struct {
			Time       int64                  `json:"time"`
			Sourcetype string                 `json:"sourcetype"`
			Event      map[string]interface{} `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d is not valid JSON: %v (%q)", i, err, line)
		}
		if event.Time != testPayload().TriggeredAt.Unix() {
			t.Errorf("line %d: time = %d", i, event.Time)
		}
		if event.Event["host.name"] == nil {
			t.Errorf("line %d: result row not embedded: %v", i, event.Event)
		}
	}
}

// Log field names contain dots, which template .Field syntax cannot address.
func TestRenderFieldHelperReachesDottedKeys(t *testing.T) {
	webhook := WebhookAction{
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{range .Results}}{{field . "host.name"}} {{end}}`,
	}
	body, _, err := RenderWebhookBody(webhook, testPayload())
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if got := strings.TrimSpace(string(body)); got != "web-01 web-02" {
		t.Errorf("got %q, want %q", got, "web-01 web-02")
	}
}

// text/template, not html/template: HTML escaping would corrupt a JSON body.
func TestRenderDoesNotHTMLEscape(t *testing.T) {
	payload := testPayload()
	payload.QueryString = `user="a&b" | where x<1`
	webhook := WebhookAction{
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{.QueryString}}`,
	}
	body, _, err := RenderWebhookBody(webhook, payload)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if string(body) != payload.QueryString {
		t.Errorf("body was escaped: got %q, want %q", body, payload.QueryString)
	}
}

func TestRenderContentTypeOverride(t *testing.T) {
	webhook := WebhookAction{
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `x`,
		ContentType:  "text/plain",
	}
	_, contentType, err := RenderWebhookBody(webhook, testPayload())
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if contentType != "text/plain" {
		t.Errorf("content type = %q, want text/plain", contentType)
	}
}

func TestTemplateModeRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		tmpl string
	}{
		{"empty template", "   "},
		{"syntax error", `{{range .Results}}`},
		{"unknown function", `{{explode .}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateWebhookBody(BodyModeTemplate, tc.tmpl); err == nil {
				t.Error("expected validation error, got nil")
			}
			if _, _, err := RenderWebhookBody(WebhookAction{BodyMode: BodyModeTemplate, BodyTemplate: tc.tmpl}, testPayload()); err == nil {
				t.Error("expected render error, got nil")
			}
		})
	}
}

// An envelope-mode action is not held to template rules.
func TestValidateIgnoresTemplateWhenEnvelope(t *testing.T) {
	if err := ValidateWebhookBody(BodyModeEnvelope, `{{broken`); err != nil {
		t.Errorf("envelope mode should not validate the template: %v", err)
	}
}

// A range over a large result set must fail loudly rather than allocate freely.
func TestRenderRejectsOversizedBody(t *testing.T) {
	payload := testPayload()
	row := map[string]interface{}{"blob": strings.Repeat("x", 4096)}
	payload.Results = make([]map[string]interface{}, 4000)
	for i := range payload.Results {
		payload.Results[i] = row
	}

	webhook := WebhookAction{
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{range .Results}}{{toJSON .}}{{end}}`,
	}
	_, _, err := RenderWebhookBody(webhook, payload)
	if err == nil {
		t.Fatal("expected an oversize error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseBodyTemplateRejectsOversizedSource(t *testing.T) {
	if _, err := ParseBodyTemplate(strings.Repeat("x", maxTemplateSize+1)); err == nil {
		t.Error("expected an error for an oversized template source")
	}
}
