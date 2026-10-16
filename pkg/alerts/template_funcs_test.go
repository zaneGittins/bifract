package alerts

import (
	"strings"
	"testing"
	"time"
)

// The helpers a body template can call are the wire-format toolkit operators build
// destinations with; each one's output is part of what a destination parses.
func TestBodyTemplateHelpers(t *testing.T) {
	triggered := time.Date(2026, 3, 1, 12, 30, 45, 500_000_000, time.FixedZone("x", 3600))
	payload := WebhookPayload{
		AlertName: "Mixed Case", Labels: []string{"a", "b"}, TriggeredAt: triggered,
		Results: []map[string]interface{}{{"host.name": "h1", "empty": ""}},
	}
	cases := map[string]string{
		`{{unixSeconds .TriggeredAt}}`:                                      "1772364645",
		`{{unixMillis .TriggeredAt}}`:                                       "1772364645500",
		`{{rfc3339 .TriggeredAt}}`:                                          "2026-03-01T11:30:45Z",
		`{{join "," .Labels}}`:                                              "a,b",
		`{{lower .AlertName}}|{{upper .AlertName}}`:                         "mixed case|MIXED CASE",
		`{{range .Results}}{{default "none" (field . "missing")}}{{end}}`:   "none",
		`{{range .Results}}{{default "none" (field . "empty")}}{{end}}`:     "none",
		`{{range .Results}}{{default "none" (field . "host.name")}}{{end}}`: "h1",
		`{{default "none" (field nil "x")}}`:                                "none",
		`{{toJSON .Labels}}`:                                                `["a","b"]`,
	}
	for tmpl, want := range cases {
		body, _, err := RenderWebhookBody(WebhookAction{BodyMode: BodyModeTemplate, BodyTemplate: tmpl}, payload)
		if err != nil {
			t.Errorf("%s: %v", tmpl, err)
			continue
		}
		if string(body) != want {
			t.Errorf("%s: got %q, want %q", tmpl, body, want)
		}
	}

	// A value JSON cannot encode is a render error, not a silently empty field.
	payload.Results = []map[string]interface{}{{"bad": make(chan int)}}
	if _, _, err := RenderWebhookBody(WebhookAction{BodyMode: BodyModeTemplate, BodyTemplate: `{{range .Results}}{{toJSON .}}{{end}}`}, payload); err == nil ||
		!strings.Contains(err.Error(), "template render error") {
		t.Errorf("got %v", err)
	}
}

func TestSeverityFromLevel(t *testing.T) {
	cases := map[string]Severity{
		"critical":       SeverityCritical,
		" HIGH ":         SeverityHigh,
		"medium":         SeverityMedium,
		"low":            SeverityLow,
		"informational":  SeverityInformational,
		"info":           SeverityMedium, // not a level Sigma defines
		"":               SeverityMedium,
		"something-else": SeverityMedium,
	}
	for level, want := range cases {
		if got := SeverityFromLevel(level); got != want {
			t.Errorf("SeverityFromLevel(%q) = %q, want %q", level, got, want)
		}
	}
}

func TestFieldStringRendersNativeTypes(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{nil, ""},
		{false, "false"},
		{int8(-3), "-3"},
		{uint16(7), "7"},
		{float32(0.5), "0.5"},
		{float64(1.7e18), "1700000000000000000"},
		{[]string{"a"}, `["a"]`},
		{[2]int{1, 2}, `[1,2]`},
		{struct{ A int }{1}, "{1}"},
		{map[string]interface{}{"bad": make(chan int)}, "map[bad:"},
	}
	for _, tc := range cases {
		if got := fieldString(tc.in); !strings.HasPrefix(got, tc.want) {
			t.Errorf("fieldString(%#v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFractalNameFallsBackToTheID(t *testing.T) {
	if got := (&FractalActionClient{}).getFractalName("f1"); got != "f1" {
		t.Errorf("without a database the id stands in for the name, got %q", got)
	}
}
