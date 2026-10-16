package alerts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestApplyAuth(t *testing.T) {
	cases := []struct {
		name     string
		webhook  WebhookAction
		wantErr  string
		wantAuth string
		basic    [2]string
	}{
		{name: "none", webhook: WebhookAction{AuthType: "none"}},
		{name: "unset", webhook: WebhookAction{}},
		{name: "bearer", webhook: WebhookAction{AuthType: "bearer", AuthConfig: map[string]string{"token": "t0k"}}, wantAuth: "Bearer t0k"},
		{name: "auth type is case-insensitive", webhook: WebhookAction{AuthType: "Bearer", AuthConfig: map[string]string{"token": "t0k"}}, wantAuth: "Bearer t0k"},
		{name: "bearer without a token", webhook: WebhookAction{AuthType: "bearer"}, wantErr: "bearer token not configured"},
		{name: "bearer with an empty token", webhook: WebhookAction{AuthType: "bearer", AuthConfig: map[string]string{"token": ""}}, wantErr: "bearer token not configured"},
		{name: "basic", webhook: WebhookAction{AuthType: "basic", AuthConfig: map[string]string{"username": "u", "password": "p"}}, basic: [2]string{"u", "p"}},
		{name: "basic without a password", webhook: WebhookAction{AuthType: "basic", AuthConfig: map[string]string{"username": "u"}}, wantErr: "username/password not configured"},
		{name: "unknown scheme", webhook: WebhookAction{AuthType: "hmac"}, wantErr: "unsupported auth type: hmac"},
	}
	wc := NewWebhookClient("")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "https://example.test/hook", nil)
			err := wc.applyAuth(req, tc.webhook)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				if req.Header.Get("Authorization") != "" {
					t.Error("a failed auth must not leave a partial header")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.basic != [2]string{} {
				u, p, ok := req.BasicAuth()
				if !ok || u != tc.basic[0] || p != tc.basic[1] {
					t.Errorf("basic auth = %q/%q/%v", u, p, ok)
				}
				return
			}
			if got := req.Header.Get("Authorization"); got != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", got, tc.wantAuth)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	wc := NewWebhookClient("")
	for raw, ok := range map[string]bool{
		"https://hooks.example.test/x": true,
		"http://10.0.0.1:8080/x":       true,
		"ftp://example.test/x":         false,
		"file:///etc/passwd":           false,
		"https://":                     false,
		"://nope":                      false,
	} {
		if err := wc.validateURL(raw); (err == nil) != ok {
			t.Errorf("%q: err = %v, want ok=%v", raw, err, ok)
		}
	}
}

// A link is only built when the action asks for one and there is a base URL and a
// scope to point it at; otherwise the envelope carries none.
func TestBuildPayloadAlertLink(t *testing.T) {
	triggered := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	alert := &Alert{ID: "a1", Name: "Login {{user}}", QueryString: `user="x"`, FractalID: "f1", Severity: "high", Labels: []string{"l"}}
	results := []map[string]interface{}{{"user": "x"}, {"user": "y"}}

	wc := NewWebhookClient("https://bifract.example.test/")
	payload := wc.BuildPayload(WebhookAction{IncludeAlertLink: true}, alert, "Login x", results, triggered)

	if payload.AlertName != "Login x" || payload.OriginalName != "Login {{user}}" {
		t.Errorf("a resolved name keeps the template as the original: %q / %q", payload.AlertName, payload.OriginalName)
	}
	if payload.MatchCount != 2 || payload.AlertID != "a1" || payload.Severity != "high" || !payload.TriggeredAt.Equal(triggered) {
		t.Errorf("envelope fields: %+v", payload)
	}
	if !strings.HasPrefix(payload.AlertLink, "https://bifract.example.test/?") {
		t.Fatalf("the trailing slash on the base must not double up: %q", payload.AlertLink)
	}
	u, err := url.Parse(payload.AlertLink)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("f") != "f1" || q.Get("tr") != "custom" || q.Get("ts") != "2026-03-01T11:00:00.000Z" || q.Get("te") != "2026-03-01T13:00:00.000Z" {
		t.Errorf("link params: %v", q)
	}

	if p := wc.BuildPayload(WebhookAction{IncludeAlertLink: false}, alert, alert.Name, nil, triggered); p.AlertLink != "" || p.OriginalName != "" {
		t.Errorf("no link requested, and an unresolved name has no original: %+v", p)
	}
	unscoped := *alert
	unscoped.FractalID = ""
	if p := wc.BuildPayload(WebhookAction{IncludeAlertLink: true}, &unscoped, "n", nil, triggered); p.AlertLink != "" {
		t.Errorf("an unscoped alert has nowhere to link: %q", p.AlertLink)
	}
	if p := NewWebhookClient("").BuildPayload(WebhookAction{IncludeAlertLink: true}, alert, "n", nil, triggered); p.AlertLink != "" {
		t.Errorf("no base URL, no link: %q", p.AlertLink)
	}
	if got := wc.buildAlertLink(alert, triggered); got != payload.AlertLink {
		t.Errorf("buildAlertLink and the payload disagree: %q vs %q", got, payload.AlertLink)
	}
}

// What goes on the wire: the default method, the action's own headers, and auth
// applied after them so a configured header cannot be used to swap the credential.
func TestSendAttemptRequestShape(t *testing.T) {
	var got *http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	webhook := WebhookAction{
		URL:        srv.URL,
		Headers:    map[string]string{"X-Team": "soc", "Authorization": "Bearer spoofed"},
		AuthType:   "bearer",
		AuthConfig: map[string]string{"token": "real"},
	}
	result := NewWebhookClient("").SendRendered(context.Background(), webhook, []byte(`{"a":1}`), "application/x-ndjson")

	if !result.Success || result.StatusCode != http.StatusAccepted || result.AttemptCount != 1 {
		t.Fatalf("result: %+v", result)
	}
	if got.Method != http.MethodPost {
		t.Errorf("an unset method must default to POST, got %s", got.Method)
	}
	if got.Header.Get("X-Team") != "soc" || got.Header.Get("Content-Type") != "application/x-ndjson" ||
		got.Header.Get("User-Agent") != "Bifract-Alert-System/1.0" {
		t.Errorf("headers: %v", got.Header)
	}
	if got.Header.Get("Authorization") != "Bearer real" {
		t.Errorf("the configured credential must win over a custom header, got %q", got.Header.Get("Authorization"))
	}
	if body != `{"a":1}` {
		t.Errorf("body = %q", body)
	}
}

// Auth and URL failures are deterministic: they must not consume the retry budget.
func TestSendFailsFastOnConfigurationErrors(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()

	wc := NewWebhookClient("")
	bad := wc.Send(context.Background(), WebhookAction{URL: "gopher://x", RetryCount: 3}, &Alert{}, "n", nil)
	if bad.Success || !strings.Contains(bad.Error, "invalid webhook URL") || bad.AttemptCount != 0 {
		t.Errorf("an invalid URL is reported without an attempt: %+v", bad)
	}

	tmpl := wc.Send(context.Background(), WebhookAction{URL: srv.URL, BodyMode: BodyModeTemplate, BodyTemplate: "{{ .Nope"}, &Alert{}, "n", nil)
	if tmpl.Success || tmpl.AttemptCount != 0 || hits.Load() != 0 {
		t.Errorf("a template that cannot render is reported without sending: %+v", tmpl)
	}

	auth := wc.Send(context.Background(), WebhookAction{URL: srv.URL, AuthType: "bearer", RetryCount: 1}, &Alert{}, "n", nil)
	if auth.Success || !strings.Contains(auth.Error, "authentication error") || hits.Load() != 0 {
		t.Errorf("a missing credential must not reach the destination: %+v (hits %d)", auth, hits.Load())
	}
}

// A retry after a failure that then succeeds reports success with no stale error,
// and a cancelled context stops the backoff rather than sleeping it out.
func TestDeliverRetryOutcome(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	result := NewWebhookClient("").SendRendered(context.Background(), WebhookAction{URL: srv.URL, RetryCount: 2}, []byte("{}"), "application/json")
	if !result.Success || result.Error != "" || result.AttemptCount != 2 {
		t.Errorf("recovered delivery: %+v", result)
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	result = NewWebhookClient("").SendRendered(ctx, WebhookAction{URL: failing.URL, RetryCount: 5}, []byte("{}"), "application/json")
	if result.Success || time.Since(start) > 500*time.Millisecond {
		t.Errorf("a cancelled delivery must stop promptly: %+v after %v", result, time.Since(start))
	}
}

func TestSendAttemptHonoursCustomTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	result := NewWebhookClient("").SendRendered(context.Background(), WebhookAction{URL: srv.URL, TimeoutSecs: 1, RetryCount: 1}, []byte("{}"), "application/json")
	if result.Success || !strings.Contains(result.Error, "request failed") {
		t.Errorf("a hung destination must time out: %+v", result)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the action's 1s timeout was not applied: took %v", elapsed)
	}
}

// The editor's preview renders against the same sample data delivery would, and is
// capped without splitting a character.
func TestTestWebhookConfigPreview(t *testing.T) {
	m := &Manager{}
	webhook := WebhookAction{
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{range .Results}}{{field . "host.name"}};{{end}}`,
		ContentType:  "text/plain",
		FractalID:    "f1",
	}
	out := m.TestWebhookConfig(context.Background(), webhook, false)
	if out.RenderError != "" || out.Sent || out.Result != nil {
		t.Fatalf("a render-only test must not send: %+v", out)
	}
	if out.RenderedBody != "test-host-01;test-host-02;" || out.ContentType != "text/plain" || out.BodyBytes != len(out.RenderedBody) {
		t.Errorf("preview = %+v", out)
	}

	broken := m.TestWebhookConfig(context.Background(), WebhookAction{BodyMode: BodyModeTemplate, BodyTemplate: `{{.Missing.Field}}`}, false)
	if broken.RenderError == "" {
		t.Error("a template that fails to execute must report the error")
	}

	// A body larger than the preview cap is cut on a rune boundary. The leading byte
	// puts the cap in the middle of a two-byte character.
	big := WebhookAction{BodyMode: BodyModeTemplate, BodyTemplate: `x{{range .Results}}` + strings.Repeat("é", 20000) + `{{end}}`}
	cut := m.TestWebhookConfig(context.Background(), big, false)
	if cut.RenderError != "" || !cut.Truncated || len(cut.RenderedBody) != maxPreviewBody-1 || !utf8.ValidString(cut.RenderedBody) {
		t.Errorf("truncated=%v len=%d valid=%v", cut.Truncated, len(cut.RenderedBody), utf8.ValidString(cut.RenderedBody))
	}
	if cut.BodyBytes <= maxPreviewBody {
		t.Errorf("BodyBytes must report the full body, got %d", cut.BodyBytes)
	}
}

// A test send reports the destination's first answer rather than retrying past it.
func TestTestWebhookConfigSendsOnce(t *testing.T) {
	var hits atomic.Int32
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	out := (&Manager{}).TestWebhookConfig(context.Background(), WebhookAction{URL: srv.URL, RetryCount: 5, TimeoutSecs: 10_000}, true)
	if !out.Sent || out.Result == nil || out.Result.Success {
		t.Fatalf("result: %+v", out)
	}
	if hits.Load() != 1 || out.Result.AttemptCount != 1 {
		t.Errorf("a test must make exactly one attempt, made %d", hits.Load())
	}
	if body != out.RenderedBody {
		t.Error("the bytes sent must be the bytes shown")
	}
}
