package alerts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end over a real HTTP round trip: a template-mode action must put the
// rendered body, its content type and its auth header on the wire.
func TestSendDeliversTemplateBody(t *testing.T) {
	var gotBody, gotContentType, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"text":"Success","code":0}`))
	}))
	defer srv.Close()

	webhook := WebhookAction{
		Name:         "hec",
		URL:          srv.URL,
		Method:       "POST",
		AuthType:     "bearer",
		AuthConfig:   map[string]string{"token": "hec-token"},
		RetryCount:   1,
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{- range .Results}}{"event":{{toJSON .}}}{{end}}`,
	}
	alert := &Alert{ID: "a1", Name: "Test", Severity: "high"}
	results := []map[string]interface{}{{"user": "alice"}}

	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, alert, alert.Name, results)

	if !result.Success {
		t.Fatalf("send failed: %+v", result)
	}
	if gotBody != `{"event":{"user":"alice"}}` {
		t.Errorf("body = %q", gotBody)
	}
	if gotContentType != "application/json" {
		t.Errorf("content type = %q", gotContentType)
	}
	if gotAuth != "Bearer hec-token" {
		t.Errorf("authorization = %q", gotAuth)
	}
	if result.ResponseBody != `{"text":"Success","code":0}` {
		t.Errorf("response body not captured: %q", result.ResponseBody)
	}
}

// A destination's rejection reason lives in the body, and must survive into the result.
func TestSendCapturesErrorResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"text":"Invalid data format","code":6}`))
	}))
	defer srv.Close()

	webhook := WebhookAction{Name: "hec", URL: srv.URL, RetryCount: 1}
	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

	if result.Success {
		t.Fatal("expected failure")
	}
	if result.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d", result.StatusCode)
	}
	if !strings.Contains(result.ResponseBody, "Invalid data format") {
		t.Errorf("response body = %q", result.ResponseBody)
	}
}

// A broken template fails identically on every attempt, so it must not consume
// the retry schedule before reporting.
func TestSendDoesNotRetryRenderErrors(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	webhook := WebhookAction{
		Name:         "bad",
		URL:          srv.URL,
		RetryCount:   4,
		BodyMode:     BodyModeTemplate,
		BodyTemplate: `{{.Nope.Missing}}`,
	}
	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

	if result.Success {
		t.Fatal("expected failure")
	}
	if hits != 0 {
		t.Errorf("a render error reached the network %d times", hits)
	}
	if result.AttemptCount != 0 {
		t.Errorf("attempt count = %d, want 0", result.AttemptCount)
	}
	if result.Error == "" {
		t.Error("expected an error message")
	}
}

// Envelope mode is what every existing action uses; it must keep its wire shape.
func TestSendEnvelopeUnchanged(t *testing.T) {
	var got WebhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	webhook := WebhookAction{Name: "plain", URL: srv.URL, RetryCount: 1}
	alert := &Alert{ID: "a1", Name: "Test Alert", Severity: "low", QueryString: "x=1"}
	results := []map[string]interface{}{{"user": "alice"}}

	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, alert, alert.Name, results)
	if !result.Success {
		t.Fatalf("send failed: %+v", result)
	}
	if got.AlertName != "Test Alert" || got.MatchCount != 1 || got.QueryString != "x=1" {
		t.Errorf("envelope changed shape: %+v", got)
	}
	if len(got.Results) != 1 || got.Results[0]["user"] != "alice" {
		t.Errorf("results not delivered: %+v", got.Results)
	}
}

// A delivery that succeeds on a retry is a success, and must not carry the
// earlier attempt's error text into the stored result or the UI.
func TestSendClearsErrorAfterRetrySucceeds(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if hits == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	webhook := WebhookAction{Name: "flaky", URL: srv.URL, RetryCount: 2}
	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

	if !result.Success {
		t.Fatalf("expected success on the second attempt: %+v", result)
	}
	if result.Error != "" {
		t.Errorf("stale error retained on a successful delivery: %q", result.Error)
	}
	if result.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", result.StatusCode)
	}
	if result.ResponseBody != "ok" {
		t.Errorf("response body = %q, want the successful attempt's", result.ResponseBody)
	}
	if result.AttemptCount != 2 {
		t.Errorf("attempt count = %d, want 2", result.AttemptCount)
	}
}

// SendRendered must put the exact bytes it was given on the wire, so a test
// sends what the operator was shown.
func TestSendRenderedDeliversExactBytes(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	body := []byte(`{"exact":"bytes","n":1}`)
	result := NewWebhookClient("", allowPrivate).SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: srv.URL, RetryCount: 1}, body, "application/json")

	if !result.Success {
		t.Fatalf("send failed: %+v", result)
	}
	if got != string(body) {
		t.Errorf("delivered %q, want %q", got, body)
	}
}

func TestSendRenderedRejectsBadURL(t *testing.T) {
	result := NewWebhookClient("", allowPrivate).SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: "ftp://nope", RetryCount: 1}, []byte("{}"), "application/json")
	if result.Success {
		t.Fatal("expected failure for a non-http scheme")
	}
	if !strings.Contains(result.Error, "only http and https") {
		t.Errorf("error = %q", result.Error)
	}
}

// A tenant admin chooses the URL, so by default it must not reach the server's
// own network, and the refusal must not say where the name resolved.
func TestSendRefusesPrivateDestination(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()

	for name, policy := range map[string]func(context.Context) bool{
		"no policy":      nil,
		"policy refuses": func(context.Context) bool { return false },
	} {
		t.Run(name, func(t *testing.T) {
			result := NewWebhookClient("", policy).SendRendered(context.Background(),
				WebhookAction{Name: "x", URL: srv.URL, RetryCount: 3}, []byte("{}"), "application/json")
			if result.Success || hits.Load() != 0 {
				t.Fatalf("private destination reached: %+v, hits=%d", result, hits.Load())
			}
			// A refusal is the same on every attempt, so it is not retried.
			if result.AttemptCount != 1 {
				t.Errorf("attempts = %d, want 1", result.AttemptCount)
			}
			if strings.Contains(result.Error, "127.0.0.1") || !strings.Contains(result.Error, "not allowed") {
				t.Errorf("error = %q, want a refusal without the address", result.Error)
			}
		})
	}
}

// A redirect would replay custom headers, which often carry a secret, to a host
// the operator never configured.
func TestSendDoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Bool
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	result := NewWebhookClient("", allowPrivate).SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: srv.URL, RetryCount: 3, Headers: map[string]string{"X-Api-Key": "k"}},
		[]byte("{}"), "application/json")
	if reached.Load() {
		t.Fatal("redirect was followed")
	}
	if hits.Load() != 1 || result.AttemptCount != 1 {
		t.Errorf("a redirect answers the same every time, yet it was tried %d times", hits.Load())
	}
	if result.Success || result.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("result = %+v, want a failed 307", result)
	}
}

func TestSendRejectsCredentialsInURL(t *testing.T) {
	result := NewWebhookClient("", allowPrivate).SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: "https://user:pw@example.com/hook", RetryCount: 1}, []byte("{}"), "application/json")
	if result.Success || !strings.Contains(result.Error, "username or password") {
		t.Errorf("error = %q", result.Error)
	}
}

// Surrounding whitespace passes URL validation, so delivery must not choke on it.
func TestSendTrimsURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	result := NewWebhookClient("", allowPrivate).SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: " " + srv.URL + "\n", RetryCount: 1}, []byte("{}"), "application/json")
	if !result.Success {
		t.Errorf("result = %+v", result)
	}
}

// The egress default would cut a slow destination off at 30s whatever the
// action's timeout, so the header wait follows the action.
func TestWebhookHeaderTimeoutFollowsAction(t *testing.T) {
	wc := NewWebhookClient("", allowPrivate)
	for secs, want := range map[int]time.Duration{0: defaultWebhookTimeout, 120: 120 * time.Second, 300: 300 * time.Second} {
		tr := wc.httpClient(context.Background(), WebhookAction{TimeoutSecs: secs}).Transport.(*http.Transport)
		if tr.ResponseHeaderTimeout != want {
			t.Errorf("timeout %ds: header timeout %v, want %v", secs, tr.ResponseHeaderTimeout, want)
		}
	}
}
