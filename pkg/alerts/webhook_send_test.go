package alerts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

	result := NewWebhookClient("").Send(context.Background(), webhook, alert, alert.Name, results)

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
	result := NewWebhookClient("").Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

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
	result := NewWebhookClient("").Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

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

	result := NewWebhookClient("").Send(context.Background(), webhook, alert, alert.Name, results)
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
	result := NewWebhookClient("").Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", nil)

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
	result := NewWebhookClient("").SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: srv.URL, RetryCount: 1}, body, "application/json")

	if !result.Success {
		t.Fatalf("send failed: %+v", result)
	}
	if got != string(body) {
		t.Errorf("delivered %q, want %q", got, body)
	}
}

func TestSendRenderedRejectsBadURL(t *testing.T) {
	result := NewWebhookClient("").SendRendered(context.Background(),
		WebhookAction{Name: "x", URL: "ftp://nope", RetryCount: 1}, []byte("{}"), "application/json")
	if result.Success {
		t.Fatal("expected failure for a non-http scheme")
	}
	if !strings.Contains(result.Error, "scheme") {
		t.Errorf("error = %q", result.Error)
	}
}
