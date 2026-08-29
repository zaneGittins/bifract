package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/aitools"
)

// stub is a fake Bifract that answers every request with one payload and keeps
// the last request, so a test can assert on what the client actually sent.
type stub struct {
	client *Client
	last   *http.Request
	// The request body, read before the handler returns: the cloned request's own
	// body is closed by then.
	sent  []byte
	calls int
}

func serve(t *testing.T, status int, body string) *stub {
	t.Helper()
	s := &stub{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.last = r.Clone(r.Context())
		s.sent, _ = io.ReadAll(r.Body)
		s.calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	s.client = clientFor(server.URL)
	return s
}

func clientFor(url string) *Client {
	return NewClient(Config{URL: url, APIKey: "bifract_test", Timeout: defaultTimeout})
}

func TestTheEnvelopeIsRemovedBeforeTheModelSeesIt(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"a1","name":"prod"}}`)
	got, err := s.client.Get(context.Background(), "/fractals/a1", nil)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected the data object, got %T", got)
	}
	if object["name"] != "prod" {
		t.Errorf("expected the payload, got %v", object)
	}
	if _, leaked := object["success"]; leaked {
		t.Error("the transport envelope reached the model")
	}
}

// A model shown 2 of 40 alerts and told nothing will reason as though it saw all
// of them, so a truncated page must say so.
func TestATruncatedPageSaysHowMuchWasNotShown(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":[{"id":"1"},{"id":"2"}],"page":{"total":40,"limit":2,"offset":0}}`)
	got, err := s.client.Get(context.Background(), "/alerts", nil)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected a paging wrapper, got %T", got)
	}
	if object["showing"] != "1-2 of 40" {
		t.Errorf("showing = %v", object["showing"])
	}
	if object["more"] != true || object["next_offset"] != 2 {
		t.Errorf("more/next_offset = %v/%v", object["more"], object["next_offset"])
	}
}

func TestACompletePageIsReturnedAsAPlainList(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":[{"id":"1"},{"id":"2"}],"page":{"total":2,"limit":50,"offset":0}}`)
	got, err := s.client.Get(context.Background(), "/alerts", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.([]any); !ok {
		t.Fatalf("a page that holds everything should not be wrapped, got %T", got)
	}
}

func TestFailuresCarryTheMachineReadableCode(t *testing.T) {
	s := serve(t, 400, `{"success":false,"error":"query_window_seconds is required","code":"invalid_argument"}`)
	_, err := s.client.Post(context.Background(), "/alerts", map[string]any{})
	if err == nil {
		t.Fatal("a 400 should be an error")
	}
	for _, want := range []string{"query_window_seconds is required", "invalid_argument"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestEachAuthFailureExplainsWhatToFix(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{
		{401, "not authenticated"},
		{403, "permissions do not cover it"},
		{404, "not found"},
	} {
		s := serve(t, tc.status, `{"success":false,"error":"nope"}`)
		_, err := s.client.Get(context.Background(), "/alerts/x", nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%d: got %v, want a message mentioning %q", tc.status, err, tc.want)
		}
	}
}

// An HTML error page from a proxy in front of Bifract must not be handed back as
// if it were data, nor fill the model's context.
func TestANonJSONResponseIsReportedAndTruncated(t *testing.T) {
	s := serve(t, 200, "<html>"+strings.Repeat("x", 5000)+"</html>")
	_, err := s.client.Get(context.Background(), "/query/fields", nil)
	if err == nil {
		t.Fatal("HTML is not a valid response")
	}
	if !strings.Contains(err.Error(), "non-JSON") {
		t.Errorf("error should say the response was not JSON: %v", err)
	}
	if len(err.Error()) > aitools.MaxErrorBody+200 {
		t.Errorf("error body was not truncated: %d chars", len(err.Error()))
	}
}

func TestTheAPIKeyIsSentAsABearerToken(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{}}`)
	if _, err := s.client.Get(context.Background(), "/auth/user", nil); err != nil {
		t.Fatal(err)
	}
	if got := s.last.Header.Get("Authorization"); got != "Bearer bifract_test" {
		t.Errorf("Authorization = %q", got)
	}
	if !strings.HasSuffix(s.last.URL.Path, "/api/v1/auth/user") {
		t.Errorf("path = %q, the versioned prefix is missing", s.last.URL.Path)
	}
}

// A redirect would either drop the Authorization header or replay the key at
// another host. Neither may happen silently.
func TestARedirectIsNotFollowed(t *testing.T) {
	var reached string
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"success":true,"data":{"leaked":true}}`))
	}))
	defer elsewhere.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/api/v1/auth/user", http.StatusFound)
	}))
	defer origin.Close()

	_, err := clientFor(origin.URL).Get(context.Background(), "/auth/user", nil)
	if err == nil {
		t.Fatal("a redirect should not silently succeed")
	}
	if reached != "" {
		t.Fatalf("the API key was replayed at another host: %q", reached)
	}
}

func TestAnEmptyBodyIsAnEmptyObjectNotAnError(t *testing.T) {
	s := serve(t, 204, "")
	got, err := s.client.Delete(context.Background(), "/alerts/x")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(got); string(b) != "{}" {
		t.Errorf("got %s", b)
	}
}

// An instance-wide key belongs to no fractal, so the session names the one it
// means on every request. A key that carries its own scope must not send one.
func TestTheConfiguredScopeIsSentAsAHeader(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{}}`)
	s.client.cfg.Scope = "fractal:f-1"
	if _, err := s.client.Get(context.Background(), "/alerts", nil); err != nil {
		t.Fatal(err)
	}
	if got := s.last.Header.Get("X-Bifract-Scope"); got != "fractal:f-1" {
		t.Errorf("X-Bifract-Scope = %q", got)
	}

	plain := serve(t, 200, `{"success":true,"data":{}}`)
	if _, err := plain.client.Get(context.Background(), "/alerts", nil); err != nil {
		t.Fatal(err)
	}
	if got := plain.last.Header.Get("X-Bifract-Scope"); got != "" {
		t.Errorf("X-Bifract-Scope = %q, want none when the key carries its own", got)
	}
}

// The server ignores the scope header for a key that already has a scope, so
// honouring it here would name a fractal this session is not acting in.
func TestTheCredentialsOwnFractalWinsOverTheConfiguredOne(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"user":{"selected_fractal":"bound-to-this"}}}`)
	s.client.cfg.Scope = "fractal:asked-for-that"
	got, err := s.client.FractalID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "bound-to-this" {
		t.Errorf("FractalID = %q, want the fractal the key is issued for", got)
	}
}

// An instance-wide key carries none, so the configured scope fills the gap.
func TestTheConfiguredFractalFillsTheGapAnInstanceWideKeyLeaves(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"user":{"selected_fractal":""}}}`)
	s.client.cfg.Scope = "fractal:f-1"
	got, err := s.client.FractalID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "f-1" {
		t.Errorf("FractalID = %q", got)
	}
}

// Resolved once: a scoped tool called repeatedly must not cost a round trip each
// time.
func TestTheResolvedFractalIsReused(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"user":{"selected_fractal":"f-1"}}}`)
	for i := 0; i < 3; i++ {
		if _, err := s.client.FractalID(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if s.calls != 1 {
		t.Errorf("the identity endpoint was called %d times, want 1", s.calls)
	}
}

// A tenant-admin key resolves to no fractal. The message has to say what to do,
// because "not bound to a fractal" is not actionable on its own.
func TestAnUnscopedSessionSaysHowToScopeIt(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"user":{"selected_fractal":""}}}`)
	_, err := s.client.FractalID(context.Background())
	if err == nil {
		t.Fatal("expected a rejection")
	}
	for _, want := range []string{"BIFRACT_FRACTAL_ID", "list_fractals"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
