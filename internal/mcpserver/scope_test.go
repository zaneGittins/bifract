package mcpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// scopeStub is a Bifract that reports which scope the credential itself carries
// and records the scope header of every call.
type scopeStub struct {
	mu sync.Mutex
	// bound is what /auth/user reports: empty for an instance-wide key.
	bound  string
	scopes []string
	calls  int
}

func (s *scopeStub) start(t *testing.T) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/auth/user") {
			_, _ = w.Write([]byte(`{"success":true,"data":{"user":{"selected_fractal":"` + s.bound + `"}}}`))
			return
		}
		s.calls++
		s.scopes = append(s.scopes, r.Header.Get("X-Bifract-Scope"))
		_, _ = w.Write([]byte(`{"success":true,"data":[]}`))
	}))
	t.Cleanup(server.Close)
	return NewClient(Config{URL: server.URL, APIKey: "bifract_admin_test"})
}

func call(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return result
}

// An instance-wide key belongs to no fractal. A call that names none would be
// answered in whichever one the server falls back to, so it is refused: an alert
// or a watchlist that lands in the wrong fractal reads as a success and watches
// nothing.
func TestAnInstanceWideKeyMustNameTheFractalOnEveryScopedCall(t *testing.T) {
	stub := &scopeStub{}
	session := connect(t, stub.start(t))

	result := call(t, session, "list_alerts", nil)
	if !result.IsError {
		t.Fatal("a scoped call with no fractal named must be refused")
	}
	if text := resultText(t, result); !strings.Contains(text, "fractal_id") {
		t.Errorf("the refusal should say how to fix it: %s", text)
	}
	if stub.calls != 0 {
		t.Errorf("the call reached the server anyway: %d requests", stub.calls)
	}

	// Naming one scopes the request.
	if result := call(t, session, "list_alerts", map[string]any{"fractal_id": "velociraptor-1"}); result.IsError {
		t.Fatalf("a call that named its fractal was refused: %s", resultText(t, result))
	}
	if len(stub.scopes) != 1 || stub.scopes[0] != "fractal:velociraptor-1" {
		t.Errorf("the request went out scoped to %v", stub.scopes)
	}
}

// Discovery has to run before a fractal can be named, so it runs unscoped.
func TestDiscoveryRunsWithoutAFractal(t *testing.T) {
	stub := &scopeStub{}
	session := connect(t, stub.start(t))

	if result := call(t, session, "list_fractals", nil); result.IsError {
		t.Fatalf("list_fractals must run without a fractal: %s", resultText(t, result))
	}
	if stub.calls != 1 {
		t.Errorf("list_fractals made %d requests, want 1", stub.calls)
	}
}

// A key issued for one fractal carries its scope, so nothing has to be named and
// the session works as it always did.
func TestAScopedKeyNeedsNoFractalArgument(t *testing.T) {
	stub := &scopeStub{bound: "fractal-1"}
	session := connect(t, stub.start(t))

	if result := call(t, session, "list_alerts", nil); result.IsError {
		t.Fatalf("a key issued for a fractal was refused: %s", resultText(t, result))
	}
}

// A malformed id must be refused here rather than sent, which the server would
// answer with a 400 on every call.
func TestAMalformedFractalIDIsRefused(t *testing.T) {
	stub := &scopeStub{}
	session := connect(t, stub.start(t))

	result := call(t, session, "list_alerts", map[string]any{"fractal_id": "not a fractal/../id"})
	if !result.IsError {
		t.Fatal("a malformed id must be refused")
	}
	if stub.calls != 0 {
		t.Error("the malformed id reached the server")
	}
}

func resultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if len(result.Content) == 0 {
		encoded, _ := json.Marshal(result)
		return string(encoded)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		return ""
	}
	return text.Text
}
