package aitools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bifract/pkg/api"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// authenticate stands in for the real auth middleware: it resolves a principal
// from the cookie and from nothing else, which is the property under test.
func authenticate(valid string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cookie, err := r.Cookie("session"); err == nil && cookie.Value == valid {
				user := &storage.User{Username: "analyst", IsAdmin: true}
				r = r.WithContext(context.WithValue(r.Context(), "user", user))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// instance mounts a viewer route and an analyst route behind that middleware.
func instance(t *testing.T, validSession string) http.Handler {
	t.Helper()
	mux := chi.NewRouter()
	mux.Use(authenticate(validSession))
	r := api.NewRouter(mux, api.NewRegistry())
	r.Route("/api/v1", func(r api.Router) {
		r.Register(api.Route{
			Method: http.MethodGet, Path: "/query/fields", Access: api.AccessViewer,
			Handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Write([]byte(`{"success":true,"data":{"fields":["host"]}}`))
			},
		})
		r.Register(api.Route{
			Method: http.MethodPost, Path: "/comments", Access: api.AccessAnalyst,
			Handler: func(w http.ResponseWriter, req *http.Request) {
				body, _ := json.Marshal(map[string]any{"success": true, "data": map[string]any{
					"seen": req.Header.Get("X-Bifract-Scope"),
				}})
				w.Write(body)
			},
		})
	})
	return mux
}

// origin is the request being served when a tool runs: authenticated, with a
// user already resolved onto its context.
func origin(session string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/chat/conversations/c1/stream", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: session})
	r.Header.Set("X-Bifract-Scope", "fractal:f1")
	return r.WithContext(context.WithValue(context.Background(), "user",
		&storage.User{Username: "analyst", IsAdmin: true}))
}

// The whole design rests on the caller being re-authenticated rather than
// inherited. If a tool call could fall back on the identity the originating
// request resolved, a session revoked or expired mid-conversation would keep
// working for as long as the conversation ran.
func TestAToolCallDoesNotInheritTheCallersIdentity(t *testing.T) {
	// The server now considers the cookie the origin carries to be stale, but
	// its context still holds the principal the request resolved on arrival.
	caller := origin("expired")
	client := NewInProcess(instance(t, "current"), caller, api.AccessViewer, "f1")

	// Called with the conversation's own context, as the chat loop calls it.
	if _, err := client.Get(caller.Context(), "/query/fields", nil); err == nil {
		t.Fatal("a call with a stale cookie succeeded, so authorization was inherited from the context")
	}
}

func TestTheCallersCredentialsAreForwarded(t *testing.T) {
	client := NewInProcess(instance(t, "current"), origin("current"), api.AccessViewer, "f1")

	got, err := client.Get(context.Background(), "/query/fields", nil)
	if err != nil {
		t.Fatal(err)
	}
	object, _ := got.(map[string]any)
	fields, _ := object["fields"].([]any)
	if len(fields) != 1 || fields[0] != "host" {
		t.Errorf("got %v, want the unwrapped payload", got)
	}
	if _, leaked := object["success"]; leaked {
		t.Error("the transport envelope reached the tool")
	}
}

// A tool that reaches past what it declared has to be stopped by the router,
// not by review. Here the caller is an admin, so only the ceiling can refuse.
func TestAToolCannotReachPastItsCeiling(t *testing.T) {
	handler, caller := instance(t, "current"), origin("current")

	reading := NewInProcess(handler, caller, api.AccessViewer, "f1")
	if _, err := reading.Post(context.Background(), "/comments", map[string]any{"text": "hi"}); err == nil {
		t.Error("a read-only tool wrote a comment")
	}

	writing := NewInProcess(handler, caller, api.AccessAnalyst, "f1")
	if _, err := writing.Post(context.Background(), "/comments", map[string]any{"text": "hi"}); err != nil {
		t.Errorf("a confirmed write was refused: %v", err)
	}
}

// The scope header decides which fractal a request acts in, so it has to reach
// the handler exactly as the browser sent it.
func TestTheScopeHeaderIsCarriedThrough(t *testing.T) {
	client := NewInProcess(instance(t, "current"), origin("current"), api.AccessAnalyst, "f1")

	got, err := client.Post(context.Background(), "/comments", map[string]any{"text": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if seen := Field[string](got, "seen"); seen != "fractal:f1" {
		t.Errorf("the handler saw scope %q", seen)
	}
}

// A tool call that outlives the conversation it belongs to is work nobody is
// waiting for.
func TestCancellingTheConversationCancelsTheCall(t *testing.T) {
	started := make(chan struct{})
	mux := chi.NewRouter()
	r := api.NewRouter(mux, api.NewRegistry())
	r.Route("/api/v1", func(r api.Router) {
		r.Register(api.Route{
			Method: http.MethodGet, Path: "/query/fields", Access: api.AccessPublic,
			Handler: func(w http.ResponseWriter, req *http.Request) {
				close(started)
				<-req.Context().Done()
				w.WriteHeader(http.StatusGatewayTimeout)
			},
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()

	client := NewInProcess(mux, origin("current"), api.AccessViewer, "f1")
	if _, err := client.Get(ctx, "/query/fields", nil); err == nil {
		t.Error("a cancelled call reported success")
	}
}

// A conversation over a prism is not scoped to one fractal, and the tools that
// name one in the path must say so rather than act on the wrong data.
func TestAPrismConversationRefusesFractalScopedTools(t *testing.T) {
	client := NewInProcess(instance(t, "current"), origin("current"), api.AccessViewer, "")
	if _, err := client.FractalID(context.Background()); err == nil {
		t.Error("a prism-scoped session named a fractal")
	}
}
