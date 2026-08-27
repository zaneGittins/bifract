package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func noopHandler(http.ResponseWriter, *http.Request) {}

// mustPanic runs fn and fails unless it panics.
func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic, got none", name)
		}
	}()
	fn()
}

// TestRegisterRecordsFullyQualifiedPath covers the prefix arithmetic: Route
// extends the prefix, Group leaves it alone, and a route's recorded path is
// the one chi actually serves it on.
func TestRegisterRecordsFullyQualifiedPath(t *testing.T) {
	reg := NewRegistry()
	r := NewRouter(chi.NewRouter(), reg)

	r.Route("/api/v1", func(r Router) {
		r.Register(Route{Method: http.MethodGet, Path: "/comments", Access: AccessPublic, Handler: noopHandler})
		r.Group(func(r Router) {
			r.Register(Route{Method: http.MethodPost, Path: "/comments/{id}", Access: AccessPublic, Handler: noopHandler})
		})
		r.Route("/logs", func(r Router) {
			r.Register(Route{Method: http.MethodDelete, Path: "/{id}/comments", Access: AccessPublic, Handler: noopHandler})
		})
	})

	want := []string{
		"GET /api/v1/comments",
		"POST /api/v1/comments/{id}",
		"DELETE /api/v1/logs/{id}/comments",
	}
	for _, key := range want {
		method, path, _ := strings.Cut(key, " ")
		if _, ok := reg.Lookup(method, path); !ok {
			t.Errorf("registry is missing %s", key)
		}
	}
}

// TestRegisterMountsTheHandler guards the property the registry exists to
// support: describing a route must also serve it, so the description can never
// drift from what is mounted.
func TestRegisterMountsTheHandler(t *testing.T) {
	reg := NewRegistry()
	mux := chi.NewRouter()
	r := NewRouter(mux, reg)

	r.Route("/api/v1", func(r Router) {
		r.Register(Route{
			Method: http.MethodGet,
			Path:   "/comments/{id}",
			Access: AccessPublic,
			Handler: func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusTeapot)
				w.Write([]byte(chi.URLParam(req, "id")))
			},
		})
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/comments/abc123", nil))

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
	if rec.Body.String() != "abc123" {
		t.Errorf("URL param = %q, want %q", rec.Body.String(), "abc123")
	}
}

// TestRegisterRejectsBadRoutes covers the failures that must stop the process
// at construction rather than become a silently shadowed or unserved route.
func TestRegisterRejectsBadRoutes(t *testing.T) {
	newRouter := func() Router { return NewRouter(chi.NewRouter(), NewRegistry()) }

	mustPanic(t, "no method", func() {
		newRouter().Register(Route{Path: "/x", Handler: noopHandler})
	})
	mustPanic(t, "relative path", func() {
		newRouter().Register(Route{Method: http.MethodGet, Path: "x", Handler: noopHandler})
	})
	mustPanic(t, "no handler", func() {
		newRouter().Register(Route{Method: http.MethodGet, Path: "/x"})
	})
	mustPanic(t, "duplicate", func() {
		r := newRouter()
		r.Register(Route{Method: http.MethodGet, Path: "/x", Handler: noopHandler})
		r.Register(Route{Method: http.MethodGet, Path: "/x", Handler: noopHandler})
	})
}
