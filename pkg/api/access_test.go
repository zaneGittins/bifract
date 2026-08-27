package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

// An access level nobody declared, and one nobody recognises, must both deny.
func TestUndeclaredAccessDenies(t *testing.T) {
	for _, a := range []Access{"", Access("typo_role"), Access("Viewer")} {
		reg := NewRegistry()
		mux := chi.NewRouter()
		r := NewRouter(mux, reg)
		r.Register(Route{
			Method: http.MethodGet, Path: "/secret", Access: a,
			Handler: func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("LEAKED")) },
		})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/secret", nil))
		if rec.Body.String() == "LEAKED" {
			t.Errorf("Access(%q): unauthenticated request reached the handler", a)
		}
	}
}
