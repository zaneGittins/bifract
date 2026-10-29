package savedqueries

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"github.com/go-chi/chi/v5"
)

func savedQueryRequest(method, body string, apiKey bool) *http.Request {
	r := httptest.NewRequest(method, "/", strings.NewReader(body))
	ctx := context.WithValue(r.Context(), "user", &storage.User{Username: "bob"})
	ctx = context.WithValue(ctx, "selected_fractal", "f1")
	if apiKey {
		ctx = context.WithValue(ctx, "auth_type", "api_key")
	}
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", "q1")
	return r.WithContext(context.WithValue(ctx, chi.RouteCtxKey, rc))
}

// A personal saved query is its author's alone. Every statement that reaches one
// by id must carry the visibility rule the list applies, or anyone in the scope
// could change a query they cannot even see.
func TestByIDWritesSpareOthersPersonalQueries(t *testing.T) {
	pg, fake := storagetest.Postgres()
	h := &Handler{pg: pg}

	h.HandleUpdate(httptest.NewRecorder(), savedQueryRequest(http.MethodPut, `{"name":"n","query_text":"*"}`, false))
	h.HandleDelete(httptest.NewRecorder(), savedQueryRequest(http.MethodDelete, "", false))
	h.HandleMarkUsed(httptest.NewRecorder(), savedQueryRequest(http.MethodPost, "", false))

	calls := fake.Calls()
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want update, delete and mark used", len(calls))
	}
	for _, call := range calls {
		if !strings.Contains(call.Query, "(COALESCE(visibility, 'shared') = 'shared' OR created_by = $") {
			t.Errorf("%q does not spare personal queries of other users", call.Query)
		}
		named := false
		for _, a := range call.Args {
			named = named || a == "bob"
		}
		if !named {
			t.Errorf("args %v do not name the caller", call.Args)
		}
	}
}

// An API key cannot list personal queries, which belong to a person, so it may
// not make one either: the query would vanish from the key the moment it saved.
func TestAPIKeysCannotMakePersonalQueries(t *testing.T) {
	pg, fake := storagetest.Postgres()
	h := &Handler{pg: pg}
	body := `{"name":"n","query_text":"*","visibility":"personal"}`

	for name, handle := range map[string]http.HandlerFunc{"create": h.HandleCreate, "update": h.HandleUpdate} {
		rec := httptest.NewRecorder()
		handle(rec, savedQueryRequest(http.MethodPost, body, true))
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", name, rec.Code)
		}
	}
	if n := len(fake.Calls()); n != 0 {
		t.Errorf("%d statements ran for a refused request", n)
	}
}
