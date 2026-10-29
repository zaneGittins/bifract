package dashboards

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"github.com/go-chi/chi/v5"
)

// A key's role is a role on the fractal it was issued for. Reaching a dashboard
// of another fractal by id must not borrow it.
func TestDashboardByIDNeedsARoleOnItsOwnFractal(t *testing.T) {
	ts := time.Date(2026, 3, 4, 4, 6, 0, 0, time.UTC)
	pg, fake := storagetest.Postgres()
	fake.On("FROM dashboards d", fakedb.Row("D1", "d", "", "24h", nil, nil, int64(0),
		"f2", nil, []byte("[]"), "UTC", "", ts, ts, "", "", ""))
	h := &DashboardHandler{pg: pg}

	keyFor := func(fractal, method string) *http.Request {
		r := httptest.NewRequest(method, "/", strings.NewReader(`{"name":"renamed"}`))
		ctx := context.WithValue(r.Context(), "user", &storage.User{Username: "apikey_1"})
		ctx = context.WithValue(ctx, "auth_type", "api_key")
		ctx = context.WithValue(ctx, "selected_fractal", fractal)
		ctx = context.WithValue(ctx, "fractal_role", "admin")
		rc := chi.NewRouteContext()
		rc.URLParams.Add("id", "D1")
		return r.WithContext(context.WithValue(ctx, chi.RouteCtxKey, rc))
	}

	for name, handle := range map[string]http.HandlerFunc{
		http.MethodGet:    h.HandleGetDashboard,
		http.MethodPut:    h.HandleUpdateDashboard,
		http.MethodDelete: h.HandleDeleteDashboard,
	} {
		rec := httptest.NewRecorder()
		handle(rec, keyFor("f1", name))
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s by a key for f1: status %d, want 403", name, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	h.HandleGetDashboard(rec, keyFor("f2", http.MethodGet))
	if rec.Code != http.StatusOK {
		t.Errorf("GET by a key for f2: status %d, want 200: %s", rec.Code, rec.Body.String())
	}
}
