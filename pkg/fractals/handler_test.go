package fractals

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// granted_by is a foreign key into users, which an API key's apikey_<id>
// principal has no row in.
func TestHandleGrantPermissionRecordsAttributedUser(t *testing.T) {
	for name, tc := range map[string]struct {
		user, attribution string
		want              any
	}{
		"session":               {"alice", "alice", "alice"},
		"api key":               {"apikey_k1", "bob", "bob"},
		"api key, creator gone": {"apikey_k1", "", nil},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			h := NewHandler(nil, nil, nil)
			h.SetRBAC(pg, nil)
			r := httptest.NewRequest("POST", "/fractals/f1/permissions", strings.NewReader(`{"username":"carol","role":"viewer"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "f1")
			ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
			ctx = context.WithValue(ctx, "user", &storage.User{Username: tc.user, IsAdmin: true})
			ctx = context.WithValue(ctx, storage.AttributionUserKey, tc.attribution)
			h.HandleGrantPermission(httptest.NewRecorder(), r.WithContext(ctx))

			ins := fake.Matching("INSERT INTO fractal_permissions")
			if len(ins) != 1 || ins[0].Args[4] != tc.want {
				t.Fatalf("granted_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

// The grant form needs names to pick from, but /users and /groups are tenant
// administration: this carries only what the picker shows, and no disabled users.
func TestHandleListAccessCandidates(t *testing.T) {
	pg, fake := storagetest.Postgres()
	now := time.Now()
	userRow := func(name string, admin, enabled bool) []driver.Value {
		return []driver.Value{name, name + " D", "#000", "A", now, nil, admin, false, "local", enabled, true}
	}
	fake.On("FROM users", fakedb.RowsOf(userRow("alice", true, true), userRow("gone", false, false)))
	fake.On("FROM groups", fakedb.Row("g1", "soc", "secret notes", "root", now, now, int64(3)))
	h := NewHandler(nil, nil, nil)
	h.SetRBAC(pg, nil)

	rec := httptest.NewRecorder()
	h.HandleListAccessCandidates(rec, httptest.NewRequest("GET", "/access-candidates", nil))

	var got struct {
		Data AccessCandidates `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("%v: %s", err, rec.Body.String())
	}
	if len(got.Data.Users) != 1 || got.Data.Users[0] != (UserCandidate{Username: "alice", DisplayName: "alice D"}) {
		t.Fatalf("users = %+v", got.Data.Users)
	}
	if len(got.Data.Groups) != 1 || got.Data.Groups[0] != (GroupCandidate{ID: "g1", Name: "soc", MemberCount: 3}) {
		t.Fatalf("groups = %+v", got.Data.Groups)
	}
	for _, leak := range []string{"is_admin", "secret notes", "auth_provider", "last_login"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("response exposes %q: %s", leak, rec.Body.String())
		}
	}
}

// A scoped key is authorized like a person with its role, on its own fractal only.
func TestScopedKeyRoleAppliesOnlyToItsFractal(t *testing.T) {
	pg, fake := storagetest.Postgres()
	fake.On("fractal_permissions", fakedb.Row(""))
	h := NewHandler(nil, nil, nil)
	h.SetRBAC(pg, rbac.NewResolver(pg))

	r := httptest.NewRequest("GET", "/", nil)
	ctx := context.WithValue(r.Context(), "user", &storage.User{Username: "apikey_k1"})
	ctx = context.WithValue(ctx, "auth_type", "api_key")
	ctx = context.WithValue(ctx, "selected_fractal", "f-own")
	ctx = context.WithValue(ctx, "fractal_role", "admin")
	r = r.WithContext(ctx)

	if got := h.resolveFractalRole(r, "f-own"); got != rbac.RoleAdmin {
		t.Errorf("own fractal role = %q, want admin", got)
	}
	if got := h.resolveFractalRole(r, "f-other"); got != rbac.RoleNone {
		t.Errorf("other fractal role = %q, want none", got)
	}
}

// Each route decides on the {id} in its path. The manager is nil, so a route that
// skipped its check would panic rather than pass.
func TestFractalRoutesCheckThePathScope(t *testing.T) {
	admin := func(h *Handler) []http.HandlerFunc {
		return []http.HandlerFunc{h.HandleUpdateFractal, h.HandleSetRetention, h.HandleSetArchiveRetention, h.HandleSetDiskQuota,
			h.HandleListPermissions, h.HandleGrantPermission, h.HandleUpdatePermission, h.HandleRevokePermission}
	}
	read := func(h *Handler) []http.HandlerFunc {
		return []http.HandlerFunc{h.HandleGetFractal, h.HandleGetStats}
	}
	for name, tc := range map[string]struct {
		role   string
		routes func(*Handler) []http.HandlerFunc
	}{
		"viewer on admin routes":   {"viewer", admin},
		"analyst on admin routes":  {"analyst", admin},
		"no grant on admin routes": {"", admin},
		"no grant on read routes":  {"", read},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("fractal_permissions", fakedb.Row(tc.role))
			h := NewHandler(nil, nil, nil)
			h.SetRBAC(pg, rbac.NewResolver(pg))
			for _, fn := range tc.routes(h) {
				rec := callAs(fn, &storage.User{Username: "someone"}, `{"role":"viewer"}`, "id", "f1")
				if rec.Code != http.StatusForbidden {
					t.Errorf("status = %d, want 403: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// Admin is granted only by a tenant admin, so only a tenant admin takes it away. A
// grant counts too: it replaces the row, so granting viewer would demote.
func TestFractalAdminCannotChangeAnAdminGrant(t *testing.T) {
	for name, tc := range map[string]struct {
		role   string
		tenant bool
		want   int
	}{
		"peer admin row":          {"admin", false, http.StatusForbidden},
		"analyst row":             {"analyst", false, http.StatusOK},
		"tenant admin, admin row": {"admin", true, http.StatusOK},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("WHERE fp.id = $1", fakedb.Row("perm1", "f1", "peer", nil, tc.role, "", time.Now(), time.Now(), "Peer"))
			fake.On("SELECT EXISTS", fakedb.Row(tc.role == "admin"))
			fake.On("INSERT INTO fractal_permissions", fakedb.Row("perm1", "f1", "peer", nil, "viewer", "", time.Now(), time.Now()))
			fake.On("UPDATE fractal_permissions", fakedb.Affected(1))
			fake.On("DELETE FROM fractal_permissions", fakedb.Affected(1))
			fake.On("fractal_permissions", fakedb.Row("admin"))
			h := NewHandler(nil, nil, nil)
			h.SetRBAC(pg, rbac.NewResolver(pg))
			user := &storage.User{Username: "fadmin", IsAdmin: tc.tenant}
			for route, fn := range map[string]http.HandlerFunc{"update": h.HandleUpdatePermission, "revoke": h.HandleRevokePermission, "grant": h.HandleGrantPermission} {
				rec := callAs(fn, user, `{"username":"peer","role":"viewer"}`, "id", "f1", "permId", "perm1")
				if rec.Code != tc.want {
					t.Errorf("%s status = %d, want %d: %s", route, rec.Code, tc.want, rec.Body.String())
				}
			}
		})
	}
}

// Settings on a system fractal are tenant admin only, whatever role was granted.
func TestSystemFractalIsManagedByTenantAdminsOnly(t *testing.T) {
	pg, fake := storagetest.Postgres()
	fake.On("SELECT is_system FROM fractals", fakedb.Row(true))
	fake.On("fractal_permissions", fakedb.Row("admin"))
	h := NewHandler(nil, nil, nil)
	h.SetRBAC(pg, rbac.NewResolver(pg))
	for route, fn := range map[string]http.HandlerFunc{"retention": h.HandleSetRetention, "permissions": h.HandleListPermissions, "grant": h.HandleGrantPermission} {
		if rec := callAs(fn, &storage.User{Username: "fadmin"}, `{"role":"viewer"}`, "id", "audit"); rec.Code != http.StatusForbidden {
			t.Errorf("%s status = %d, want 403: %s", route, rec.Code, rec.Body.String())
		}
	}
}

func callAs(fn http.HandlerFunc, user *storage.User, body string, params ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("PUT", "/", strings.NewReader(body))
	rctx := chi.NewRouteContext()
	for i := 0; i+1 < len(params); i += 2 {
		rctx.URLParams.Add(params[i], params[i+1])
	}
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
	rec := httptest.NewRecorder()
	fn(rec, r.WithContext(context.WithValue(ctx, "user", user)))
	return rec
}
