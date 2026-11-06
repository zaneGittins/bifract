package prisms

import (
	"context"
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

// principal builds the context the auth middleware produces: a session credits
// its own user, an API key credits the key's creator (empty once deleted).
func principal(r *http.Request, user, attribution string) *http.Request {
	ctx := context.WithValue(r.Context(), "user", &storage.User{Username: user, IsAdmin: true})
	ctx = context.WithValue(ctx, storage.AttributionUserKey, attribution)
	return r.WithContext(ctx)
}

// granted_by is a foreign key into users, which an API key's apikey_<id>
// principal has no row in.
func TestHandleGrantPrismPermissionRecordsAttributedUser(t *testing.T) {
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
			h := NewHandler(NewManager(pg), pg, nil)
			r := httptest.NewRequest("POST", "/prisms/p1/permissions", strings.NewReader(`{"username":"carol","role":"viewer"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "p1")
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			h.HandleGrantPrismPermission(httptest.NewRecorder(), principal(r, tc.user, tc.attribution))

			ins := fake.Matching("INSERT INTO prism_permissions")
			if len(ins) != 1 || ins[0].Args[4] != tc.want {
				t.Fatalf("granted_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

// An API key's principal is not a user, so there is nobody to auto-grant.
func TestHandleCreatePrismGrantsOnlyAUserCreator(t *testing.T) {
	for name, tc := range map[string]struct {
		user, attribution string
		wantGrants        int
	}{
		"session": {"alice", "alice", 1},
		"api key": {"apikey_k1", "bob", 0},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("INSERT INTO prisms", fakedb.Row("p1", "p", "", tc.attribution, time.Now(), time.Now()))
			h := NewHandler(NewManager(pg), pg, nil)
			r := httptest.NewRequest("POST", "/prisms", strings.NewReader(`{"name":"p"}`))
			w := httptest.NewRecorder()
			h.HandleCreatePrism(w, principal(r, tc.user, tc.attribution))
			if w.Code != http.StatusOK {
				t.Fatalf("code = %d", w.Code)
			}

			grants := fake.Matching("INSERT INTO prism_permissions")
			if len(grants) != tc.wantGrants {
				t.Fatalf("grants = %+v, want %d", grants, tc.wantGrants)
			}
			if tc.wantGrants == 1 && (grants[0].Args[1] != tc.user || grants[0].Args[4] != tc.user) {
				t.Fatalf("grant = %+v, want the creator as grantee and grantor", grants[0])
			}
		})
	}
}

// Prism admins can grant the prism to others, so adding a fractal shares it. That
// takes the authority to share the fractal, not just to read it.
func TestHandleAddMemberNeedsAdminOnTheFractal(t *testing.T) {
	for name, tc := range map[string]struct {
		fractalRole string
		refused     bool
	}{
		"fractal admin":  {"admin", false},
		"fractal viewer": {"viewer", true},
		"no grant":       {"", true},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("prism_permissions", fakedb.Row("admin"))
			fake.On("fractal_permissions", fakedb.Row(tc.fractalRole))
			h := NewHandler(NewManager(pg), pg, nil)
			h.SetRBACResolver(rbac.NewResolver(pg))

			r := httptest.NewRequest("POST", "/prisms/p1/members", strings.NewReader(`{"fractal_id":"f1"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "p1")
			ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
			ctx = context.WithValue(ctx, "user", &storage.User{Username: "padmin"})
			rec := httptest.NewRecorder()
			h.HandleAddMember(rec, r.WithContext(ctx))

			if refused := rec.Code == http.StatusForbidden; refused != tc.refused {
				t.Fatalf("status = %d, refused want %v: %s", rec.Code, tc.refused, rec.Body.String())
			}
			if added := len(fake.Matching("INSERT INTO prism_members")) != 0; added == tc.refused {
				t.Fatalf("added = %v, refused = %v", added, tc.refused)
			}
		})
	}
}

// Each route decides on the {id} in its path. The manager is nil, so a route that
// skipped its check would panic rather than pass.
func TestPrismRoutesCheckThePathScope(t *testing.T) {
	admin := func(h *Handler) []http.HandlerFunc {
		return []http.HandlerFunc{h.HandleUpdatePrism, h.HandleAddMember, h.HandleRemoveMember, h.HandleListPrismPermissions,
			h.HandleGrantPrismPermission, h.HandleUpdatePrismPermission, h.HandleRevokePrismPermission}
	}
	for name, tc := range map[string]struct {
		role   string
		routes func(*Handler) []http.HandlerFunc
	}{
		"viewer on admin routes":   {"viewer", admin},
		"analyst on admin routes":  {"analyst", admin},
		"no grant on admin routes": {"", admin},
		"no grant on read":         {"", func(h *Handler) []http.HandlerFunc { return []http.HandlerFunc{h.HandleGetPrism} }},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("prism_permissions", fakedb.Row(tc.role))
			h := NewHandler(nil, pg, nil)
			h.SetRBACResolver(rbac.NewResolver(pg))
			for _, fn := range tc.routes(h) {
				r := httptest.NewRequest("PUT", "/", strings.NewReader(`{"role":"viewer","fractal_id":"f1"}`))
				rctx := chi.NewRouteContext()
				rctx.URLParams.Add("id", "p1")
				ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
				ctx = context.WithValue(ctx, "user", &storage.User{Username: "someone"})
				rec := httptest.NewRecorder()
				fn(rec, r.WithContext(ctx))
				if rec.Code != http.StatusForbidden {
					t.Errorf("status = %d, want 403: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// Admin is granted only by a tenant admin, so only a tenant admin takes it away. A
// grant counts too: it replaces the row, so granting viewer would demote.
func TestPrismAdminCannotChangeAnAdminGrant(t *testing.T) {
	pg, fake := storagetest.Postgres()
	fake.On("WHERE pp.id = $1", fakedb.Row("perm1", "p1", "peer", nil, "admin", "", time.Now(), time.Now(), "Peer"))
	fake.On("SELECT EXISTS", fakedb.Row(true))
	fake.On("prism_permissions", fakedb.Row("admin"))
	h := NewHandler(NewManager(pg), pg, nil)
	h.SetRBACResolver(rbac.NewResolver(pg))
	for name, fn := range map[string]http.HandlerFunc{"update": h.HandleUpdatePrismPermission, "revoke": h.HandleRevokePrismPermission, "grant": h.HandleGrantPrismPermission} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/", strings.NewReader(`{"username":"peer","role":"viewer"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "p1")
			rctx.URLParams.Add("permId", "perm1")
			ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
			ctx = context.WithValue(ctx, "user", &storage.User{Username: "padmin"})
			rec := httptest.NewRecorder()
			fn(rec, r.WithContext(ctx))
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403: %s", rec.Code, rec.Body.String())
			}
		})
	}
}
