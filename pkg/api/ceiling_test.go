package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// serveAt mounts one route at the given access and calls it as an instance
// admin, under ceiling. The admin is deliberate: it removes the caller's own
// role from the question, leaving only the ceiling.
func serveAt(t *testing.T, route Access, ceiling Access, capped bool) int {
	t.Helper()
	reg := NewRegistry()
	mux := chi.NewRouter()
	r := NewRouter(mux, reg)
	r.Register(Route{
		Method: http.MethodPost, Path: "/thing", Access: route,
		Handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) },
	})

	ctx := context.WithValue(context.Background(), "user", &storage.User{Username: "root", IsAdmin: true})
	if capped {
		ctx = WithCeiling(ctx, ceiling)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/thing", nil).WithContext(ctx))
	return rec.Code
}

// A ceiling is what lets chat run an unconfirmed tool without trusting the tool
// to stay within what it declared. If it did not bind an instance admin, it
// would bind nobody worth binding.
func TestACeilingCapsEvenAnInstanceAdmin(t *testing.T) {
	cases := []struct {
		route, ceiling Access
		reachable      bool
	}{
		{AccessViewer, AccessViewer, true},
		{AccessAnalyst, AccessViewer, false},
		{AccessFractalAdmin, AccessViewer, false},
		{AccessTenantAdmin, AccessViewer, false},
		{AccessViewer, AccessAnalyst, true},
		{AccessAnalyst, AccessAnalyst, true},
		{AccessFractalAdmin, AccessAnalyst, false},
		// Not rungs on the ladder, so no ceiling admits them. An in-process
		// caller must never reach an ingest or private-network route.
		{AccessIngestToken, AccessAnalyst, false},
		{AccessInternal, AccessAnalyst, false},
		// These carry no authority to exceed.
		{AccessPublic, AccessViewer, true},
		{AccessAuthenticated, AccessViewer, true},
	}
	for _, c := range cases {
		got := serveAt(t, c.route, c.ceiling, true)
		if reached := got == http.StatusTeapot; reached != c.reachable {
			t.Errorf("a %s route under a %s ceiling answered %d, want reachable=%v",
				c.route, c.ceiling, got, c.reachable)
		}
	}
}

// Nothing changes for a request that carries no ceiling, which is every request
// that arrives over the network.
func TestARequestWithoutACeilingIsUnaffected(t *testing.T) {
	for _, route := range []Access{AccessViewer, AccessAnalyst, AccessFractalAdmin, AccessTenantAdmin} {
		if got := serveAt(t, route, "", false); got != http.StatusTeapot {
			t.Errorf("an uncapped admin request to a %s route answered %d", route, got)
		}
	}
}

// A ceiling that is not itself a rung must admit nothing: a typo in one has to
// close the door, not open it.
func TestAnUnrecognisedCeilingAdmitsNoRoute(t *testing.T) {
	for _, ceiling := range []Access{"", Access("Analyst"), Access("root")} {
		for _, route := range []Access{AccessViewer, AccessAnalyst} {
			if got := serveAt(t, route, ceiling, true); got == http.StatusTeapot {
				t.Errorf("ceiling %q admitted a %s route", ceiling, route)
			}
		}
	}
}

// A handler that checks a role on an object's own scope, rather than relying on
// its route's declared access, is held to the ceiling as well. Without this a
// write route declared as viewer let a viewer-capped call write, so long as the
// user behind it could.
func TestAnObjectRoleCheckRespectsTheCeiling(t *testing.T) {
	admin := &storage.User{Username: "root", IsAdmin: true}
	analyst := &storage.User{Username: "a"}
	req := func(ceiling Access, capped bool) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		if capped {
			r = r.WithContext(WithCeiling(r.Context(), ceiling))
		}
		return r
	}
	cases := []struct {
		name     string
		r        *http.Request
		user     *storage.User
		held     rbac.Role
		required rbac.Role
		want     bool
	}{
		{"uncapped analyst writes", req("", false), analyst, rbac.RoleAnalyst, rbac.RoleAnalyst, true},
		{"viewer cap stops an analyst writing", req(AccessViewer, true), analyst, rbac.RoleAnalyst, rbac.RoleAnalyst, false},
		{"viewer cap stops an admin writing", req(AccessViewer, true), admin, rbac.RoleNone, rbac.RoleAnalyst, false},
		{"viewer cap still reads", req(AccessViewer, true), analyst, rbac.RoleAnalyst, rbac.RoleViewer, true},
		{"analyst cap writes", req(AccessAnalyst, true), analyst, rbac.RoleAnalyst, rbac.RoleAnalyst, true},
		{"the cap grants nothing", req(AccessAnalyst, true), analyst, rbac.RoleViewer, rbac.RoleAnalyst, false},
	}
	for _, c := range cases {
		if got := GrantedRole(c.r, c.user, c.held, c.required); got != c.want {
			t.Errorf("%s: GrantedRole = %v, want %v", c.name, got, c.want)
		}
	}
}
