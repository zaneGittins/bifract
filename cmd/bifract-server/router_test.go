package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"bifract/pkg/api"
	"bifract/pkg/oidc"

	"github.com/go-chi/chi/v5"
)

var updateRoutes = flag.Bool("update", false, "rewrite routetable_test.go from the current router")

const (
	routeTableFile = "routetable_test.go"

	// authMiddlewareName identifies auth.AuthHandler.AuthMiddleware in a walked
	// middleware chain. It is a method value, so its symbol survives refactoring
	// of the router in a way an anonymous closure's would not.
	authMiddlewareName = "bifract/pkg/auth.(*AuthHandler).AuthMiddleware-fm"
)

// testDeps builds a router with no live dependencies: handlers are method
// values on nil receivers, which bind without being called. oidcHandler is the
// one dependency buildRouter branches on, so it must be non-nil to mount the
// full table.
func testDeps() routerDeps {
	return routerDeps{oidcHandler: &oidc.Handler{}}
}

type mountedRoute struct {
	method        string
	path          string
	middlewares   int
	authenticated bool
	registered    bool
	hasRequest    bool
	hasResponse   bool
	access        string
}

func (m mountedRoute) key() string { return api.Key(m.method, m.path) }

// String renders the route in the form routeTable stores.
func (m mountedRoute) String() string {
	access := "public"
	if m.authenticated {
		access = "auth"
	}
	line := fmt.Sprintf("%s %d %s", m.key(), m.middlewares, access)
	if m.hasRequest {
		line += " body"
	}
	if m.hasResponse {
		line += " resp"
	}
	return line + " " + m.access
}

// walkRouter returns every route mounted on mux, ordered by path then method.
func walkRouter(t *testing.T, mux *chi.Mux, reg *api.Registry) []mountedRoute {
	t.Helper()

	var routes []mountedRoute
	err := chi.Walk(mux, func(method, route string, _ http.Handler, middlewares ...func(http.Handler) http.Handler) error {
		authenticated := false
		for _, mw := range middlewares {
			if runtime.FuncForPC(reflect.ValueOf(mw).Pointer()).Name() == authMiddlewareName {
				authenticated = true
				break
			}
		}
		described, ok := reg.Lookup(method, route)
		routes = append(routes, mountedRoute{
			method:        method,
			path:          route,
			middlewares:   len(middlewares),
			authenticated: authenticated,
			registered:    ok,
			hasRequest:    described.Request != nil || described.Consumes != "",
			hasResponse:   described.Response != nil,
			access:        string(described.Access),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking router: %v", err)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].path != routes[j].path {
			return routes[i].path < routes[j].path
		}
		return routes[i].method < routes[j].method
	})
	return routes
}

// TestRouteTable pins the router's mounted surface against routeTable. It reads
// the live route table rather than a document, so a stale description cannot
// satisfy it, and every line of a diff is a real change:
//
//   - a route appearing or disappearing changes the API contract
//   - "auth" becoming "public" means a route left the authenticated group
//   - a changed middleware count means the chain around a route changed
//   - "body" or "resp" appearing or vanishing changes the wire contract
//   - a changed access level changes who may reach the route
func TestRouteTable(t *testing.T) {
	mux, registry := buildRouter(testDeps())
	routes := walkRouter(t, mux, registry)

	got := make([]string, len(routes))
	for i, route := range routes {
		got[i] = route.String()
	}

	if *updateRoutes {
		if err := os.WriteFile(routeTableFile, renderRouteTable(got), 0o644); err != nil {
			t.Fatalf("writing %s: %v", routeTableFile, err)
		}
		t.Logf("wrote %d routes to %s", len(got), routeTableFile)
		return
	}

	if diff := diffLines(routeTable, got); diff != "" {
		t.Errorf("the mounted route table no longer matches routeTable.\n"+
			"Review each line below, then if the change is intended regenerate with:\n"+
			"  go test ./cmd/bifract-server -run TestRouteTable -update\n%s", diff)
	}
}

// TestUnauthenticatedRoutesAreIntentional is the security reading of the same
// table. It restates as its own failure that only routes with a reason to skip
// auth.AuthMiddleware do so, so widening the unauthenticated surface cannot be
// waved through as one more line in a large diff.
func TestUnauthenticatedRoutesAreIntentional(t *testing.T) {
	mux, registry := buildRouter(testDeps())

	public := map[string]bool{}
	for _, line := range routeTable {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[3] == "public" {
			public[api.Key(fields[0], fields[1])] = true
		}
	}

	for _, route := range walkRouter(t, mux, registry) {
		switch {
		case !route.authenticated && !public[route.key()]:
			t.Errorf("%s is reachable without authentication and is not recorded as public in routeTable", route.key())
		case route.authenticated && public[route.key()]:
			t.Errorf("%s is authenticated but recorded as public in routeTable", route.key())
		}
	}
}

// TestEveryRouteDeclaresAccess is the authorization invariant: a route with no
// declared access level is not gated at all, which is the failure mode this
// whole mechanism exists to prevent.
func TestEveryRouteDeclaresAccess(t *testing.T) {
	for _, build := range []func() (*chi.Mux, *api.Registry){
		func() (*chi.Mux, *api.Registry) { return buildRouter(testDeps()) },
		func() (*chi.Mux, *api.Registry) { return buildIngestRouter(ingestRouterDeps{}) },
	} {
		mux, registry := build()
		for _, route := range walkRouter(t, mux, registry) {
			if route.access == "" {
				t.Errorf("%s declares no Access; every route must state who may reach it", route.key())
			}
		}
	}
}

// TestEveryRouteIsRegistered is the completeness invariant: every route the
// router serves is described in the api registry. It walks the live router, so
// no document can satisfy it, and it has no exemptions, so a route added with
// the bare chi verbs instead of Register fails the build.
func TestEveryRouteIsRegistered(t *testing.T) {
	mux, registry := buildRouter(testDeps())

	for _, route := range walkRouter(t, mux, registry) {
		if !route.registered {
			t.Errorf("%s is served but not described; mount it with api.Router.Register", route.key())
		}
	}
}

// ingestRouteTable is the ingest tier's entire surface. Every route is marked
// public because that tier mounts no session middleware: callers authenticate
// with an ingest token inside the handler, and /internal/ingest additionally
// requires a private-network source. The list is short and is the only surface
// log shippers reach, so it is written out here rather than generated.
var ingestRouteTable = []string{
	"POST /_bulk 4 public ingest_token",
	"PUT /_bulk 4 public ingest_token",
	"GET /api/v1/health 3 public public",
	"POST /api/v1/ingest 4 public ingest_token",
	"POST /api/v1/internal/ingest/{fractal} 5 public internal",
	"POST /v1/logs 4 public ingest_token",
}

// TestIngestRouteTable pins the ingest tier's surface and proves its routes are
// described too, so `bifract-server ingest` cannot drift from the registry.
func TestIngestRouteTable(t *testing.T) {
	mux, registry := buildIngestRouter(ingestRouterDeps{})
	routes := walkRouter(t, mux, registry)

	got := make([]string, len(routes))
	for i, route := range routes {
		got[i] = route.String()
		if !route.registered {
			t.Errorf("%s is served but not described; mount it with api.Router.Register", route.key())
		}
	}
	if diff := diffLines(ingestRouteTable, got); diff != "" {
		t.Errorf("the ingest route table changed; update ingestRouteTable if intended%s", diff)
	}
}

// renderRouteTable produces the source of routetable_test.go.
func renderRouteTable(routes []string) []byte {
	var b strings.Builder
	b.WriteString(`package main

// routeTable is every route the server mounts, in the form:
//
//	METHOD /path <middleware count> auth|public [body] [resp] <access>
//
// "auth" means the route sits behind auth.AuthHandler.AuthMiddleware; "body"
// and "resp" mean the route declares a typed request and response; the last
// field is the access level the route enforces.
//
// Regenerate after an intended change, then review every line of the diff:
//
//	go test ./cmd/bifract-server -run TestRouteTable -update
var routeTable = []string{
`)
	for _, route := range routes {
		fmt.Fprintf(&b, "\t%q,\n", route)
	}
	b.WriteString("}\n")
	return []byte(b.String())
}

// diffLines reports the lines that differ between want and got.
func diffLines(want, got []string) string {
	inWant := map[string]bool{}
	for _, line := range want {
		inWant[line] = true
	}
	inGot := map[string]bool{}
	for _, line := range got {
		inGot[line] = true
	}

	var b strings.Builder
	for _, line := range want {
		if !inGot[line] {
			fmt.Fprintf(&b, "\n  -%s", line)
		}
	}
	for _, line := range got {
		if !inWant[line] {
			fmt.Fprintf(&b, "\n  +%s", line)
		}
	}
	return b.String()
}
