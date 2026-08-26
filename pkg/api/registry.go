// Package api holds the route registry: a description of every HTTP operation
// the server exposes, recorded where the route is mounted. It makes the
// router's contents inspectable instead of implicit, which is what lets a test
// prove no route is undescribed and later work derive authorization and an
// OpenAPI document from the same source.
package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// Route describes one HTTP operation. Method, Path and Handler drive routing;
// the rest is metadata later phases read and is optional today.
type Route struct {
	Method string
	// Path is relative to the enclosing mount prefix, e.g. "/comments/{id}".
	Path    string
	Summary string
	// Permission is the capability a caller must hold. Unenforced today.
	Permission string
	// Request and Response are zero values of the wire types, for schema
	// generation. Unpopulated today.
	Request  any
	Response any
	Handler  http.HandlerFunc
}

// Registry records described routes keyed by method and fully qualified path.
// It is populated while the router is built and read only afterwards, so it
// carries no lock.
type Registry struct {
	routes map[string]Route
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{routes: make(map[string]Route)}
}

// Key is the registry key for a method and fully qualified path.
func Key(method, path string) string {
	return strings.ToUpper(method) + " " + path
}

// add records route under prefix. It panics on a malformed or duplicate route:
// both are programming errors that must not reach a running server.
func (reg *Registry) add(prefix string, route Route) {
	if route.Method == "" {
		panic(fmt.Sprintf("api: route %q has no method", route.Path))
	}
	if !strings.HasPrefix(route.Path, "/") {
		panic(fmt.Sprintf("api: route path %q must start with /", route.Path))
	}
	if route.Handler == nil {
		panic(fmt.Sprintf("api: route %s %s has no handler", route.Method, route.Path))
	}
	full := prefix + route.Path
	key := Key(route.Method, full)
	if _, dup := reg.routes[key]; dup {
		panic(fmt.Sprintf("api: duplicate route %s", key))
	}
	route.Path = full
	reg.routes[key] = route
}

// Lookup returns the route registered for a fully qualified method and path.
func (reg *Registry) Lookup(method, path string) (Route, bool) {
	route, ok := reg.routes[Key(method, path)]
	return route, ok
}

// Router wraps a chi.Router with the registry and the mount prefix its routes
// resolve against. Methods it does not define fall through to the embedded
// chi.Router, so routes can be moved onto Register one at a time without
// disturbing the rest.
type Router struct {
	chi.Router
	reg    *Registry
	prefix string
}

// NewRouter binds r to reg at the root prefix.
func NewRouter(r chi.Router, reg *Registry) Router {
	return Router{Router: r, reg: reg}
}

// Route mounts a sub-router at pattern, extending the prefix.
func (rt Router) Route(pattern string, fn func(Router)) {
	rt.Router.Route(pattern, func(sub chi.Router) {
		fn(Router{Router: sub, reg: rt.reg, prefix: rt.prefix + pattern})
	})
}

// Group scopes middleware to a set of routes without changing the prefix.
func (rt Router) Group(fn func(Router)) {
	rt.Router.Group(func(sub chi.Router) {
		fn(Router{Router: sub, reg: rt.reg, prefix: rt.prefix})
	})
}

// With returns a Router carrying the given middleware, keeping the prefix.
func (rt Router) With(middlewares ...func(http.Handler) http.Handler) Router {
	return Router{Router: rt.Router.With(middlewares...), reg: rt.reg, prefix: rt.prefix}
}

// Register mounts route and records it in the registry.
func (rt Router) Register(route Route) {
	rt.reg.add(rt.prefix, route)
	rt.Router.Method(route.Method, route.Path, route.Handler)
}
