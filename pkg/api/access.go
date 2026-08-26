package api

import (
	"net/http"

	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// Access is what a caller must hold to reach a route. Most values are a rung on
// the RBAC ladder resolved against the caller's scope; the rest name an
// authentication scheme that is not a session role at all.
type Access string

const (
	// AccessPublic needs no authentication.
	AccessPublic Access = "public"
	// AccessIngestToken is authenticated by an ingest token, not a session, and
	// is enforced by the ingest handler rather than here.
	AccessIngestToken Access = "ingest_token"
	// AccessInternal is reachable only from the private network.
	AccessInternal Access = "internal"
	// AccessAuthenticated needs any principal; the route is self-service and
	// carries no role requirement.
	AccessAuthenticated Access = "authenticated"

	// The RBAC ladder, resolved against the fractal or prism in scope.
	AccessViewer       Access = "viewer"
	AccessAnalyst      Access = "analyst"
	AccessFractalAdmin Access = "fractal_admin"

	// AccessTenantAdmin is instance-wide and is not scoped to a fractal.
	AccessTenantAdmin Access = "tenant_admin"
)

// role maps an Access to the RBAC role it requires, and reports whether it is a
// scoped rung at all.
func (a Access) role() (rbac.Role, bool) {
	switch a {
	case AccessViewer:
		return rbac.RoleViewer, true
	case AccessAnalyst:
		return rbac.RoleAnalyst, true
	case AccessFractalAdmin:
		return rbac.RoleAdmin, true
	}
	return "", false
}

// Allows reports whether the request's principal may reach a route with this
// access level. It reads only what the auth middleware already resolved, so it
// costs no lookups.
func (a Access) Allows(r *http.Request) bool {
	switch a {
	case AccessPublic, AccessIngestToken, AccessInternal:
		return true
	}

	user, _ := r.Context().Value("user").(*storage.User)
	if user == nil {
		return false
	}
	if user.IsAdmin {
		return true
	}
	if a == AccessTenantAdmin {
		return false
	}
	if a == AccessAuthenticated {
		return true
	}

	required, scoped := a.role()
	if !scoped {
		return false
	}
	// Either scope may satisfy the requirement: a request is scoped to a fractal
	// or to a prism, and the middleware resolves whichever applies.
	return rbac.HasAccess(user, rbac.RoleFromContext(r.Context()), required) ||
		rbac.HasAccess(user, rbac.PrismRoleFromContext(r.Context()), required)
}
