package rbac

import (
	"context"

	"bifract/pkg/storage"
)

// Role represents a fractal-level permission role.
type Role string

const (
	RoleNone    Role = ""
	RoleViewer  Role = "viewer"
	RoleAnalyst Role = "analyst"
	RoleAdmin   Role = "admin"
)

// EnumValues lists the roles a grant can name. RoleNone is the absence of a
// grant rather than a value anyone assigns, so it is not offered.
func (Role) EnumValues() []string {
	return []string{string(RoleViewer), string(RoleAnalyst), string(RoleAdmin)}
}

func roleWeight(r Role) int {
	switch r {
	case RoleAdmin:
		return 3
	case RoleAnalyst:
		return 2
	case RoleViewer:
		return 1
	default:
		return 0
	}
}

// Satisfies returns true if the role meets or exceeds the required level.
func (r Role) Satisfies(required Role) bool {
	return roleWeight(r) >= roleWeight(required)
}

// FractalAccess pairs a fractal ID with the user's effective role.
type FractalAccess struct {
	FractalID string `json:"fractal_id"`
	Role      Role   `json:"role"`
}

// HasAccess checks if a user has at least the required role.
// Tenant admins (user.IsAdmin) always pass. Returns false if user is nil.
func HasAccess(user *storage.User, fractalRole Role, requiredRole Role) bool {
	if user == nil {
		return false
	}
	if user.IsAdmin {
		return true
	}
	return fractalRole.Satisfies(requiredRole)
}

// RoleFromContext extracts the fractal role from a request context.
func RoleFromContext(ctx context.Context) Role {
	if r, ok := ctx.Value("fractal_role").(string); ok {
		return Role(r)
	}
	return RoleNone
}

// PrismRoleFromContext extracts the prism role from a request context.
func PrismRoleFromContext(ctx context.Context) Role {
	if r, ok := ctx.Value("prism_role").(string); ok {
		return Role(r)
	}
	return RoleNone
}

// Resolver loads RBAC data from PostgreSQL.
type Resolver struct {
	pg *storage.PostgresClient
}

// NewResolver creates a new RBAC resolver.
func NewResolver(pg *storage.PostgresClient) *Resolver {
	return &Resolver{pg: pg}
}

// ResolveFractalRole returns the effective role for a user on a fractal.
// Checks both direct and group-based grants, returning the highest.
// Tenant admin bypass should be handled by callers before calling this.
func (r *Resolver) ResolveFractalRole(ctx context.Context, username, fractalID string) (Role, error) {
	var role string
	err := r.pg.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT role FROM (
				SELECT role, CASE role
					WHEN 'admin' THEN 3
					WHEN 'analyst' THEN 2
					WHEN 'viewer' THEN 1
					ELSE 0
				END AS weight
				FROM fractal_permissions
				WHERE fractal_id = $1 AND username = $2

				UNION ALL

				SELECT fp.role, CASE fp.role
					WHEN 'admin' THEN 3
					WHEN 'analyst' THEN 2
					WHEN 'viewer' THEN 1
					ELSE 0
				END AS weight
				FROM fractal_permissions fp
				JOIN group_members gm ON gm.group_id = fp.group_id
				WHERE fp.fractal_id = $1 AND gm.username = $2
			) sub
			ORDER BY weight DESC
			LIMIT 1),
		'')
	`, fractalID, username).Scan(&role)
	if err != nil {
		return RoleNone, err
	}
	return Role(role), nil
}

// GetAccessibleFractals returns all fractal IDs the user has any access to,
// with their effective role on each.
func (r *Resolver) GetAccessibleFractals(ctx context.Context, username string) ([]FractalAccess, error) {
	rows, err := r.pg.Query(ctx, `
		SELECT fractal_id,
			(ARRAY['', 'viewer', 'analyst', 'admin'])[
				MAX(CASE role
					WHEN 'admin' THEN 4
					WHEN 'analyst' THEN 3
					WHEN 'viewer' THEN 2
					ELSE 1
				END)
			] AS effective_role
		FROM (
			SELECT fractal_id, role FROM fractal_permissions WHERE username = $1
			UNION ALL
			SELECT fp.fractal_id, fp.role
			FROM fractal_permissions fp
			JOIN group_members gm ON gm.group_id = fp.group_id
			WHERE gm.username = $1
		) perms
		GROUP BY fractal_id
	`, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []FractalAccess
	for rows.Next() {
		var fa FractalAccess
		var roleStr string
		if err := rows.Scan(&fa.FractalID, &roleStr); err != nil {
			return nil, err
		}
		fa.Role = Role(roleStr)
		result = append(result, fa)
	}
	return result, rows.Err()
}

// ResolveRole resolves a user's role on a specific fractal, handling tenant admin bypass.
// This is a convenience method that short-circuits for tenant admins.
func (r *Resolver) ResolveRole(ctx context.Context, user *storage.User, fractalID string) Role {
	if user == nil {
		return RoleNone
	}
	if user.IsAdmin {
		return RoleAdmin
	}
	if fractalID == "" {
		return RoleNone
	}
	if role := apiKeyRole(ctx, fractalID); role != RoleNone {
		return role
	}
	role, err := r.ResolveFractalRole(ctx, user.Username, fractalID)
	if err != nil {
		return RoleNone
	}
	return role
}

// apiKeyRole returns the role an API key carries for fractalID, or RoleNone.
//
// An API key authenticates as a synthetic apikey_<id> principal that owns no row
// in fractal_permissions, so resolving it by username always denies. The key's
// real grant is put in the request context by the auth middleware when the key is
// validated. It is trusted only for the one fractal the key is bound to, so a key
// cannot carry its role into a fractal it was never issued for.
// apiKeyPrismRole mirrors apiKeyRole for prism scope. A machine principal has no
// prism_permissions row, so without this every prism-scoped key resolves to RoleNone and
// is refused on endpoints that check a prism role.
func apiKeyPrismRole(ctx context.Context, prismID string) Role {
	scoped, _ := ctx.Value("selected_prism").(string)
	if scoped == "" || scoped != prismID {
		return RoleNone
	}
	if authType, _ := ctx.Value("auth_type").(string); authType != "api_key" {
		return RoleNone
	}
	role, _ := ctx.Value("prism_role").(string)
	switch Role(role) {
	case RoleViewer, RoleAnalyst, RoleAdmin:
		return Role(role)
	}
	return RoleNone
}

func apiKeyRole(ctx context.Context, fractalID string) Role {
	scoped, _ := ctx.Value("selected_fractal").(string)
	if scoped == "" || scoped != fractalID {
		return RoleNone
	}
	if authType, _ := ctx.Value("auth_type").(string); authType != "api_key" {
		return RoleNone
	}
	role, _ := ctx.Value("fractal_role").(string)
	switch Role(role) {
	case RoleViewer, RoleAnalyst, RoleAdmin:
		return Role(role)
	}
	return RoleNone
}

// PrismAccess pairs a prism ID with the user's effective role.
type PrismAccess struct {
	PrismID string `json:"prism_id"`
	Role    Role   `json:"role"`
}

// ResolvePrismRole returns the effective role for a user on a prism.
// Checks both direct and group-based grants, returning the highest.
func (r *Resolver) ResolvePrismRole(ctx context.Context, username, prismID string) (Role, error) {
	var role string
	err := r.pg.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT role FROM (
				SELECT role, CASE role
					WHEN 'admin' THEN 3
					WHEN 'analyst' THEN 2
					WHEN 'viewer' THEN 1
					ELSE 0
				END AS weight
				FROM prism_permissions
				WHERE prism_id = $1 AND username = $2

				UNION ALL

				SELECT pp.role, CASE pp.role
					WHEN 'admin' THEN 3
					WHEN 'analyst' THEN 2
					WHEN 'viewer' THEN 1
					ELSE 0
				END AS weight
				FROM prism_permissions pp
				JOIN group_members gm ON gm.group_id = pp.group_id
				WHERE pp.prism_id = $1 AND gm.username = $2
			) sub
			ORDER BY weight DESC
			LIMIT 1),
		'')
	`, prismID, username).Scan(&role)
	if err != nil {
		return RoleNone, err
	}
	return Role(role), nil
}

// GetAccessiblePrisms returns all prism IDs the user has any access to,
// with their effective role on each.
func (r *Resolver) GetAccessiblePrisms(ctx context.Context, username string) ([]PrismAccess, error) {
	rows, err := r.pg.Query(ctx, `
		SELECT prism_id,
			(ARRAY['', 'viewer', 'analyst', 'admin'])[
				MAX(CASE role
					WHEN 'admin' THEN 4
					WHEN 'analyst' THEN 3
					WHEN 'viewer' THEN 2
					ELSE 1
				END)
			] AS effective_role
		FROM (
			SELECT prism_id, role FROM prism_permissions WHERE username = $1
			UNION ALL
			SELECT pp.prism_id, pp.role
			FROM prism_permissions pp
			JOIN group_members gm ON gm.group_id = pp.group_id
			WHERE gm.username = $1
		) perms
		GROUP BY prism_id
	`, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PrismAccess
	for rows.Next() {
		var pa PrismAccess
		var roleStr string
		if err := rows.Scan(&pa.PrismID, &roleStr); err != nil {
			return nil, err
		}
		pa.Role = Role(roleStr)
		result = append(result, pa)
	}
	return result, rows.Err()
}

// ResolvePrismRole resolves a user's role on a specific prism, handling tenant admin bypass.
func (r *Resolver) ResolvePrismRoleWithAdmin(ctx context.Context, user *storage.User, prismID string) Role {
	if user == nil {
		return RoleNone
	}
	if user.IsAdmin {
		return RoleAdmin
	}
	if prismID == "" {
		return RoleNone
	}
	if role := apiKeyPrismRole(ctx, prismID); role != RoleNone {
		return role
	}
	role, err := r.ResolvePrismRole(ctx, user.Username, prismID)
	if err != nil {
		return RoleNone
	}
	return role
}
