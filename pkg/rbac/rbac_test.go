package rbac

import (
	"context"
	"testing"

	"bifract/pkg/storage"
)

// The hierarchy is admin > analyst > viewer > none, and every check in the app
// reduces to Satisfies. A role string that is not one of the three grants must
// never satisfy anything above "no access": rows in the permission tables are
// free text, and an unknown value sorting above viewer would be a silent grant.
func TestSatisfies(t *testing.T) {
	roles := []Role{RoleNone, RoleViewer, RoleAnalyst, RoleAdmin}
	for i, have := range roles {
		for j, need := range roles {
			want := i >= j
			if got := have.Satisfies(need); got != want {
				t.Errorf("%q.Satisfies(%q) = %v, want %v", have, need, got, want)
			}
		}
	}

	for _, bogus := range []Role{"superuser", "Admin", "ADMIN", " admin", "owner"} {
		for _, need := range []Role{RoleViewer, RoleAnalyst, RoleAdmin} {
			if bogus.Satisfies(need) {
				t.Errorf("unknown role %q satisfies %q", bogus, need)
			}
		}
		if !bogus.Satisfies(RoleNone) {
			t.Errorf("unknown role %q should still satisfy the empty requirement", bogus)
		}
	}
}

func TestHasAccess(t *testing.T) {
	tenantAdmin := &storage.User{Username: "root", IsAdmin: true}
	member := &storage.User{Username: "alice"}

	tests := []struct {
		name     string
		user     *storage.User
		role     Role
		required Role
		want     bool
	}{
		{"nil user is denied even for no requirement", nil, RoleAdmin, RoleNone, false},
		{"tenant admin passes with no fractal grant", tenantAdmin, RoleNone, RoleAdmin, true},
		{"viewer may view", member, RoleViewer, RoleViewer, true},
		{"viewer may not analyze", member, RoleViewer, RoleAnalyst, false},
		{"analyst may not administer", member, RoleAnalyst, RoleAdmin, false},
		{"fractal admin may analyze", member, RoleAdmin, RoleAnalyst, true},
		{"no grant may not view", member, RoleNone, RoleViewer, false},
		{"unknown grant may not view", member, Role("superuser"), RoleViewer, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasAccess(tt.user, tt.role, tt.required); got != tt.want {
				t.Errorf("HasAccess = %v, want %v", got, tt.want)
			}
		})
	}
}

// The middleware stores roles as plain strings. A typed Role in the context is
// not what it writes, and must read as no role rather than be trusted.
func TestRoleFromContext(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  any
		want Role
	}{
		{"string role", "fractal_role", "analyst", RoleAnalyst},
		{"typed role is not trusted", "fractal_role", RoleAdmin, RoleNone},
		{"missing", "", nil, RoleNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pctx := context.Background()
			if tt.key != "" {
				ctx = context.WithValue(ctx, "fractal_role", tt.val)
				pctx = context.WithValue(pctx, "prism_role", tt.val)
			}
			if got := RoleFromContext(ctx); got != tt.want {
				t.Errorf("RoleFromContext = %q, want %q", got, tt.want)
			}
			if got := PrismRoleFromContext(pctx); got != tt.want {
				t.Errorf("PrismRoleFromContext = %q, want %q", got, tt.want)
			}
		})
	}

	// The two scopes are read from separate keys; a fractal role must not be
	// read back as a prism role or the reverse.
	ctx := context.WithValue(context.Background(), "fractal_role", "admin")
	if got := PrismRoleFromContext(ctx); got != RoleNone {
		t.Errorf("fractal role leaked into prism role: %q", got)
	}
}

// RoleNone is the absence of a grant. Offering it as an assignable value would
// let a grant row exist that looks like access but carries none.
func TestEnumValuesExcludeNone(t *testing.T) {
	got := RoleNone.EnumValues()
	want := []string{"viewer", "analyst", "admin"}
	if len(got) != len(want) {
		t.Fatalf("EnumValues = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("EnumValues = %v, want %v", got, want)
		}
	}
}

// These are the branches of ResolveRole / ResolvePrismRoleWithAdmin that decide
// without a database. The resolver has no Postgres client here, so any case that
// fell through to a lookup would panic rather than pass.
func TestResolveRoleWithoutLookup(t *testing.T) {
	r := NewResolver(nil)
	member := &storage.User{Username: "apikey_k1"}
	admin := &storage.User{Username: "root", IsAdmin: true}

	keyCtx := context.WithValue(context.Background(), "auth_type", "api_key")
	keyCtx = context.WithValue(keyCtx, "selected_fractal", "f-1")
	keyCtx = context.WithValue(keyCtx, "fractal_role", "viewer")
	keyCtx = context.WithValue(keyCtx, "selected_prism", "p-1")
	keyCtx = context.WithValue(keyCtx, "prism_role", "analyst")

	t.Run("fractal", func(t *testing.T) {
		cases := []struct {
			name    string
			ctx     context.Context
			user    *storage.User
			fractal string
			want    Role
		}{
			{"nil user", context.Background(), nil, "f-1", RoleNone},
			{"tenant admin on any fractal", context.Background(), admin, "f-9", RoleAdmin},
			{"tenant admin with no fractal", context.Background(), admin, "", RoleAdmin},
			{"no fractal", keyCtx, member, "", RoleNone},
			{"api key on its own fractal", keyCtx, member, "f-1", RoleViewer},
		}
		for _, c := range cases {
			if got := r.ResolveRole(c.ctx, c.user, c.fractal); got != c.want {
				t.Errorf("%s: ResolveRole = %q, want %q", c.name, got, c.want)
			}
		}
	})

	t.Run("prism", func(t *testing.T) {
		cases := []struct {
			name  string
			ctx   context.Context
			user  *storage.User
			prism string
			want  Role
		}{
			{"nil user", context.Background(), nil, "p-1", RoleNone},
			{"tenant admin", context.Background(), admin, "p-9", RoleAdmin},
			{"no prism", keyCtx, member, "", RoleNone},
			{"api key on its own prism", keyCtx, member, "p-1", RoleAnalyst},
		}
		for _, c := range cases {
			if got := r.ResolvePrismRoleWithAdmin(c.ctx, c.user, c.prism); got != c.want {
				t.Errorf("%s: ResolvePrismRoleWithAdmin = %q, want %q", c.name, got, c.want)
			}
		}
	})
}
