package rbac

import (
	"context"
	"testing"

	"bifract/pkg/storage"
)

// An API key authenticates as a principal with no fractal_permissions row, so a
// username lookup denies it. The role it was issued with is only in the request
// context, and honouring it there is what makes a fractal-scoped key work on the
// handlers that resolve a role rather than read the context themselves.
func TestAPIKeyRoleIsHonouredOnlyForItsOwnFractal(t *testing.T) {
	const fractal = "f-1"

	keyCtx := func(scope, role, authType string) context.Context {
		ctx := context.WithValue(context.Background(), "selected_fractal", scope)
		ctx = context.WithValue(ctx, "fractal_role", role)
		return context.WithValue(ctx, "auth_type", authType)
	}

	tests := []struct {
		name string
		ctx  context.Context
		want Role
	}{
		{"key's own fractal", keyCtx(fractal, "analyst", "api_key"), RoleAnalyst},
		{"another fractal", keyCtx("f-2", "admin", "api_key"), RoleNone},
		{"session, not a key", keyCtx(fractal, "admin", "session"), RoleNone},
		{"role not a real grant", keyCtx(fractal, "superuser", "api_key"), RoleNone},
		{"no role carried", keyCtx(fractal, "", "api_key"), RoleNone},
		{"nothing in context", context.Background(), RoleNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apiKeyRole(tt.ctx, fractal); got != tt.want {
				t.Errorf("apiKeyRole = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPIKeyPrismRole(t *testing.T) {
	prism := "p-1"
	ctx := func(scoped, authType, role string) context.Context {
		c := context.WithValue(context.Background(), "selected_prism", scoped)
		c = context.WithValue(c, "auth_type", authType)
		return context.WithValue(c, "prism_role", role)
	}

	if got := apiKeyPrismRole(ctx(prism, "api_key", "admin"), prism); got != RoleAdmin {
		t.Errorf("a prism-scoped key must carry its role, got %q", got)
	}
	// A key scoped to another prism must not borrow authority here.
	if got := apiKeyPrismRole(ctx("other", "api_key", "admin"), prism); got != RoleNone {
		t.Errorf("a key scoped elsewhere must not resolve, got %q", got)
	}
	if got := apiKeyPrismRole(ctx(prism, "session", "admin"), prism); got != RoleNone {
		t.Errorf("only an api_key principal resolves this way, got %q", got)
	}
	if got := apiKeyPrismRole(ctx(prism, "api_key", "superuser"), prism); got != RoleNone {
		t.Errorf("an unknown role must not resolve, got %q", got)
	}
}

// ObjectRole answers for the scope that owns an object, not the request's. A key
// scoped to one fractal holds no role on an object in another, whatever its own
// role, so guessing an id cannot reach across fractals.
func TestObjectRoleConfinesAPIKeysToTheirScope(t *testing.T) {
	key := func(fractal, prism string) context.Context {
		ctx := context.WithValue(context.Background(), "auth_type", "api_key")
		ctx = context.WithValue(ctx, "selected_fractal", fractal)
		ctx = context.WithValue(ctx, "selected_prism", prism)
		ctx = context.WithValue(ctx, "fractal_role", "admin")
		return context.WithValue(ctx, "prism_role", "admin")
	}
	keyUser := &storage.User{Username: "apikey_1"}

	tests := []struct {
		name           string
		ctx            context.Context
		user           *storage.User
		fractal, prism string
		want           Role
	}{
		{"key on its own fractal", key("f1", ""), keyUser, "f1", "", RoleAdmin},
		{"key on another fractal", key("f1", ""), keyUser, "f2", "", RoleNone},
		{"key on its own prism", key("", "p1"), keyUser, "", "p1", RoleAdmin},
		{"key on another prism", key("", "p1"), keyUser, "", "p2", RoleNone},
		{"fractal key on a prism object", key("f1", ""), keyUser, "", "p1", RoleNone},
		{"object with no scope", key("f1", ""), keyUser, "", "", RoleNone},
		{"tenant admin anywhere", key("f1", ""), &storage.User{IsAdmin: true}, "f2", "", RoleAdmin},
		{"no user", key("f1", ""), nil, "f1", "", RoleNone},
	}
	var resolver *Resolver
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolver.ObjectRole(tt.ctx, tt.user, tt.fractal, tt.prism); got != tt.want {
				t.Errorf("ObjectRole = %q, want %q", got, tt.want)
			}
		})
	}
}

// Without a resolver only the request's own role is known, and it is a role on
// the selected scope. It must not stand in for a role on any other.
func TestObjectRoleWithoutResolverStaysOnTheSelectedScope(t *testing.T) {
	ctx := context.WithValue(context.Background(), "selected_fractal", "f1")
	ctx = context.WithValue(ctx, "fractal_role", "analyst")
	user := &storage.User{Username: "alice"}
	var resolver *Resolver

	if got := resolver.ObjectRole(ctx, user, "f1", ""); got != RoleAnalyst {
		t.Errorf("selected fractal: %q", got)
	}
	if got := resolver.ObjectRole(ctx, user, "f2", ""); got != RoleNone {
		t.Errorf("another fractal: %q, want none", got)
	}
	if got := resolver.ObjectRole(ctx, user, "", ""); got != RoleNone {
		t.Errorf("an object with no scope: %q, want none", got)
	}
}
