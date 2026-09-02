package rbac

import (
	"context"
	"testing"
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
