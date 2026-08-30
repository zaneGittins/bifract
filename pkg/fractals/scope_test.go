package fractals

import (
	"context"
	"testing"
)

func TestNoScopeDeclared(t *testing.T) {
	if NoScopeDeclared(context.Background()) {
		t.Fatal("a plain context must not read as a declared no-scope")
	}
	if !NoScopeDeclared(WithNoScope(context.Background())) {
		t.Fatal("WithNoScope must be observable")
	}
}

// The refusal has to happen before the storage lookup, since a Manager with no
// storage is exactly what a scopeless request must not reach.
func TestGetDefaultFractalRefusesDeclaredNoScope(t *testing.T) {
	m := &Manager{}
	if _, err := m.GetDefaultFractal(WithNoScope(context.Background())); err != ErrNoScope {
		t.Fatalf("GetDefaultFractal = %v, want ErrNoScope", err)
	}
}
