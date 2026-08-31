package fractals

import (
	"context"
	"testing"
)

func TestAccessibleFractalIDs(t *testing.T) {
	var s LogScope

	ctx := context.WithValue(context.Background(), "selected_fractal", "f-1")
	ids, err := s.AccessibleFractalIDs(ctx)
	if err != nil || len(ids) != 1 || ids[0] != "f-1" {
		t.Fatalf("fractal session = (%v, %v), want ([f-1], nil)", ids, err)
	}

	// No prism manager wired: the prism resolves to nothing rather than falling
	// through to the default fractal, which is not in the prism.
	ctx = context.WithValue(context.Background(), "selected_prism", "p-1")
	if ids, err = s.AccessibleFractalIDs(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("prism session = (%v, %v), want (empty, nil)", ids, err)
	}

	s = LogScope{Fractals: &Manager{}}
	if _, err = s.AccessibleFractalIDs(WithNoScope(context.Background())); err != ErrNoScope {
		t.Fatalf("declared no scope = %v, want ErrNoScope", err)
	}
}

func TestLogScopeAllows(t *testing.T) {
	var s LogScope
	accessible := []string{"f-1", "f-2"}

	if !s.Allows(context.Background(), "f-2", accessible) {
		t.Error("a log in the accessible set must be allowed")
	}
	if s.Allows(context.Background(), "f-9", accessible) {
		t.Error("a log outside the accessible set must be refused")
	}
	// An unresolvable empty fractal_id must not match anything.
	if s.Allows(context.Background(), "", accessible) {
		t.Error("an empty fractal_id must not fail open")
	}
	if s.Allows(context.Background(), "f-1", nil) {
		t.Error("an empty accessible set must allow nothing")
	}
}

func TestReadFilterIDs(t *testing.T) {
	// No fractal manager, so '' cannot resolve to the default fractal and the
	// set is passed through unwidened.
	var s LogScope
	ctx := context.WithValue(context.Background(), "selected_fractal", "f-1")
	ids, err := s.ReadFilterIDs(ctx)
	if err != nil || len(ids) != 1 || ids[0] != "f-1" {
		t.Fatalf("ReadFilterIDs = (%v, %v), want ([f-1], nil)", ids, err)
	}

	// An empty scope stays empty: widening it would match legacy rows the caller
	// cannot read.
	ctx = context.WithValue(context.Background(), "selected_prism", "p-1")
	if ids, err = s.ReadFilterIDs(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("ReadFilterIDs = (%v, %v), want (empty, nil)", ids, err)
	}
}
