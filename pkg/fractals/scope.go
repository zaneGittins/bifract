package fractals

import (
	"context"
	"errors"
)

type noScopeKey struct{}

// ErrNoScope reports a caller that stated it holds no fractal or prism.
var ErrNoScope = errors.New("caller declared no scope")

// WithNoScope marks a request whose caller stated it holds no scope.
func WithNoScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, noScopeKey{}, true)
}

// NoScopeDeclared reports whether the caller stated it holds no scope.
func NoScopeDeclared(ctx context.Context) bool {
	v, _ := ctx.Value(noScopeKey{}).(bool)
	return v
}
