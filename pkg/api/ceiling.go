package api

import "context"

// ceilingKey types the context value so nothing outside this package can set it.
type ceilingKey struct{}

// WithCeiling caps what a request may reach, below whatever the caller's own
// role allows. It exists for in-process callers that dispatch on a user's behalf
// but must not inherit that user's full authority: an AI tool the user has not
// confirmed runs under a viewer ceiling, so a tool body that reaches for a write
// is refused by the same guard that enforces the route's declared access.
func WithCeiling(ctx context.Context, a Access) context.Context {
	return context.WithValue(ctx, ceilingKey{}, a)
}

// CeilingFromContext returns the ceiling in force, and whether one is set.
func CeilingFromContext(ctx context.Context) (Access, bool) {
	a, ok := ctx.Value(ceilingKey{}).(Access)
	return a, ok
}

// rank orders the RBAC ladder. Values off the ladder rank 0.
func (a Access) rank() int {
	switch a {
	case AccessViewer:
		return 1
	case AccessAnalyst:
		return 2
	case AccessFractalAdmin:
		return 3
	case AccessTenantAdmin:
		return 4
	}
	return 0
}

// Permits reports whether a route requiring required may run under this ceiling.
// Anything off the ladder, including the non-session schemes, is refused: a
// ceiling is a deliberate reduction and must fail closed on a value it does not
// recognise.
func (ceiling Access) Permits(required Access) bool {
	if required == AccessPublic || required == AccessAuthenticated {
		return true
	}
	r, c := required.rank(), ceiling.rank()
	return r > 0 && c > 0 && r <= c
}
