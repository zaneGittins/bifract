package storage

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// stubConn is an identity marker for routing assertions; no method is ever called.
type stubConn struct{ driver.Conn }

// Untagged work must never run on a class identity. Inserts, merges, mutations,
// materialized views and alert evaluation all reach ClickHouse through the same
// helpers as search, and routing any of them onto a capped user would subject
// non-optional work to a query class's memory ceiling.
func TestConnForRoutesOnlyTaggedWork(t *testing.T) {
	c := &ClickHouseClient{}
	marker := &stubConn{}
	c.queryConns.Store(&map[string]*queryIdentity{
		QuerySearchWorkload: {conn: marker, memCapBytes: 1},
	})

	if got := c.connFor(context.Background()); got == marker {
		t.Error("unmarked context must not run on a class identity")
	}
	if got := c.connFor(UserSearchContext(context.Background())); got != marker {
		t.Error("search context must run on the search identity")
	}
	// A class with no provisioned identity falls back rather than borrowing another's.
	if got := c.connFor(RecallContext(context.Background())); got == marker {
		t.Error("recall must not run on the search identity")
	}
}

// With no identities provisioned at all (access management unavailable, or every
// class uncapped) every query must fall back to the default connection.
func TestConnForFallsBackWithoutIdentities(t *testing.T) {
	c := &ClickHouseClient{}
	if got := c.connFor(UserSearchContext(context.Background())); got != nil {
		t.Errorf("expected the default connection, got %v", got)
	}
	c.queryConns.Store(&map[string]*queryIdentity{})
	if got := c.connFor(UserSearchContext(context.Background())); got != nil {
		t.Errorf("expected the default connection, got %v", got)
	}
}

// Each class must get its own credential, and it must be stable across restarts and
// replicas: the pools of other replicas keep using the value already set on the user,
// so a password that varied per process would lock them out.
func TestQueryIdentityPasswordIsDerivedAndStable(t *testing.T) {
	c := &ClickHouseClient{Password: "privileged"}
	search := c.queryIdentityPassword(SearchCHUser)
	if search != c.queryIdentityPassword(SearchCHUser) {
		t.Error("password must be deterministic")
	}
	if search == c.queryIdentityPassword(RecallCHUser) {
		t.Error("each identity must get a distinct password")
	}
	if search == "" || search == c.Password {
		t.Errorf("derived password must not be empty or the source secret, got %q", search)
	}

	// Derived from the privileged password, so rotating that rotates these too.
	other := &ClickHouseClient{Password: "different"}
	if other.queryIdentityPassword(SearchCHUser) == search {
		t.Error("password must depend on the source secret")
	}
}

// ClickHouse recomputes max_server_memory_usage from currently-available memory, so a
// class's ceiling resolves to a slightly different byte count on every reconcile.
// Rebuilding the identity on that noise closes a live pool and aborts the searches
// running on it, so only a real change to the share may do so.
func TestWithinCapTolerance(t *testing.T) {
	const live = int64(3_669_841_500)

	// Observed drift between two reconciles seconds apart: must be treated as unchanged.
	if !withinCapTolerance(live, 3_546_368_650) {
		t.Error("server-memory drift must not rebuild the identity")
	}
	if !withinCapTolerance(live, live) {
		t.Error("an identical ceiling must not rebuild the identity")
	}

	// An admin halving the share (50% -> 25%) must take effect.
	if withinCapTolerance(live, live/2) {
		t.Error("a halved share must rebuild the identity")
	}
	if withinCapTolerance(live, live*2) {
		t.Error("a doubled share must rebuild the identity")
	}

	// Going uncapped is not a drift, and neither is coming back from it.
	if withinCapTolerance(live, 0) || withinCapTolerance(0, live) {
		t.Error("0 must never compare within tolerance of a real ceiling")
	}
}

func TestQueryIdentityUser(t *testing.T) {
	if got := queryIdentityUser(QuerySearchWorkload); got != SearchCHUser {
		t.Errorf("search workload mapped to %q", got)
	}
	if got := queryIdentityUser(QueryRecallWorkload); got != RecallCHUser {
		t.Errorf("recall workload mapped to %q", got)
	}
	// An unknown class must not resolve to an identity: it runs unrestricted on the
	// default connection instead of silently borrowing a class budget.
	if got := queryIdentityUser("something_else"); got != "" {
		t.Errorf("unknown workload mapped to %q", got)
	}
}

// A managed ClickHouse enforces password complexity and rejects CREATE USER with
// code 36 otherwise, which silently costs the class its memory ceiling. The hex
// digest alone has no uppercase and no special character.
func TestQueryIdentityPasswordMeetsComplexityPolicy(t *testing.T) {
	c := &ClickHouseClient{Password: "admin-password"}
	for _, user := range []string{SearchCHUser, RecallCHUser} {
		pw := c.queryIdentityPassword(user)
		var hasLower, hasUpper, hasDigit, hasSpecial bool
		for _, r := range pw {
			switch {
			case r >= 'a' && r <= 'z':
				hasLower = true
			case r >= 'A' && r <= 'Z':
				hasUpper = true
			case r >= '0' && r <= '9':
				hasDigit = true
			default:
				hasSpecial = true
			}
		}
		if !hasLower || !hasUpper || !hasDigit || !hasSpecial {
			t.Errorf("%s password lacks a required class (lower=%v upper=%v digit=%v special=%v)",
				user, hasLower, hasUpper, hasDigit, hasSpecial)
		}
	}
}
