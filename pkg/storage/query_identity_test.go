package storage

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// stubConn is an identity marker for routing assertions; no method is ever called.
type stubConn struct{ driver.Conn }

// Untagged work must never run on a capped class identity: internal reads share the
// helpers search uses, and a search ceiling must not throttle them.
func TestConnForRoutesOnlyTaggedWork(t *testing.T) {
	c := &ClickHouseClient{}
	marker := &stubConn{}
	c.queryConns.Store(&map[string]*queryIdentity{
		QuerySearchWorkload: {conn: marker, memCapBytes: 1},
	})

	if got := c.connFor(context.Background()); got == marker {
		t.Error("unmarked context must not run on a class identity")
	}
	if got := c.connFor(UserSearchContext(context.Background(), FractalScope{})); got != marker {
		t.Error("search context must run on the search identity")
	}
	// A class with no provisioned identity falls back rather than borrowing another's.
	if got := c.connFor(RecallContext(context.Background())); got == marker {
		t.Error("recall must not run on the search identity")
	}
}

// User-authored reads outside the search budget run on their own identity, and fall
// back to the default connection like every class when it is not provisioned.
func TestConnForRoutesUncappedReads(t *testing.T) {
	c := &ClickHouseClient{}
	marker, search := &stubConn{}, &stubConn{}
	ctx := UncappedReadContext(context.Background())
	if got := c.connFor(ctx); got != nil {
		t.Errorf("expected the default connection, got %v", got)
	}
	c.queryConns.Store(&map[string]*queryIdentity{QuerySearchWorkload: {conn: search}})
	if got := c.connFor(ctx); got == search {
		t.Error("an uncapped read must not borrow the search identity")
	}
	c.queryConns.Store(&map[string]*queryIdentity{QueryAlertClass: {conn: marker}, QuerySearchWorkload: {conn: search}})
	if got := c.connFor(ctx); got != marker {
		t.Error("an uncapped read must run on the alert identity")
	}
	if got := QueryClassOf(ctx); got != QueryAlertClass {
		t.Errorf("class = %q", got)
	}
}

// The alert class has no workload behind it, so a query in it must not name one.
func TestUncappedReadsCarryNoWorkloadOrCeiling(t *testing.T) {
	c := &ClickHouseClient{}
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	c.setQueryMemoryCaps(map[string]int64{QuerySearchWorkload: 100})
	got := c.applyQuerySettings(UncappedReadContext(context.Background()), nil)
	if len(got) != 0 {
		t.Errorf("settings = %v, want none", got)
	}
}

// An unmarked read runs read-only, so a caller that forgets its marker never gains
// admin. Admin is an explicit opt-in for reads with no class.
func TestConnForRoutesUnmarkedReadsToTheReader(t *testing.T) {
	ctx := context.Background()
	c := &ClickHouseClient{}
	reader, search := &stubConn{}, &stubConn{}
	if got := c.connFor(ctx); got != nil {
		t.Errorf("no reader provisioned: expected the default connection, got %v", got)
	}
	c.queryConns.Store(&map[string]*queryIdentity{QueryReaderClass: {conn: reader}, QuerySearchWorkload: {conn: search}})
	if got := c.connFor(ctx); got != reader {
		t.Error("an unmarked read must run on the reader identity")
	}
	if got := c.connFor(UserSearchContext(ctx, FractalScope{})); got != search {
		t.Error("a marked read must keep its class identity")
	}
	if got := c.connFor(AdminReadContext(ctx)); got != nil {
		t.Errorf("an admin read must run on the default connection, got %v", got)
	}
	if got := c.connFor(AdminReadContext(UserSearchContext(ctx, FractalScope{}))); got != search {
		t.Errorf("a class must win over a leaked admin marker, got %v", got)
	}
	// The reader has no workload and no ceiling, like the alert class.
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	c.setQueryMemoryCaps(map[string]int64{QuerySearchWorkload: 100})
	if got := c.applyQuerySettings(ctx, nil); len(got) != 0 {
		t.Errorf("unmarked read settings = %v, want none", got)
	}
}

// With no identities provisioned at all (access management unavailable, or every
// class uncapped) every query must fall back to the default connection.
func TestConnForFallsBackWithoutIdentities(t *testing.T) {
	c := &ClickHouseClient{}
	if got := c.connFor(UserSearchContext(context.Background(), FractalScope{})); got != nil {
		t.Errorf("expected the default connection, got %v", got)
	}
	c.queryConns.Store(&map[string]*queryIdentity{})
	if got := c.connFor(UserSearchContext(context.Background(), FractalScope{})); got != nil {
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
	if got := queryIdentityUser(QueryAlertClass); got != AlertCHUser {
		t.Errorf("alert class mapped to %q", got)
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

// A pin taken while the parent has no alert identity stays on the default connection
// rather than guessing, and a pin that already holds one is not reopened.
func TestEnsurePinnedIdentityWithoutDialing(t *testing.T) {
	ctx := context.Background()
	parent, pinned := &ClickHouseClient{}, &ClickHouseClient{}
	parent.EnsurePinnedIdentity(ctx, pinned, "unreachable:1")
	if pinned.hasQueryIdentity(QueryAlertClass) {
		t.Error("pinned an identity the parent does not have")
	}

	held := &stubConn{}
	parent.queryConns.Store(&map[string]*queryIdentity{QueryAlertClass: {conn: &stubConn{}}})
	pinned.queryConns.Store(&map[string]*queryIdentity{QueryAlertClass: {conn: held}})
	parent.EnsurePinnedIdentity(ctx, pinned, "unreachable:1")
	if got := pinned.connFor(UncappedReadContext(ctx)); got != held {
		t.Error("a held pinned identity was replaced")
	}
}

// A node that refuses the identity is not asked again on every rule-test session.
func TestEnsurePinnedIdentityThrottlesARefusingNode(t *testing.T) {
	ctx := context.Background()
	parent, pinned := &ClickHouseClient{}, &ClickHouseClient{}
	parent.queryConns.Store(&map[string]*queryIdentity{QueryAlertClass: {conn: &stubConn{}}})
	parent.EnsurePinnedIdentity(ctx, pinned, "127.0.0.1:1")
	if pinned.hasQueryIdentity(QueryAlertClass) {
		t.Fatal("pinned an identity the node refused")
	}
	if !pinned.pinRetryAfter.After(time.Now()) {
		t.Errorf("no retry backoff after a refusal: %v", pinned.pinRetryAfter)
	}
}

// Keeper holds a cluster's DDL verbatim, including every CREATE/ALTER USER password,
// so no least-privilege identity may read it.
func TestPrivateSystemTablesIncludeKeeper(t *testing.T) {
	for _, want := range []string{"zookeeper", "query_log", "processes"} {
		if !slices.Contains(privateSystemTables, want) {
			t.Errorf("system.%s is readable by the identities", want)
		}
	}
}

// A client that shares another's identities (the archive client) runs its reads as
// that client's classes, with its workloads and ceilings, keeps its own connection for
// admin work, and never closes the pools it borrowed.
func TestSharedIdentitiesRouteReads(t *testing.T) {
	ctx := context.Background()
	src := &ClickHouseClient{}
	recall, reader := &stubConn{}, &stubConn{}
	src.queryConns.Store(&map[string]*queryIdentity{QueryRecallWorkload: {conn: recall}, QueryReaderClass: {conn: reader}})
	src.setActiveWorkloads(map[string]bool{QueryRecallWorkload: true})
	src.setQueryMemoryCaps(map[string]int64{QueryRecallWorkload: 4096})

	own := &stubConn{}
	c := &ClickHouseClient{conn: own}
	if got := c.connFor(RecallContext(ctx)); got != own {
		t.Fatal("a client sharing nothing must use its own connection")
	}
	c.ShareQueryIdentities(src)
	if got := c.connFor(RecallContext(ctx)); got != recall {
		t.Error("a recall read must run on the shared recall identity")
	}
	if got := c.connFor(ctx); got != reader {
		t.Error("an unmarked read must run on the shared reader identity")
	}
	if got := c.connFor(AdminReadContext(ctx)); got != own {
		t.Error("an admin read must use the client's own connection")
	}
	s := c.applyQuerySettings(RecallContext(ctx), nil)
	if s["workload"] != QueryRecallWorkload || s["max_memory_usage"] != int64(4096) {
		t.Errorf("recall settings = %v, want the shared workload and ceiling", s)
	}
	c.closeQueryIdentities()
	if !src.hasQueryIdentity(QueryRecallWorkload) {
		t.Error("closing the borrower closed the shared identities")
	}
}
