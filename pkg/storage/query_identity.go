package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Each query class connects to ClickHouse as its own user so its memory share can be
// enforced with max_memory_usage_for_user -- a ceiling on everything the class is
// running at once, not on one query at a time.
//
// The per-user cap is what makes a class share mean what it says. A per-query ceiling
// alone bounds one search; it does nothing about ten concurrent searches each claiming
// that ceiling, which is the case that exhausts the node and starves the work this
// mechanism exists to protect. ClickHouse's own aggregate mechanism for a workload
// (max_memory, backed by the MEMORY RESERVATION scheduler) is unusable: it deadlocks
// queries into a permanent "Stopping" state that KILL QUERY cannot clear, which is why
// dropMemoryReservationResource removes it. max_memory_usage_for_user is the mature
// path to the same guarantee, and it fails a query cleanly with code 241.
//
// Inserts, merges, mutations and materialized views are outside this boundary by
// construction: writes go through Exec and batches on the default connection, never
// through connFor, so a query class's ceiling can never throttle them. The identities
// are granted read-only, so a write mistakenly issued on one fails loudly instead of
// silently drawing on a search budget.
//
// The alert and reader classes have identities for privilege alone, with no ceiling
// and no workload. Alert evaluation and rule tests, field stats, exports and model
// scoring run user-authored SQL on the alert class. Every read no caller marked runs
// on the reader, so a forgotten marker stays read-only; the few reads that need more
// ask for the default connection with AdminReadContext.
const (
	// SearchCHUser and RecallCHUser deliberately match their workload names: the
	// workload schedules the class's CPU, the user accounts for its memory. ClickHouse
	// keeps users and workloads in separate namespaces, so the shared name is only a
	// label.
	SearchCHUser = "bifract_search"
	RecallCHUser = "bifract_recall"

	// QueryAlertClass marks reads for AlertCHUser. Unlike the two above it names no
	// workload, so it is never scheduled and never tagged.
	AlertCHUser     = "bifract_alert"
	QueryAlertClass = AlertCHUser

	// QueryReaderClass is every read no caller marked: internal reads such as status
	// and model health, and any user-derived read whose caller forgot its marker. It
	// gets its own pool so background reads never take an alert's connections. Reads
	// the identities cannot serve opt into admin with AdminReadContext.
	ReaderCHUser     = "bifract_reader"
	QueryReaderClass = ReaderCHUser

	// queryIdentityRepairInterval is how often a missing identity is retried.
	queryIdentityRepairInterval = time.Minute

	// queryIdentityDDLTimeout bounds one access-control statement. These are
	// metadata-only, so a slow one means the node is in trouble.
	queryIdentityDDLTimeout = 30 * time.Second

	// queryIdentityCapTolerance is how far a class's recomputed ceiling may drift
	// before its identity is rebuilt. ClickHouse derives max_server_memory_usage from
	// memory available at the time it is read, so the same share resolves to a slightly
	// different byte count on each reconcile. Rebuilding on that noise would close a
	// live pool -- aborting whatever searches were running on it -- for a ceiling
	// change no user could perceive.
	queryIdentityCapTolerance = 0.10
)

// queryIdentity is one class's provisioned connection and the ceiling it was built
// with, so reconcile can tell an unchanged identity from one that must be rebuilt.
type queryIdentity struct {
	conn        driver.Conn
	memCapBytes int64
}

// queryIdentityClasses are the classes that run on their own identity.
var queryIdentityClasses = []string{QuerySearchWorkload, QueryRecallWorkload, QueryAlertClass, QueryReaderClass}

// queryIdentityUser maps a workload class to the ClickHouse user that runs it.
func queryIdentityUser(workload string) string {
	switch workload {
	case QuerySearchWorkload:
		return SearchCHUser
	case QueryRecallWorkload:
		return RecallCHUser
	case QueryAlertClass:
		return AlertCHUser
	case QueryReaderClass:
		return ReaderCHUser
	}
	return ""
}

// queryIdentityPassword derives a class identity's password from the password this
// client already holds. Deterministic on purpose: every app replica derives the same
// value without shared storage, and a restart re-derives rather than rotating a
// credential the other replicas' pools are still using. Knowing this password conveys
// nothing new, since deriving it requires the privileged password already.
func (c *ClickHouseClient) queryIdentityPassword(user string) string {
	mac := hmac.New(sha256.New, []byte(c.Password))
	mac.Write([]byte("bifract-query-identity:" + user))
	// The hex digest is lowercase and digits only. A managed ClickHouse enforces
	// a complexity policy and rejects CREATE USER with code 36, which costs the
	// class its memory ceiling. The suffix is fixed rather than derived so the
	// value stays deterministic: every replica must derive the same password
	// without shared state, and it adds no entropy the digest does not already
	// carry.
	return hex.EncodeToString(mac.Sum(nil)) + queryIdentityPasswordSuffix
}

// queryIdentityPasswordSuffix supplies the uppercase and special characters a
// hex digest lacks. See queryIdentityPassword.
const queryIdentityPasswordSuffix = "Aa1!"

// reconcileQueryIdentities provisions one ClickHouse user per class and opens a pool
// for it. Classes absent from memCaps are uncapped: they keep their identity, without a
// memory ceiling.
//
// An identity whose ceiling has not changed is reused rather than rebuilt. Reconcile
// runs on every settings save, and closing a live pool would abort whatever searches
// were running on it.
//
// Never fatal. A deployment whose privileged user lacks access management cannot create
// users at all; that class then falls back to the default connection, where any
// per-query ceiling from applyQuerySettings still applies. The result is the previous
// behaviour, not a broken search.
func (c *ClickHouseClient) reconcileQueryIdentities(ctx context.Context, memCaps map[string]int64) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.reconcileIdentitiesLocked(ctx, memCaps)
}

func (c *ClickHouseClient) reconcileIdentitiesLocked(ctx context.Context, memCaps map[string]int64) {
	var current map[string]*queryIdentity
	if m := c.queryConns.Load(); m != nil {
		current = *m
	}

	next := map[string]*queryIdentity{}
	var firstErr error
	for _, workload := range queryIdentityClasses {
		limit := memCaps[workload]
		if limit < 0 {
			limit = 0
		}
		// Provisioned whether or not the class is capped. The identity is the
		// privilege boundary a query runs inside, and an uncapped class running on
		// the privileged default connection would give an injected query the
		// admin's reach. limit == 0 means no memory ceiling, not no identity.
		if live := current[workload]; live != nil && withinCapTolerance(live.memCapBytes, limit) {
			next[workload] = live
			continue
		}
		user := queryIdentityUser(workload)
		conn, err := c.ensureQueryIdentity(ctx, user, limit)
		if err != nil {
			log.Printf("[ClickHouse] Query identity %q unavailable, %s falls back to the default connection: %v", user, workload, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		next[workload] = &queryIdentity{conn: conn, memCapBytes: limit}
		if limit > 0 {
			log.Printf("[ClickHouse] Query identity %q ensured (read-only on %s, max_memory_usage_for_user = %d)", user, c.logsDatabase(), limit)
		} else {
			log.Printf("[ClickHouse] Query identity %q ensured (read-only on %s, uncapped)", user, c.logsDatabase())
		}
	}

	// A later class succeeding must not mask an earlier class's failure.
	if firstErr != nil {
		c.recordCapability(CapQueryIdentity, firstErr)
	}

	// After the identities: the policy that requires a scope names the search user.
	c.ensureFractalScopePolicies(ctx, next[QuerySearchWorkload] != nil)

	// Swap first, then close only what this reconcile actually replaced, so a reused
	// identity never loses the connection underneath an in-flight query.
	c.queryConns.Store(&next)
	for workload, old := range current {
		if old != nil && next[workload] != old {
			old.conn.Close()
		}
	}
}

// withinCapTolerance reports whether a recomputed ceiling is close enough to the one an
// identity already carries to leave it alone. An admin changing the share moves it far
// more than the tolerance; server-memory drift moves it far less.
func withinCapTolerance(live, next int64) bool {
	// Both uncapped, or one becoming (un)capped, is an exact comparison: the
	// difference is whether the ceiling exists at all, not how big it is.
	if live <= 0 || next <= 0 {
		return live == next
	}
	diff := live - next
	if diff < 0 {
		diff = -diff
	}
	return float64(diff) <= float64(live)*queryIdentityCapTolerance
}

// ensureQueryIdentity creates or updates one class identity and returns a pool
// connected as it. The grants are read-shaped; a missing one surfaces as a query
// failure for the user.
func (c *ClickHouseClient) ensureQueryIdentity(ctx context.Context, user string, memCapBytes int64) (driver.Conn, error) {
	onCluster := c.OnClusterSQL()
	pw := EscCHStr(c.queryIdentityPassword(user))

	// Required: without these the identity does not exist or cannot read, so a
	// failure means no identity.
	//
	// Scoped to the logs database plus system rather than *.*: the identity is also
	// the blast radius of a SQL injection in the query path, and a search has no
	// reason to reach another database. system is needed for profiling and for the
	// backpressure/readiness reads the query path makes.
	db := "`" + EscCHIdent(c.logsDatabase()) + "`"
	create := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s%s IDENTIFIED BY '%s'", user, onCluster, pw),
		fmt.Sprintf("ALTER USER %s%s IDENTIFIED BY '%s'", user, onCluster, pw),
	}
	for _, stmt := range create {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			c.recordCapability(CapQueryIdentity, err)
			return nil, fmt.Errorf("%s: %w", user, err)
		}
	}
	// Before the grants, because ClickHouse revokes hierarchically: revoking SELECT
	// ON *.* also removes SELECT ON <db>.*, so a revoke after the grants would undo
	// them. Only runs when something stale is actually present, so the window in
	// which the identity holds nothing is confined to the one-time migration off a
	// broader release -- not every settings save and replica start.
	c.revokeStaleGrants(ctx, user)

	required := []string{
		// SELECT and dictGet cover log reads and model_lookup/match() enrichment.
		fmt.Sprintf("GRANT%s SELECT, dictGet ON %s.* TO %s", onCluster, db, user),
		fmt.Sprintf("GRANT%s SELECT ON system.* TO %s", onCluster, user),
	}
	// Nothing granted below is a capability an injected query should inherit:
	// SOURCES (file/url/s3/remote -- local file reads and an outbound exfiltration
	// channel), INTROSPECTION (addressToLine/demangle), ACCESS MANAGEMENT, SYSTEM,
	// and every form of write or DDL. A release that granted them more broadly is
	// narrowed by revokeStaleGrants below.
	for _, stmt := range required {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			c.recordCapability(CapQueryIdentity, err)
			return nil, fmt.Errorf("%s: %w", user, err)
		}
	}
	if err := c.applyIdentitySettings(ctx, user, memCapBytes); err != nil {
		c.recordCapability(CapQueryIdentity, err)
		return nil, fmt.Errorf("%s: %w", user, err)
	}

	// Best effort: failing leaves the identity as broad as the release before, not
	// broken.
	for _, t := range privateSystemTables {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, fmt.Sprintf("REVOKE%s SELECT ON system.%s FROM %s", onCluster, t, user))
		cancel()
		if err != nil {
			log.Printf("[ClickHouse] %s: could not revoke system.%s: %v", user, t, err)
		}
	}

	// Optional, granted one privilege at a time. These widen what a search can
	// reach rather than making the identity work at all, and not every server
	// defines all of them. Bundled into one statement, a single unsupported
	// privilege would cost the whole identity and with it the class's memory
	// ceiling, which is a far worse outcome than losing one capability.
	optional := queryIdentityOptionalGrants(user)
	for _, o := range optional {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, fmt.Sprintf("GRANT%s %s ON *.* TO %s", onCluster, o.priv, user))
		cancel()
		if err != nil {
			log.Printf("[ClickHouse] %s: no %s privilege, %s is unavailable to this class: %v", user, o.priv, o.covers, err)
		}
	}
	c.recordCapability(CapQueryIdentity, nil)

	identityConn := ConnOptions{
		Addrs:    c.addrs,
		Database: c.Database,
		User:     user,
		Password: c.queryIdentityPassword(user),
		TLS:      c.tls,
		Pool:     c.identityPool(),
	}
	return openIdentityConn(ctx, identityConn)
}

// openIdentityConn connects as an identity and proves the credentials and grants work
// now rather than on the user's next search.
func openIdentityConn(ctx context.Context, opts ConnOptions) (driver.Conn, error) {
	conn, err := openClickHouseConn(opts)
	if err != nil {
		return nil, fmt.Errorf("connect as %s: %w", opts.User, err)
	}
	pctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
	defer cancel()
	if err := conn.Exec(pctx, "SELECT 1"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("probe as %s: %w", opts.User, err)
	}
	return conn, nil
}

// EnsurePinnedIdentity gives a client pinned to addr the alert and reader identities
// on that node, so the rule tests that run there read as them. The users already
// exist on every node, so there is nothing to provision. Like every class each falls
// back to the default connection when c has no identity or the node refuses it, and a
// later call picks the missing ones up in place, so a pin made during an outage does
// not stay on admin. A node that refuses is retried at the repair interval, not on
// every call.
func (c *ClickHouseClient) EnsurePinnedIdentity(ctx context.Context, pinned *ClickHouseClient, addr string) {
	pinned.identityMu.Lock()
	defer pinned.identityMu.Unlock()
	if time.Now().Before(pinned.pinRetryAfter) {
		return
	}
	next := map[string]*queryIdentity{}
	if m := pinned.queryConns.Load(); m != nil {
		for class, id := range *m {
			next[class] = id
		}
	}
	added := false
	for _, class := range pinnedIdentityClasses {
		if next[class] != nil || !c.hasQueryIdentity(class) {
			continue
		}
		user := queryIdentityUser(class)
		opts := c.nodeConnOptions(addr, adminNodePool)
		opts.User, opts.Password = user, c.queryIdentityPassword(user)
		conn, err := openIdentityConn(ctx, opts)
		if err != nil {
			log.Printf("[ClickHouse] Query identity %q unavailable on %s, its reads fall back to the default connection: %v", user, addr, err)
			pinned.pinRetryAfter = time.Now().Add(queryIdentityRepairInterval)
			continue
		}
		next[class] = &queryIdentity{conn: conn}
		added = true
	}
	if added {
		pinned.queryConns.Store(&next)
	}
}

// pinnedIdentityClasses are the classes a pinned client reads as: rule tests and the
// unmarked reads around them.
var pinnedIdentityClasses = []string{QueryAlertClass, QueryReaderClass}

// hasQueryIdentity reports whether class has a provisioned identity on c.
func (c *ClickHouseClient) hasQueryIdentity(class string) bool {
	m := c.queryConns.Load()
	return m != nil && (*m)[class] != nil
}

// privateSystemTables are revoked from every least-privilege identity. The query-text
// tables name other fractals' values and carry their scopes. system.zookeeper exposes
// Keeper itself, where a cluster's DDL queue holds each ON CLUSTER statement verbatim,
// including the passwords in CREATE/ALTER USER, and the replicated access store holds
// every user's password hash. No read on these identities needs any of them.
var privateSystemTables = []string{"query_log", "query_thread_log", "query_views_log", "processes", "text_log", "query_cache", "zookeeper"}

// identityPool sizes a class pool like the client's own, so BIFRACT_CH_QUERY_MAX_CONNS
// applies to it. A client wrapped around an existing conn has no sizing of its own.
func (c *ClickHouseClient) identityPool() ClickHousePoolConfig {
	if c.queryPool.MaxOpenConns > 0 {
		return c.queryPool
	}
	return DefaultQueryPoolConfig()
}

// StartQueryIdentityRepair retries missing identities in the background. Without it a
// class that lost its identity to a failure at boot stays on the default connection,
// with the admin's reach, until the next settings save or restart.
func (c *ClickHouseClient) StartQueryIdentityRepair(ctx context.Context) {
	go c.runIdentityRepair(ctx, queryIdentityRepairInterval)
}

func (c *ClickHouseClient) runIdentityRepair(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.repairQueryIdentities(ctx)
		}
	}
}

// repairQueryIdentities provisions any class without an identity and leaves live ones
// alone. A server that has permanently refused is not asked again until the next
// settings save or restart.
func (c *ClickHouseClient) repairQueryIdentities(ctx context.Context) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	if c.caps.state(CapQueryIdentity) == CapUnavailable {
		return
	}
	var live map[string]*queryIdentity
	if m := c.queryConns.Load(); m != nil {
		live = *m
	}
	caps := map[string]int64{}
	missing := false
	for _, class := range queryIdentityClasses {
		if id := live[class]; id != nil {
			caps[class] = id.memCapBytes
			continue
		}
		caps[class] = c.queryMemoryCap(class)
		missing = true
	}
	if missing {
		c.reconcileIdentitiesLocked(ctx, caps)
	}
}

// applyIdentitySettings sets the class's memory ceiling and pins what an injected
// SETTINGS clause could otherwise use to escape it. Constraints bind a query's own
// SETTINGS clause exactly as they bind the client, so the ceiling is CONST rather
// than a default. serialize_query_plan is pinned because a plan shipped to a shard
// bypasses the row policies there. A server that predates that setting rejects the
// whole statement, so it is retried without it.
func (c *ClickHouseClient) applyIdentitySettings(ctx context.Context, user string, memCapBytes int64) error {
	var base []string
	if memCapBytes > 0 {
		// Applied only when the class is capped. The identity exists regardless: it
		// is a privilege boundary first and a memory accounting unit second.
		base = append(base, fmt.Sprintf("max_memory_usage_for_user = %d CONST", memCapBytes))
	}
	attempts := [][]string{append(append([]string(nil), base...), "serialize_query_plan = 0 CONST"), base}
	var err error
	for _, settings := range attempts {
		clause := "NONE"
		if len(settings) > 0 {
			clause = strings.Join(settings, ", ")
		}
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err = c.conn.Exec(sctx, fmt.Sprintf("ALTER USER %s%s SETTINGS %s", user, c.OnClusterSQL(), clause))
		cancel()
		if err == nil || !isUnknownSetting(err) {
			return err
		}
	}
	return err
}

// isUnknownSetting reports ClickHouse error 115. Any other failure must not cost an
// identity its serialize_query_plan pin.
func isUnknownSetting(err error) bool {
	var ex *proto.Exception
	return errors.As(err, &ex) && ex.Code == 115
}

// revokeStaleGrants narrows an identity provisioned by an earlier, broader release.
//
// It runs AFTER the grants it is narrowing towards, and only when something stale is
// actually present, for two reasons. A fresh user has no grants at all, so an
// unconditional revoke logs a "no role in user directories" failure on every first
// startup -- a security-shaped error at the moment an operator is watching. And these
// identities are re-ensured on every settings save and every replica start, while
// their pools are serving live queries: a revoke that is not needed still opens a
// window in which the identity holds nothing and an in-flight query fails with "Not
// enough privileges".
//
// ClickHouse revokes hierarchically, so a privilege listed here is removed from the
// narrower grant too. That is why it names only the forms an older release used and
// this one does not, rather than REVOKE ALL.
func (c *ClickHouseClient) revokeStaleGrants(ctx context.Context, user string) {
	stale := c.staleGrantsFor(ctx, user)
	if len(stale) == 0 {
		return
	}
	for _, priv := range stale {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, fmt.Sprintf("REVOKE%s %s ON *.* FROM %s", c.OnClusterSQL(), priv, user))
		cancel()
		if err != nil {
			log.Printf("[ClickHouse] %s: could not narrow prior grant %s: %v", user, priv, err)
			continue
		}
		log.Printf("[ClickHouse] %s: narrowed prior grant %s ON *.*", user, priv)
	}
}

// staleGrantsFor reads what the identity currently holds and returns the instance-wide
// privileges this release does not grant. An unreadable grant list yields nothing to
// revoke: guessing would risk removing a privilege the identity is still using.
func (c *ClickHouseClient) staleGrantsFor(ctx context.Context, user string) []string {
	sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
	defer cancel()
	rows, err := c.conn.Query(sctx, fmt.Sprintf("SHOW GRANTS FOR %s", user))
	if err != nil {
		return nil
	}
	defer rows.Close()
	var held string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil
		}
		held += line + "\n"
	}
	if rows.Err() != nil {
		return nil
	}

	return staleInstanceWideGrants(held, user)
}

// staleInstanceWideGrants parses SHOW GRANTS output and returns the instance-wide
// privileges this release does not grant.
//
// The privilege list has to be parsed rather than substring-matched: ClickHouse
// coalesces grants, so the same privilege appears as "SELECT ON *.*" one moment and
// "SELECT, CREATE TEMPORARY TABLE ON *.*" the next, and a substring check silently
// misses the second form -- leaving the broad grant in place, which is the whole
// thing this is here to remove.
func staleInstanceWideGrants(showGrants, user string) []string {
	// Forms an earlier release granted instance-wide that no identity needs now.
	// URL was granted for "recall over URL-addressed archives", which no backend
	// uses; SELECT/dictGet ON *.* predate scoping them to the logs database.
	unwanted := map[string]bool{"SELECT": true, "dictGet": true, "URL": true}
	// Search reads Distributed tables, which need no REMOTE grant. REMOTE only adds
	// remote()/cluster(): an outbound channel and a way around the cluster's routing.
	if user == SearchCHUser {
		unwanted["REMOTE"] = true
	}

	var stale []string
	seen := map[string]bool{}
	for _, line := range strings.Split(showGrants, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "GRANT ") {
			continue
		}
		idx := strings.Index(line, " ON *.*")
		if idx < 0 {
			continue // scoped to a database: not what this removes
		}
		for _, priv := range strings.Split(line[len("GRANT "):idx], ",") {
			priv = strings.TrimSpace(priv)
			if unwanted[priv] && !seen[priv] {
				seen[priv] = true
				stale = append(stale, priv)
			}
		}
	}
	return stale
}

// queryIdentityOptionalGrants is what a class may additionally reach. Per class,
// because an object-store grant is also the one outbound channel an injected query
// could exfiltrate through: only recall reads archives, so only recall gets S3/AZURE,
// and only recall keeps REMOTE (see staleInstanceWideGrants). URL is granted to no
// class, since no archive backend addresses data by URL (see objstore.Backend) and it
// would be reach with no use.
func queryIdentityOptionalGrants(user string) []struct{ priv, covers string } {
	optional := []struct{ priv, covers string }{
		{"CREATE TEMPORARY TABLE", "GLOBAL IN"},
	}
	if user == RecallCHUser {
		optional = append(optional,
			struct{ priv, covers string }{"REMOTE", "cross-shard fan-out"},
			struct{ priv, covers string }{"S3", "recall over S3-backed archives"},
			struct{ priv, covers string }{"AZURE", "recall over Azure-backed archives"})
	}
	return optional
}

// readConn resolves the connection a read on ctx runs on and applies its query id,
// carrying the fractal scope when ctx has one. Every read helper goes through here,
// so no path can run a scoped query without the scope reaching ClickHouse.
func (c *ClickHouseClient) readConn(ctx context.Context) (driver.Conn, context.Context, error) {
	id, err := scopedQueryID(ctx)
	if err != nil {
		return nil, ctx, err
	}
	if id != "" {
		ctx = clickhouse.Context(context.WithValue(ctx, sentQueryIDKey{}, id), clickhouse.WithQueryID(id))
	}
	return c.connFor(ctx), ctx, nil
}

// rawReadConn is readConn for helpers that hand the driver's rows to the caller, which
// have no settings of their own to merge with what ctx implies.
func (c *ClickHouseClient) rawReadConn(ctx context.Context) (driver.Conn, context.Context, error) {
	if s := c.applyQuerySettings(ctx, nil); s != nil {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(s))
	}
	return c.readConn(ctx)
}

// refusedRow is the driver.Row of a read readConn refused, since QueryRow has no error
// return of its own.
type refusedRow struct{ err error }

func (r refusedRow) Err() error           { return r.err }
func (r refusedRow) Scan(...any) error    { return r.err }
func (r refusedRow) ScanStruct(any) error { return r.err }

// ShareQueryIdentities makes c's reads run on src's identities, workloads and
// ceilings, as if src had issued them. For a second client on the same servers, such
// as the archive client, that would otherwise hold no identities and run every read,
// user recall SQL included, as admin. c keeps its own connection for writes and admin
// reads, and never closes src's pools.
func (c *ClickHouseClient) ShareQueryIdentities(src *ClickHouseClient) {
	c.identitySrc = src
}

// identityState is the client whose identities and workload state c's reads use.
func (c *ClickHouseClient) identityState() *ClickHouseClient {
	if c.identitySrc != nil {
		return c.identitySrc
	}
	return c
}

// connFor returns the connection a read on ctx must run on: the class identity when
// ctx is marked, the reader identity when it is not, and the default connection for
// an admin read or when the identity is not provisioned. A class wins over the admin
// marker, so a marker that leaks into user SQL's context gains it nothing. Writes
// never come through here.
func (c *ClickHouseClient) connFor(ctx context.Context) driver.Conn {
	class := QueryClassOf(ctx)
	if class == "" {
		if IsAdminRead(ctx) {
			return c.conn
		}
		class = QueryReaderClass
	}
	m := c.identityState().queryConns.Load()
	if m == nil {
		return c.conn
	}
	if id := (*m)[class]; id != nil {
		return id.conn
	}
	return c.conn
}

// closeQueryIdentities closes the class pools. Called from Close.
func (c *ClickHouseClient) closeQueryIdentities() {
	// Under identityMu, so an EnsurePinnedIdentity racing Close cannot store pools
	// after they were closed.
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	if m := c.queryConns.Swap(nil); m != nil {
		for _, id := range *m {
			if id != nil {
				id.conn.Close()
			}
		}
	}
}
