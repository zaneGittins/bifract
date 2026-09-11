package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
// Inserts, merges, mutations, materialized views and alert evaluation are outside this
// boundary by construction: they never carry a workload marker, so they run on the
// default connection and can never be throttled by a query class's ceiling. The
// identities are granted read-only, so a write mistakenly issued on one fails loudly
// instead of silently drawing on a search budget.
const (
	// SearchCHUser and RecallCHUser deliberately match their workload names: the
	// workload schedules the class's CPU, the user accounts for its memory. ClickHouse
	// keeps users and workloads in separate namespaces, so the shared name is only a
	// label.
	SearchCHUser = "bifract_search"
	RecallCHUser = "bifract_recall"

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

// queryIdentityUser maps a workload class to the ClickHouse user that runs it.
func queryIdentityUser(workload string) string {
	switch workload {
	case QuerySearchWorkload:
		return SearchCHUser
	case QueryRecallWorkload:
		return RecallCHUser
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

// reconcileQueryIdentities provisions one ClickHouse user per capped class and opens a
// pool for it. Classes absent from memCaps are uncapped and keep using the default
// connection, so their pool is closed.
//
// An identity whose ceiling has not changed is reused rather than rebuilt. Reconcile
// runs on every settings save, and closing a live pool would abort whatever searches
// were running on it.
//
// Never fatal. A deployment whose privileged user lacks access management cannot create
// users at all; that class then falls back to the default connection, where the
// per-query ceiling from applyQuerySettings still applies. The result is the previous
// behaviour, not a broken search.
func (c *ClickHouseClient) reconcileQueryIdentities(ctx context.Context, memCaps map[string]int64) {
	var current map[string]*queryIdentity
	if m := c.queryConns.Load(); m != nil {
		current = *m
	}

	next := map[string]*queryIdentity{}
	for _, workload := range []string{QuerySearchWorkload, QueryRecallWorkload} {
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
			log.Printf("[ClickHouse] Query identity %q unavailable, %s falls back to a per-query ceiling only: %v", user, workload, err)
			continue
		}
		next[workload] = &queryIdentity{conn: conn, memCapBytes: limit}
		if limit > 0 {
			log.Printf("[ClickHouse] Query identity %q ensured (read-only on %s, max_memory_usage_for_user = %d)", user, c.logsDatabase(), limit)
		} else {
			log.Printf("[ClickHouse] Query identity %q ensured (read-only on %s, uncapped)", user, c.logsDatabase())
		}
	}

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
// connected as it. The grants are read-shaped and deliberately broad: this identity
// exists to account for memory, not to restrict what a search may read, and a missing
// grant would surface as a query failure for the user.
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
	if memCapBytes > 0 {
		// Applied only when the class is capped. The identity exists regardless: it
		// is a privilege boundary first and a memory accounting unit second.
		required = append(required,
			fmt.Sprintf("ALTER USER %s%s SETTINGS max_memory_usage_for_user = %d", user, onCluster, memCapBytes))
	} else {
		required = append(required,
			fmt.Sprintf("ALTER USER %s%s SETTINGS NONE", user, onCluster))
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

	// Optional, granted one privilege at a time. These widen what a search can
	// reach rather than making the identity work at all, and not every server
	// defines all of them. Bundled into one statement, a single unsupported
	// privilege would cost the whole identity and with it the class's memory
	// ceiling, which is a far worse outcome than losing one capability.
	//
	// Per class, because an object-store grant is also the one outbound channel an
	// injected query could exfiltrate through. Only recall reads archives, so only
	// recall gets S3/AZURE. URL is granted to neither: no archive backend addresses
	// data by URL (see objstore.Backend), so it would be reach with no use.
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
		Pool:     DefaultQueryPoolConfig(),
	}
	conn, err := openClickHouseConn(identityConn)
	if err != nil {
		return nil, fmt.Errorf("connect as %s: %w", user, err)
	}
	// Prove the credentials and grants work now rather than on the user's next search.
	pctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
	defer cancel()
	if err := conn.Exec(pctx, "SELECT 1"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("probe as %s: %w", user, err)
	}
	return conn, nil
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

	return staleInstanceWideGrants(held)
}

// staleInstanceWideGrants parses SHOW GRANTS output and returns the instance-wide
// privileges this release does not grant.
//
// The privilege list has to be parsed rather than substring-matched: ClickHouse
// coalesces grants, so the same privilege appears as "SELECT ON *.*" one moment and
// "SELECT, CREATE TEMPORARY TABLE ON *.*" the next, and a substring check silently
// misses the second form -- leaving the broad grant in place, which is the whole
// thing this is here to remove.
func staleInstanceWideGrants(showGrants string) []string {
	// Forms an earlier release granted instance-wide that no identity needs now.
	// URL was granted for "recall over URL-addressed archives", which no backend
	// uses; SELECT/dictGet ON *.* predate scoping them to the logs database.
	unwanted := map[string]bool{"SELECT": true, "dictGet": true, "URL": true}

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
// could exfiltrate through: only recall reads archives, so only recall gets S3/AZURE.
// URL is granted to neither, since no archive backend addresses data by URL
// (see objstore.Backend) and it would be reach with no use.
func queryIdentityOptionalGrants(user string) []struct{ priv, covers string } {
	optional := []struct{ priv, covers string }{
		{"REMOTE", "cross-shard fan-out"},
		{"CREATE TEMPORARY TABLE", "GLOBAL IN"},
	}
	if user == RecallCHUser {
		optional = append(optional,
			struct{ priv, covers string }{"S3", "recall over S3-backed archives"},
			struct{ priv, covers string }{"AZURE", "recall over Azure-backed archives"})
	}
	return optional
}

// connFor returns the connection a query on ctx must run on: the class identity when
// ctx is marked and that identity is provisioned, otherwise the default connection.
// Untagged work -- inserts, merges, alert evaluation, schema reconciliation -- always
// lands on the default connection and is never subject to a class's memory ceiling.
func (c *ClickHouseClient) connFor(ctx context.Context) driver.Conn {
	workload := contextWorkload(ctx)
	if workload == "" {
		return c.conn
	}
	m := c.queryConns.Load()
	if m == nil {
		return c.conn
	}
	if id := (*m)[workload]; id != nil {
		return id.conn
	}
	return c.conn
}

// closeQueryIdentities closes the class pools. Called from Close.
func (c *ClickHouseClient) closeQueryIdentities() {
	if m := c.queryConns.Swap(nil); m != nil {
		for _, id := range *m {
			if id != nil {
				id.conn.Close()
			}
		}
	}
}
