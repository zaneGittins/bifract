package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
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
	return hex.EncodeToString(mac.Sum(nil))
}

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
		limit, capped := memCaps[workload]
		if !capped || limit <= 0 {
			continue
		}
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
		log.Printf("[ClickHouse] Query identity %q ensured (max_memory_usage_for_user = %d)", user, limit)
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
	onCluster := ""
	if c.IsCluster() {
		onCluster = " ON CLUSTER '" + EscCHStr(c.Cluster) + "'"
	}
	pw := EscCHStr(c.queryIdentityPassword(user))

	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s%s IDENTIFIED BY '%s'", user, onCluster, pw),
		fmt.Sprintf("ALTER USER %s%s IDENTIFIED BY '%s'", user, onCluster, pw),
		// SELECT and dictGet cover log reads and model_lookup enrichment; REMOTE and
		// CREATE TEMPORARY TABLE cover a cluster's fan-out and GLOBAL IN; S3, AZURE and
		// URL cover recall's Iceberg table functions.
		fmt.Sprintf("GRANT%s SELECT, dictGet ON *.* TO %s", onCluster, user),
		fmt.Sprintf("GRANT%s REMOTE, CREATE TEMPORARY TABLE, S3, AZURE, URL ON *.* TO %s", onCluster, user),
		// The whole point of the identity.
		fmt.Sprintf("ALTER USER %s%s SETTINGS max_memory_usage_for_user = %d", user, onCluster, memCapBytes),
	}
	for _, stmt := range stmts {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", user, err)
		}
	}

	conn, err := openClickHouseConn(c.addrs, c.Database, user, c.queryIdentityPassword(user), DefaultQueryPoolConfig())
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
