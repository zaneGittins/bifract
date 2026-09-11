package storage

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SchemaCHUser is the ClickHouse identity that builds user-defined schema: the
// dictionary and analytics-model objects whose DDL is generated from stored
// configuration rather than from a constant.
//
// That generation is the reason it exists. A dictionary column name comes from an
// ingested log field, and a model's extraction fields come from an API request, so
// the statement text is partly caller-supplied. Validation is the control that keeps
// it well-formed; this identity is what bounds the damage when validation is wrong,
// which it has been. It holds read, write and DDL on the logs database and nothing
// else -- notably not SOURCES (file/url/s3/remote: local file reads and an outbound
// exfiltration channel), INTROSPECTION, ACCESS MANAGEMENT, SYSTEM SHUTDOWN, or any
// access to another database.
//
// It is deliberately NOT used for migrations, the logs table itself, or any statement
// built entirely from constants: those run as the privileged user, where a narrower
// identity would buy nothing and a missing grant would block startup.
const SchemaCHUser = "bifract_schema"

// schemaIdentityGrants is what generating dictionary and model objects needs, scoped
// to the logs database. Curated rather than "ALL": the point is the privileges that
// are absent.
func schemaIdentityGrants(onCluster, db, user string) []string {
	return []string{
		// Dictionary rows are read back and written through this identity.
		fmt.Sprintf("GRANT%s SELECT, INSERT, dictGet ON %s.* TO %s", onCluster, db, user),
		// Backing tables, dictionary objects, and model materialized views.
		fmt.Sprintf("GRANT%s CREATE TABLE, CREATE VIEW, CREATE DICTIONARY ON %s.* TO %s", onCluster, db, user),
		fmt.Sprintf("GRANT%s DROP TABLE, DROP VIEW, DROP DICTIONARY ON %s.* TO %s", onCluster, db, user),
		// Column, index and TTL changes as a dictionary's schema follows its data.
		fmt.Sprintf("GRANT%s ALTER ON %s.* TO %s", onCluster, db, user),
		fmt.Sprintf("GRANT%s TRUNCATE, OPTIMIZE ON %s.* TO %s", onCluster, db, user),
	}
}

// schemaIdentityOptionalGrants widen what the identity can do without being required
// for it to work. Granted one at a time so an unsupported privilege costs only itself.
func schemaIdentityOptionalGrants(onCluster, user string) []struct{ priv, covers string } {
	return []struct{ priv, covers string }{
		// A dictionary is reloaded after its rows change.
		{"SYSTEM RELOAD DICTIONARY", "refreshing a dictionary after a write"},
		// Materialized views are created SQL SECURITY DEFINER so the ingest identity
		// needs no read on logs; see ReconcileMaterializedViewSecurity.
		{"SET DEFINER", "creating materialized views that run as their definer"},
		// Cluster deployments read their own topology before issuing ON CLUSTER DDL.
		{"REMOTE", "cluster-aware object creation"},
	}
}

// schemaConn holds the provisioned schema pool, or nil when the identity could not be
// created. Read on every schema statement, so an atomic rather than a mutex.
type schemaIdentity struct {
	conn atomic.Pointer[driver.Conn]
}

// EnsureSchemaIdentity creates or updates the schema identity and opens its pool.
// Never fatal: a deployment whose privileged user cannot manage access keeps running
// schema DDL as that user, which is exactly the behaviour that existed before this.
func (c *ClickHouseClient) EnsureSchemaIdentity(ctx context.Context) {
	onCluster := c.OnClusterSQL()
	db := "`" + EscCHIdent(c.logsDatabase()) + "`"
	pw := c.queryIdentityPassword(SchemaCHUser)

	required := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s%s IDENTIFIED BY '%s'", SchemaCHUser, onCluster, EscCHStr(pw)),
		fmt.Sprintf("ALTER USER %s%s IDENTIFIED BY '%s'", SchemaCHUser, onCluster, EscCHStr(pw)),
	}
	// Create the user, then narrow any prior broad grants, then grant. The revoke
	// precedes the grants because ClickHouse revokes hierarchically; see
	// revokeStaleGrants.
	for _, stmt := range required {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			c.recordCapability(CapSchemaIdentity, err)
			log.Printf("[ClickHouse] Schema identity %q unavailable, generated schema DDL runs as %q: %v", SchemaCHUser, c.User, err)
			return
		}
	}
	c.revokeStaleGrants(ctx, SchemaCHUser)

	for _, stmt := range schemaIdentityGrants(onCluster, db, SchemaCHUser) {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			c.recordCapability(CapSchemaIdentity, err)
			log.Printf("[ClickHouse] Schema identity %q unavailable, generated schema DDL runs as %q: %v", SchemaCHUser, c.User, err)
			return
		}
	}
	for _, o := range schemaIdentityOptionalGrants(onCluster, SchemaCHUser) {
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, fmt.Sprintf("GRANT%s %s ON *.* TO %s", onCluster, o.priv, SchemaCHUser))
		cancel()
		if err != nil {
			log.Printf("[ClickHouse] %s: no %s privilege, %s falls back to the privileged user: %v", SchemaCHUser, o.priv, o.covers, err)
		}
	}

	conn, err := openClickHouseConn(ConnOptions{
		Addrs:    c.addrs,
		Database: c.Database,
		User:     SchemaCHUser,
		Password: pw,
		TLS:      c.tls,
		Pool:     DefaultQueryPoolConfig(),
	})
	if err != nil {
		c.recordCapability(CapSchemaIdentity, err)
		log.Printf("[ClickHouse] Schema identity %q could not connect, generated schema DDL runs as %q: %v", SchemaCHUser, c.User, err)
		return
	}
	// Prove it now rather than on the next dictionary edit.
	pctx, pcancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
	defer pcancel()
	if err := conn.Exec(pctx, "SELECT 1"); err != nil {
		conn.Close()
		c.recordCapability(CapSchemaIdentity, err)
		log.Printf("[ClickHouse] Schema identity %q failed its probe, generated schema DDL runs as %q: %v", SchemaCHUser, c.User, err)
		return
	}
	if old := c.schema.conn.Swap(&conn); old != nil {
		(*old).Close()
	}
	c.recordCapability(CapSchemaIdentity, nil)
	log.Printf("[ClickHouse] Schema identity %q ensured (read/write/DDL on %s only)", SchemaCHUser, c.logsDatabase())
}

// schemaConn returns the schema identity's pool, or the privileged connection when it
// is not provisioned. Same fallback shape as connFor: a deployment that cannot create
// the identity keeps working.
func (c *ClickHouseClient) schemaConn() driver.Conn {
	if p := c.schema.conn.Load(); p != nil {
		return *p
	}
	return c.conn
}

// ExecSchema runs a statement that builds user-defined schema (dictionary and model
// objects) as the schema identity. Use it wherever the statement text is generated
// from stored configuration rather than written as a constant; see SchemaCHUser.
func (c *ClickHouseClient) ExecSchema(ctx context.Context, query string) error {
	if err := c.schemaConn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// ExecSchemaArgs is ExecSchema with bound parameters, for the batched row insert a
// dictionary edit makes.
func (c *ClickHouseClient) ExecSchemaArgs(ctx context.Context, query string, args ...interface{}) error {
	if err := c.schemaConn().Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// QuerySchema is Query for the schema identity, for reads that belong to the same
// generated-object boundary (dictionary rows, row counts).
func (c *ClickHouseClient) QuerySchema(ctx context.Context, query string) ([]map[string]interface{}, error) {
	rows, err := c.schemaConn().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	return collectRowMaps(rows)
}

// closeSchemaIdentity closes the schema pool. Called from Close.
func (c *ClickHouseClient) closeSchemaIdentity() {
	if p := c.schema.conn.Swap(nil); p != nil {
		(*p).Close()
	}
}
