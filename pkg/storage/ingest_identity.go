package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IngestPGRole is the least-privilege Postgres role the ingest tier connects as. It is
// granted only the tables the ingest data path touches -- notably NOT users/sessions/
// api_keys -- so a compromised ingest tier cannot read auth material or other fractals'
// sensitive metadata. Grants are a curated allowlist; new ingest-path tables need adding.
const IngestPGRole = "bifract_ingest"

// ingestPGGrants is the curated grant set (see EnsureIngestRole). SELECT-only except
// where the ingest path writes: token usage counters, settings (spool status upsert),
// health notifications, and the monitors' system_metrics samples.
var ingestPGGrants = []string{
	"GRANT USAGE ON SCHEMA public TO " + IngestPGRole,
	"GRANT SELECT ON ingest_tokens, normalizers, fractals, settings, health_notifications, system_metrics TO " + IngestPGRole,
	"GRANT INSERT ON settings, health_notifications, system_metrics TO " + IngestPGRole,
	"GRANT UPDATE ON ingest_tokens, settings TO " + IngestPGRole,
}

// EnsureIngestRole creates/updates the least-privilege Postgres ingest role and its
// grants. Idempotent; syncs the password each call so a rotated secret takes effect.
// No-op when password is empty (the ingest tier then falls back to the app DB user).
func (c *PostgresClient) EnsureIngestRole(ctx context.Context, password string) error {
	if password == "" {
		log.Printf("[Postgres] WARNING: BIFRACT_INGEST_POSTGRES_PASSWORD is empty; skipping %q provisioning. The ingest tier cannot authenticate until it is set.", IngestPGRole)
		return nil
	}
	pw := strings.ReplaceAll(password, "'", "''")
	// CREATE ROLE has no IF NOT EXISTS; guard with a DO block, then sync password.
	create := fmt.Sprintf(`DO $do$ BEGIN
		IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%s') THEN
			CREATE ROLE %s LOGIN PASSWORD '%s';
		END IF;
	END $do$;`, IngestPGRole, IngestPGRole, pw)
	if _, err := c.Exec(ctx, create); err != nil {
		return fmt.Errorf("ensure ingest role: %w", err)
	}
	if _, err := c.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", IngestPGRole, pw)); err != nil {
		return fmt.Errorf("ensure ingest role password: %w", err)
	}
	for _, g := range ingestPGGrants {
		if _, err := c.Exec(ctx, g); err != nil {
			return fmt.Errorf("ensure ingest role grants: %w", err)
		}
	}
	log.Printf("[Postgres] Least-privilege ingest role %q ensured", IngestPGRole)
	return nil
}

// IngestCHUser is the least-privilege ClickHouse user the ingest tier connects as.
// It can INSERT into logs (and MV targets) and read system tables (for backpressure),
// but cannot SELECT log data -- so a compromised ingest tier cannot exfiltrate logs.
const IngestCHUser = "bifract_ingest"

// ReconcileMaterializedViewSecurity makes every materialized view on the logs database
// run with DEFINER privileges (SQL SECURITY DEFINER, DEFINER = default). A legacy MV
// (created without a security clause) executes with the INSERTING user's privileges, so
// pushing an insert through it requires SELECT on the source log columns -- which would
// force the restricted ingest user to have read access to logs. With DEFINER the MV runs
// as the privileged definer, so the ingest user needs only INSERT. Idempotent: converts
// legacy MVs on existing installs at startup and backstops any MV created without the
// clause. Runs before the ingest tier begins inserting.
//
// Cluster: an MV is a per-node trigger, so this reconciles every node directly (mirrors
// the migration/endpoint-analysis sync paths), unlike ON CLUSTER which can stall on a
// restarting peer.
func (c *ClickHouseClient) ReconcileMaterializedViewSecurity(ctx context.Context) error {
	if !c.IsCluster() {
		n, err := reconcileMVSecurityOnConn(ctx, c.conn)
		if err != nil {
			return err
		}
		if n > 0 {
			log.Printf("[ClickHouse] Converted %d materialized view(s) to SQL SECURITY DEFINER", n)
		}
		return nil
	}

	initPool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
	var firstErr error
	for _, addr := range c.addrs {
		hostConn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, initPool)
		if err != nil {
			log.Printf("Warning: MV security sync to %s failed: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if n, err := reconcileMVSecurityOnConn(ctx, hostConn); err != nil {
			log.Printf("Warning: MV security reconcile on %s: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
		} else if n > 0 {
			log.Printf("[ClickHouse] Converted %d materialized view(s) to DEFINER on shard %s", n, addr)
		}
		hostConn.Close()
	}
	return firstErr
}

// reconcileMVSecurityOnConn converts every non-DEFINER MV on one node. Returns the count
// converted.
func reconcileMVSecurityOnConn(ctx context.Context, conn driver.Conn) (int, error) {
	rows, err := conn.Query(ctx, `SELECT name FROM system.tables
		WHERE database = currentDatabase()
		  AND engine = 'MaterializedView'
		  AND create_table_query NOT LIKE '%SQL SECURITY DEFINER%'`)
	if err != nil {
		return 0, fmt.Errorf("list materialized views: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return 0, err
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// Convert every view even if one fails. Returning on the first error would leave the
	// remaining views on INVOKER, and a single missed view is enough to fail every insert
	// with code 497 and drive the retry-duplication path.
	converted := 0
	var firstErr error
	for _, name := range names {
		stmt := fmt.Sprintf("ALTER TABLE `%s` MODIFY SQL SECURITY DEFINER DEFINER = default", name)
		sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		execErr := conn.Exec(sctx, stmt)
		cancel()
		if execErr != nil {
			log.Printf("Warning: could not convert materialized view %q to DEFINER: %v", name, execErr)
			if firstErr == nil {
				firstErr = fmt.Errorf("alter %s security: %w", name, execErr)
			}
			continue
		}
		converted++
	}
	return converted, firstErr
}

// EnsureIngestUser creates/updates the least-privilege ClickHouse ingest user and its
// grants (INSERT on logs, SELECT on system for backpressure/readiness). Idempotent;
// syncs the password each call so a rotated secret takes effect. No-op when password is
// empty (feature not provisioned -- the ingest tier then falls back to the default user).
// Requires the connecting (default) user to have access management enabled
// (CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 in docker; access_management on the k8s CHI).
func (c *ClickHouseClient) EnsureIngestUser(ctx context.Context, password string) error {
	if password == "" {
		log.Printf("[ClickHouse] WARNING: BIFRACT_INGEST_CLICKHOUSE_PASSWORD is empty; skipping %q provisioning. The ingest tier cannot authenticate until it is set.", IngestCHUser)
		return nil
	}
	onCluster := ""
	grantOnCluster := ""
	if c.IsCluster() {
		onCluster = " ON CLUSTER '" + EscCHStr(c.Cluster) + "'"
		// The ON CLUSTER clause sits right after the GRANT keyword, not after the name.
		grantOnCluster = " ON CLUSTER '" + EscCHStr(c.Cluster) + "'"
	}
	pw := EscCHStr(password)
	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s%s IDENTIFIED BY '%s'", IngestCHUser, onCluster, pw),
		fmt.Sprintf("ALTER USER %s%s IDENTIFIED BY '%s'", IngestCHUser, onCluster, pw),
		fmt.Sprintf("GRANT%s INSERT ON logs.* TO %s", grantOnCluster, IngestCHUser),
		fmt.Sprintf("GRANT%s SELECT ON system.* TO %s", grantOnCluster, IngestCHUser),
	}
	for _, stmt := range stmts {
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := c.conn.Exec(sctx, stmt)
		cancel()
		if err != nil {
			return fmt.Errorf("ensure ingest user: %w", err)
		}
	}
	log.Printf("[ClickHouse] Least-privilege ingest user %q ensured (INSERT on logs, SELECT on system)", IngestCHUser)
	return nil
}
