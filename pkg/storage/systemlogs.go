package storage

import (
	"context"
	"fmt"
	"log"
	"regexp"
)

// orphanedSystemLogRe matches the tables ClickHouse leaves behind when a system
// log's schema changes across a server version: it renames system.text_log to
// system.text_log_0 and creates a fresh one. Nothing ever reads the renamed copy
// again, and it carries no TTL, so each upgrade strands another one on disk.
var orphanedSystemLogRe = regexp.MustCompile(`^[a-z_]+_log_[0-9]+$`)

// DropOrphanedSystemLogTables removes system.*_log_N tables stranded by past
// ClickHouse version upgrades.
//
// This is what makes the system-log TTL config actually reclaim space. ClickHouse
// applies a newly configured TTL by renaming the populated table aside and starting
// a fresh one, so on an install that has already grown a large system.text_log the
// config alone moves the bloat rather than bounding it (verified on 26.6.2.81).
// Observed: 28 GiB stranded across 17 such tables on a five-month install.
//
// Best-effort and idempotent: a failure here is logged and retried next start
// rather than blocking the app.
func (c *ClickHouseClient) DropOrphanedSystemLogTables(ctx context.Context) error {
	// Names are read from every node: system tables are node-local, so a
	// load-balanced read describes only whichever node the driver happened to pick.
	// The DROP is issued ON CLUSTER, which reaches all of them.
	rows, err := c.conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT name FROM %s WHERE database = 'system' AND match(name, '_log_[0-9]+$')`,
		c.topo.FanoutSystemTable("system.tables")))
	if err != nil {
		return fmt.Errorf("list orphaned system log tables: %w", err)
	}

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scan orphaned system log table: %w", err)
		}
		// The regex above anchors both ends; the SQL match() is only a pre-filter.
		// Never interpolate a name into DDL without re-checking it here.
		if orphanedSystemLogRe.MatchString(name) {
			names = append(names, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate orphaned system log tables: %w", err)
	}
	if len(names) == 0 {
		return nil
	}

	// max_table_size_to_drop otherwise refuses anything over 50GB, which a stranded
	// text_log routinely exceeds.
	var dropped int
	for _, name := range names {
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS system.`%s`%s SYNC SETTINGS max_table_size_to_drop = 0",
			name, c.OnClusterSQL())
		if err := c.conn.Exec(ctx, stmt); err != nil {
			log.Printf("Warning: could not drop stranded system log table %s (will retry next start): %v", name, err)
			continue
		}
		dropped++
	}
	if dropped > 0 {
		log.Printf("Dropped %d system log table(s) stranded by past ClickHouse upgrades", dropped)
	}
	return nil
}
