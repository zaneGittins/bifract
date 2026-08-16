package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// endpointAnalysisMVNames are the materialized views gated by the "Advanced endpoint
// analysis" toggle: the process tree graph (ptg) and provenance graph (pgr) MVs. Each
// fires synchronously on every insert into logs and runs heavy regex/aggregation work.
//
// Their definitions remain the source of truth in db/init-clickhouse.sql and the
// numbered migrations; this toggle only ATTACHes/DETACHes the insert-time triggers.
// DETACH ... PERMANENTLY keeps an MV detached across restarts while preserving its
// definition AND its target table's accumulated data, so turning the feature off then
// back on is an instant ATTACH with no rebuild and no data loss.
var endpointAnalysisMVNames = []string{
	"proc_lineage_mv",
	"proc_freq_spawn_mv",
	"proc_freq_file_mv",
	"proc_freq_net_mv",
	"proc_freq_dns_mv",
	"proc_freq_rthread_mv",
	"proc_freq_pacc_mv",
	"process_edges_mv",
}

// endpointMVInList renders endpointAnalysisMVNames as a SQL IN-list literal, so the
// attached/detached state queries derive from the single source of truth above and can
// never drift from it (a drift previously left the EID 8/10 MVs ungated).
func endpointMVInList() string {
	quoted := make([]string, len(endpointAnalysisMVNames))
	for i, n := range endpointAnalysisMVNames {
		quoted[i] = "'" + n + "'"
	}
	return strings.Join(quoted, ", ")
}

// AdvancedEndpointAnalysisSetting is the Postgres settings key backing the toggle.
const AdvancedEndpointAnalysisSetting = "advanced_endpoint_analysis"

// ReconcileEndpointAnalysisMVs attaches (enabled) or permanently detaches (disabled)
// the endpoint-analysis MVs to match the setting. A detached MV does not receive
// inserts, which removes its per-insert cost without dropping anything. Idempotent:
// safe to run at every startup and on each toggle.
//
// Cluster note: each node carries its own MV trigger, so this reconciles every node
// directly (mirroring the migration sync path, resilient to a peer mid-restart). A
// cluster ATTACH/DETACH is not atomic across nodes, so a toggle may briefly leave the
// feature active on some nodes and not others; it converges once this completes.
func (c *ClickHouseClient) ReconcileEndpointAnalysisMVs(ctx context.Context, enabled bool) error {
	if !c.topo.PerNodeAdmin {
		if err := reconcileEndpointMVsOnConn(ctx, c.conn, enabled); err != nil {
			return err
		}
		log.Printf("[ClickHouse] Advanced endpoint analysis MVs reconciled (enabled=%v)", enabled)
		return nil
	}

	var firstErr error
	for _, addr := range c.addrs {
		hostConn, err := openClickHouseConn(c.nodeConnOptions(addr, adminNodePool))
		if err != nil {
			log.Printf("Warning: endpoint analysis sync to %s failed: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := reconcileEndpointMVsOnConn(ctx, hostConn, enabled); err != nil {
			log.Printf("Warning: endpoint analysis reconcile on %s: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
		}
		hostConn.Close()
	}
	if firstErr == nil {
		log.Printf("[ClickHouse] Advanced endpoint analysis MVs reconciled on all shards (enabled=%v)", enabled)
	}
	return firstErr
}

// reconcileEndpointMVsOnConn drives one node's gated MVs to the desired state. It reads
// which of the MVs are currently attached (system.tables) and which exist as detached
// metadata (system.detached_tables), so ATTACH/DETACH only run when a change is needed
// AND only against MVs that actually exist. In particular, ATTACH is issued solely for
// an MV present in system.detached_tables: an MV that was never created (e.g. a cluster
// where the schema migration was skipped because a shard was down) is left alone and
// gets created attached by a later startup once migrations run, rather than erroring on
// ATTACH of a non-existent table.
func reconcileEndpointMVsOnConn(ctx context.Context, conn driver.Conn, enabled bool) error {
	inList := endpointMVInList()
	attached, err := loadEndpointMVSet(ctx, conn, `SELECT name FROM system.tables
		WHERE database = currentDatabase() AND name IN (`+inList+`)`)
	if err != nil {
		return fmt.Errorf("check endpoint MV state: %w", err)
	}
	detached, err := loadEndpointMVSet(ctx, conn, `SELECT table FROM system.detached_tables
		WHERE database = currentDatabase() AND table IN (`+inList+`)`)
	if err != nil {
		return fmt.Errorf("check detached endpoint MV state: %w", err)
	}

	for _, name := range endpointAnalysisMVNames {
		var stmt string
		switch {
		case enabled && !attached[name] && detached[name]:
			// Re-attach a previously permanently-detached trigger; its definition and
			// target-table data are intact, so this is an instant metadata operation.
			stmt = "ATTACH TABLE " + name
		case !enabled && attached[name]:
			stmt = "DETACH TABLE " + name + " PERMANENTLY"
		default:
			// Already in the desired state, or (for ATTACH) not yet created on this node.
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		execErr := conn.Exec(sctx, stmt)
		cancel()
		if execErr != nil {
			return fmt.Errorf("%s: %w", stmt, execErr)
		}
	}
	return nil
}

// loadEndpointMVSet runs a single-string-column query and returns the values as a set.
func loadEndpointMVSet(ctx context.Context, conn driver.Conn, query string) (map[string]bool, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		set[name] = true
	}
	return set, rows.Err()
}
