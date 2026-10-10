package storage

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	dbsql "bifract/db"
)

// repartitionTarget is a table whose partition key gained fractal_id, so a fractal's rows
// leave with a partition drop instead of a mutation. init-clickhouse.sql carries the key
// for new installs; ConvertPartitioning rebuilds tables provisioned before the change.
type repartitionTarget struct {
	table string
	key   string // partition_key as system.tables reports it
}

var repartitionTargets = []repartitionTarget{
	{table: "logs_histogram", key: "(fractal_id, ingest_day)"},
	{table: "proc_lineage", key: "fractal_id"},
	{table: "proc_freq", key: "fractal_id"},
	{table: "process_edges", key: "fractal_id"},
}

// Working tables a conversion keeps while in progress: old holds the rows from before the
// swap, stage receives one fractal's copy at a time, done records fractals whose copy
// completed, and skip records fractals cleared while the conversion ran.
const (
	repartOld   = "__old"
	repartStage = "__stage"
	repartDone  = "__done"
	repartSkip  = "__skip"
)

// repartitionProbeFractal is the fractal id of the row the preflight moves between the
// new tables, chosen so it cannot collide with a real (UUID) fractal.
const repartitionProbeFractal = "__bifract_repartition_probe__"

// repartitionCopySettings throttle each fractal's copy so a conversion running beside
// live ingest and queries cannot monopolise a node. Deduplication is off because a copy
// interrupted part way is dropped and reinserted, and a replicated table would otherwise
// discard the retry as a duplicate block.
const repartitionCopySettings = " SETTINGS max_threads = 2, max_insert_threads = 1, insert_deduplicate = 0, max_execution_time = 0"

// repartitionWorkingTables lists every working table a conversion can leave behind, so a
// reset drops them with the rest of the log data.
func repartitionWorkingTables() []string {
	var names []string
	for _, t := range repartitionTargets {
		for _, suffix := range []string{repartOld, repartStage, repartDone, repartSkip} {
			names = append(names, t.table+suffix)
		}
	}
	return names
}

// ConvertPartitioning rebuilds every repartition target still on its old partition key,
// on every shard, without losing a row or running a mutation:
//
//  1. A partitioned copy of the table is created empty, checked by moving a probe row
//     through it, and swapped in with EXCHANGE TABLES. The swap is atomic and the
//     materialized views write by name, so ingest never sees a missing table and every
//     row from then on lands partitioned. The old table, now t__old, holds exactly the
//     rows from before the swap.
//  2. Each fractal's rows are copied into t__stage and moved into the live table with
//     MOVE PARTITION. A fractal is recorded in t__done once its copy completes; a copy
//     interrupted before that is dropped from the stage and redone, and a recorded one
//     is moved only while its stage partitions still exist, so no row is copied twice.
//     That matters because these tables sum, so a duplicate would inflate counts.
//  3. The working tables are dropped.
//
// keep lists the fractal ids whose rows are worth copying; rows of deleted fractals are
// left behind. Every step is resumable, so callers run this on every startup.
func (c *ClickHouseClient) ConvertPartitioning(ctx context.Context, keep map[string]bool) error {
	return c.onEveryShard(ctx, "convert partitioning", func(conn driver.Conn) error {
		for _, t := range repartitionTargets {
			if err := c.convertTarget(ctx, conn, t, keep); err != nil {
				return fmt.Errorf("convert %s: %w", t.table, err)
			}
		}
		return nil
	})
}

func (c *ClickHouseClient) convertTarget(ctx context.Context, conn driver.Conn, t repartitionTarget, keep map[string]bool) error {
	oldName := t.table + repartOld
	live, err := describeTable(ctx, conn, t.table)
	if err != nil || !live.exists {
		return err
	}
	old, err := describeTable(ctx, conn, oldName)
	if err != nil {
		return err
	}

	if live.partitionKey != t.key {
		if old.exists && old.partitionKey != t.key {
			return fmt.Errorf("%s exists with partition key %q, expected %q", oldName, old.partitionKey, t.key)
		}
		if err := c.swapIn(ctx, conn, t, live, old.exists); err != nil {
			return err
		}
	} else if !old.exists {
		// Converted; a crash during the final cleanup can leave the bookkeeping tables.
		return dropTables(ctx, conn, t.table+repartStage, t.table+repartDone, t.table+repartSkip)
	}
	return c.copyOld(ctx, conn, t, keep)
}

// swapIn creates the partitioned table (unless a crash left it from an earlier attempt),
// proves rows can move into it, and exchanges it with the live table.
func (c *ClickHouseClient) swapIn(ctx context.Context, conn driver.Conn, t repartitionTarget, live tableInfo, oldExists bool) error {
	oldName, stageName := t.table+repartOld, t.table+repartStage
	if !oldExists {
		if err := c.createLike(ctx, conn, t.table, oldName, live.ttl); err != nil {
			return err
		}
	}
	if err := c.createLike(ctx, conn, t.table, stageName, live.ttl); err != nil {
		return err
	}
	for _, name := range []string{t.table + repartDone, t.table + repartSkip} {
		if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+quoteCHIdent(name)+" (fractal_id String) ENGINE = MergeTree ORDER BY fractal_id"); err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
	}
	if err := sameColumns(ctx, conn, t.table, oldName); err != nil {
		return err
	}
	if err := probeMove(ctx, conn, t.table, stageName, oldName); err != nil {
		return fmt.Errorf("preflight move into %s: %w", oldName, err)
	}
	if err := conn.Exec(ctx, "EXCHANGE TABLES "+quoteCHIdent(t.table)+" AND "+quoteCHIdent(oldName)); err != nil {
		return fmt.Errorf("swap: %w", err)
	}
	log.Printf("[ClickHouse] %s now partitioned by %s; copying earlier rows from %s", t.table, t.key, oldName)
	return nil
}

// copyOld moves the rows from before the swap into the live table one fractal at a time,
// then drops the working tables.
func (c *ClickHouseClient) copyOld(ctx context.Context, conn driver.Conn, t repartitionTarget, keep map[string]bool) error {
	oldName, stageName := t.table+repartOld, t.table+repartStage
	doneName, skipName := t.table+repartDone, t.table+repartSkip

	cols, err := columnList(ctx, conn, stageName)
	if err != nil {
		return err
	}
	fractals, err := stringColumn(ctx, conn, "SELECT DISTINCT fractal_id FROM "+quoteCHIdent(oldName)+" ORDER BY fractal_id")
	if err != nil {
		return err
	}
	done, err := stringSet(ctx, conn, "SELECT fractal_id FROM "+quoteCHIdent(doneName))
	if err != nil {
		return err
	}

	copied := 0
	for _, f := range fractals {
		if !keep[f] {
			continue
		}
		skipped, err := inSet(ctx, conn, skipName, f)
		if err != nil {
			return err
		}
		if !done[f] && !skipped {
			if err := dropFractalPartitions(ctx, conn, stageName, []string{f}); err != nil {
				return err
			}
			stmt := "INSERT INTO " + quoteCHIdent(stageName) + " (" + cols + ") SELECT " + cols +
				" FROM " + quoteCHIdent(oldName) + " WHERE fractal_id = ?" + repartitionCopySettings
			if err := conn.Exec(ctx, stmt, f); err != nil {
				return fmt.Errorf("copy fractal %q: %w", f, err)
			}
			if err := conn.Exec(ctx, "INSERT INTO "+quoteCHIdent(doneName)+" VALUES (?)", f); err != nil {
				return fmt.Errorf("record fractal %q: %w", f, err)
			}
			copied++
		}
		// Re-read: a clear can land while the copy runs, and its rows must not move in.
		if skipped, err = inSet(ctx, conn, skipName, f); err != nil {
			return err
		}
		if skipped {
			if err := dropFractalPartitions(ctx, conn, stageName, []string{f}); err != nil {
				return err
			}
			continue
		}
		ids, err := fractalPartitionIDs(ctx, conn, stageName, []string{f})
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(stageName)+" MOVE PARTITION ID '"+id+"' TO TABLE "+quoteCHIdent(t.table)); err != nil {
				return fmt.Errorf("move fractal %q: %w", f, err)
			}
		}
	}

	if err := dropTables(ctx, conn, oldName, stageName, doneName, skipName); err != nil {
		return err
	}
	log.Printf("[ClickHouse] %s conversion complete (%d fractals copied)", t.table, copied)
	return nil
}

// markRepartitionSkip records fractals cleared while a conversion is in progress, so their
// earlier rows are not copied back afterwards, and drops any already staged. A no-op once
// the table is converted.
func markRepartitionSkip(ctx context.Context, conn driver.Conn, ids []string) error {
	for _, t := range repartitionTargets {
		skip, err := describeTable(ctx, conn, t.table+repartSkip)
		if err != nil {
			return err
		}
		if !skip.exists {
			continue
		}
		for _, id := range ids {
			if err := conn.Exec(ctx, "INSERT INTO "+quoteCHIdent(t.table+repartSkip)+" VALUES (?)", id); err != nil {
				return fmt.Errorf("record cleared fractal in %s: %w", t.table, err)
			}
		}
		if err := dropFractalPartitions(ctx, conn, t.table+repartStage, ids); err != nil {
			return err
		}
	}
	return nil
}

type tableInfo struct {
	exists       bool
	partitionKey string
	ttl          string
}

var engineTTLRe = regexp.MustCompile(` TTL (.+?)(?: SETTINGS |$)`)

func describeTable(ctx context.Context, conn driver.Conn, name string) (tableInfo, error) {
	rows, err := conn.Query(ctx,
		"SELECT partition_key, engine_full FROM system.tables WHERE database = currentDatabase() AND name = ?", name)
	if err != nil {
		return tableInfo{}, fmt.Errorf("describe %s: %w", name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return tableInfo{}, rows.Err()
	}
	var key, engine string
	if err := rows.Scan(&key, &engine); err != nil {
		return tableInfo{}, err
	}
	info := tableInfo{exists: true, partitionKey: key}
	if m := engineTTLRe.FindStringSubmatch(engine); m != nil {
		info.ttl = m[1]
	}
	return info, nil
}

// createLike creates name from table's definition in init-clickhouse.sql (engine rewritten
// for the cluster, so its Keeper path follows its own name) and gives it the live table's
// TTL, which an operator may have changed from the default.
func (c *ClickHouseClient) createLike(ctx context.Context, conn driver.Conn, table, name, ttl string) error {
	ddl, err := initTableDDL(table)
	if err != nil {
		return err
	}
	ddl = strings.Replace(ddl, "CREATE TABLE IF NOT EXISTS "+table+" (", "CREATE TABLE IF NOT EXISTS "+name+" (", 1)
	if err := conn.Exec(ctx, c.RewriteEngine(ddl)); err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if ttl != "" {
		// Metadata only: the table is empty.
		if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(name)+" MODIFY TTL "+ttl+" SETTINGS materialize_ttl_after_modify = 0"); err != nil {
			return fmt.Errorf("copy TTL to %s: %w", name, err)
		}
	}
	return nil
}

// initTableDDL returns table's CREATE TABLE statement from init-clickhouse.sql.
func initTableDDL(table string) (string, error) {
	header := "CREATE TABLE IF NOT EXISTS " + table + " ("
	for _, stmt := range splitClickHouseSQL(dbsql.ClickHouseSQL) {
		if i := strings.Index(stmt, header); i >= 0 {
			return stmt[i:], nil
		}
	}
	return "", fmt.Errorf("no CREATE TABLE %s in init-clickhouse.sql", table)
}

// sameColumns refuses a swap when the live table's columns differ from the definition the
// new table was built from, since the copy names columns and would drop or fail on any
// the two do not share.
func sameColumns(ctx context.Context, conn driver.Conn, a, b string) error {
	q := "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY name"
	list := func(table string) (string, error) {
		rows, err := conn.Query(ctx, q, table)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		var parts []string
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				return "", err
			}
			parts = append(parts, name+" "+typ)
		}
		return strings.Join(parts, ", "), rows.Err()
	}
	ca, err := list(a)
	if err != nil {
		return err
	}
	cb, err := list(b)
	if err != nil {
		return err
	}
	if ca != cb {
		return fmt.Errorf("columns of %s (%s) differ from init-clickhouse.sql (%s); not converting", a, ca, cb)
	}
	return nil
}

// probeMove proves MOVE PARTITION works from stage into dst on this server before the
// swap commits to it. Moving a well-formed but absent partition id is rejected exactly
// when the tables are incompatible, so that check always runs. When the live table has
// rows, one of them, relabelled to the probe fractal, is also moved through and dropped:
// a row of defaults would not do, since a SummingMergeTree discards all-zero rows.
func probeMove(ctx context.Context, conn driver.Conn, live, stage, dst string) error {
	const absentPartition = "00000000000000000000000000000000"
	if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(stage)+" MOVE PARTITION ID '"+absentPartition+"' TO TABLE "+quoteCHIdent(dst)); err != nil {
		return err
	}
	probe := []string{repartitionProbeFractal}
	if err := dropFractalPartitions(ctx, conn, stage, probe); err != nil {
		return err
	}
	cols, err := columnList(ctx, conn, stage)
	if err != nil {
		return err
	}
	sel := strings.Replace(cols, "`fractal_id`", "? AS `fractal_id`", 1)
	if err := conn.Exec(ctx, "INSERT INTO "+quoteCHIdent(stage)+" ("+cols+") SELECT "+sel+" FROM "+quoteCHIdent(live)+
		" LIMIT 1 SETTINGS insert_deduplicate = 0", repartitionProbeFractal); err != nil {
		return err
	}
	ids, err := fractalPartitionIDs(ctx, conn, stage, probe)
	if err != nil || len(ids) == 0 {
		return err
	}
	for _, id := range ids {
		if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(stage)+" MOVE PARTITION ID '"+id+"' TO TABLE "+quoteCHIdent(dst)); err != nil {
			return err
		}
	}
	moved, err := fractalPartitionIDs(ctx, conn, dst, probe)
	if err != nil {
		return err
	}
	if len(moved) == 0 {
		return fmt.Errorf("probe row did not arrive")
	}
	return dropFractalPartitions(ctx, conn, dst, probe)
}

// fractalPartitionIDs returns the ids of table's active partitions on conn's node that
// belong to any of the fractal ids, for a key of fractal_id alone or a tuple led by it.
// The match is on ClickHouse's own rendering of the partition: the bare value for a
// single String key, ('id',... with backslash escapes for a tuple.
func fractalPartitionIDs(ctx context.Context, conn driver.Conn, table string, fractalIDs []string) ([]string, error) {
	prefixes := make([]string, len(fractalIDs))
	for i, id := range fractalIDs {
		prefixes[i] = "(" + chPartitionString(id) + ","
	}
	return stringColumn(ctx, conn,
		"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = ? AND active"+
			" AND (has(?, partition) OR arrayExists(p -> startsWith(partition, p), ?))",
		table, fractalIDs, prefixes)
}

// chPartitionString renders a string the way system.parts.partition does inside a tuple.
func chPartitionString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// dropFractalPartitions drops every partition of table on conn's node belonging to the
// fractal ids, in batches.
func dropFractalPartitions(ctx context.Context, conn driver.Conn, table string, fractalIDs []string) error {
	ids, err := fractalPartitionIDs(ctx, conn, table, fractalIDs)
	if err != nil {
		return err
	}
	for start := 0; start < len(ids); start += dropPartitionBatch {
		batch := ids[start:min(start+dropPartitionBatch, len(ids))]
		drops := make([]string, len(batch))
		for i, id := range batch {
			drops[i] = "DROP PARTITION ID '" + id + "'"
		}
		if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(table)+" "+strings.Join(drops, ", ")+dropPartitionSettings); err != nil {
			return fmt.Errorf("drop %s partitions: %w", table, err)
		}
	}
	return nil
}

func dropTables(ctx context.Context, conn driver.Conn, names ...string) error {
	for _, name := range names {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+quoteCHIdent(name)+" SYNC SETTINGS max_table_size_to_drop = 0"); err != nil {
			return fmt.Errorf("drop %s: %w", name, err)
		}
	}
	return nil
}

func columnList(ctx context.Context, conn driver.Conn, table string) (string, error) {
	names, err := stringColumn(ctx, conn,
		"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("%s has no columns", table)
	}
	for i, n := range names {
		names[i] = quoteCHIdent(n)
	}
	return strings.Join(names, ", "), nil
}

func inSet(ctx context.Context, conn driver.Conn, table, value string) (bool, error) {
	var n uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM "+quoteCHIdent(table)+" WHERE fractal_id = ?", value).Scan(&n)
	return n > 0, err
}

func stringSet(ctx context.Context, conn driver.Conn, query string, args ...any) (map[string]bool, error) {
	values, err := stringColumn(ctx, conn, query, args...)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(values))
	for _, v := range values {
		set[v] = true
	}
	return set, nil
}

func stringColumn(ctx context.Context, conn driver.Conn, query string, args ...any) ([]string, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
