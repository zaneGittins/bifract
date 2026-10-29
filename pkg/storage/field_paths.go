package storage

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// FieldPathCap bounds the distinct paths kept per fractal and ingest day: per insert
// block in the MV, and per closed day once reconciled. Field names are sender
// controlled. Past the cap, counts for the least common paths are lower bounds.
const FieldPathCap = 1024

// fieldPathsExpr lists the paths one row contributes: the empty path once (the row
// count), then every top-level key up to 255 bytes with a non-empty value.
const fieldPathsExpr = "arrayPushFront(arrayMap(kv -> kv.1, arrayFilter(kv -> kv.1 != '' AND kv.2 != '' AND length(kv.1) <= 255, JSONExtractKeysAndValues(norm_log, 'String'))), '')"

// fieldPathsCapClause keeps the row count first and then the most common paths, per
// fractal and day, as the MV needs for a block spanning several.
var fieldPathsCapClause = fmt.Sprintf("ORDER BY path = '' DESC, cnt DESC LIMIT %d BY fractal_id, ingest_day", FieldPathCap)

// fieldPathsTopClause is the same cap within one partition, where fractal and day are
// fixed. A plain LIMIT is a bounded top-N sort; LIMIT BY sorts every group and
// exceeded the memory limit on a day of random keys (measured on 26.8).
var fieldPathsTopClause = fmt.Sprintf("ORDER BY path = '' DESC, cnt DESC LIMIT %d", FieldPathCap)

// fieldPathChunkRows sizes one rebuild query, so a firehose day is rebuilt in slices
// that each finish inside the query budget. A var so tests can force slicing.
var fieldPathChunkRows uint64 = 20_000_000

// fieldPathQueryBudgetSeconds is the server-side ceiling for one rebuild slice.
const fieldPathQueryBudgetSeconds = 600

// fieldPathsStage receives a partition's recount, which then replaces the live
// partition in one atomic step, so a recount that fails or is cut short leaves
// the existing counts in place.
const fieldPathsStage = "logs_field_paths__rebuild"

// FieldPathsReadTable returns the table the schema sweep reads path counts from.
func (c *ClickHouseClient) FieldPathsReadTable() string {
	return c.topo.Table("logs_field_paths")
}

type fieldPathRepair int

const (
	repairNone fieldPathRepair = iota
	repairRebuild
	repairCompact
	repairDrop
)

// fieldPathPartition pairs a logs partition with its rollup partition on one node.
type fieldPathPartition struct {
	Partition string
	LogsID    string
	RollupID  string
	Day       time.Time
	LogRows   uint64 // active rows in the logs partition
	Counted   uint64 // rows the rollup counted (path '')
	PathRows  uint64 // active rows in the rollup partition, an upper bound on its paths
}

// planFieldPathRepair decides what a partition needs. Only closed days are touched:
// today and yesterday still take inserts, so their counts legitimately trail logs.
// Comparing counts rather than recording progress makes the repair self-healing: an
// upgrade's history, a gap while the MV was missing, and a rebuild that raced a late
// insert all show up as a mismatch.
func planFieldPathRepair(p fieldPathPartition, today time.Time) fieldPathRepair {
	if !p.Day.Before(today.AddDate(0, 0, -1)) {
		return repairNone
	}
	switch {
	case p.LogsID == "":
		return repairDrop
	case p.LogRows != p.Counted:
		return repairRebuild
	case p.PathRows > FieldPathCap:
		return repairCompact
	}
	return repairNone
}

type partStat struct {
	ID   string
	Rows uint64
}

// joinFieldPathPartitions pairs logs and rollup partitions by their rendered key,
// newest day first.
func joinFieldPathPartitions(logs, rollup map[string]partStat, counted map[string]uint64) []fieldPathPartition {
	byKey := map[string]*fieldPathPartition{}
	get := func(key string) *fieldPathPartition {
		p := byKey[key]
		if p == nil {
			_, day, ok := ParseLogPartition(key)
			if !ok {
				return nil // a day that cannot be read cannot be known to be closed
			}
			p = &fieldPathPartition{Partition: key, Day: day}
			byKey[key] = p
		}
		return p
	}
	for key, s := range logs {
		if p := get(key); p != nil {
			p.LogsID, p.LogRows = s.ID, s.Rows
		}
	}
	for key, s := range rollup {
		if p := get(key); p != nil {
			p.RollupID, p.PathRows, p.Counted = s.ID, s.Rows, counted[s.ID]
		}
	}
	out := make([]fieldPathPartition, 0, len(byKey))
	for _, p := range byKey {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Day.Equal(out[j].Day) {
			return out[i].Day.After(out[j].Day)
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func fieldPathChunks(rows uint64) int {
	return int(max(1, (rows+fieldPathChunkRows-1)/fieldPathChunkRows))
}

// sliceBounds renders quantile bounds as ascending, distinct literals. Approximate
// quantiles promise no order, and slices from unordered bounds would overlap.
func sliceBounds(qs []time.Time) []string {
	sort.Slice(qs, func(i, j int) bool { return qs[i].Before(qs[j]) })
	var out []string
	for _, q := range qs {
		b := fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", q.UTC().Format("2006-01-02 15:04:05.000"))
		if len(out) == 0 || out[len(out)-1] != b {
			out = append(out, b)
		}
	}
	return out
}

// chunkConditions turns ascending event-time bounds into predicates that tile the
// partition: the first slice is open below and the last open above.
func chunkConditions(bounds []string) []string {
	if len(bounds) == 0 {
		return []string{"1"}
	}
	out := make([]string, 0, len(bounds)+1)
	out = append(out, "timestamp < "+bounds[0])
	for i := 1; i < len(bounds); i++ {
		out = append(out, "timestamp >= "+bounds[i-1]+" AND timestamp < "+bounds[i])
	}
	return append(out, "timestamp >= "+bounds[len(bounds)-1])
}

// SpillSettings bounds a query's GROUP BY and sort to a third of its memory limit
// before they spill to disk. The ratio settings do not take effect under a per-query
// limit (measured on 26.8), so the thresholds are absolute.
func SpillSettings(maxMemoryBytes int64) clickhouse.Settings {
	return clickhouse.Settings{
		"max_bytes_before_external_group_by": maxMemoryBytes / 3,
		"max_bytes_before_external_sort":     maxMemoryBytes / 3,
	}
}

// fieldPathSettings applies to every reconcile statement.
func fieldPathSettings(maxMemoryBytes int64) clickhouse.Settings {
	s := clickhouse.Settings{
		"priority":                            5,
		"max_threads":                         1,
		"max_memory_usage":                    maxMemoryBytes,
		"memory_overcommit_ratio_denominator": 0,
		"max_execution_time":                  fieldPathQueryBudgetSeconds,
		"timeout_overflow_mode":               "throw",
		// A recount inserts blocks identical to an earlier attempt's, which a
		// replicated table would otherwise discard as duplicates.
		"insert_deduplicate": 0,
		// ARRAY JOIN multiplies a block by its rows' key counts; small blocks keep
		// that expansion well under the limit.
		"max_block_size": 8192,
	}
	for k, v := range SpillSettings(maxMemoryBytes) {
		s[k] = v
	}
	return s
}

// ReconcileFieldPaths repairs every closed rollup partition that disagrees with
// logs, on every shard, newest first. Each shard reads and writes only its own
// local tables, where its logs rows and their counts live. It runs until done or
// ctx ends; each statement is bounded server side.
func (c *ClickHouseClient) ReconcileFieldPaths(ctx context.Context, maxMemoryBytes int64) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(fieldPathSettings(maxMemoryBytes)))
	today := time.Now().UTC().Truncate(24 * time.Hour)
	return c.onEveryShard(ctx, "reconcile field paths", func(conn driver.Conn) error {
		if err := c.createLike(ctx, conn, "logs_field_paths", fieldPathsStage, ""); err != nil {
			return err
		}
		return reconcileFieldPathsOn(ctx, conn, today)
	})
}

func reconcileFieldPathsOn(ctx context.Context, conn driver.Conn, today time.Time) error {
	logs, err := activePartitions(ctx, conn, "logs")
	if err != nil {
		return err
	}
	rollup, err := activePartitions(ctx, conn, "logs_field_paths")
	if err != nil {
		return err
	}
	counted, err := countedRows(ctx, conn)
	if err != nil {
		return err
	}
	repaired := 0
	var firstErr error
	for _, p := range joinFieldPathPartitions(logs, rollup, counted) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch planFieldPathRepair(p, today) {
		case repairNone:
			continue
		case repairDrop:
			err = dropPartitionID(ctx, conn, "logs_field_paths", p.RollupID)
		case repairRebuild:
			err = rebuildFieldPaths(ctx, conn, p)
		case repairCompact:
			err = compactFieldPaths(ctx, conn, p)
		}
		if err != nil {
			// Older partitions still get their turn; this one is retried next pass.
			log.Printf("[FieldPaths] repair %s: %v", p.Partition, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("field paths %s: %w", p.Partition, err)
			}
			continue
		}
		repaired++
	}
	if repaired > 0 {
		log.Printf("[FieldPaths] repaired %d partition(s)", repaired)
	}
	return firstErr
}

func activePartitions(ctx context.Context, conn driver.Conn, table string) (map[string]partStat, error) {
	rows, err := conn.Query(ctx, "SELECT partition, any(partition_id), sum(rows) FROM system.parts"+
		" WHERE database = currentDatabase() AND table = ? AND active GROUP BY partition", table)
	if err != nil {
		return nil, fmt.Errorf("list %s partitions: %w", table, err)
	}
	defer rows.Close()
	out := map[string]partStat{}
	for rows.Next() {
		var key string
		var s partStat
		if err := rows.Scan(&key, &s.ID, &s.Rows); err != nil {
			return nil, err
		}
		out[key] = s
	}
	return out, rows.Err()
}

// countedRows reads each rollup partition's row count, a primary-key range per
// partition since the empty path sorts first.
func countedRows(ctx context.Context, conn driver.Conn) (map[string]uint64, error) {
	rows, err := conn.Query(ctx, "SELECT _partition_id, sum(cnt) FROM logs_field_paths WHERE path = '' GROUP BY _partition_id")
	if err != nil {
		return nil, fmt.Errorf("read field path counts: %w", err)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var id string
		var n uint64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// rebuildFieldPaths recounts a partition from logs in event-time slices, sums the
// slices' top paths, and swaps the result in through the stage. Late rows the MV
// writes to the live partition meanwhile are replaced, which the next pass sees as
// a mismatch.
func rebuildFieldPaths(ctx context.Context, conn driver.Conn, p fieldPathPartition) error {
	var bounds []string
	if n := fieldPathChunks(p.LogRows); n > 1 {
		levels := make([]string, n-1)
		for i := range levels {
			levels[i] = fmt.Sprintf("%g", float64(i+1)/float64(n))
		}
		var qs []time.Time
		if err := conn.QueryRow(ctx, "SELECT quantiles("+strings.Join(levels, ", ")+")(timestamp) FROM logs WHERE _partition_id = ?",
			p.LogsID).Scan(&qs); err != nil {
			return fmt.Errorf("slice bounds: %w", err)
		}
		bounds = sliceBounds(qs)
	}
	sums := map[string]*pathRow{}
	for _, cond := range chunkConditions(bounds) {
		rows, err := scanPathRows(ctx, conn, "SELECT fractal_id, toDate(ingest_timestamp) AS ingest_day, path, count() AS cnt FROM logs"+
			" ARRAY JOIN "+fieldPathsExpr+" AS path"+
			" WHERE _partition_id = ? AND ("+cond+")"+
			" GROUP BY fractal_id, ingest_day, path "+fieldPathsTopClause, p.LogsID)
		if err != nil {
			return fmt.Errorf("recount: %w", err)
		}
		for _, r := range rows {
			if s := sums[r.path]; s != nil {
				s.cnt += r.cnt
			} else {
				sums[r.path] = &r
			}
		}
	}
	top := make([]pathRow, 0, len(sums))
	for _, r := range sums {
		top = append(top, *r)
	}
	sort.Slice(top, func(i, j int) bool {
		if (top[i].path == "") != (top[j].path == "") {
			return top[i].path == ""
		}
		if top[i].cnt != top[j].cnt {
			return top[i].cnt > top[j].cnt
		}
		return top[i].path < top[j].path
	})
	// Each slice kept its own top paths, so the union can exceed the cap.
	if len(top) > FieldPathCap {
		top = top[:FieldPathCap]
	}
	if err := stageRows(ctx, conn, p.Partition, top); err != nil {
		return err
	}
	return swapInStage(ctx, conn, p.Partition)
}

// compactFieldPaths trims a closed partition to its FieldPathCap most common paths.
// When unmerged parts only made it look over the cap, it is merged instead, so the
// next pass does not look again.
func compactFieldPaths(ctx context.Context, conn driver.Conn, p fieldPathPartition) error {
	top, err := topPaths(ctx, conn, "logs_field_paths", p.Partition)
	if err != nil {
		return err
	}
	if len(top) <= FieldPathCap {
		return conn.Exec(ctx, "OPTIMIZE TABLE logs_field_paths PARTITION ID '"+p.RollupID+"' FINAL")
	}
	if err := stageRows(ctx, conn, p.Partition, top[:FieldPathCap]); err != nil {
		return err
	}
	return swapInStage(ctx, conn, p.Partition)
}

type pathRow struct {
	fractal, path string
	day           time.Time
	cnt           uint64
}

// topPaths reads up to FieldPathCap+1 of a partition's paths, row count first, so
// the caller can tell whether it is over the cap.
func topPaths(ctx context.Context, conn driver.Conn, table, partition string) ([]pathRow, error) {
	return scanPathRows(ctx, conn, "SELECT fractal_id, ingest_day, path, sum(cnt) AS cnt FROM "+quoteCHIdent(table)+
		" WHERE _partition_id IN (SELECT partition_id FROM system.parts WHERE database = currentDatabase() AND table = ? AND active AND partition = ?)"+
		" GROUP BY fractal_id, ingest_day, path ORDER BY path = '' DESC, cnt DESC"+
		fmt.Sprintf(" LIMIT %d", FieldPathCap+1), table, partition)
}

func scanPathRows(ctx context.Context, conn driver.Conn, query string, args ...any) ([]pathRow, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pathRow
	for rows.Next() {
		var r pathRow
		if err := rows.Scan(&r.fractal, &r.day, &r.path, &r.cnt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// stageRows makes rows the stage's whole content for the partition.
func stageRows(ctx context.Context, conn driver.Conn, partition string, rows []pathRow) error {
	if err := dropPartitionsOf(ctx, conn, fieldPathsStage, partition); err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+fieldPathsStage+" (fractal_id, ingest_day, path, cnt)")
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.fractal, r.day, r.path, r.cnt); err != nil {
			batch.Abort()
			return err
		}
	}
	return batch.Send()
}

// swapInStage atomically replaces the live partition with the stage's, then clears
// the stage.
func swapInStage(ctx context.Context, conn driver.Conn, partition string) error {
	ids, err := partitionIDs(ctx, conn, fieldPathsStage, partition)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := conn.Exec(ctx, "ALTER TABLE logs_field_paths REPLACE PARTITION ID '"+id+"' FROM "+fieldPathsStage); err != nil {
			return fmt.Errorf("swap in recount: %w", err)
		}
	}
	return dropPartitionsOf(ctx, conn, fieldPathsStage, partition)
}

// partitionIDs returns the ids of table's active parts in the partition rendered as
// partition, on conn's node.
func partitionIDs(ctx context.Context, conn driver.Conn, table, partition string) ([]string, error) {
	return stringColumn(ctx, conn, "SELECT DISTINCT partition_id FROM system.parts"+
		" WHERE database = currentDatabase() AND table = ? AND active AND partition = ?", table, partition)
}

func dropPartitionsOf(ctx context.Context, conn driver.Conn, table, partition string) error {
	ids, err := partitionIDs(ctx, conn, table, partition)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := dropPartitionID(ctx, conn, table, id); err != nil {
			return err
		}
	}
	return nil
}

func dropPartitionID(ctx context.Context, conn driver.Conn, table, id string) error {
	if err := conn.Exec(ctx, "ALTER TABLE "+quoteCHIdent(table)+" DROP PARTITION ID '"+id+"'"+dropPartitionSettings); err != nil {
		return fmt.Errorf("drop %s partition %s: %w", table, id, err)
	}
	return nil
}
