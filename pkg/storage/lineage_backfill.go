package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"bifract/pkg/sqlquote"
)

// lineageBackfillDone records, on each shard, how far each logs partition has been copied into
// proc_lineage, by ingest time. A partition with rows past that is copied again, which covers a
// fractal cleared and re-ingested under the same partition, at no cost to correctness.
const lineageBackfillDone = "proc_lineage__backfilled"

// lineageBackfillSettings throttle a partition's copy beside live ingest and queries.
const lineageBackfillSettings = " SETTINGS max_threads = 2, max_insert_threads = 1, insert_deduplicate = 0, max_execution_time = 0"

// BackfillLineage fills proc_lineage from the process creations already in logs, on each shard
// from its own logs, with the lineage view's own SELECT. A process the view already wrote is the
// same row, which ReplacingMergeTree keeps once. Callers run it at startup while Collect
// baselines is on.
func (c *ClickHouseClient) BackfillLineage(ctx context.Context) error {
	sel, err := initViewSelect("proc_lineage_mv")
	if err != nil {
		return err
	}
	return c.onEveryShardPinned(ctx, "backfill lineage", func(conn driver.Conn) error {
		return backfillLineageOnShard(ctx, conn, sel)
	})
}

// backfillLineageOnShard copies, per logs partition, the rows ingested after what an earlier run
// copied.
func backfillLineageOnShard(ctx context.Context, conn driver.Conn, sel string) error {
	cols, err := selectColumns(ctx, conn, sel)
	if err != nil {
		return err
	}
	if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+lineageBackfillDone+" (partition_id String, upto DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY partition_id"); err != nil {
		return fmt.Errorf("create %s: %w", lineageBackfillDone, err)
	}
	parts, err := stringColumn(ctx, conn, "SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'logs' AND active ORDER BY partition_id")
	if err != nil {
		return err
	}
	copied := 0
	for _, id := range parts {
		var done, last time.Time
		if err := conn.QueryRow(ctx, "SELECT (SELECT max(upto) FROM "+lineageBackfillDone+" WHERE partition_id = ?) AS done, max(ingest_timestamp) "+
			"FROM logs WHERE _partition_id = ?", id, id).Scan(&done, &last); err != nil {
			return fmt.Errorf("read partition %s: %w", id, err)
		}
		if !last.After(done) {
			continue
		}
		window := " AND _partition_id = " + sqlquote.String(id) + " AND ingest_timestamp > " + chTime(done) + " AND ingest_timestamp <= " + chTime(last)
		if err := conn.Exec(ctx, "INSERT INTO proc_lineage ("+cols+") "+sel+window+lineageBackfillSettings); err != nil {
			return fmt.Errorf("backfill partition %s: %w", id, err)
		}
		if err := conn.Exec(ctx, "INSERT INTO "+lineageBackfillDone+" VALUES (?, ?)", id, last); err != nil {
			return fmt.Errorf("record partition %s: %w", id, err)
		}
		copied++
	}
	if copied > 0 {
		log.Printf("[ClickHouse] lineage backfilled from %d logs partitions", copied)
	}
	return nil
}

func chTime(t time.Time) string {
	return "toDateTime64(" + sqlquote.String(t.UTC().Format("2006-01-02 15:04:05.000")) + ", 3, 'UTC')"
}

// selectColumns names the columns sel returns, as ClickHouse resolves them.
func selectColumns(ctx context.Context, conn driver.Conn, sel string) (string, error) {
	rows, err := conn.Query(ctx, sel+" LIMIT 0")
	if err != nil {
		return "", fmt.Errorf("resolve columns: %w", err)
	}
	defer rows.Close()
	names := rows.Columns()
	if len(names) == 0 {
		return "", fmt.Errorf("resolve columns: none")
	}
	for i, n := range names {
		names[i] = quoteCHIdent(n)
	}
	return strings.Join(names, ", "), nil
}
