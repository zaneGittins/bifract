package archive

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// NewCHClient builds the ClickHouse client used by restore/reconcile. In cluster
// mode (CLICKHOUSE_CLUSTER + CLICKHOUSE_HOSTS both set) it is cluster-aware, so
// ch.WriteTable()/ch.ReadTable() resolve to logs_distributed and restored rows
// shard across nodes with cluster-wide dedup/counts. Otherwise it is a plain
// single-node client targeting the local logs table.
func NewCHClient(cfg Config) (*storage.ClickHouseClient, error) {
	if cfg.CHCluster != "" && cfg.CHHosts != "" {
		hosts := strings.Split(cfg.CHHosts, ",")
		return storage.NewClickHouseClusterClient(
			hosts, cfg.CHPort, cfg.CHDatabase, cfg.CHUser, cfg.CHPassword,
			cfg.CHCluster, storage.DefaultQueryPoolConfig())
	}
	return storage.NewClickHouseClient(cfg.CHHost, cfg.CHPort, cfg.CHDatabase, cfg.CHUser, cfg.CHPassword)
}

// chIcebergTableFunc builds the ClickHouse iceberg*() table-function expression
// that reads a fractal's Iceberg table directly from object storage. ClickHouse
// reads the metadata from the storage location (it does NOT use our Postgres
// catalog), so we hand it the table's base location.
//
// When cluster is non-empty the *Cluster variant is emitted, which distributes
// the Parquet reads (and the decompress/parse work that dominates both restore
// and recall) across every node instead of funnelling them through the single
// initiator node. Per the ClickHouse docs the cluster name is always the first
// argument and the remaining arguments are identical to the single-node form.
func chIcebergTableFunc(obj objstore.Config, tableLocation, cluster string) (string, error) {
	loc := strings.TrimRight(tableLocation, "/") + "/"

	// call assembles fn(args...), switching to the distributed variant when a
	// cluster is configured. The cluster name comes from the ClickHouse client,
	// which validates it at construction, so it is safe to interpolate.
	call := func(fn string, args ...string) string {
		if cluster != "" {
			fn += "Cluster"
			args = append([]string{chQuote(cluster)}, args...)
		}
		return fn + "(" + strings.Join(args, ", ") + ")"
	}

	switch obj.Backend {
	case objstore.BackendDisk:
		// No icebergLocal cluster variant exists, and none is needed: the disk
		// backend is pod-local and is rejected upstream by both restore and
		// recall before reaching here.
		path := strings.TrimPrefix(loc, "file://")
		return fmt.Sprintf("icebergLocal(%s)", chQuote(path)), nil

	case objstore.BackendS3, objstore.BackendMinIO:
		// s3://bucket/key... -> an HTTP(S) URL ClickHouse can fetch.
		rest := strings.TrimPrefix(loc, "s3://")
		bucket, key, _ := strings.Cut(rest, "/")
		var url string
		if obj.S3Endpoint != "" {
			// MinIO / custom endpoint: path-style.
			url = strings.TrimRight(obj.S3Endpoint, "/") + "/" + bucket + "/" + key
		} else {
			url = fmt.Sprintf("https://s3.%s.amazonaws.com/%s/%s", obj.S3Region, bucket, key)
		}
		if obj.S3AccessKey != "" {
			return call("icebergS3", chQuote(url), chQuote(obj.S3AccessKey), chQuote(obj.S3SecretKey)), nil
		}
		return call("icebergS3", chQuote(url)), nil

	case objstore.BackendAzure:
		// icebergAzure(storage_account_url, container, blobpath, account_name, account_key)
		// Location is abfs://<container>@<account>.<host>/<path> (container in the
		// userinfo position, matching iceberg-go's ADLS URI parser).
		rest := strings.TrimPrefix(strings.TrimPrefix(loc, "abfs://"), "abfss://")
		container, hostAndPath, _ := strings.Cut(rest, "@")
		_, blobPath, _ := strings.Cut(hostAndPath, "/")
		endpoint := obj.AzureEndpoint
		if endpoint == "" {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", obj.AzureAccount)
		}
		return call("icebergAzure", chQuote(endpoint), chQuote(container), chQuote(blobPath),
			chQuote(obj.AzureAccount), chQuote(obj.AzureKey)), nil
	}
	return "", fmt.Errorf("archive: unsupported backend %q for restore", obj.Backend)
}

// rawLogTTLDays mirrors the raw_log column TTL on the logs table
// (db/init-clickhouse.sql: `TTL toDateTime(timestamp) + INTERVAL 7 DAY`). Keep
// the two in sync: restore uses it to decide whether shipping raw_log back is
// worth the I/O.
const rawLogTTLDays = 7

// restoreMaxPartitionsPerInsert raises ClickHouse's max_partitions_per_insert_block
// (default 100) for restore inserts. Chunking bounds the Iceberg read side to one
// ingest_date, but the logs table partitions by EVENT date, so one ingest day can
// still land in many logs partitions when that day's data carries a wide event-time
// spread (backfills, replayed sources, clock skew). The default would fail such a
// chunk outright; this is a guard against that, not an invitation to insert wide.
const restoreMaxPartitionsPerInsert = 1000

// buildRestoreInsert assembles one chunk's INSERT ... SELECT. Extracted so the
// correctness-critical clauses stay under test: dropping LIMIT 1 BY would silently
// duplicate rows, and dropping the SETTINGS would make wide chunks fail outright.
func buildRestoreInsert(writeTable, rawLogExpr, tableFunc, where string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (timestamp, raw_log, log_id, fields, fractal_id, ingest_timestamp, normalizer) "+
			"SELECT timestamp, %s, log_id, norm_log::JSON, fractal_id, ingest_timestamp, normalizer "+
			"FROM %s WHERE %s LIMIT 1 BY log_id SETTINGS max_partitions_per_insert_block = %d",
		writeTable, rawLogExpr, tableFunc, where, restoreMaxPartitionsPerInsert)
}

// RestoreProgress is called after each chunk commits, with the timestamp the next
// attempt should resume from, the number of chunks finished, and the running row
// total. Persisting `next` is what makes a restore resumable.
type RestoreProgress func(next time.Time, chunksDone int, rowsSoFar int64)

// dayChunks splits [from, to) into UTC-day-aligned windows, one per ingest_date
// partition in the archive. The first and last chunks are clipped to the requested
// bounds, so a sub-day window stays a single chunk.
func dayChunks(from, to time.Time) [][2]time.Time {
	var out [][2]time.Time
	if !to.After(from) {
		return out
	}
	cur := from.UTC()
	end := to.UTC()
	for cur.Before(end) {
		next := time.Date(cur.Year(), cur.Month(), cur.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
		if next.After(end) {
			next = end
		}
		out = append(out, [2]time.Time{cur, next})
		cur = next
	}
	return out
}

// Restore replays a fractal's logs from Iceberg back into the ClickHouse logs
// table for the given INGEST-time window, one chunk per ingest day.
//
// The window is ingest-time because that is the archive's partition axis: each
// chunk reads exactly the ingest_date partitions it needs instead of scanning the
// whole table, and it matches how Recall queries the archive. Chunking also keeps
// each insert bounded and, via onChunk, makes an interrupted restore resumable
// rather than an all-or-nothing replay.
//
// When dedup is true each chunk skips log_ids already present in that chunk's
// window (idempotent re-restore); when false it does a straight insert (fastest
// for a window that was fully deleted by retention). queryID, when non-empty, is
// applied as the ClickHouse query_id so an in-flight chunk can be interrupted with
// KILL QUERY. onChunk may be nil.
//
// Returns the total number of rows inserted across all chunks. On error it returns
// the rows restored so far alongside the error, so a caller can record partial
// progress.
func (c *Catalog) Restore(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time, dedup bool, queryID string, onChunk RestoreProgress) (int64, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return 0, fmt.Errorf("archive: no Iceberg table for fractal %s: %w", fractalID, err)
	}
	tf, err := chIcebergTableFunc(obj, loc, ch.Cluster)
	if err != nil {
		return 0, err
	}

	// In cluster mode ReadTable/WriteTable resolve to logs_distributed so dedup
	// and placement span the whole cluster; single-node they are the local logs.
	readTable := ch.ReadTable()
	writeTable := ch.WriteTable()

	var total int64
	for i, chunk := range dayChunks(from, to) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		cf, ct := chunk[0], chunk[1]

		// ingest_date is the archive's partition column, so pinning it to this
		// chunk's day is what prunes the read down to a single partition. The
		// ingest_timestamp bounds clip the first and last (possibly partial) days.
		where := fmt.Sprintf(
			"fractal_id = %s AND ingest_date = %s AND ingest_timestamp >= %s AND ingest_timestamp < %s",
			chQuote(fractalID), chQuote(chDate(cf)), chQuote(chTime(cf)), chQuote(chTime(ct)))
		if dedup {
			// Anti-join bounded by this chunk only, so it stays tractable no matter
			// how long the overall restore window is; never a full-table NOT IN.
			where += fmt.Sprintf(
				" AND log_id NOT IN (SELECT log_id FROM %s WHERE fractal_id = %s AND ingest_timestamp >= %s AND ingest_timestamp < %s)",
				readTable, chQuote(fractalID), chQuote(chTime(cf)), chQuote(chTime(ct)))
		}

		// raw_log carries a column TTL on the logs table keyed on EVENT time, so for
		// a chunk ingested entirely before that horizon every raw_log byte would be
		// read out of Parquet, shipped, inserted, and then reclaimed by the next
		// merge. raw_log is the bulk of a row, so skip it and restore the normalized
		// fields only (all a deep restore can keep anyway). Logs are ingested at or
		// after the event they describe, so ingest before the horizon implies the
		// event is too; a future-dated log inside an old chunk is the one case this
		// drops a raw_log ClickHouse would briefly have kept.
		rawLogExpr := "raw_log"
		if !ct.After(time.Now().AddDate(0, 0, -rawLogTTLDays)) {
			rawLogExpr = "''"
		}

		// norm_log is a flat JSON String in Iceberg; cast it to the logs JSON column
		// so ClickHouse re-applies the `fields` type hints. The hot-store norm_log
		// column is left to its DEFAULT toString(fields), regenerated from the
		// restored fields.
		//
		// LIMIT 1 BY log_id collapses duplicates that exist WITHIN the archive. The
		// anti-join above only excludes ids already in the hot store, so without this
		// an archive holding the same log_id twice would insert it twice into a plain
		// MergeTree, permanently inflating counts. Archive-side duplicates are not
		// hypothetical: the spool is replayed at-least-once from its checkpoint after
		// a crash or restart, so a re-commit of the same records is expected. The
		// duplicates are byte-identical copies, so picking an arbitrary one is safe.
		insert := buildRestoreInsert(writeTable, rawLogExpr, tf, where)

		before, err := countLogs(ctx, ch, fractalID, cf, ct)
		if err != nil {
			return total, err
		}
		if queryID != "" {
			err = ch.ExecWithID(ctx, queryID, insert)
		} else {
			err = ch.Exec(ctx, insert)
		}
		if err != nil {
			return total, fmt.Errorf("archive: restore insert failed for %s: %w", chDate(cf), err)
		}
		after, err := countLogs(ctx, ch, fractalID, cf, ct)
		if err != nil {
			return total, err
		}
		if n := after - before; n > 0 {
			total += n
		}
		if onChunk != nil {
			onChunk(ct, i+1, total)
		}
	}
	return total, nil
}

// Reconcile compares the log_id count in ClickHouse vs Iceberg for an ingest-time
// window and, if Iceberg holds more (a gap in the hot store), restores the missing
// rows (with dedup). queryID and onChunk are threaded through to the underlying
// restore so the insert stays interruptible and resumable. Returns rows restored.
func (c *Catalog) Reconcile(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time, queryID string, onChunk RestoreProgress) (int64, error) {
	chCount, err := countLogs(ctx, ch, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	iceCount, err := c.countIceberg(ctx, ch, obj, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	if iceCount <= chCount {
		return 0, nil
	}
	return c.Restore(ctx, ch, obj, fractalID, from, to, true, queryID, onChunk)
}

// countIceberg counts archived rows in an ingest-time window. The ingest_date
// bounds let ClickHouse prune whole partitions before the ingest_timestamp
// predicate is evaluated.
func (c *Catalog) countIceberg(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time) (int64, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return 0, err
	}
	tf, err := chIcebergTableFunc(obj, loc, ch.Cluster)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(
		"SELECT count() AS c FROM %s WHERE fractal_id = %s AND ingest_date >= %s AND ingest_date <= %s "+
			"AND ingest_timestamp >= %s AND ingest_timestamp < %s",
		tf, chQuote(fractalID), chQuote(chDate(from)), chQuote(chDate(to)),
		chQuote(chTime(from)), chQuote(chTime(to)))
	return scalarCount(ctx, ch, q)
}

// countLogs counts hot-store rows in an ingest-time window, matching the axis
// restore and countIceberg use so the two counts are directly comparable. The
// logs table is ordered by event timestamp, but it carries a minmax skip index on
// ingest_timestamp (db/init-clickhouse.sql) and ingest_timestamp is near-monotonic
// with insertion order, so this prunes granules rather than scanning the fractal.
func countLogs(ctx context.Context, ch *storage.ClickHouseClient, fractalID string, from, to time.Time) (int64, error) {
	q := fmt.Sprintf("SELECT count() AS c FROM %s WHERE fractal_id = %s AND ingest_timestamp >= %s AND ingest_timestamp < %s",
		ch.ReadTable(), chQuote(fractalID), chQuote(chTime(from)), chQuote(chTime(to)))
	return scalarCount(ctx, ch, q)
}

func scalarCount(ctx context.Context, ch *storage.ClickHouseClient, query string) (int64, error) {
	rows, err := ch.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	switch v := rows[0]["c"].(type) {
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("archive: unexpected count type %T", v)
	}
}

// chQuote single-quotes a string for a ClickHouse SQL literal, escaping quotes
// and backslashes.
func chQuote(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return "'" + s + "'"
}

func chTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

// chDate formats a UTC date literal for the archive's ingest_date partition column.
func chDate(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}
