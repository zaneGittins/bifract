package archive

import (
	"context"
	"fmt"
	"log"
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
	opts, err := cfg.CH.ClientOptions(storage.DefaultQueryPoolConfig(), storage.RoleArchive)
	if err != nil {
		return nil, err
	}
	return storage.NewClickHouseClient(opts)
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
//
// The credentials below are interpolated into the query text, which is safe:
// ClickHouse redacts the secret argument to '[HIDDEN]' before the query reaches
// system.query_log or any exception message. Verified on 26.6 for icebergS3,
// icebergAzure, and both *Cluster variants (the masker tracks the argument shift
// from the leading cluster name). The redaction is positional, so keep these
// argument orders as the docs specify; reordering them would silently defeat it.
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

// restoreMaxPartitionsPerInsert raises ClickHouse's max_partitions_per_insert_block
// (default 100) for restore inserts. Chunking bounds the Iceberg read side to one
// ingest_date, but the logs table partitions by EVENT date, so one ingest day can
// still land in many logs partitions when that day's data carries a wide event-time
// spread (backfills, replayed sources, clock skew). The default would fail such a
// chunk outright; this is a guard against that, not an invitation to insert wide.
const restoreMaxPartitionsPerInsert = 1000

// restoreChunkRowTarget bounds the rows any single restore INSERT touches, so the
// in-memory structures ClickHouse builds for it stay bounded regardless of how
// dense an ingest day is: the LIMIT 1 BY log_id set over the inserted rows, and
// the anti-join set over the destination window. A window whose estimated row
// count (the larger of source and destination, see windowRowEstimate) exceeds
// this is bisected in time until each piece is under it.
const restoreChunkRowTarget = 50_000_000

// restoreMaxRowsInSet caps the dedup anti-join's IN-set cardinality as a backstop
// to chunk sizing: if a row-count estimate was stale (the window grew between
// planning and execution) the insert fails loudly instead of building an
// unbounded set and OOM-ing the node. Set to a multiple of the chunk target so
// normal chunks pass with headroom and only a real runaway trips it.
//
// set_overflow_mode MUST be 'throw', never 'break': 'break' would silently
// truncate the dedup set, so log_ids past the cap would not be excluded and would
// double-insert. A failed chunk is recoverable (retry/resume); silent duplication
// is not.
const restoreMaxRowsInSet = 4 * restoreChunkRowTarget

// minChunkDuration floors time-bisection so a pathological burst (millions of
// rows sharing one instant) cannot recurse forever. A window at this floor that
// still exceeds the row target is emitted as a single chunk; restoreMaxRowsInSet
// is the backstop that keeps its anti-join from OOM-ing the node.
const minChunkDuration = time.Second

// buildRestoreInsert assembles one chunk's INSERT ... SELECT. Extracted so the
// correctness-critical clauses stay under test: dropping LIMIT 1 BY would silently
// duplicate archive-internal dupes, dropping max_partitions_per_insert_block would
// make wide chunks fail outright, and set_overflow_mode must stay 'throw' so a
// too-large dedup set fails rather than silently truncating.
//
// fractalLiteral is the fractal_id written into the destination rows, as a quoted
// SQL literal. It replaces the archive's own fractal_id column so a restore can
// land in a fractal different from the one it was archived under (restore into a
// dedicated no-retention fractal); for a self-restore it is just the source
// fractal's own id.
func buildRestoreInsert(writeTable, fractalLiteral, tableFunc, where string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (timestamp, log_id, fields, fractal_id, ingest_timestamp, normalizer) "+
			"SELECT timestamp, log_id, norm_log::JSON, %s, ingest_timestamp, normalizer "+
			"FROM %s WHERE %s LIMIT 1 BY log_id "+
			"SETTINGS max_partitions_per_insert_block = %d, max_rows_in_set = %d, set_overflow_mode = 'throw'",
		writeTable, fractalLiteral, tableFunc, where, restoreMaxPartitionsPerInsert, restoreMaxRowsInSet)
}

// RestoreProgress is called after each chunk commits, with the timestamp the next
// attempt should resume from, the number of chunks finished and planned in THIS
// attempt, and the running row total for this attempt. Persisting `next` is what
// makes a restore resumable; chunksTotal comes from the row-count plan rather than
// a day count, so it reflects the actual bisected chunk count. A caller that
// resumes adds its own prior-attempt offsets.
type RestoreProgress func(next time.Time, chunksDone, chunksTotal int, rowsSoFar int64)

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

// windowRowEstimate returns the larger of the source (archive) and destination
// (hot store) row counts for a window. That maximum is the figure that bounds the
// restore insert's memory: the LIMIT 1 BY set scales with the source rows read,
// the dedup anti-join set with the destination rows in the same window, and a
// dense destination can dwarf a small source (e.g. reconcile healing a tiny gap
// in a full window). Both counts are cheap -- the archive count prunes to one
// ingest_date partition, the hot-store count rides the ingest_timestamp minmax
// skip index.
func (c *Catalog) windowRowEstimate(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, sourceFractalID, targetFractalID string, from, to time.Time) (int64, error) {
	src, err := c.countIceberg(ctx, ch, obj, sourceFractalID, from, to)
	if err != nil {
		return 0, err
	}
	dst, err := countLogs(ctx, ch, targetFractalID, from, to)
	if err != nil {
		return 0, err
	}
	if dst > src {
		return dst, nil
	}
	return src, nil
}

// planChunks splits [from, to) into restore chunks bounded by row count. It first
// splits on UTC day (the archive's ingest_date partition axis, so each chunk
// prunes to one partition), then bisects any day whose row estimate exceeds
// restoreChunkRowTarget in time until every chunk is under the target or hits the
// minChunkDuration floor. Boundaries stay real timestamps so the resume cursor can
// address them and a re-plan of [cursor, to) tiles the remainder without gaps.
func (c *Catalog) planChunks(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, sourceFractalID, targetFractalID string, from, to time.Time) ([][2]time.Time, error) {
	count := func(f, t time.Time) (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return c.windowRowEstimate(ctx, ch, obj, sourceFractalID, targetFractalID, f, t)
	}
	return planChunksWith(from, to, count)
}

// windowCounter estimates the row count of a [from, to) window. Injected so the
// chunk-planning math can be tested without ClickHouse.
type windowCounter func(from, to time.Time) (int64, error)

// planChunksWith is the ClickHouse-free core of planChunks: day-split, then
// bisect each day by the injected counter. Kept pure so the tiling properties the
// resume cursor depends on (contiguous, no gaps/overlaps, each chunk inside one
// UTC day) are unit-testable.
func planChunksWith(from, to time.Time, count windowCounter) ([][2]time.Time, error) {
	var out [][2]time.Time
	for _, day := range dayChunks(from, to) {
		if err := bisectWindow(day[0], day[1], count, &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// bisectWindow recursively halves a within-a-day window until its row estimate is
// under restoreChunkRowTarget, appending the leaf windows to out in order. The
// midpoint of a window inside one UTC day is still inside that day, so the
// single-partition pruning premise is preserved at every level.
func bisectWindow(from, to time.Time, count windowCounter, out *[][2]time.Time) error {
	n, err := count(from, to)
	if err != nil {
		return err
	}
	mid := from.Add(to.Sub(from) / 2)
	// Emit as a leaf when under target, or when the window is too small to split
	// further (a burst sharing one instant). max_rows_in_set on the insert is the
	// backstop for an over-target leaf.
	if n <= restoreChunkRowTarget || to.Sub(from) <= minChunkDuration || !mid.After(from) || !mid.Before(to) {
		if n > restoreChunkRowTarget {
			log.Printf("[Restore] window [%s, %s) holds ~%d rows, over the %d target but at the time-bisection floor; the max_rows_in_set guard bounds its dedup set",
				chTime(from), chTime(to), n, restoreChunkRowTarget)
		}
		*out = append(*out, [2]time.Time{from, to})
		return nil
	}
	if err := bisectWindow(from, mid, count, out); err != nil {
		return err
	}
	return bisectWindow(mid, to, count, out)
}

// Restore replays a fractal's logs from Iceberg back into the ClickHouse logs
// table for the given INGEST-time window, in row-count-bounded chunks.
//
// The window is ingest-time because that is the archive's partition axis: each
// chunk reads exactly the ingest_date partitions it needs instead of scanning the
// whole table, and it matches how Recall queries the archive. Chunks are planned
// by row count (see planChunks) so a single insert never builds an unbounded
// in-memory set, however dense an ingest day is; onChunk after each keeps an
// interrupted restore resumable rather than an all-or-nothing replay.
//
// Restore is always idempotent: every chunk excludes log_ids already present in
// the destination window (the anti-join) and collapses archive-internal
// duplicates (LIMIT 1 BY). There is no straight-insert mode -- skipping the
// anti-join is the only way to double-insert, and a re-run or crash-resume must
// stay safe. queryID, when non-empty, is applied as the ClickHouse query_id so an
// in-flight chunk can be interrupted with KILL QUERY. onChunk may be nil.
//
// sourceFractalID selects the archive (its per-fractal Iceberg table) and the
// ingest_date/fractal_id read filter. targetFractalID is the fractal the rows are
// written under: equal to source for a self-restore, or a different (typically
// no-retention) fractal to restore into a dedicated workspace. The hot-store
// counts and the dedup anti-join key on the TARGET, so they measure and dedup
// against the destination, not the source.
//
// Returns the total number of rows inserted across all chunks. On error it returns
// the rows restored so far alongside the error, so a caller can record partial
// progress.
func (c *Catalog) Restore(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, sourceFractalID, targetFractalID string, from, to time.Time, queryID string, onChunk RestoreProgress) (int64, error) {
	loc, err := c.TableLocation(ctx, sourceFractalID)
	if err != nil {
		return 0, fmt.Errorf("archive: no Iceberg table for fractal %s: %w", sourceFractalID, err)
	}
	tf, err := chIcebergTableFunc(obj, loc, ch.Topology().FanoutCluster)
	if err != nil {
		return 0, err
	}

	// In cluster mode ReadTable/WriteTable resolve to logs_distributed so dedup
	// and placement span the whole cluster; single-node they are the local logs.
	readTable := ch.ReadTable()
	writeTable := ch.WriteTable()
	targetLiteral := chQuote(targetFractalID)

	// Plan the whole window up front so chunksTotal is known and the progress bar
	// is accurate. The planning counts are cheap (partition-pruned / skip-indexed).
	chunks, err := c.planChunks(ctx, ch, obj, sourceFractalID, targetFractalID, from, to)
	if err != nil {
		return 0, fmt.Errorf("archive: plan restore chunks: %w", err)
	}

	var total int64
	for i, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		cf, ct := chunk[0], chunk[1]

		// The read filter is on the SOURCE fractal. ingest_date is the archive's
		// partition column, so pinning it to this chunk's day prunes the read to a
		// single partition; a bisected chunk stays inside one UTC day, so its
		// ingest_date is cf's day. The ingest_timestamp bounds clip within the day.
		where := fmt.Sprintf(
			"fractal_id = %s AND ingest_date = %s AND ingest_timestamp >= %s AND ingest_timestamp < %s",
			chQuote(sourceFractalID), chQuote(chDate(cf)), chQuote(chTime(cf)), chQuote(chTime(ct)))
		// Anti-join on the TARGET fractal: skip log_ids already present in the
		// destination so a re-restore or crash-resume into the same fractal is
		// idempotent. Bounded to this chunk's window and capped by max_rows_in_set,
		// so it stays tractable no matter how long the overall restore window is.
		where += fmt.Sprintf(
			" AND log_id NOT IN (SELECT log_id FROM %s WHERE fractal_id = %s AND ingest_timestamp >= %s AND ingest_timestamp < %s)",
			readTable, targetLiteral, chQuote(chTime(cf)), chQuote(chTime(ct)))

		// raw_log is not restored: the logs table no longer has the column (it lives in
		// the separate logs_raw 7-day troubleshooting table, which a restore of archived
		// data -- typically older than that window -- would not meaningfully repopulate).
		// A restore keeps the normalized fields, which is all a deep restore can anyway.

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
		insert := buildRestoreInsert(writeTable, targetLiteral, tf, where)

		// before/after count the TARGET fractal, so the delta is rows actually
		// landed in the destination this chunk.
		before, err := countLogs(ctx, ch, targetFractalID, cf, ct)
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
		after, err := countLogs(ctx, ch, targetFractalID, cf, ct)
		if err != nil {
			return total, err
		}
		if n := after - before; n > 0 {
			total += n
		}
		if onChunk != nil {
			onChunk(ct, i+1, len(chunks), total)
		}
	}
	return total, nil
}

// Reconcile compares the log_id count in ClickHouse vs Iceberg for an ingest-time
// window and, if Iceberg holds more (a gap in the hot store), restores the missing
// rows. Restore is always deduped, so healing a partial gap only inserts what is
// actually missing. queryID and onChunk are threaded through to the underlying
// restore so the insert stays interruptible and resumable. Returns rows restored.
//
// Reconcile is always same-fractal: it heals a fractal's own hot store from its
// own archive, so source and target are identical. Cross-fractal reconcile is
// meaningless (a different fractal has no gap to compare against this archive).
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
	return c.Restore(ctx, ch, obj, fractalID, fractalID, from, to, queryID, onChunk)
}

// countIceberg counts archived rows in an ingest-time window. The ingest_date
// bounds let ClickHouse prune whole partitions before the ingest_timestamp
// predicate is evaluated.
func (c *Catalog) countIceberg(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time) (int64, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return 0, err
	}
	tf, err := chIcebergTableFunc(obj, loc, ch.Topology().FanoutCluster)
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
